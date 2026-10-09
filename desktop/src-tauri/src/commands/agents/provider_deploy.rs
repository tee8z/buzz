use std::sync::Arc;

use tauri::AppHandle;

use crate::{
    app_state::AppState,
    managed_agents::{
        discover_provider_candidates, load_managed_agents, provider_deploy,
        resolve_provider_binary, save_managed_agents, BackendKind, REPLAY_FLOOR_ENV_VAR,
    },
    util::now_iso,
};

use super::build_deploy_payload;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Published channel and canonical thread that own a remote workspace.
pub struct RemoteSessionScope {
    channel_id: uuid::Uuid,
    thread_root: String,
}

/// How an explicit user action recovers an ended thread session. Sent as
/// `provider_config.sandbox.recovery` for one invocation only; never stored
/// on the agent record, so no later deploy can resurrect a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RemoteRecoveryMode {
    /// Restore the ended session's verified checkpoint.
    Checkpoint,
    /// Start a new session with an empty workspace.
    Fresh,
}

/// Whether a provider config launches one Sandbox per channel thread.
pub(super) fn uses_thread_sandbox(config: &serde_json::Value) -> bool {
    config.get("sandbox") == Some(&serde_json::Value::Bool(true))
}

fn bind_sandbox_scope(
    config: &mut serde_json::Value,
    scope: Option<&RemoteSessionScope>,
    agent_pubkey: &str,
    payload: &serde_json::Value,
    recovery: Option<RemoteRecoveryMode>,
) -> Result<(), String> {
    if !uses_thread_sandbox(config) {
        return match recovery {
            Some(_) => Err("Only thread workspaces can be recovered".into()),
            None => Ok(()),
        };
    }
    let scope = scope.ok_or("Mention this agent in a channel thread to start its workspace")?;
    if scope.thread_root.len() != 64
        || !scope
            .thread_root
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err("Remote workspace requires a canonical thread root".into());
    }
    let owner = payload
        .pointer("/launch/owner_pubkey")
        .and_then(|v| v.as_str())
        .ok_or("Remote workspace requires a verified owner")?;
    config["sandbox"] = serde_json::json!({
        "channel_id": scope.channel_id.to_string(), "thread_root": scope.thread_root,
    });
    if let Some(mode) = recovery {
        config["sandbox"]["recovery"] = serde_json::json!({ "mode": mode });
    }
    config["identity_policy"] = serde_json::json!({
        "agent_pubkey": agent_pubkey, "owner_pubkey": owner,
    });
    Ok(())
}

/// Serialize provider operations per agent. The guard must stay alive until
/// the provider invocation finishes.
pub(super) async fn lock_provider_operations(
    state: &AppState,
    pubkey: &str,
) -> Result<tokio::sync::OwnedMutexGuard<()>, String> {
    let lock = {
        let mut locks = state
            .provider_deploy_locks
            .lock()
            .map_err(|error| error.to_string())?;
        Arc::clone(
            locks
                .entry(pubkey.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    };
    Ok(lock.lock_owned().await)
}

/// A provider invocation rebuilt from the current record after the lock.
pub(super) struct ProviderInvocation {
    pub(super) binary: std::path::PathBuf,
    pub(super) config: serde_json::Value,
    pub(super) agent_json: serde_json::Value,
    /// Whether the record launches one Sandbox per channel thread.
    pub(super) thread_sandbox: bool,
}

/// Session inputs for one provider invocation. None of them are persisted.
pub(super) struct InvocationScope<'a> {
    pub(super) expected_relay_url: Option<&'a str>,
    pub(super) expected_signer_pubkey: Option<&'a str>,
    pub(super) session: Option<&'a RemoteSessionScope>,
    pub(super) recovery: Option<RemoteRecoveryMode>,
}

/// Rebuild the invocation from the current record. Call only while holding
/// [`lock_provider_operations`]: the payload may have waited behind another
/// operation, so the final invocation must carry the newest saved policy, not
/// the stale snapshot its caller captured.
pub(super) fn prepare_provider_invocation<R: tauri::Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    pubkey: &str,
    scope: &InvocationScope<'_>,
) -> Result<ProviderInvocation, String> {
    let (provider_id, mut config, cached_binary_path, agent_json) = {
        let _store_guard = state
            .managed_agents_store_lock
            .lock()
            .map_err(|error| error.to_string())?;
        let records = load_managed_agents(app)?;
        let record = records
            .iter()
            .find(|record| record.pubkey == pubkey)
            .ok_or_else(|| format!("agent {pubkey} not found"))?;
        let (provider_id, config) = match &record.backend {
            BackendKind::Provider { id, config } => (id.clone(), config.clone()),
            BackendKind::Local => return Err(format!("agent {pubkey} is not provider-backed")),
        };
        (
            provider_id,
            config,
            record.provider_binary_path.clone(),
            build_deploy_payload(app, state, record)?,
        )
    };
    // The rebuild above re-read the live workspace relay and owner identity.
    // Assert the caller's captured scope against THIS payload — the exact
    // value invoked — not the pre-lock snapshot its caller validated.
    assert_payload_scope(
        &agent_json,
        scope.expected_relay_url,
        scope.expected_signer_pubkey,
    )?;
    let thread_sandbox = uses_thread_sandbox(&config);
    // `config` is a copy: binding the session and any recovery mode never
    // reaches the record's saved `sandbox: true`.
    bind_sandbox_scope(
        &mut config,
        scope.session,
        pubkey,
        &agent_json,
        scope.recovery,
    )?;
    Ok(ProviderInvocation {
        binary: resolve_record_provider_binary(&provider_id, cached_binary_path.as_deref())?,
        config,
        agent_json,
        thread_sandbox,
    })
}

/// Resolve via discovered candidates only. Cached path must match BOTH
/// "is a discovered candidate" AND "belongs to this provider_id". A tampered
/// record cannot redirect provider calls to a different provider's binary.
pub(super) fn resolve_record_provider_binary(
    provider_id: &str,
    cached_binary_path: Option<&str>,
) -> Result<std::path::PathBuf, String> {
    cached_binary_path
        .map(std::path::PathBuf::from)
        .filter(|p| p.exists())
        .map(|p| p.canonicalize().unwrap_or(p))
        .filter(|canonical| {
            discover_provider_candidates().iter().any(|(id, cp)| {
                id == provider_id && cp.canonicalize().ok().as_ref() == Some(canonical)
            })
        })
        .map_or_else(|| resolve_provider_binary(provider_id), Ok)
}

/// Deploy an agent to a provider backend. Resolves the binary, calls deploy via
/// spawn_blocking, and persists the result (backend_agent_id or last_error).
///
/// Idempotency: calling deploy on an already-deployed agent sends the same payload
/// again. Providers are expected to handle this as an update-in-place or no-op.
/// Thread-sandbox providers end a session with the separate `stop` operation
/// (`end_remote_agent_session`); other providers have no undeploy operation, so
/// a successful redeploy delegates access-policy revocation semantics to the
/// provider implementation.
/// Returns Ok(()) on success, Err(message) on failure. Either way the record is
/// updated and saved before returning.
///
/// Callers with a captured tenant scope (Projects agent starts) pass
/// `expected_relay_url` / `expected_signer_pubkey`; they are asserted against
/// the payload REBUILT after the deploy lock — the exact value invoked — so a
/// workspace or identity switch landing while this call waited behind another
/// deployment fails closed instead of deploying a stale start into the new
/// tenant under the new tenant's owner identity. `None` preserves the
/// unscoped behavior for callers without a tenant boundary.
///
/// `replay_floor_unix`: optional unix-seconds replay floor from a
/// publish-first mention send. It is injected into the rebuilt payload's
/// `launch.policy_env` as `BUZZ_ACP_REPLAY_FLOOR`, so the remote harness's
/// startup watermark replays back past the already-published triggering
/// message exactly like a local spawn. Per-invocation only — never persisted
/// on the record, so later redeploys do not carry a stale floor.
///
/// `recovery`: an explicit user choice to recover the thread's ended session.
/// Like the floor, it rides only this invocation's config.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn deploy_to_provider<R: tauri::Runtime>(
    app: &AppHandle<R>,
    state: &AppState,
    pubkey: &str,
    _provider_id: &str,
    _config: &serde_json::Value,
    _agent_json: serde_json::Value,
    _cached_binary_path: Option<&str>,
    expected_relay_url: Option<&str>,
    expected_signer_pubkey: Option<&str>,
    replay_floor_unix: Option<u64>,
    session_scope: Option<&RemoteSessionScope>,
    recovery: Option<RemoteRecoveryMode>,
) -> Result<(), String> {
    let _deploy_guard = lock_provider_operations(state, pubkey).await?;
    let ProviderInvocation {
        binary: bin_path,
        config,
        mut agent_json,
        ..
    } = prepare_provider_invocation(
        app,
        state,
        pubkey,
        &InvocationScope {
            expected_relay_url,
            expected_signer_pubkey,
            session: session_scope,
            recovery,
        },
    )?;
    // The floor is invocation state, not record state, so the post-lock
    // rebuild cannot restore it — inject it into the payload actually invoked.
    apply_replay_floor(&mut agent_json, replay_floor_unix);

    let deployed_agent_json = agent_json.clone();
    let deploy_result =
        tokio::task::spawn_blocking(move || provider_deploy(&bin_path, &agent_json, &config))
            .await
            .map_err(|e| format!("spawn_blocking failed: {e}"))?;

    // Persist result under lock.
    let _store_guard = state
        .managed_agents_store_lock
        .lock()
        .map_err(|e| e.to_string())?;
    let mut records = load_managed_agents(app)?;
    let rec = records
        .iter_mut()
        .find(|r| r.pubkey == pubkey)
        .ok_or_else(|| format!("agent {pubkey} not found"))?;

    let result = apply_deploy_result(rec, deploy_result, &deployed_agent_json);
    save_managed_agents(app, &records)?;
    result
}

/// Assert a caller-captured tenant scope against the payload that will
/// actually be invoked. The relay lives at the payload's top-level
/// `relay_url`; the deploying identity lives at `launch.owner_pubkey` — both
/// were re-resolved from live workspace state by `build_deploy_payload`, so
/// this is the check tied to the use. When the caller carries an expectation
/// a missing payload field fails closed: an unverifiable payload must never
/// deploy on behalf of a scoped callback.
fn assert_payload_scope(
    agent_json: &serde_json::Value,
    expected_relay_url: Option<&str>,
    expected_signer_pubkey: Option<&str>,
) -> Result<(), String> {
    let has_expectation =
        |expected: Option<&str>| expected.map(str::trim).filter(|s| !s.is_empty()).is_some();
    match agent_json.get("relay_url").and_then(|v| v.as_str()) {
        Some(embedded_relay) => crate::relay::assert_expected_relay_scope(
            expected_relay_url,
            &crate::relay::relay_http_base_url(embedded_relay),
        )?,
        None if has_expectation(expected_relay_url) => {
            return Err("deploy payload carries no relay; not deployed".to_string());
        }
        None => {}
    }
    match agent_json
        .get("launch")
        .and_then(|launch| launch.get("owner_pubkey"))
        .and_then(|v| v.as_str())
    {
        Some(owner) => crate::relay::assert_expected_signer(expected_signer_pubkey, owner)?,
        None if has_expectation(expected_signer_pubkey) => {
            return Err("deploy payload carries no owner identity; not deployed".to_string());
        }
        None => {}
    }
    Ok(())
}

/// Inject a caller-supplied replay floor into the deploy payload so the
/// remote harness consumes it exactly like a local spawn: as the
/// [`REPLAY_FLOOR_ENV_VAR`] environment variable. The floor rides
/// `launch.policy_env` (tier 1); any same-named key in `launch.env` (tier 2)
/// is stripped because that tier later-wins and a persisted user value must
/// not shadow this send's floor — the remote mirror of
/// `apply_replay_floor_env`'s post-`descriptor.env` write on the local spawn.
/// With no caller floor the payload is left untouched — a user-supplied
/// `launch.env` value passes through, and plain redeploys never carry a stale
/// floor.
fn apply_replay_floor(agent_json: &mut serde_json::Value, replay_floor_unix: Option<u64>) {
    let Some(floor) = replay_floor_unix else {
        return;
    };
    let Some(launch) = agent_json
        .get_mut("launch")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    if let Some(env) = launch
        .get_mut("env")
        .and_then(serde_json::Value::as_object_mut)
    {
        let shadowed: Vec<String> = env
            .keys()
            .filter(|key| key.eq_ignore_ascii_case(REPLAY_FLOOR_ENV_VAR))
            .cloned()
            .collect();
        for key in shadowed {
            env.remove(&key);
        }
    }
    match launch
        .get_mut("policy_env")
        .and_then(serde_json::Value::as_object_mut)
    {
        Some(policy_env) => {
            policy_env.insert(
                REPLAY_FLOOR_ENV_VAR.to_string(),
                serde_json::Value::String(floor.to_string()),
            );
        }
        None => {
            launch.insert(
                "policy_env".to_string(),
                serde_json::json!({ (REPLAY_FLOOR_ENV_VAR): floor.to_string() }),
            );
        }
    }
}

fn policy_matches_payload(
    record: &crate::managed_agents::ManagedAgentRecord,
    deployed_agent_json: &serde_json::Value,
) -> bool {
    deployed_agent_json
        .get("respond_to")
        .and_then(serde_json::Value::as_str)
        == Some(record.respond_to.as_str())
        && deployed_agent_json.get("respond_to_allowlist")
            == Some(&serde_json::json!(record.respond_to_allowlist))
}

fn apply_deploy_result(
    record: &mut crate::managed_agents::ManagedAgentRecord,
    deploy_result: Result<String, String>,
    deployed_agent_json: &serde_json::Value,
) -> Result<(), String> {
    match deploy_result {
        Ok(backend_agent_id) => {
            record.backend_agent_id = Some(backend_agent_id);
            if policy_matches_payload(record, deployed_agent_json) {
                record.provider_policy_pending = false;
            }
            record.last_started_at = Some(now_iso());
            record.updated_at = now_iso();
            record.last_error = None;
            Ok(())
        }
        Err(error) => {
            record.last_error = Some(error.clone());
            record.updated_at = now_iso();
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_scope_is_required_and_bound_to_the_rebuilt_owner() {
        let scope = RemoteSessionScope {
            channel_id: uuid::Uuid::new_v4(),
            thread_root: "a".repeat(64),
        };
        let mut config = serde_json::json!({"sandbox":true});
        let payload = serde_json::json!({"launch":{"owner_pubkey":"owner"}});
        assert!(bind_sandbox_scope(&mut config, None, "agent", &payload, None).is_err());
        assert_eq!(config["sandbox"], true);
        bind_sandbox_scope(&mut config, Some(&scope), "agent", &payload, None).unwrap();
        assert_eq!(config["sandbox"]["thread_root"], scope.thread_root);
        assert_eq!(config["identity_policy"]["owner_pubkey"], "owner");
        assert!(
            config["sandbox"].get("recovery").is_none(),
            "an ordinary deploy must never carry a recovery mode"
        );
        let mut missing_owner = serde_json::json!({"sandbox":true});
        assert!(bind_sandbox_scope(
            &mut missing_owner,
            Some(&scope),
            "agent",
            &serde_json::json!({}),
            None
        )
        .is_err());
    }

    fn thread_scope() -> RemoteSessionScope {
        RemoteSessionScope {
            channel_id: uuid::Uuid::new_v4(),
            thread_root: "b".repeat(64),
        }
    }

    #[test]
    fn recovery_mode_rides_only_the_bound_invocation_config() {
        let payload = serde_json::json!({"launch":{"owner_pubkey":"owner"}});
        for (mode, wire) in [
            (RemoteRecoveryMode::Checkpoint, "checkpoint"),
            (RemoteRecoveryMode::Fresh, "fresh"),
        ] {
            let mut record = record();
            record.backend = BackendKind::Provider {
                id: "kubernetes".into(),
                config: serde_json::json!({"sandbox": true}),
            };
            // Same copy-then-bind sequence as `prepare_provider_invocation`.
            let BackendKind::Provider { config, .. } = &record.backend else {
                unreachable!()
            };
            let mut invoked = config.clone();
            bind_sandbox_scope(
                &mut invoked,
                Some(&thread_scope()),
                "agent",
                &payload,
                Some(mode),
            )
            .unwrap();
            assert_eq!(
                invoked["sandbox"]["recovery"],
                serde_json::json!({"mode": wire})
            );
            assert_eq!(invoked["sandbox"]["thread_root"], "b".repeat(64));

            apply_deploy_result(&mut record, Ok("remote".into()), &payload).unwrap();
            let saved = serde_json::to_value(&record).unwrap();
            assert_eq!(
                saved["backend"]["config"],
                serde_json::json!({"sandbox": true}),
                "recovery must never be persisted on the agent record"
            );
        }
    }

    #[test]
    fn recovery_is_refused_without_a_thread_workspace() {
        let payload = serde_json::json!({"launch":{"owner_pubkey":"owner"}});
        let mut sandbox = serde_json::json!({"sandbox": true});
        let error = bind_sandbox_scope(
            &mut sandbox,
            None,
            "agent",
            &payload,
            Some(RemoteRecoveryMode::Fresh),
        )
        .unwrap_err();
        assert!(error.contains("channel thread"), "{error}");
        assert_eq!(sandbox, serde_json::json!({"sandbox": true}));

        let mut plain = serde_json::json!({"namespace": "agents"});
        let error = bind_sandbox_scope(
            &mut plain,
            Some(&thread_scope()),
            "agent",
            &payload,
            Some(RemoteRecoveryMode::Checkpoint),
        )
        .unwrap_err();
        assert!(error.contains("Only thread workspaces"), "{error}");
        assert_eq!(plain, serde_json::json!({"namespace": "agents"}));
    }

    #[test]
    fn recovery_mode_deserializes_from_lowercase_wire_names() {
        assert_eq!(
            serde_json::from_str::<RemoteRecoveryMode>(r#""checkpoint""#).unwrap(),
            RemoteRecoveryMode::Checkpoint
        );
        assert_eq!(
            serde_json::from_str::<RemoteRecoveryMode>(r#""fresh""#).unwrap(),
            RemoteRecoveryMode::Fresh
        );
        assert!(serde_json::from_str::<RemoteRecoveryMode>(r#""Checkpoint""#).is_err());
    }

    fn record() -> crate::managed_agents::ManagedAgentRecord {
        serde_json::from_value(serde_json::json!({
            "pubkey": "agent", "name": "Agent", "relay_url": "", "acp_command": "",
            "agent_command": "", "agent_args": [], "mcp_command": "",
            "turn_timeout_seconds": 0, "system_prompt": null, "created_at": "",
            "updated_at": "", "last_started_at": null, "last_stopped_at": null,
            "last_exit_code": null, "last_error": null,
            "provider_policy_pending": true
        }))
        .unwrap()
    }

    fn policy_payload(respond_to: &str) -> serde_json::Value {
        serde_json::json!({"respond_to": respond_to, "respond_to_allowlist": []})
    }

    fn scoped_payload(relay: &str, owner: &str) -> serde_json::Value {
        serde_json::json!({
            "relay_url": relay,
            "launch": { "owner_pubkey": owner },
        })
    }

    // ── assert_payload_scope: post-lock rebuilt-payload validation ──────────

    #[test]
    fn matching_scope_and_signer_pass_on_the_rebuilt_payload() {
        assert_payload_scope(
            &scoped_payload("wss://tenant-a.example", "aa11"),
            Some("wss://tenant-a.example"),
            Some("aa11"),
        )
        .unwrap();
    }

    #[test]
    fn relay_switch_during_the_lock_wait_fails_closed() {
        // Round-8 P1: a stale Projects-A start waited behind another deploy;
        // the rebuild resolved tenant B. The payload actually invoked must be
        // refused — the pre-lock snapshot its caller validated is irrelevant.
        let error = assert_payload_scope(
            &scoped_payload("wss://tenant-b.example", "aa11"),
            Some("wss://tenant-a.example"),
            Some("aa11"),
        )
        .unwrap_err();
        assert!(error.contains("active community changed"), "{error}");
    }

    #[test]
    fn same_relay_identity_switch_during_the_lock_wait_fails_closed() {
        // Same relay, different owner: an identity switch alone must also be
        // refused — the rebuilt launch.owner_pubkey belongs to a tenant the
        // caller never validated.
        let error = assert_payload_scope(
            &scoped_payload("wss://tenant-a.example", "bb22"),
            Some("wss://tenant-a.example"),
            Some("aa11"),
        )
        .unwrap_err();
        assert!(error.contains("active identity changed"), "{error}");
    }

    #[test]
    fn scoped_caller_with_an_unverifiable_payload_fails_closed() {
        let payload = serde_json::json!({});
        let relay_error =
            assert_payload_scope(&payload, Some("wss://tenant-a.example"), None).unwrap_err();
        assert!(relay_error.contains("no relay"), "{relay_error}");
        let signer_error = assert_payload_scope(&payload, None, Some("aa11")).unwrap_err();
        assert!(signer_error.contains("no owner identity"), "{signer_error}");
    }

    #[test]
    fn unscoped_callers_deploy_any_payload() {
        assert_payload_scope(
            &scoped_payload("wss://anywhere.example", "cc33"),
            None,
            None,
        )
        .unwrap();
        assert_payload_scope(&serde_json::json!({}), None, None).unwrap();
    }

    // ── apply_replay_floor: publish-first floor threading into the payload ──

    fn launch_payload() -> serde_json::Value {
        serde_json::json!({
            "launch": {
                "env": { "KEEP_ME": "yes" },
                "policy_env": { "BUZZ_ACP_LAZY_POOL": "true" },
            },
        })
    }

    #[test]
    fn caller_replay_floor_rides_launch_policy_env() {
        // A publish-first mention send's floor must reach the remote harness
        // as BUZZ_ACP_REPLAY_FLOOR, exactly like a local spawn's env.
        let mut payload = launch_payload();
        apply_replay_floor(&mut payload, Some(1_756_600_000));
        assert_eq!(
            payload["launch"]["policy_env"]["BUZZ_ACP_REPLAY_FLOOR"],
            "1756600000"
        );
        assert_eq!(payload["launch"]["env"]["KEEP_ME"], "yes");
        assert_eq!(
            payload["launch"]["policy_env"]["BUZZ_ACP_LAZY_POOL"],
            "true"
        );
    }

    #[test]
    fn caller_replay_floor_strips_user_env_shadow() {
        // launch.env later-wins over policy_env in the remote three-tier
        // model; a persisted user floor must not shadow this send's floor.
        let mut payload = launch_payload();
        payload["launch"]["env"]["BUZZ_ACP_REPLAY_FLOOR"] = "1".into();
        payload["launch"]["env"]["buzz_acp_replay_floor"] = "2".into();
        apply_replay_floor(&mut payload, Some(42));
        assert_eq!(
            payload["launch"]["policy_env"]["BUZZ_ACP_REPLAY_FLOOR"],
            "42"
        );
        assert!(payload["launch"]["env"]["BUZZ_ACP_REPLAY_FLOOR"].is_null());
        assert!(payload["launch"]["env"]["buzz_acp_replay_floor"].is_null());
        assert_eq!(payload["launch"]["env"]["KEEP_ME"], "yes");
    }

    #[test]
    fn no_caller_floor_leaves_payload_untouched() {
        // Create-flow deploys and plain redeploys carry no floor: user env
        // passthrough stands and no stale floor is invented.
        let mut payload = launch_payload();
        payload["launch"]["env"]["BUZZ_ACP_REPLAY_FLOOR"] = "1".into();
        let before = payload.clone();
        apply_replay_floor(&mut payload, None);
        assert_eq!(payload, before);
    }

    #[test]
    fn replay_floor_tolerates_payload_without_launch() {
        let mut payload = serde_json::json!({});
        apply_replay_floor(&mut payload, Some(42));
        assert_eq!(payload, serde_json::json!({}));
    }

    #[test]
    fn replay_floor_creates_missing_policy_env() {
        let mut payload = serde_json::json!({ "launch": {} });
        apply_replay_floor(&mut payload, Some(42));
        assert_eq!(
            payload["launch"]["policy_env"]["BUZZ_ACP_REPLAY_FLOOR"],
            "42"
        );
    }

    #[test]
    fn successful_deploy_acknowledges_pending_policy() {
        let mut record = record();

        apply_deploy_result(
            &mut record,
            Ok("provider-agent".into()),
            &policy_payload("owner-only"),
        )
        .unwrap();

        assert!(!record.provider_policy_pending);
        assert_eq!(record.backend_agent_id.as_deref(), Some("provider-agent"));
        assert_eq!(record.last_error, None);
    }

    #[test]
    fn successful_stale_deploy_preserves_newer_pending_policy() {
        let mut record = record();
        record.respond_to = crate::managed_agents::RespondTo::Anyone;

        apply_deploy_result(
            &mut record,
            Ok("provider-agent".into()),
            &policy_payload("owner-only"),
        )
        .unwrap();

        assert!(record.provider_policy_pending);
    }

    #[test]
    fn failed_deploy_preserves_pending_policy() {
        let mut record = record();

        let error = apply_deploy_result(
            &mut record,
            Err("provider unavailable".into()),
            &policy_payload("owner-only"),
        )
        .expect_err("deployment should fail");

        assert_eq!(error, "provider unavailable");
        assert!(record.provider_policy_pending);
        assert_eq!(record.last_error.as_deref(), Some("provider unavailable"));
    }
}
