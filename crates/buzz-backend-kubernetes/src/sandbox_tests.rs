use super::*;
use buzz_backend_kubernetes::lifecycle;
use buzz_backend_kubernetes::lifecycle::LIFECYCLE_ENDED;
use nostr::nips::nip19::ToBech32;

#[path = "sandbox_fake.rs"]
mod fake;

#[test]
fn custom_image_is_inside_a_real_sandbox_with_a_downward_uid() {
    let identity = naming::AgentIdentity::from_nsec(
        &nostr::Keys::generate().secret_key().to_bech32().unwrap(),
    )
    .unwrap();
    let cfg = crate::config::parse(&json!({"namespace":"agents", "image":format!("example.com/agent@sha256:{}", "a".repeat(64)),
        "pod_options":{"active_deadline_seconds":3600,"workspace_size_limit":"50Gi"}})).unwrap();
    let scope = Options {
        channel_id: uuid::Uuid::new_v4().to_string(),
        thread_root: "c".repeat(64),
        recovery: None,
    };
    let fps = Fingerprints::new(&cfg, &BTreeMap::new());
    let new = NewSession {
        generation: "generation",
        checkpointing: false,
        restore: None,
        recovered_from: None,
    };
    let mut sandbox = build(&identity, &cfg, &scope, "owner", &new, &fps.current).unwrap();
    assert_eq!(
        sandbox.types.as_ref().unwrap().api_version,
        "agents.x-k8s.io/v1beta1"
    );
    let spec = &sandbox.data["spec"]["podTemplate"]["spec"];
    assert_eq!(spec["containers"][0]["image"], cfg.image.as_str());
    assert_eq!(
        spec["containers"][0]["env"][0]["valueFrom"]["fieldRef"]["fieldPath"],
        "metadata.uid"
    );
    assert_eq!(spec["automountServiceAccountToken"], false);
    assert_eq!(spec["restartPolicy"], "Never");
    assert_eq!(spec["volumes"][0]["emptyDir"]["sizeLimit"], "50Gi");
    assert_eq!(spec["containers"][0]["terminationMessagePolicy"], "File");
    assert_eq!(annotation(&sandbox, CHECKPOINTING), Some("disabled"));
    assert_eq!(annotation(&sandbox, RESTORE_CHECKPOINT), None);
    verify_identity(&sandbox, &identity, &scope, "owner").unwrap();
    verify_live(&sandbox, &fps).unwrap();
    sandbox
        .annotations_mut()
        .insert(OWNER.into(), "different-owner".into());
    assert!(verify_identity(&sandbox, &identity, &scope, "owner").is_err());
}

fn fixture_env() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("BUZZ_RELAY_URL".into(), "wss://fixture.invalid".into()),
        ("BUZZ_SETUP_MODE".into(), "codex-device-v1".into()),
        ("BUZZ_ACP_AGENT_COMMAND".into(), "codex-acp".into()),
        (
            "BUZZ_ACP_PERMISSION_MODE".into(),
            "agent-full-access".into(),
        ),
        (
            "BUZZ_SETUP_AGENT_PUBKEY".into(),
            "stale-caller-value".into(),
        ),
        ("BUZZ_SETUP_POD_UID".into(), "stale-caller-value".into()),
    ])
}

#[tokio::test]
#[ignore = "requires the isolated kind-remote-agent-check cluster and fixture image"]
async fn live_controller_reuses_scopes_and_refuses_lost_workspaces() {
    use k8s_openapi::api::core::v1::Namespace;
    use kube::api::DeleteParams;
    use kube::config::{KubeConfigOptions, Kubeconfig};

    let config =
        Kubeconfig::read_from(std::env::var("BUZZ_SANDBOX_TEST_KUBECONFIG").unwrap()).unwrap();
    let config = kube::Config::from_custom_kubeconfig(
        config,
        &KubeConfigOptions {
            context: Some("kind-remote-agent-check".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        config.cluster_url.host(),
        Some("127.0.0.1"),
        "never use a shared cluster for this test"
    );
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = Client::try_from(config).unwrap();
    let namespace = format!(
        "buzz-agent-staging-test-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let namespaces: Api<Namespace> = Api::all(client.clone());
    namespaces.create(&PostParams::default(), &serde_json::from_value(json!({
        "metadata":{"name":namespace,"labels":{"pod-security.kubernetes.io/enforce":"restricted"}}
    })).unwrap()).await.unwrap();
    let cfg = crate::config::parse(&json!({
        "namespace":namespace, "image":std::env::var("BUZZ_SANDBOX_TEST_IMAGE").unwrap(),
        "cpu_request":"10m", "memory_request":"16Mi", "cpu_limit":"100m", "memory_limit":"32Mi",
        "pod_options":{"active_deadline_seconds":600}
    }))
    .unwrap();
    let identity = naming::AgentIdentity::from_nsec(
        &nostr::Keys::generate().secret_key().to_bech32().unwrap(),
    )
    .unwrap();
    let scope = Options {
        channel_id: uuid::Uuid::new_v4().to_string(),
        thread_root: "a".repeat(64),
        recovery: None,
    };
    let (first, concurrent) = tokio::join!(
        deploy(
            client.clone(),
            &identity,
            &cfg,
            &scope,
            "owner",
            fixture_env()
        ),
        deploy(
            client.clone(),
            &identity,
            &cfg,
            &scope,
            "owner",
            fixture_env()
        ),
    );
    let first = first.unwrap();
    assert_eq!(
        concurrent.unwrap(),
        first,
        "concurrent mentions reuse one Sandbox"
    );
    let secrets: Api<Secret> = Api::namespaced(client.clone(), &namespace);
    let before = secrets.list(&ListParams::default()).await.unwrap();
    assert_eq!(before.items.len(), 1);
    let data = before.items[0].data.as_ref().unwrap();
    assert_eq!(
        data["BUZZ_SETUP_AGENT_PUBKEY"].0,
        identity.pubkey_hex().as_bytes()
    );
    assert_eq!(data["BUZZ_SETUP_NONCE"].0.len(), 32);
    assert_eq!(data["BUZZ_MANAGED_AGENT_START_NONCE"].0.len(), 32);
    assert!(!data.contains_key("BUZZ_SETUP_POD_UID"));
    assert_eq!(
        deploy(
            client.clone(),
            &identity,
            &cfg,
            &scope,
            "owner",
            fixture_env()
        )
        .await
        .unwrap(),
        first
    );
    let after = secrets.list(&ListParams::default()).await.unwrap();
    assert_eq!(after.items.len(), 1);
    assert_eq!(
        after.items[0].data, before.items[0].data,
        "retry must preserve setup binding and deadline"
    );
    assert!(deploy(
        client.clone(),
        &identity,
        &cfg,
        &scope,
        "other-owner",
        fixture_env()
    )
    .await
    .is_err());
    let mut other_relay = fixture_env();
    other_relay.insert("BUZZ_RELAY_URL".into(), "wss://other.invalid".into());
    assert!(deploy(
        client.clone(),
        &identity,
        &cfg,
        &scope,
        "owner",
        other_relay
    )
    .await
    .unwrap_err()
    .contains("different relay"));
    let second_scope = Options {
        channel_id: scope.channel_id.clone(),
        thread_root: "b".repeat(64),
        recovery: None,
    };
    let second = deploy(
        client.clone(),
        &identity,
        &cfg,
        &second_scope,
        "owner",
        fixture_env(),
    )
    .await
    .unwrap();
    assert_ne!(first, second);
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let original = pods.get(&first).await.unwrap();
    let pod_uid = original.metadata.uid.unwrap();
    pods.delete(
        &first,
        &DeleteParams {
            grace_period_seconds: Some(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut replacement = None;
    for _ in 0..30 {
        if let Some(pod) = pods.get_opt(&first).await.unwrap() {
            if pod.metadata.uid.as_deref() != Some(&pod_uid) {
                replacement = Some(pod);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        replacement.is_some(),
        "upstream controller recreates the Pod"
    );
    let error = deploy(
        client.clone(),
        &identity,
        &cfg,
        &scope,
        "owner",
        fixture_env(),
    )
    .await
    .unwrap_err();
    assert!(error.contains("replaced"), "{error}");
    let sandboxes = api(client.clone(), &namespace)
        .list(&ListParams::default())
        .await
        .unwrap();
    assert_eq!(sandboxes.items.len(), 2, "one Sandbox per canonical thread");

    // Lifecycle: recovery is refused on a live session; stop suspends it; once
    // tombstoned (the manager's job, simulated here) only explicit recovery
    // resumes it, under a new generation.
    let fresh = Options {
        recovery: Some(Recovery {
            mode: RecoveryMode::Fresh,
        }),
        ..Options {
            channel_id: second_scope.channel_id.clone(),
            thread_root: second_scope.thread_root.clone(),
            recovery: None,
        }
    };
    let refused = deploy(
        client.clone(),
        &identity,
        &cfg,
        &fresh,
        "owner",
        fixture_env(),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("still active"), "{refused}");
    let (_, state) = stop(
        client.clone(),
        &identity,
        &namespace,
        &second_scope,
        "owner",
    )
    .await
    .unwrap();
    assert_eq!(state, StopState::Ending);
    let sandboxes = api(client.clone(), &namespace);
    let old = sandboxes.get(&second).await.unwrap();
    assert_eq!(old.data["spec"]["operatingMode"], "Suspended");
    let tombstone = json!({"metadata": {"annotations": {
        (LIFECYCLE): LIFECYCLE_ENDED, (CHECKPOINT): lifecycle::CHECKPOINT_MISSING,
        (lifecycle::ENDED_AT): "2026-01-01T00:00:00Z"}}});
    sandboxes
        .patch(&second, &PatchParams::default(), &Patch::Merge(&tombstone))
        .await
        .unwrap();
    let ended = deploy(
        client.clone(),
        &identity,
        &cfg,
        &second_scope,
        "owner",
        fixture_env(),
    )
    .await
    .unwrap_err();
    assert!(ended.contains("explicit recovery is required"), "{ended}");
    let from_checkpoint = Options {
        recovery: Some(Recovery {
            mode: RecoveryMode::Checkpoint,
        }),
        channel_id: second_scope.channel_id.clone(),
        thread_root: second_scope.thread_root.clone(),
    };
    let no_checkpoint = deploy(
        client.clone(),
        &identity,
        &cfg,
        &from_checkpoint,
        "owner",
        fixture_env(),
    )
    .await
    .unwrap_err();
    assert!(no_checkpoint.contains("start fresh"), "{no_checkpoint}");
    assert_eq!(
        deploy(
            client.clone(),
            &identity,
            &cfg,
            &fresh,
            "owner",
            fixture_env()
        )
        .await
        .unwrap(),
        second
    );
    let recovered = sandboxes.get(&second).await.unwrap();
    assert_eq!(
        annotation(&recovered, RECOVERED_FROM),
        annotation(&old, GENERATION)
    );
    assert_ne!(
        annotation(&recovered, GENERATION),
        annotation(&old, GENERATION)
    );
    namespaces
        .delete(&namespace, &DeleteParams::default())
        .await
        .unwrap();
}

// ── Fake-apiserver tests: the real deploy/stop paths over kube::Client ──────

fn fake_identity() -> naming::AgentIdentity {
    naming::AgentIdentity::from_nsec(&nostr::Keys::generate().secret_key().to_bech32().unwrap())
        .unwrap()
}

fn fake_cfg() -> ProviderConfig {
    crate::config::parse(&json!({"namespace":"agents",
        "image":format!("example.com/agent@sha256:{}", "a".repeat(64)),
        "pod_options":{"active_deadline_seconds":3600}}))
    .unwrap()
}

fn fake_scope(recovery: Option<RecoveryMode>) -> Options {
    Options {
        channel_id: "0b6c1f3e-5a7d-4c2b-9e8f-1a2b3c4d5e6f".into(),
        thread_root: "d".repeat(64),
        recovery: recovery.map(|mode| Recovery { mode }),
    }
}

fn plain_env() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("BUZZ_RELAY_URL".into(), "wss://fixture.invalid".into()),
        ("BUZZ_CHECKPOINT_BUCKET".into(), "bucket".into()),
        ("BUZZ_CHECKPOINT_PREFIX".into(), "dev/".into()),
    ])
}

fn secret_env(state: &fake::Shared, key: &str) -> Vec<String> {
    state
        .lock()
        .unwrap()
        .secrets
        .values()
        .map(|secret| {
            let secret: Secret = serde_json::from_value(secret.clone()).unwrap();
            String::from_utf8(secret.data.unwrap()[key].0.clone()).unwrap()
        })
        .collect()
}

fn sandbox_json(state: &fake::Shared, name: &str) -> serde_json::Value {
    state.lock().unwrap().sandboxes[name].clone()
}

/// Turn the live session into an Ended tombstone, as the manager would.
fn tombstone(state: &fake::Shared, name: &str, checkpoint: &str) {
    let mut guard = state.lock().unwrap();
    let sandbox = guard.sandboxes.get_mut(name).unwrap();
    sandbox["spec"]["operatingMode"] = "Suspended".into();
    let annotations = &mut sandbox["metadata"]["annotations"];
    annotations[LIFECYCLE] = LIFECYCLE_ENDED.into();
    annotations[ENDED_REASON] = "completed".into();
    annotations[CHECKPOINT] = checkpoint.into();
    guard.pods.remove(name);
}

#[tokio::test(start_paused = true)]
async fn fresh_deploy_binds_and_always_carries_an_empty_restore_key() {
    let state = fake::Shared::default();
    let (identity, cfg, scope) = (fake_identity(), fake_cfg(), fake_scope(None));
    let name = deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    let sandbox = sandbox_json(&state, &name);
    let annotations = &sandbox["metadata"]["annotations"];
    assert_eq!(annotations[CHECKPOINTING], "enabled");
    assert!(annotations[INITIAL_POD].is_string());
    assert!(annotations.get(RESTORE_CHECKPOINT).is_none());
    assert_eq!(secret_env(&state, RESTORE_KEY_ENV), [""]);
    assert_eq!(
        secret_env(&state, "BUZZ_MANAGED_AGENT_START_NONCE"),
        [annotations[GENERATION].as_str().unwrap()]
    );
    // A repeat deploy reuses the session.
    assert_eq!(
        deploy(
            fake::client(&state),
            &identity,
            &cfg,
            &scope,
            "owner",
            plain_env()
        )
        .await
        .unwrap(),
        name
    );
    assert_eq!(state.lock().unwrap().secrets.len(), 1);
}

/// The medium bug: bound to the first Pod, Secret never written, container
/// never started. A later deploy must finish the binding, not strand it.
#[tokio::test(start_paused = true)]
async fn a_bound_session_without_its_secret_is_resumed() {
    let state = fake::Shared::default();
    let (identity, cfg, scope) = (fake_identity(), fake_cfg(), fake_scope(None));
    deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    let name = scope.name(&identity);
    let (generation, pod_uid) = {
        let mut guard = state.lock().unwrap();
        guard.secrets.clear();
        let sandbox = &guard.sandboxes[&name];
        (
            sandbox["metadata"]["annotations"][GENERATION].clone(),
            guard.pods[&name].clone(),
        )
    };
    deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    let guard = state.lock().unwrap();
    let (secret_name, secret) = guard.secrets.iter().next().unwrap();
    assert!(secret_name.ends_with(generation.as_str().unwrap()));
    assert_eq!(secret["metadata"]["ownerReferences"][0]["uid"], pod_uid);
    assert_eq!(
        sandbox_json_locked(&guard, &name)["metadata"]["annotations"][GENERATION],
        generation
    );
}

fn sandbox_json_locked(state: &fake::State, name: &str) -> serde_json::Value {
    state.sandboxes[name].clone()
}

#[tokio::test(start_paused = true)]
async fn a_secret_create_failure_unbinds_and_the_retry_succeeds() {
    let state = fake::Shared::default();
    state.lock().unwrap().fail_secret_creates = 1;
    let (identity, cfg, scope) = (fake_identity(), fake_cfg(), fake_scope(None));
    let error = deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap_err();
    assert!(error.contains("retry deploy"), "{error}");
    let name = scope.name(&identity);
    assert!(
        sandbox_json(&state, &name)["metadata"]["annotations"]
            .get(INITIAL_POD)
            .is_none(),
        "the failed binding must be handed back"
    );
    deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    assert_eq!(state.lock().unwrap().secrets.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn recovery_is_refused_on_a_live_session_and_without_a_tombstone_checkpoint() {
    let state = fake::Shared::default();
    let (identity, cfg) = (fake_identity(), fake_cfg());
    let live = fake_scope(None);
    for mode in [RecoveryMode::Fresh, RecoveryMode::Checkpoint] {
        // No session at all: checkpoint recovery has nothing to restore.
        let absent = deploy(
            fake::client(&state),
            &identity,
            &cfg,
            &fake_scope(Some(RecoveryMode::Checkpoint)),
            "owner",
            plain_env(),
        )
        .await
        .unwrap_err();
        assert!(absent.contains("start fresh"), "{absent}");
        deploy(
            fake::client(&state),
            &identity,
            &cfg,
            &live,
            "owner",
            plain_env(),
        )
        .await
        .unwrap();
        let refused = deploy(
            fake::client(&state),
            &identity,
            &cfg,
            &fake_scope(Some(mode)),
            "owner",
            plain_env(),
        )
        .await
        .unwrap_err();
        assert!(refused.contains("still active"), "{mode:?}: {refused}");
        state.lock().unwrap().sandboxes.clear();
        state.lock().unwrap().pods.clear();
        state.lock().unwrap().secrets.clear();
    }
    deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &live,
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    let name = live.name(&identity);
    tombstone(&state, &name, lifecycle::CHECKPOINT_MISSING);
    let ended = deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &live,
        "owner",
        plain_env(),
    )
    .await
    .unwrap_err();
    assert!(
        ended.contains("ended (completed); explicit recovery is required"),
        "{ended}"
    );
    let missing = deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &fake_scope(Some(RecoveryMode::Checkpoint)),
        "owner",
        plain_env(),
    )
    .await
    .unwrap_err();
    assert!(missing.contains("no verified checkpoint"), "{missing}");
    assert_eq!(
        sandbox_json(&state, &name)["metadata"]["annotations"][LIFECYCLE],
        LIFECYCLE_ENDED,
        "a refused recovery must leave the tombstone in place"
    );
}

/// Restore key only in checkpoint recovery; fingerprint stable across a
/// recovery, so the next ordinary deploy reuses the recovered session.
#[tokio::test(start_paused = true)]
async fn checkpoint_recovery_restores_once_under_a_new_generation() {
    let state = fake::Shared::default();
    let (identity, cfg, scope) = (fake_identity(), fake_cfg(), fake_scope(None));
    let name = deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    let old = sandbox_json(&state, &name);
    let old_generation = old["metadata"]["annotations"][GENERATION].clone();
    let key = format!(
        "dev/{}/6f9619ff-8b86-d011-b42d-00cf4fc964ff.tar.gz",
        old_generation.as_str().unwrap()
    );
    tombstone(&state, &name, &key);
    state.lock().unwrap().secrets.clear();
    deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &fake_scope(Some(RecoveryMode::Checkpoint)),
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    let recovered = sandbox_json(&state, &name);
    let annotations = &recovered["metadata"]["annotations"];
    assert_eq!(annotations[RECOVERED_FROM], old_generation);
    assert_ne!(annotations[GENERATION], old_generation);
    assert_eq!(annotations[RESTORE_CHECKPOINT], key.as_str());
    assert!(annotations.get(LIFECYCLE).is_none());
    assert_ne!(recovered["metadata"]["uid"], old["metadata"]["uid"]);
    assert_eq!(
        annotations[naming::ANNOTATION_CREATE_INTENT],
        old["metadata"]["annotations"][naming::ANNOTATION_CREATE_INTENT],
        "fingerprint must survive recovery"
    );
    assert_eq!(
        secret_env(&state, RESTORE_KEY_ENV),
        std::slice::from_ref(&key)
    );
    // The next ordinary deploy reuses it and does not re-restore.
    assert_eq!(
        deploy(
            fake::client(&state),
            &identity,
            &cfg,
            &scope,
            "owner",
            plain_env()
        )
        .await
        .unwrap(),
        name
    );
    assert_eq!(secret_env(&state, RESTORE_KEY_ENV), [key]);

    // Fresh recovery of a new tombstone carries no restore key.
    tombstone(&state, &name, "missing");
    state.lock().unwrap().secrets.clear();
    deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &fake_scope(Some(RecoveryMode::Fresh)),
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    assert!(sandbox_json(&state, &name)["metadata"]["annotations"]
        .get(RESTORE_CHECKPOINT)
        .is_none());
    assert_eq!(secret_env(&state, RESTORE_KEY_ENV), [""]);
}

#[test]
fn the_restore_key_is_fingerprinted_by_name_not_value() {
    let cfg = fake_cfg();
    let scope = fake_scope(None);
    let mut plain = plain_env();
    apply_session_env(&scope, &mut plain);
    assert_eq!(plain[RESTORE_KEY_ENV], "");
    let mut restored = plain.clone();
    restored.insert(RESTORE_KEY_ENV.into(), "dev/g/x.tar.gz".into());
    assert_eq!(
        pod::intent_template(&cfg, plain.keys().cloned()).fingerprint(),
        pod::intent_template(&cfg, restored.keys().cloned()).fingerprint()
    );
    let mut without = plain.clone();
    without.remove(RESTORE_KEY_ENV);
    assert_ne!(
        pod::intent_template(&cfg, plain.keys().cloned()).fingerprint(),
        pod::intent_template(&cfg, without.keys().cloned()).fingerprint()
    );
}

/// Sandboxes created before the restore key existed carry a fingerprint
/// without it. Mixed desktop versions must keep reusing them; any other
/// fingerprint is still a configuration change.
#[tokio::test(start_paused = true)]
async fn a_sandbox_from_before_the_restore_key_is_still_reused() {
    let state = fake::Shared::default();
    let (identity, cfg, scope) = (fake_identity(), fake_cfg(), fake_scope(None));
    let name = deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    let mut env = plain_env();
    apply_session_env(&scope, &mut env);
    let fingerprints = Fingerprints::new(&cfg, &env);
    assert_ne!(fingerprints.legacy, fingerprints.current);
    let set_intent = |value: &str| {
        state.lock().unwrap().sandboxes.get_mut(&name).unwrap()["metadata"]["annotations"]
            [naming::ANNOTATION_CREATE_INTENT] = value.into();
    };
    set_intent(fingerprints.legacy.as_str());
    let redeploy = || {
        deploy(
            fake::client(&state),
            &identity,
            &cfg,
            &scope,
            "owner",
            plain_env(),
        )
    };
    assert_eq!(redeploy().await.unwrap(), name);
    set_intent(&"0".repeat(64));
    assert!(redeploy()
        .await
        .unwrap_err()
        .contains("launch configuration differs"));
}

#[tokio::test(start_paused = true)]
async fn stop_suspends_with_a_cas_patch_and_retries_conflicts() {
    let state = fake::Shared::default();
    let (identity, cfg, scope) = (fake_identity(), fake_cfg(), fake_scope(None));
    assert_eq!(
        stop(fake::client(&state), &identity, "agents", &scope, "owner")
            .await
            .unwrap()
            .1,
        StopState::Absent
    );
    let name = deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap();
    assert!(stop(
        fake::client(&state),
        &identity,
        "agents",
        &scope,
        "other-owner"
    )
    .await
    .is_err());
    state.lock().unwrap().conflict_patches = 1;
    let (agent_id, stopped) = stop(fake::client(&state), &identity, "agents", &scope, "owner")
        .await
        .unwrap();
    assert_eq!(
        (agent_id.as_str(), stopped),
        (name.as_str(), StopState::Ending)
    );
    {
        let guard = state.lock().unwrap();
        let patch = guard.patches.last().unwrap();
        assert!(patch["metadata"]["resourceVersion"].is_string(), "{patch}");
        assert_eq!(patch["spec"]["operatingMode"], "Suspended");
        assert_eq!(
            patch["metadata"]["annotations"][LIFECYCLE],
            LIFECYCLE_ENDING
        );
        assert_eq!(patch["metadata"]["annotations"][ENDED_REASON], "stopped");
        assert_eq!(guard.patches.len(), 2, "one conflict, one retry");
        assert!(!guard.pods.contains_key(&name), "Suspended removes the Pod");
    }
    assert_eq!(
        stop(fake::client(&state), &identity, "agents", &scope, "owner")
            .await
            .unwrap()
            .1,
        StopState::Ending
    );
    let ending = deploy(
        fake::client(&state),
        &identity,
        &cfg,
        &scope,
        "owner",
        plain_env(),
    )
    .await
    .unwrap_err();
    assert!(ending.contains("ending"), "{ending}");
    let recovery = fake_scope(Some(RecoveryMode::Fresh));
    assert!(stop(
        fake::client(&state),
        &identity,
        "agents",
        &recovery,
        "owner"
    )
    .await
    .unwrap_err()
    .contains("recovery"));
}
