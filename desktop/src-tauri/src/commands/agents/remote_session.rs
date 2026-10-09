//! Explicit lifecycle actions for a thread-sandbox agent's remote session:
//! end it, or recover it after it ended. Both run only on user action and
//! share the deploy path's per-agent lock, post-lock payload rebuild, tenant
//! scope assertion, and session binding.

use tauri::{AppHandle, State};

use crate::{
    app_state::AppState,
    managed_agents::{
        load_managed_agents, provider_ops, provider_stop, BackendKind, ManagedAgentSummary,
    },
};

use super::provider_deploy::{
    deploy_to_provider, lock_provider_operations, prepare_provider_invocation,
    resolve_record_provider_binary, uses_thread_sandbox, InvocationScope, ProviderInvocation,
    RemoteRecoveryMode, RemoteSessionScope,
};

/// End the remote session for one channel thread with the provider's `stop`
/// operation. Returns the session state the provider reports: `ending`,
/// `ended`, or `absent`. The agent record is not changed: the provider owns
/// the session lifecycle, and recovery is a separate explicit action.
#[tauri::command]
pub async fn end_remote_agent_session(
    pubkey: String,
    session_scope: RemoteSessionScope,
    expected_relay_url: Option<String>,
    expected_signer_pubkey: Option<String>,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let _guard = lock_provider_operations(&state, &pubkey).await?;
    let ProviderInvocation {
        binary,
        config,
        agent_json,
        thread_sandbox,
    } = prepare_provider_invocation(
        &app,
        &state,
        &pubkey,
        &InvocationScope {
            expected_relay_url: expected_relay_url.as_deref(),
            expected_signer_pubkey: expected_signer_pubkey.as_deref(),
            session: Some(&session_scope),
            recovery: None,
        },
    )?;
    if !thread_sandbox {
        return Err("Only thread workspaces can be ended".to_string());
    }
    tokio::task::spawn_blocking(move || provider_stop(&binary, &agent_json, &config))
        .await
        .map_err(|e| format!("spawn_blocking failed: {e}"))?
}

/// Recover the ended remote session for one channel thread, from its
/// checkpoint or fresh. The mode is sent for this deploy only and is never
/// persisted. Returns the updated agent, like `start_managed_agent`.
#[tauri::command]
pub async fn recover_remote_agent_session(
    pubkey: String,
    session_scope: RemoteSessionScope,
    mode: RemoteRecoveryMode,
    expected_relay_url: Option<String>,
    expected_signer_pubkey: Option<String>,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<ManagedAgentSummary, String> {
    deploy_to_provider(
        &app,
        &state,
        &pubkey,
        "",
        &serde_json::Value::Null,
        serde_json::Value::Null,
        None,
        expected_relay_url.as_deref(),
        expected_signer_pubkey.as_deref(),
        None,
        Some(&session_scope),
        Some(mode),
    )
    .await?;
    let _store_guard = state
        .managed_agents_store_lock
        .lock()
        .map_err(|e| e.to_string())?;
    let records = load_managed_agents(&app)?;
    let runtimes = state
        .managed_agent_processes
        .lock()
        .map_err(|e| e.to_string())?;
    let record = records
        .iter()
        .find(|r| r.pubkey == pubkey)
        .ok_or_else(|| format!("agent {pubkey} not found"))?;
    super::summarize_from_disk(&app, record, &runtimes)
}

/// Whether the agent's provider can end and recover thread sessions, which
/// it advertises as `stop` in `info.ops`. Runs only `info`. Local agents and
/// provider agents without thread workspaces return `false`.
#[tauri::command]
pub async fn remote_agent_session_supports_lifecycle(
    pubkey: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<bool, String> {
    let target = {
        let _store_guard = state
            .managed_agents_store_lock
            .lock()
            .map_err(|e| e.to_string())?;
        let records = load_managed_agents(&app)?;
        let record = records
            .iter()
            .find(|r| r.pubkey == pubkey)
            .ok_or_else(|| format!("agent {pubkey} not found"))?;
        match &record.backend {
            BackendKind::Provider { id, config } if uses_thread_sandbox(config) => {
                Some((id.clone(), record.provider_binary_path.clone()))
            }
            _ => None,
        }
    };
    let Some((provider_id, cached_binary_path)) = target else {
        return Ok(false);
    };
    let binary = resolve_record_provider_binary(&provider_id, cached_binary_path.as_deref())?;
    let ops = tokio::task::spawn_blocking(move || provider_ops(&binary))
        .await
        .map_err(|e| format!("spawn_blocking failed: {e}"))??;
    Ok(ops.iter().any(|op| op == "stop"))
}
