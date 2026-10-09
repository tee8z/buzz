//! Agent Sandbox integration. The upstream controller owns the Pod; this
//! provider owns its launch contract and binds credentials to the first Pod UID.
//!
//! Lifecycle (`docs/remote-agents.md` §Sandbox lifecycle): a session that ended
//! — stopped, completed, replaced, lost, or abandoned while binding — is
//! tombstoned by `buzz-agent-manager` and is only ever resumed by an explicit
//! `recovery` deploy, never by an ordinary one.

use std::collections::BTreeMap;
use std::time::Duration;

use buzz_backend_kubernetes::lifecycle::{
    annotation, is_checkpoint_key, owned_pod, sandbox_resource, EndedReason, Lifecycle,
    OperatingMode, CHANNEL, CHECKPOINT, CHECKPOINTING, CHECKPOINTING_DISABLED,
    CHECKPOINTING_ENABLED, ENDED_REASON, GENERATION, INITIAL_POD, LIFECYCLE, LIFECYCLE_ENDING,
    OWNER, RECOVERED_FROM, RESTORE_CHECKPOINT, SANDBOX_BINDING_VERSION, SESSION, THREAD,
};
use k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, ObjectFieldSelector, Pod, Secret};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions};
use kube::core::DynamicObject;
use kube::{Client, ResourceExt};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{config::ProviderConfig, intent::Fingerprint, naming, pod};

/// Secret env key carrying the checkpoint a recovered session restores from.
/// Always present (empty unless recovering) so its *key* is part of every
/// fingerprint and a later ordinary deploy matches the recovered Sandbox.
pub const RESTORE_KEY_ENV: &str = "BUZZ_CHECKPOINT_RESTORE_KEY";

/// How an ended session is resumed. Sent only on explicit user action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecoveryMode {
    /// Restore the tombstone's verified checkpoint into a new session.
    Checkpoint,
    /// Start a new session with an empty workspace.
    Fresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recovery {
    pub mode: RecoveryMode,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    pub channel_id: String,
    pub thread_root: String,
    #[serde(default)]
    pub recovery: Option<Recovery>,
}

impl Options {
    fn name(&self, identity: &naming::AgentIdentity) -> String {
        let scope = Sha256::digest(format!("{}:{}", self.channel_id, self.thread_root).as_bytes());
        format!("{}-{}", identity.pod_name(), &hex::encode(scope)[..12])
    }

    pub fn parse(value: &serde_json::Value) -> Result<Option<Self>, String> {
        let Some(value) = value.get("sandbox") else {
            return Ok(None);
        };
        if value == &serde_json::Value::Bool(false) {
            return Ok(None);
        }
        if let Some(recovery) = value.get("recovery") {
            serde_json::from_value::<Recovery>(recovery.clone()).map_err(|_| {
                "sandbox recovery must be {\"mode\":\"checkpoint\"} or {\"mode\":\"fresh\"}"
                    .to_string()
            })?;
        }
        let options: Self = serde_json::from_value(value.clone())
            .map_err(|_| "sandbox requires channel_id and canonical thread_root".to_string())?;
        if uuid::Uuid::parse_str(&options.channel_id)
            .map(|id| id.to_string())
            .ok()
            .as_deref()
            != Some(&options.channel_id)
        {
            return Err("sandbox channel_id must be a canonical UUID".into());
        }
        if options.thread_root.len() != 64
            || !options
                .thread_root
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err("sandbox thread_root must be a lowercase 64-character event ID".into());
        }
        Ok(Some(options))
    }
}

fn api(client: Client, namespace: &str) -> Api<DynamicObject> {
    Api::namespaced_with(client, namespace, &sandbox_resource())
}

fn reason_is(error: &kube::Error, reason: &str) -> bool {
    matches!(error, kube::Error::Api(e) if e.reason == reason)
}

/// Session-specific inputs to [`build`] beyond the launch contract.
struct NewSession<'a> {
    generation: &'a str,
    checkpointing: bool,
    restore: Option<&'a str>,
    recovered_from: Option<&'a str>,
}

fn build(
    identity: &naming::AgentIdentity,
    cfg: &ProviderConfig,
    scope: &Options,
    owner: &str,
    session: &NewSession<'_>,
    fingerprint: &Fingerprint,
) -> Result<DynamicObject, String> {
    let generation = session.generation;
    let name = scope.name(identity);
    let mut child = pod::build_pod(identity, cfg, generation, fingerprint);
    child.metadata.name = Some(name.clone());
    // Older bare-Pod providers must refuse these controller-owned resources.
    child.labels_mut().insert(
        naming::LABEL_BINDING_VERSION.into(),
        SANDBOX_BINDING_VERSION.into(),
    );
    let annotations = child.metadata.annotations.get_or_insert_with(BTreeMap::new);
    annotations.extend([
        (OWNER.into(), owner.into()),
        (CHANNEL.into(), scope.channel_id.clone()),
        (THREAD.into(), scope.thread_root.clone()),
        (SESSION.into(), generation.into()),
        (GENERATION.into(), generation.into()),
        (
            CHECKPOINTING.into(),
            if session.checkpointing {
                CHECKPOINTING_ENABLED
            } else {
                CHECKPOINTING_DISABLED
            }
            .into(),
        ),
    ]);
    if let Some(key) = session.restore {
        annotations.insert(RESTORE_CHECKPOINT.into(), key.into());
    }
    if let Some(previous) = session.recovered_from {
        annotations.insert(RECOVERED_FROM.into(), previous.into());
    }
    let spec = child.spec.as_mut().ok_or("agent Pod has no spec")?;
    // SIGTERM must leave time to drain sessions and run the harness's bounded
    // (120s) workspace checkpoint before the kubelet kills the container.
    spec.termination_grace_period_seconds = Some(240);
    // The harness writes its checkpoint receipt here; the manager reads it
    // back from the container status (and cross-checks it against S3).
    spec.containers[0].termination_message_policy = Some("File".into());
    spec.containers[0]
        .env
        .get_or_insert_with(Vec::new)
        .push(EnvVar {
            name: "BUZZ_SANDBOX_POD_UID".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".into()),
                    field_path: "metadata.uid".into(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
    let mut sandbox = DynamicObject::new(&name, &sandbox_resource());
    sandbox.metadata = child.metadata.clone();
    sandbox.data = json!({"spec": {
        "operatingMode": "Running",
        "service": false,
        "podTemplate": {
            "metadata": {"labels": child.metadata.labels, "annotations": child.metadata.annotations},
            "spec": child.spec,
        },
    }});
    Ok(sandbox)
}

/// The Sandbox belongs to this agent, owner, and thread. Recovery cannot fix a
/// mismatch here, so the error does not offer it.
fn verify_identity(
    sandbox: &DynamicObject,
    identity: &naming::AgentIdentity,
    scope: &Options,
    owner: &str,
) -> Result<(), String> {
    let labels = sandbox.labels();
    let marked = labels.get(naming::LABEL_MANAGED_BY).map(String::as_str)
        == Some(naming::MANAGED_BY)
        && labels
            .get(naming::LABEL_BINDING_VERSION)
            .map(String::as_str)
            == Some(SANDBOX_BINDING_VERSION);
    let matches = [
        (naming::ANNOTATION_PUBKEY_FULL, identity.pubkey_hex()),
        (OWNER, owner),
        (CHANNEL, scope.channel_id.as_str()),
        (THREAD, scope.thread_root.as_str()),
    ]
    .into_iter()
    .all(|(key, expected)| annotation(sandbox, key) == Some(expected));
    if !marked || !matches {
        return Err("Sandbox belongs to a different agent, owner, or thread".into());
    }
    Ok(())
}

/// Launch fingerprints a live Sandbox may carry and still be reused.
struct Fingerprints {
    /// Every key this version writes, including [`RESTORE_KEY_ENV`].
    current: Fingerprint,
    /// The same launch from a provider that predates [`RESTORE_KEY_ENV`].
    /// Accepted only on reuse, so Sandboxes created before this version keep
    /// working while desktop versions are mixed; never written.
    legacy: Fingerprint,
}

impl Fingerprints {
    fn new(cfg: &ProviderConfig, env: &BTreeMap<String, String>) -> Self {
        let keys = || env.keys().cloned();
        Self {
            current: pod::intent_template(cfg, keys()).fingerprint(),
            legacy: pod::intent_template(cfg, keys().filter(|key| key != RESTORE_KEY_ENV))
                .fingerprint(),
        }
    }

    fn accepts(&self, sandbox: &DynamicObject) -> bool {
        let recorded = annotation(sandbox, naming::ANNOTATION_CREATE_INTENT);
        recorded == Some(self.current.as_str()) || recorded == Some(self.legacy.as_str())
    }
}

/// The Sandbox is a live session this deploy may reuse. Every refusal names the
/// lifecycle state so the desktop can offer the right explicit action.
fn verify_live(sandbox: &DynamicObject, fingerprints: &Fingerprints) -> Result<(), String> {
    if sandbox.metadata.deletion_timestamp.is_some() {
        return Err("Sandbox is being deleted; retry once it is gone".into());
    }
    match Lifecycle::of(sandbox) {
        Lifecycle::Ended => {
            let reason = annotation(sandbox, ENDED_REASON).unwrap_or("ended");
            return Err(format!(
                "Sandbox session ended ({reason}); explicit recovery is required"
            ));
        }
        Lifecycle::Ending => {
            return Err("Sandbox session is ending; recover it explicitly once it has ended".into())
        }
        Lifecycle::Unrecognized => {
            return Err("Sandbox has a lifecycle this provider does not know; update Buzz".into())
        }
        Lifecycle::Live => {}
    }
    if OperatingMode::of(sandbox) != OperatingMode::Running {
        return Err(
            "Sandbox is not an active managed session; explicit recovery is required".into(),
        );
    }
    if !fingerprints.accepts(sandbox) {
        return Err("Sandbox launch configuration differs; end the session, then explicit recovery is required".into());
    }
    Ok(())
}

fn configure_setup(
    cfg: &mut ProviderConfig,
    identity: &naming::AgentIdentity,
    env: &mut BTreeMap<String, String>,
) -> Result<(), String> {
    let Some(mode) = env.get("BUZZ_SETUP_MODE") else {
        return Ok(());
    };
    if mode != "codex-device-v1"
        || env.get("BUZZ_ACP_AGENT_COMMAND").map(String::as_str) != Some("codex-acp")
        || env.get("BUZZ_ACP_PERMISSION_MODE").map(String::as_str) != Some("agent-full-access")
    {
        return Err(
            "Sandbox account setup requires codex-device-v1, codex-acp, and agent-full-access"
                .into(),
        );
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "could not establish account setup time")?
        .as_secs();
    let (_, digest) = cfg
        .image
        .as_str()
        .rsplit_once('@')
        .ok_or("image digest is missing")?;
    // The image acknowledges this exact Pod/agent/image generation. The Pod UID
    // comes from the downward API, overriding any caller-supplied environment.
    cfg.pod_options.setup_pending = true;
    env.remove("BUZZ_SETUP_POD_UID");
    env.extend([
        (
            "BUZZ_SETUP_AGENT_PUBKEY".into(),
            identity.pubkey_hex().into(),
        ),
        ("BUZZ_SETUP_IMAGE_DIGEST".into(), digest.into()),
        (
            "BUZZ_SETUP_NONCE".into(),
            uuid::Uuid::new_v4().simple().to_string(),
        ),
        ("BUZZ_SETUP_DEADLINE".into(), (now + 900).to_string()),
        ("CODEX_HOME".into(), "/home/agent/.codex".into()),
        (
            "CODEX_CONFIG".into(),
            r#"{"cli_auth_credentials_store":"file"}"#.into(),
        ),
        ("NO_BROWSER".into(), "1".into()),
    ]);
    Ok(())
}

/// Per-deploy handles and invariants.
struct Session<'a> {
    sandboxes: Api<DynamicObject>,
    pods: Api<Pod>,
    secrets: Api<Secret>,
    name: String,
    identity: &'a naming::AgentIdentity,
    scope: &'a Options,
    owner: &'a str,
    fingerprints: Fingerprints,
}

impl<'a> Session<'a> {
    fn new(
        client: Client,
        namespace: &str,
        identity: &'a naming::AgentIdentity,
        scope: &'a Options,
        owner: &'a str,
        fingerprints: Fingerprints,
    ) -> Self {
        Self {
            sandboxes: api(client.clone(), namespace),
            pods: Api::namespaced(client.clone(), namespace),
            secrets: Api::namespaced(client, namespace),
            name: scope.name(identity),
            identity,
            scope,
            owner,
            fingerprints,
        }
    }
}

/// Reuses a running, identically scoped Sandbox. Ended, replaced, or lost
/// sessions are never redeployed implicitly: only `scope.recovery` replaces a
/// tombstone. Namespace creation remains Terraform's concern.
pub async fn deploy(
    client: Client,
    identity: &naming::AgentIdentity,
    cfg: &ProviderConfig,
    scope: &Options,
    owner: &str,
    mut env: BTreeMap<String, String>,
) -> Result<String, String> {
    let mut effective_cfg = cfg.clone();
    configure_setup(&mut effective_cfg, identity, &mut env)?;
    let cfg = &effective_cfg;
    if cfg.pod_options.active_deadline_seconds.is_none() {
        return Err("Sandbox requires pod_options.active_deadline_seconds".into());
    }
    let checkpointing = apply_session_env(scope, &mut env);
    let fingerprints = Fingerprints::new(cfg, &env);
    let session = Session::new(client, &cfg.namespace, identity, scope, owner, fingerprints);
    let existing = session
        .sandboxes
        .get_opt(&session.name)
        .await
        .map_err(|e| format!("read Sandbox: {e}"))?;
    let sandbox = match (existing, scope.recovery) {
        (Some(tombstone), Some(recovery)) => {
            recover(&session, cfg, tombstone, recovery, checkpointing).await?
        }
        (Some(existing), None) => existing,
        (
            None,
            Some(Recovery {
                mode: RecoveryMode::Checkpoint,
            }),
        ) => {
            return Err(
                "no ended session exists for this thread to recover; start fresh instead".into(),
            )
        }
        (None, _) => create_new(&session, cfg, checkpointing).await?,
    };
    verify_identity(&sandbox, identity, scope, owner)?;
    verify_live(&sandbox, &session.fingerprints)?;
    bind(&session, cfg, sandbox, env).await
}

/// Thread-session env shared by every deploy. Returns whether
/// checkpoint-on-stop is configured.
fn apply_session_env(scope: &Options, env: &mut BTreeMap<String, String>) -> bool {
    env.insert("BUZZ_ACP_ACTIVITY_LOG".into(), "true".into());
    env.insert("BUZZ_ACP_RESPOND_TO".into(), "owner".into());
    env.insert("BUZZ_ACP_ALLOWED_RESPOND_TO".into(), "owner".into());
    env.insert(
        "BUZZ_ACP_BOUND_SESSION".into(),
        json!({"channel_id":scope.channel_id,"thread_root":scope.thread_root}).to_string(),
    );
    env.insert("BUZZ_ACP_SESSION_POLICY".into(), "thread".into());
    env.insert("BUZZ_ACP_SUBSCRIBE".into(), "mentions".into());
    env.insert("BUZZ_ACP_CHANNELS".into(), scope.channel_id.clone());
    env.insert("BUZZ_ACP_HEARTBEAT_INTERVAL".into(), "0".into());
    let checkpointing =
        env.contains_key("BUZZ_CHECKPOINT_BUCKET") && env.contains_key("BUZZ_CHECKPOINT_PREFIX");
    if checkpointing {
        env.insert("BUZZ_ACP_CHECKPOINT_ON_STOP".into(), "true".into());
    }
    // Include the binding keys in the fingerprint without hashing their
    // values; both are filled in from the Sandbox when the Secret is written.
    env.insert("BUZZ_SANDBOX_INITIAL_POD_UID".into(), String::new());
    env.insert(RESTORE_KEY_ENV.into(), String::new());
    checkpointing
}

async fn create_new(
    session: &Session<'_>,
    cfg: &ProviderConfig,
    checkpointing: bool,
) -> Result<DynamicObject, String> {
    let identity = session.identity;
    if session
        .pods
        .get_opt(&session.name)
        .await
        .map_err(|e| format!("check Pod collision: {e}"))?
        .is_some()
        || !session
            .pods
            .list(&ListParams::default().labels(&identity.selector()))
            .await
            .map_err(|e| format!("check existing agent consumers: {e}"))?
            .items
            .iter()
            .all(|pod| {
                pod.labels()
                    .get(naming::LABEL_BINDING_VERSION)
                    .map(String::as_str)
                    == Some(SANDBOX_BINDING_VERSION)
                    && pod.annotations().get(OWNER).map(String::as_str) == Some(session.owner)
                    && pod.owner_references().iter().any(|r| {
                        r.api_version == "agents.x-k8s.io/v1beta1"
                            && r.kind == "Sandbox"
                            && r.controller == Some(true)
                    })
            })
    {
        return Err(
            "agent already has a Pod; stop and recover it explicitly before using Sandbox".into(),
        );
    }
    let generation = uuid::Uuid::new_v4().simple().to_string();
    let new = NewSession {
        generation: &generation,
        checkpointing,
        restore: None,
        recovered_from: None,
    };
    let desired = build(
        identity,
        cfg,
        session.scope,
        session.owner,
        &new,
        &session.fingerprints.current,
    )?;
    match session
        .sandboxes
        .create(&PostParams::default(), &desired)
        .await
    {
        Ok(created) => Ok(created),
        Err(e) if reason_is(&e, "AlreadyExists") => session
            .sandboxes
            .get(&session.name)
            .await
            .map_err(|e| format!("read concurrent Sandbox: {e}")),
        Err(e) => Err(format!(
            "create Sandbox (install the Agent Sandbox v1beta1 CRD/controller first): {e}"
        )),
    }
}

/// Replace an Ended tombstone with a new session (new generation). Refused on
/// anything that is not a tombstone, so a live workspace is never discarded.
async fn recover(
    session: &Session<'_>,
    cfg: &ProviderConfig,
    tombstone: DynamicObject,
    recovery: Recovery,
    checkpointing: bool,
) -> Result<DynamicObject, String> {
    verify_identity(&tombstone, session.identity, session.scope, session.owner)?;
    if Lifecycle::of(&tombstone) != Lifecycle::Ended
        || OperatingMode::of(&tombstone) != OperatingMode::Suspended
        || tombstone.metadata.deletion_timestamp.is_some()
    {
        return Err(
            "recovery requires an ended session; this Sandbox is still active, so end it first"
                .into(),
        );
    }
    let previous = annotation(&tombstone, GENERATION)
        .ok_or("ended Sandbox has no generation")?
        .to_string();
    let restore = match recovery.mode {
        RecoveryMode::Fresh => None,
        RecoveryMode::Checkpoint => Some(
            annotation(&tombstone, CHECKPOINT)
                .filter(|key| is_checkpoint_key(key))
                .ok_or("this session has no verified checkpoint; start fresh instead")?
                .to_string(),
        ),
    };
    let uid = tombstone.metadata.uid.clone();
    let preconditions = Preconditions {
        uid: uid.clone(),
        resource_version: tombstone.metadata.resource_version.clone(),
    };
    match session
        .sandboxes
        .delete(
            &session.name,
            &DeleteParams {
                preconditions: Some(preconditions),
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => {}
        Err(e) if reason_is(&e, "NotFound") => {}
        Err(e) if reason_is(&e, "Conflict") => {
            return Err("Sandbox changed during recovery; retry".into())
        }
        Err(e) => return Err(format!("delete ended Sandbox: {e}")),
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        match session
            .sandboxes
            .get_opt(&session.name)
            .await
            .map_err(|e| format!("observe ended Sandbox deletion: {e}"))?
        {
            Some(current) if current.metadata.uid == uid => {
                if tokio::time::Instant::now() >= deadline {
                    return Err("ended Sandbox is still being deleted; retry recovery".into());
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            // A concurrent recovery of the same tombstone already won.
            Some(current) => return recovered_by_peer(current, &previous),
            None => break,
        }
    }
    let generation = uuid::Uuid::new_v4().simple().to_string();
    let new = NewSession {
        generation: &generation,
        checkpointing,
        restore: restore.as_deref(),
        recovered_from: Some(&previous),
    };
    let desired = build(
        session.identity,
        cfg,
        session.scope,
        session.owner,
        &new,
        &session.fingerprints.current,
    )?;
    match session
        .sandboxes
        .create(&PostParams::default(), &desired)
        .await
    {
        Ok(created) => Ok(created),
        Err(e) if reason_is(&e, "AlreadyExists") => {
            let current = session
                .sandboxes
                .get(&session.name)
                .await
                .map_err(|e| format!("read concurrent Sandbox: {e}"))?;
            recovered_by_peer(current, &previous)
        }
        Err(e) => Err(format!("create recovered Sandbox: {e}")),
    }
}

fn recovered_by_peer(current: DynamicObject, previous: &str) -> Result<DynamicObject, String> {
    if annotation(&current, RECOVERED_FROM) == Some(previous) {
        Ok(current)
    } else {
        Err("another session replaced the ended Sandbox during recovery".into())
    }
}

fn container_started(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .and_then(|containers| containers.iter().find(|c| c.name == "agent"))
        .is_some_and(|c| {
            c.restart_count > 0
                || c.last_state
                    .as_ref()
                    .is_some_and(|s| s.terminated.is_some() || s.running.is_some())
                || c.state
                    .as_ref()
                    .is_some_and(|s| s.terminated.is_some() || s.running.is_some())
        })
}

/// Bind credentials to the Sandbox's first Pod and wait for it to start.
///
/// Resumable: a session bound to its first Pod whose Secret is missing and
/// whose container never started is completed by whichever deploy observes
/// it, with the Sandbox's own generation and an ownerRef to that Pod.
async fn bind(
    session: &Session<'_>,
    cfg: &ProviderConfig,
    mut sandbox: DynamicObject,
    env: BTreeMap<String, String>,
) -> Result<String, String> {
    let identity = session.identity;
    let name = &session.name;
    let generation = annotation(&sandbox, GENERATION)
        .ok_or("Sandbox generation is missing")?
        .to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    // Set only after this call bound INITIAL_POD, so a failed Secret write
    // can hand the binding back.
    let mut bound_here = false;
    loop {
        let expired = tokio::time::Instant::now() >= deadline;
        let Some(child) = session
            .pods
            .get_opt(name)
            .await
            .map_err(|e| format!("read Sandbox Pod: {e}"))?
        else {
            if annotation(&sandbox, INITIAL_POD).is_some() {
                return Err("Sandbox lost its original Pod; explicit recovery is required".into());
            }
            if expired {
                return Err(
                    "Sandbox controller has not created its Pod; inspect the Sandbox and retry"
                        .into(),
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        if !owned_pod(&child, &sandbox) {
            return Err("Pod is not owned by this Sandbox".into());
        }
        let uid = child.metadata.uid.as_deref().ok_or("Pod UID is missing")?;
        if child.metadata.deletion_timestamp.is_some()
            || child
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .is_some_and(|p| matches!(p, "Failed" | "Succeeded"))
        {
            return Err("Sandbox session ended; explicit recovery is required".into());
        }
        let Some(initial_uid) = annotation(&sandbox, INITIAL_POD) else {
            // resourceVersion makes this an atomic first-Pod binding. A
            // concurrent deploy must retry and observe the winner's binding.
            sandbox
                .annotations_mut()
                .insert(INITIAL_POD.into(), uid.into());
            match session
                .sandboxes
                .replace(name, &PostParams::default(), &sandbox)
                .await
            {
                Ok(bound) => {
                    sandbox = bound;
                    bound_here = true;
                }
                Err(e) if reason_is(&e, "Conflict") => {
                    sandbox = refresh(session, &generation).await?;
                    if expired {
                        return Err(
                            "Sandbox kept changing during credential binding; retry deploy".into(),
                        );
                    }
                }
                Err(e) => return Err(format!("bind Sandbox to its first Pod: {e}")),
            }
            continue;
        };
        if uid != initial_uid {
            return Err("Sandbox Pod was replaced; credentials remain bound to the original session, explicit recovery is required".into());
        }
        let secret_name = identity.secret_name(&generation);
        let secret = session
            .secrets
            .get_opt(&secret_name)
            .await
            .map_err(|_| "could not check Sandbox launch Secret".to_string())?;
        if let Some(secret) = secret {
            check_secret(&secret, identity, uid, &env)?;
            return wait_for_start(&session.pods, &sandbox, name, uid).await;
        }
        if container_started(&child) {
            return Err("Sandbox credentials are missing from a started session; explicit recovery is required".into());
        }
        if expired {
            return Err("Sandbox credential binding is incomplete; retry deploy".into());
        }
        let secret = bound_secret(identity, cfg, &sandbox, &generation, name, uid, env.clone());
        match session
            .secrets
            .create(&PostParams::default(), &secret)
            .await
        {
            // Either way, the next pass re-reads and verifies what was stored.
            Ok(_) => {}
            Err(e) if reason_is(&e, "AlreadyExists") => {}
            Err(e) => {
                if bound_here {
                    unbind(session, &sandbox).await;
                }
                // Only the apiserver's reason: the message could echo the
                // request, which carries credentials.
                let reason = match &e {
                    kube::Error::Api(status) => status.reason.as_str(),
                    _ => "transport error",
                };
                return Err(format!(
                    "could not create bound Sandbox credentials ({reason}); retry deploy"
                ));
            }
        }
    }
}

/// Re-read a Sandbox after a lost CAS and re-check what the binding relies on.
async fn refresh(session: &Session<'_>, generation: &str) -> Result<DynamicObject, String> {
    let sandbox = session
        .sandboxes
        .get(&session.name)
        .await
        .map_err(|e| format!("refresh Sandbox binding: {e}"))?;
    verify_identity(&sandbox, session.identity, session.scope, session.owner)?;
    verify_live(&sandbox, &session.fingerprints)?;
    if annotation(&sandbox, GENERATION) != Some(generation) {
        return Err("Sandbox generation changed; explicit recovery is required".into());
    }
    Ok(sandbox)
}

/// Best-effort CAS removal of the first-Pod binding this deploy wrote, after
/// its Secret could not be created. A failure is harmless: the next deploy
/// resumes the binding instead.
async fn unbind(session: &Session<'_>, sandbox: &DynamicObject) {
    let mut unbound = sandbox.clone();
    unbound.annotations_mut().remove(INITIAL_POD);
    let _ = session
        .sandboxes
        .replace(&session.name, &PostParams::default(), &unbound)
        .await;
}

fn bound_secret(
    identity: &naming::AgentIdentity,
    cfg: &ProviderConfig,
    sandbox: &DynamicObject,
    generation: &str,
    name: &str,
    uid: &str,
    mut env: BTreeMap<String, String>,
) -> Secret {
    env.insert("BUZZ_MANAGED_AGENT_START_NONCE".into(), generation.into());
    env.insert("BUZZ_SANDBOX_INITIAL_POD_UID".into(), uid.into());
    // The restore key comes from the Sandbox, never from the request, so a
    // resumed binding keeps the recovery its Sandbox was created with.
    env.insert(
        RESTORE_KEY_ENV.into(),
        annotation(sandbox, RESTORE_CHECKPOINT)
            .unwrap_or_default()
            .into(),
    );
    let mut secret = pod::build_secret(identity, &cfg.namespace, generation, env);
    secret.labels_mut().insert(
        naming::LABEL_BINDING_VERSION.into(),
        SANDBOX_BINDING_VERSION.into(),
    );
    secret.metadata.owner_references = Some(vec![OwnerReference {
        api_version: "v1".into(),
        kind: "Pod".into(),
        name: name.into(),
        uid: uid.into(),
        controller: Some(true),
        block_owner_deletion: Some(false),
    }]);
    secret
}

fn check_secret(
    secret: &Secret,
    identity: &naming::AgentIdentity,
    uid: &str,
    env: &BTreeMap<String, String>,
) -> Result<(), String> {
    if secret.immutable != Some(true)
        || !secret
            .owner_references()
            .iter()
            .any(|owner| owner.uid == uid && owner.kind == "Pod")
        || secret
            .annotations()
            .get(naming::ANNOTATION_PUBKEY_FULL)
            .map(String::as_str)
            != Some(identity.pubkey_hex())
    {
        return Err("Sandbox credentials belong to a different Pod or agent".into());
    }
    let saved_relay = secret
        .data
        .as_ref()
        .and_then(|data| data.get("BUZZ_RELAY_URL"))
        .map(|value| value.0.as_slice());
    if saved_relay != env.get("BUZZ_RELAY_URL").map(|value| value.as_bytes()) {
        return Err(
            "Sandbox belongs to a different relay; use a new thread or explicit recovery".into(),
        );
    }
    Ok(())
}

async fn wait_for_start(
    pods: &Api<Pod>,
    sandbox: &DynamicObject,
    name: &str,
    uid: &str,
) -> Result<String, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let child = pods
            .get_opt(name)
            .await
            .map_err(|e| format!("observe Sandbox startup: {e}"))?
            .ok_or("Sandbox lost its Pod during startup; explicit recovery is required")?;
        if child.metadata.uid.as_deref() != Some(uid)
            || !owned_pod(&child, sandbox)
            || child.metadata.deletion_timestamp.is_some()
        {
            return Err("Sandbox Pod changed during startup; explicit recovery is required".into());
        }
        let state = child
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .and_then(|containers| containers.iter().find(|c| c.name == "agent"))
            .and_then(|c| c.state.as_ref());
        if state.is_some_and(|s| s.terminated.is_some()) {
            return Err("Sandbox process ended during startup; inspect its retained logs".into());
        }
        if state.is_some_and(|s| s.running.is_some()) {
            return Ok(name.into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(
                "Sandbox startup timed out; inspect scheduling and image status before retrying"
                    .into(),
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// What `stop` left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopState {
    /// Suspended and draining; the manager writes the tombstone.
    Ending,
    /// Already a tombstone.
    Ended,
    /// No Sandbox exists for this thread.
    Absent,
}

impl StopState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ending => "ending",
            Self::Ended => "ended",
            Self::Absent => "absent",
        }
    }
}

/// End a (possibly wedged) session: one CAS merge patch sets
/// `lifecycle=ending`, reason `stopped`, and `operatingMode: Suspended`. The
/// controller then deletes the Pod (SIGTERM, so the harness still gets its
/// checkpoint window). The normal stop remains `!shutdown`.
pub async fn stop(
    client: Client,
    identity: &naming::AgentIdentity,
    namespace: &str,
    scope: &Options,
    owner: &str,
) -> Result<(String, StopState), String> {
    if scope.recovery.is_some() {
        return Err("stop does not accept a recovery mode".into());
    }
    let sandboxes = api(client, namespace);
    let name = scope.name(identity);
    for _ in 0..5 {
        let Some(sandbox) = sandboxes
            .get_opt(&name)
            .await
            .map_err(|e| format!("read Sandbox: {e}"))?
        else {
            return Ok((name, StopState::Absent));
        };
        verify_identity(&sandbox, identity, scope, owner)?;
        match Lifecycle::of(&sandbox) {
            Lifecycle::Ended => return Ok((name, StopState::Ended)),
            Lifecycle::Ending => return Ok((name, StopState::Ending)),
            Lifecycle::Unrecognized => {
                return Err(
                    "Sandbox has a lifecycle this provider does not know; update Buzz".into(),
                )
            }
            Lifecycle::Live => {}
        }
        let patch = json!({
            "metadata": {
                "resourceVersion": sandbox.metadata.resource_version,
                "annotations": {
                    (LIFECYCLE): LIFECYCLE_ENDING,
                    (ENDED_REASON): EndedReason::Stopped.as_str(),
                },
            },
            "spec": {"operatingMode": "Suspended"},
        });
        match sandboxes
            .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => return Ok((name, StopState::Ending)),
            Err(e) if reason_is(&e, "Conflict") => continue,
            Err(e) => return Err(format!("stop Sandbox: {e}")),
        }
    }
    Err("Sandbox kept changing while stopping; retry".into())
}

#[cfg(test)]
#[path = "sandbox_tests.rs"]
mod tests;
