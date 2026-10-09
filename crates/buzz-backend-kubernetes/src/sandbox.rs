//! Agent Sandbox integration. The upstream controller owns the Pod; this
//! provider owns its launch contract and binds credentials to the first Pod UID.

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, ObjectFieldSelector, Pod, Secret};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, ListParams, PostParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use kube::{Client, ResourceExt};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{config::ProviderConfig, intent::Fingerprint, naming, pod};

const INITIAL_POD: &str = "buzz.block.xyz/initial-pod-uid";
const GENERATION: &str = "buzz.block.xyz/generation";
const OWNER: &str = "buzz.block.xyz/owner-pubkey";
const CHANNEL: &str = "buzz.block.xyz/channel-id";
const THREAD: &str = "buzz.block.xyz/thread-root";
const SESSION: &str = "buzz.block.xyz/session-id";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    pub channel_id: String,
    pub thread_root: String,
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

fn resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk(
        "agents.x-k8s.io",
        "v1beta1",
        "Sandbox",
    ))
}

fn api(client: Client, namespace: &str) -> Api<DynamicObject> {
    Api::namespaced_with(client, namespace, &resource())
}

fn annotation<'a>(object: &'a DynamicObject, key: &str) -> Option<&'a str> {
    object
        .metadata
        .annotations
        .as_ref()?
        .get(key)
        .map(String::as_str)
}

fn build(
    identity: &naming::AgentIdentity,
    cfg: &ProviderConfig,
    scope: &Options,
    owner: &str,
    generation: &str,
    fingerprint: &Fingerprint,
) -> Result<DynamicObject, String> {
    let name = scope.name(identity);
    let mut child = pod::build_pod(identity, cfg, generation, fingerprint);
    child.metadata.name = Some(name.clone());
    // Older bare-Pod providers must refuse these controller-owned resources.
    child
        .labels_mut()
        .insert(naming::LABEL_BINDING_VERSION.into(), "2".into());
    let annotations = child.metadata.annotations.get_or_insert_with(BTreeMap::new);
    annotations.extend([
        (OWNER.into(), owner.into()),
        (CHANNEL.into(), scope.channel_id.clone()),
        (THREAD.into(), scope.thread_root.clone()),
        (SESSION.into(), generation.into()),
        (GENERATION.into(), generation.into()),
    ]);
    let spec = child.spec.as_mut().ok_or("agent Pod has no spec")?;
    // SIGTERM must leave time to drain sessions and run the harness's bounded
    // (120s) workspace checkpoint before the kubelet kills the container.
    spec.termination_grace_period_seconds = Some(240);
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
    let mut sandbox = DynamicObject::new(&name, &resource());
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

fn verify(
    sandbox: &DynamicObject,
    identity: &naming::AgentIdentity,
    scope: &Options,
    owner: &str,
    fingerprint: &Fingerprint,
) -> Result<(), String> {
    for (key, expected) in [
        (naming::ANNOTATION_PUBKEY_FULL, identity.pubkey_hex()),
        (naming::ANNOTATION_CREATE_INTENT, fingerprint.as_str()),
        (OWNER, owner),
        (CHANNEL, scope.channel_id.as_str()),
        (THREAD, scope.thread_root.as_str()),
    ] {
        if annotation(sandbox, key) != Some(expected) {
            return Err("Sandbox identity, thread, or launch configuration differs; explicit recovery is required".into());
        }
    }
    if sandbox
        .labels()
        .get(naming::LABEL_MANAGED_BY)
        .map(String::as_str)
        != Some(naming::MANAGED_BY)
        || sandbox.metadata.deletion_timestamp.is_some()
        || sandbox.data["spec"]["operatingMode"] != "Running"
    {
        return Err(
            "Sandbox is not an active managed session; explicit recovery is required".into(),
        );
    }
    Ok(())
}

fn owned_pod(pod: &Pod, sandbox: &DynamicObject) -> bool {
    sandbox.metadata.uid.as_ref().is_some_and(|uid| {
        pod.owner_references().iter().any(|r| {
            &r.uid == uid
                && r.kind == "Sandbox"
                && r.api_version == "agents.x-k8s.io/v1beta1"
                && r.controller == Some(true)
        })
    })
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

/// Reuses a running, identically scoped Sandbox. Terminal or replaced Pods are
/// never redeployed implicitly. Namespace creation remains Terraform's concern.
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
    if env.contains_key("BUZZ_CHECKPOINT_BUCKET") && env.contains_key("BUZZ_CHECKPOINT_PREFIX") {
        env.insert("BUZZ_ACP_CHECKPOINT_ON_STOP".into(), "true".into());
    }
    // Include the UID binding key in the fingerprint without hashing its value.
    env.insert("BUZZ_SANDBOX_INITIAL_POD_UID".into(), String::new());
    let fingerprint = pod::intent_template(cfg, env.keys().cloned()).fingerprint();
    let sandboxes = api(client.clone(), &cfg.namespace);
    let pods: Api<Pod> = Api::namespaced(client.clone(), &cfg.namespace);
    let secrets: Api<Secret> = Api::namespaced(client, &cfg.namespace);
    let name = scope.name(identity);
    let mut sandbox = match sandboxes
        .get_opt(&name)
        .await
        .map_err(|e| format!("read Sandbox: {e}"))?
    {
        Some(existing) => existing,
        None => {
            if pods
                .get_opt(&name)
                .await
                .map_err(|e| format!("check Pod collision: {e}"))?
                .is_some()
                || !pods
                    .list(&ListParams::default().labels(&identity.selector()))
                    .await
                    .map_err(|e| format!("check existing agent consumers: {e}"))?
                    .items
                    .iter()
                    .all(|pod| {
                        pod.labels()
                            .get(naming::LABEL_BINDING_VERSION)
                            .map(String::as_str)
                            == Some("2")
                            && pod.annotations().get(OWNER).map(String::as_str) == Some(owner)
                            && pod.owner_references().iter().any(|r| {
                                r.api_version == "agents.x-k8s.io/v1beta1"
                                    && r.kind == "Sandbox"
                                    && r.controller == Some(true)
                            })
                    })
            {
                return Err(
                    "agent already has a Pod; stop and recover it explicitly before using Sandbox"
                        .into(),
                );
            }
            let generation = uuid::Uuid::new_v4().simple().to_string();
            let desired = build(identity, cfg, scope, owner, &generation, &fingerprint)?;
            match sandboxes.create(&PostParams::default(), &desired).await {
                Ok(created) => created,
                Err(kube::Error::Api(e)) if e.reason == "AlreadyExists" => sandboxes
                    .get(&name)
                    .await
                    .map_err(|e| format!("read concurrent Sandbox: {e}"))?,
                Err(e) => {
                    return Err(format!(
                    "create Sandbox (install the Agent Sandbox v1beta1 CRD/controller first): {e}"
                ))
                }
            }
        }
    };
    verify(&sandbox, identity, scope, owner, &fingerprint)?;
    let generation = annotation(&sandbox, GENERATION)
        .ok_or("Sandbox generation is missing")?
        .to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(child) = pods
            .get_opt(&name)
            .await
            .map_err(|e| format!("read Sandbox Pod: {e}"))?
        {
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
                return Err(
                    "Sandbox session ended; save or recover its workspace explicitly".into(),
                );
            }
            if let Some(initial_uid) = annotation(&sandbox, INITIAL_POD) {
                if uid != initial_uid {
                    return Err("Sandbox Pod was replaced; credentials remain bound to the original session, explicit recovery is required".into());
                }
                let secret = secrets
                    .get_opt(&identity.secret_name(&generation))
                    .await
                    .map_err(|_| "could not check Sandbox launch Secret".to_string())?;
                let Some(secret) = secret else {
                    if tokio::time::Instant::now() >= deadline {
                        return Err("Sandbox credential binding is incomplete; explicit recovery is required".into());
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                };
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
                    return Err("Sandbox belongs to a different relay; use a new thread or explicit recovery".into());
                }
            } else {
                // resourceVersion makes this an atomic first-Pod binding. A
                // concurrent deploy must retry and observe the winner's binding.
                sandbox
                    .annotations_mut()
                    .insert(INITIAL_POD.into(), uid.into());
                sandbox = match sandboxes
                    .replace(&name, &PostParams::default(), &sandbox)
                    .await
                {
                    Ok(bound) => bound,
                    Err(kube::Error::Api(error)) if error.reason == "Conflict" => {
                        sandbox = sandboxes
                            .get(&name)
                            .await
                            .map_err(|e| format!("refresh Sandbox binding: {e}"))?;
                        verify(&sandbox, identity, scope, owner, &fingerprint)?;
                        if annotation(&sandbox, GENERATION) != Some(&generation) {
                            return Err(
                                "Sandbox generation changed; explicit recovery is required".into(),
                            );
                        }
                        if tokio::time::Instant::now() >= deadline {
                            return Err(
                                "Sandbox kept changing during credential binding; retry deploy"
                                    .into(),
                            );
                        }
                        continue;
                    }
                    Err(error) => return Err(format!("bind Sandbox to its first Pod: {error}")),
                };
                env.insert("BUZZ_MANAGED_AGENT_START_NONCE".into(), generation.clone());
                env.insert("BUZZ_SANDBOX_INITIAL_POD_UID".into(), uid.into());
                let mut secret = pod::build_secret(identity, &cfg.namespace, &generation, env);
                secret
                    .labels_mut()
                    .insert(naming::LABEL_BINDING_VERSION.into(), "2".into());
                secret.metadata.owner_references = Some(vec![OwnerReference {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    name: name.clone(),
                    uid: uid.into(),
                    controller: Some(true),
                    block_owner_deletion: Some(false),
                }]);
                secrets
                    .create(&PostParams::default(), &secret)
                    .await
                    .map_err(|_| {
                        "could not create bound Sandbox credentials; explicit recovery is required"
                            .to_string()
                    })?;
            }
            return wait_for_start(&pods, &sandbox, &name, uid).await;
        }
        if annotation(&sandbox, INITIAL_POD).is_some() {
            return Err("Sandbox lost its original Pod; explicit recovery is required".into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(
                "Sandbox controller has not created its Pod; inspect the Sandbox and retry".into(),
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
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

#[cfg(test)]
#[path = "sandbox_tests.rs"]
mod tests;
