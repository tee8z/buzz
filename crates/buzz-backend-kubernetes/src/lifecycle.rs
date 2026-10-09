//! Agent Sandbox session lifecycle, shared by the desktop provider and the
//! in-cluster `buzz-agent-manager` reconciler.
//!
//! The classifier here is pure: callers observe the Sandbox, its Pod, and the
//! apiserver clock, and this module decides which lifecycle state the session
//! is in. Planning in the manager turns a state into at most one action. No
//! state maps to "set `operatingMode: Running`" or "create a Sandbox": a
//! session that has ended is only ever resumed by an explicit recovery deploy.

use chrono::{DateTime, Duration, Utc};
use k8s_openapi::api::core::v1::Pod;
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use kube::ResourceExt;

/// `app.kubernetes.io/managed-by` label key and the value this project writes.
pub const LABEL_MANAGED_BY: &str = "app.kubernetes.io/managed-by";
/// Management marker value for every object the provider creates.
pub const MANAGED_BY: &str = "buzz-backend-kubernetes";
/// Object-layout schema label key.
pub const LABEL_BINDING_VERSION: &str = "buzz.block.xyz/binding-version";
/// Binding version of controller-owned Sandbox sessions.
pub const SANDBOX_BINDING_VERSION: &str = "2";
/// Full agent pubkey annotation.
pub const ANNOTATION_PUBKEY_FULL: &str = "buzz.block.xyz/agent-pubkey-full";

/// UID of the first Pod the session's credentials were bound to.
pub const INITIAL_POD: &str = "buzz.block.xyz/initial-pod-uid";
/// Session generation; also the checkpoint key segment.
pub const GENERATION: &str = "buzz.block.xyz/generation";
/// Verified owner pubkey (hex) that launched the session.
pub const OWNER: &str = "buzz.block.xyz/owner-pubkey";
/// Channel the session is bound to.
pub const CHANNEL: &str = "buzz.block.xyz/channel-id";
/// Thread root the session is bound to.
pub const THREAD: &str = "buzz.block.xyz/thread-root";
/// Session identifier (equal to the generation).
pub const SESSION: &str = "buzz.block.xyz/session-id";
/// `enabled` when the session was launched with checkpoint-on-stop.
pub const CHECKPOINTING: &str = "buzz.block.xyz/checkpointing";
/// Checkpoint key a recovered session restores from on first start.
pub const RESTORE_CHECKPOINT: &str = "buzz.block.xyz/restore-checkpoint";

/// `ending` | `ended`.
pub const LIFECYCLE: &str = "buzz.block.xyz/lifecycle";
/// Why the session ended; see [`EndedReason`].
pub const ENDED_REASON: &str = "buzz.block.xyz/ended-reason";
/// RFC 3339 apiserver time at which the tombstone was written.
pub const ENDED_AT: &str = "buzz.block.xyz/ended-at";
/// `<s3 key>` | `missing` | `unconfigured`.
pub const CHECKPOINT: &str = "buzz.block.xyz/checkpoint";
/// Generation of the tombstone a recovered session replaced.
pub const RECOVERED_FROM: &str = "buzz.block.xyz/recovered-from";

/// [`LIFECYCLE`] value while a stop is draining.
pub const LIFECYCLE_ENDING: &str = "ending";
/// [`LIFECYCLE`] value of a tombstone.
pub const LIFECYCLE_ENDED: &str = "ended";
/// [`CHECKPOINT`] value when no verified checkpoint exists.
pub const CHECKPOINT_MISSING: &str = "missing";
/// [`CHECKPOINT`] value when the session never had checkpointing.
pub const CHECKPOINT_UNCONFIGURED: &str = "unconfigured";
/// [`CHECKPOINTING`] value written when checkpoint-on-stop is configured.
pub const CHECKPOINTING_ENABLED: &str = "enabled";
/// [`CHECKPOINTING`] value written when checkpoint-on-stop is not configured.
pub const CHECKPOINTING_DISABLED: &str = "disabled";

/// Agent container name inside the Sandbox Pod.
pub const AGENT_CONTAINER: &str = "agent";

/// Age after which an unbound Sandbox or a never-started bound Pod is abandoned.
pub const BIND_TIMEOUT_SECS: i64 = 1200;
/// How long a bound session's Pod may be absent before the session is lost.
pub const LOST_GRACE_SECS: i64 = 60;
/// Hold before a completed session without a checkpoint is tombstoned.
pub const MISSING_CHECKPOINT_HOLD_SECS: i64 = 24 * 60 * 60;
/// Tombstone retention; matches the 30-day checkpoint object expiry.
pub const ENDED_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;

/// The Agent Sandbox `v1beta1` resource.
pub fn sandbox_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk(
        "agents.x-k8s.io",
        "v1beta1",
        "Sandbox",
    ))
}

/// Read one annotation.
pub fn annotation<'a>(object: &'a DynamicObject, key: &str) -> Option<&'a str> {
    object
        .metadata
        .annotations
        .as_ref()?
        .get(key)
        .map(String::as_str)
}

/// Is `pod` controlled by `sandbox`?
pub fn owned_pod(pod: &Pod, sandbox: &DynamicObject) -> bool {
    sandbox.metadata.uid.as_ref().is_some_and(|uid| {
        pod.owner_references().iter().any(|r| {
            &r.uid == uid
                && r.kind == "Sandbox"
                && r.api_version == "agents.x-k8s.io/v1beta1"
                && r.controller == Some(true)
        })
    })
}

/// `spec.operatingMode` of a Sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatingMode {
    Running,
    Suspended,
    /// Absent or a value this version does not know; never acted on.
    Other,
}

impl OperatingMode {
    /// Read `spec.operatingMode`.
    pub fn of(sandbox: &DynamicObject) -> Self {
        match sandbox.data["spec"]["operatingMode"].as_str() {
            Some("Running") => Self::Running,
            Some("Suspended") => Self::Suspended,
            _ => Self::Other,
        }
    }
}

/// The [`LIFECYCLE`] annotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    /// No lifecycle record: an ordinary (possibly live) session.
    Live,
    /// A stop was requested and is draining.
    Ending,
    /// A tombstone.
    Ended,
    /// A value this version does not write; the object is left alone.
    Unrecognized,
}

impl Lifecycle {
    /// Read the [`LIFECYCLE`] annotation.
    pub fn of(sandbox: &DynamicObject) -> Self {
        match annotation(sandbox, LIFECYCLE) {
            None => Self::Live,
            Some(LIFECYCLE_ENDING) => Self::Ending,
            Some(LIFECYCLE_ENDED) => Self::Ended,
            Some(_) => Self::Unrecognized,
        }
    }
}

/// Why a session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndedReason {
    Stopped,
    Completed,
    Replaced,
    Lost,
    BindingFailed,
    BindAbandoned,
}

impl EndedReason {
    /// Annotation spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Completed => "completed",
            Self::Replaced => "replaced",
            Self::Lost => "lost",
            Self::BindingFailed => "binding-failed",
            Self::BindAbandoned => "bind-abandoned",
        }
    }

    /// Parse the annotation spelling.
    pub fn parse(value: &str) -> Option<Self> {
        [
            Self::Stopped,
            Self::Completed,
            Self::Replaced,
            Self::Lost,
            Self::BindingFailed,
            Self::BindAbandoned,
        ]
        .into_iter()
        .find(|reason| reason.as_str() == value)
    }
}

/// Agent container progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Container {
    /// No running or terminated state has ever been reported.
    NeverStarted,
    Running,
    Terminated {
        finished_at: Option<DateTime<Utc>>,
        /// Raw termination message (bounded by the kubelet to 4 KiB).
        message: Option<String>,
    },
}

/// The observed Pod named after the Sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodObservation {
    pub uid: String,
    pub created: Option<DateTime<Utc>>,
    pub deleting: bool,
    /// Controlled by the observed Sandbox.
    pub owned: bool,
    pub container: Container,
}

impl PodObservation {
    /// Observe a Pod against its Sandbox.
    pub fn from_pod(pod: &Pod, sandbox: &DynamicObject) -> Self {
        let state = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .and_then(|containers| containers.iter().find(|c| c.name == AGENT_CONTAINER))
            .and_then(|c| c.state.as_ref());
        let container = match state {
            Some(state) if state.terminated.is_some() => {
                let terminated = state.terminated.as_ref();
                Container::Terminated {
                    finished_at: terminated.and_then(|t| t.finished_at.as_ref()).map(|t| t.0),
                    message: terminated.and_then(|t| t.message.clone()),
                }
            }
            Some(state) if state.running.is_some() => Container::Running,
            _ => Container::NeverStarted,
        };
        Self {
            uid: pod.metadata.uid.clone().unwrap_or_default(),
            created: pod.metadata.creation_timestamp.as_ref().map(|t| t.0),
            deleting: pod.metadata.deletion_timestamp.is_some(),
            owned: owned_pod(pod, sandbox),
            container,
        }
    }
}

/// Everything the classifier needs, extracted from live objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    /// Apiserver clock.
    pub now: DateTime<Utc>,
    pub managed: bool,
    pub binding_version: Option<String>,
    pub owner: Option<String>,
    pub created: Option<DateTime<Utc>>,
    pub deleting: bool,
    pub operating_mode: OperatingMode,
    pub lifecycle: Lifecycle,
    pub ended_reason: Option<EndedReason>,
    pub ended_at: Option<DateTime<Utc>>,
    pub initial_pod: Option<String>,
    pub pod: Option<PodObservation>,
    /// First apiserver time at which the bound Pod was seen absent.
    pub pod_missing_since: Option<DateTime<Utc>>,
}

impl Observation {
    /// Observe a Sandbox and the Pod with its name, if any.
    pub fn from_objects(
        sandbox: &DynamicObject,
        pod: Option<&Pod>,
        now: DateTime<Utc>,
        pod_missing_since: Option<DateTime<Utc>>,
    ) -> Self {
        let labels = sandbox.labels();
        Self {
            now,
            managed: labels.get(LABEL_MANAGED_BY).map(String::as_str) == Some(MANAGED_BY),
            binding_version: labels.get(LABEL_BINDING_VERSION).cloned(),
            owner: annotation(sandbox, OWNER).map(str::to_string),
            created: sandbox.metadata.creation_timestamp.as_ref().map(|t| t.0),
            deleting: sandbox.metadata.deletion_timestamp.is_some(),
            operating_mode: OperatingMode::of(sandbox),
            lifecycle: Lifecycle::of(sandbox),
            ended_reason: annotation(sandbox, ENDED_REASON).and_then(EndedReason::parse),
            ended_at: annotation(sandbox, ENDED_AT)
                .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
                .map(|t| t.with_timezone(&Utc)),
            initial_pod: annotation(sandbox, INITIAL_POD).map(str::to_string),
            pod: pod.map(|pod| PodObservation::from_pod(pod, sandbox)),
            pod_missing_since,
        }
    }
}

/// Why an object is left alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ignored {
    /// Missing the management marker or binding version 2.
    Unmarked,
    /// Owner annotation differs from the namespace's configured owner.
    ForeignOwner,
    /// The Pod with the Sandbox's name is not controlled by it.
    ForeignPod,
    /// Suspended or otherwise not Running, without a lifecycle record.
    NotRunning,
    /// An `ended` lifecycle that is not Suspended, or an unknown lifecycle.
    Inconsistent,
}

impl Ignored {
    /// Event/metric spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unmarked => "unmarked",
            Self::ForeignOwner => "foreign-owner",
            Self::ForeignPod => "foreign-pod",
            Self::NotRunning => "not-running",
            Self::Inconsistent => "inconsistent",
        }
    }
}

/// A session's lifecycle state (spec table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Ignored(Ignored),
    /// Running, no first-Pod binding yet; `abandoned` once the Sandbox is
    /// [`BIND_TIMEOUT_SECS`] old.
    Binding {
        abandoned: bool,
    },
    /// Bound to this Pod, whose container never started; `expired` once the
    /// Pod is [`BIND_TIMEOUT_SECS`] old (until then a deploy repairs it).
    BindingIncomplete {
        expired: bool,
    },
    Active,
    /// Pod deleting, or a stop is draining while the Pod still exists.
    Draining,
    /// Stop requested and its Pod is gone; finalize with the recorded reason.
    Ending {
        reason: EndedReason,
    },
    /// The bound container exited.
    Completed {
        finished_at: Option<DateTime<Utc>>,
        message: Option<String>,
    },
    /// The controller replaced the first Pod.
    Replaced,
    /// The bound Pod is gone; `confirmed` after [`LOST_GRACE_SECS`].
    Lost {
        confirmed: bool,
    },
    /// A tombstone; `expired` once retained for [`ENDED_RETENTION_SECS`].
    Ended {
        expired: bool,
    },
}

fn age(now: DateTime<Utc>, since: Option<DateTime<Utc>>) -> Option<Duration> {
    since.map(|since| now - since)
}

fn at_least(now: DateTime<Utc>, since: Option<DateTime<Utc>>, secs: i64) -> bool {
    age(now, since).is_some_and(|age| age >= Duration::seconds(secs))
}

/// Classify one observed session. `expected_owner` is the namespace's
/// configured owner (hex); a mismatching Sandbox is never touched.
pub fn classify(observation: &Observation, expected_owner: &str) -> State {
    let o = observation;
    if !o.managed || o.binding_version.as_deref() != Some(SANDBOX_BINDING_VERSION) {
        return State::Ignored(Ignored::Unmarked);
    }
    if o.owner.as_deref() != Some(expected_owner) {
        return State::Ignored(Ignored::ForeignOwner);
    }
    if o.deleting {
        return State::Draining;
    }
    match o.lifecycle {
        Lifecycle::Ended if o.operating_mode == OperatingMode::Suspended => {
            // Without a parseable timestamp the tombstone is kept forever:
            // a deferred deletion is free, a wrong one loses the record.
            return State::Ended {
                expired: at_least(o.now, o.ended_at, ENDED_RETENTION_SECS),
            };
        }
        Lifecycle::Ending => {
            return match &o.pod {
                Some(pod) if pod.owned => State::Draining,
                Some(_) => State::Ignored(Ignored::ForeignPod),
                None => State::Ending {
                    reason: o.ended_reason.unwrap_or(EndedReason::Stopped),
                },
            };
        }
        Lifecycle::Ended | Lifecycle::Unrecognized => return State::Ignored(Ignored::Inconsistent),
        Lifecycle::Live => {}
    }
    if o.operating_mode != OperatingMode::Running {
        return State::Ignored(Ignored::NotRunning);
    }
    if let Some(pod) = &o.pod {
        if !pod.owned {
            return State::Ignored(Ignored::ForeignPod);
        }
        if pod.deleting {
            return State::Draining;
        }
    }
    let Some(initial) = o.initial_pod.as_deref() else {
        return State::Binding {
            abandoned: at_least(o.now, o.created, BIND_TIMEOUT_SECS),
        };
    };
    let Some(pod) = &o.pod else {
        return State::Lost {
            confirmed: at_least(o.now, o.pod_missing_since, LOST_GRACE_SECS),
        };
    };
    if pod.uid != initial {
        return State::Replaced;
    }
    match &pod.container {
        Container::NeverStarted => State::BindingIncomplete {
            expired: at_least(o.now, pod.created, BIND_TIMEOUT_SECS),
        },
        Container::Running => State::Active,
        Container::Terminated {
            finished_at,
            message,
        } => State::Completed {
            finished_at: *finished_at,
            message: message.clone(),
        },
    }
}

/// The checkpoint receipt the harness writes to its termination message.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointReceipt {
    pub version: u32,
    pub checkpoint: String,
}

/// Parse a termination-message receipt and bind it to the session's own
/// prefix (`<developer prefix><generation>/`). A receipt naming any other
/// generation or developer is refused: a compromised Pod must not be able to
/// point a tombstone at someone else's checkpoint.
pub fn receipt_key(message: &str, session_prefix: &str) -> Option<String> {
    let receipt: CheckpointReceipt = serde_json::from_str(message.trim()).ok()?;
    (receipt.version == 1 && is_session_key(&receipt.checkpoint, session_prefix))
        .then_some(receipt.checkpoint)
}

/// Is `key` one checkpoint object directly under `session_prefix`
/// (`<developer prefix><generation>/`)?
pub fn is_session_key(key: &str, session_prefix: &str) -> bool {
    let Some(rest) = key.strip_prefix(session_prefix) else {
        return false;
    };
    !session_prefix.is_empty()
        && session_prefix.ends_with('/')
        && !rest.is_empty()
        && !rest.contains('/')
        && !key.contains("..")
        && key.len() <= 512
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-/._".contains(&b))
}

/// Is a tombstone `checkpoint` value an actual key?
pub fn is_checkpoint_key(value: &str) -> bool {
    !value.is_empty() && value != CHECKPOINT_MISSING && value != CHECKPOINT_UNCONFIGURED
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
