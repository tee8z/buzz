//! Reconciliation of one developer namespace.
//!
//! Each namespace has one loop task that owns its [`Memory`]: a periodic pass
//! over the Sandboxes, and one Pod watch whose only job is to capture
//! termination-message checkpoint receipts before a deleted Pod takes them
//! with it.
//!
//! Reads: Sandboxes (with the apiserver `Date` as the only clock) and Pods.
//! Writes: one CAS merge patch per tombstone, a precondition-guarded delete
//! per expired tombstone, and Events. Never Secrets, never `pods/exec`, never
//! a create or a `Running` write. A CAS conflict re-reads the Sandbox and
//! re-classifies it rather than retrying a stale decision. Every write is a
//! single apiserver request, so a pass cancelled at shutdown leaves nothing
//! half-done: the next process re-observes and re-decides.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use buzz_backend_kubernetes::lifecycle::{
    annotation, classify, sandbox_resource, Container, Observation, PodObservation, State,
    AGENT_CONTAINER, CHECKPOINT, CHECKPOINTING, CHECKPOINTING_DISABLED, ENDED_AT, ENDED_REASON,
    GENERATION, INITIAL_POD, LABEL_BINDING_VERSION, LABEL_MANAGED_BY, LIFECYCLE, LIFECYCLE_ENDED,
    MANAGED_BY, RECOVERED_FROM, RESTORE_CHECKPOINT, SANDBOX_BINDING_VERSION,
};
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::StreamExt;
use k8s_openapi::api::core::v1::{Event, EventSource, ObjectReference, Pod};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions};
use kube::core::{DynamicObject, ObjectMeta};
use kube::runtime::{watcher, WatchStreamExt};
use kube::Client;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::config::Namespace;
use crate::evidence::{self, CheckpointStore};
use crate::plan::{needs_evidence, plan, Action, Evidence};
use crate::server::Health;

/// Re-classifications after a CAS conflict before giving up until next pass.
const MAX_ATTEMPTS: usize = 3;

/// Receipts retained per namespace. A developer namespace runs a handful of
/// sessions; beyond this the watch is being flooded and new receipts are
/// dropped (their sessions tombstone with `checkpoint=missing`, which is
/// recoverable by starting fresh, never by guessing).
const MAX_RECEIPTS: usize = 256;

/// The kubelet caps termination messages at 4 KiB; anything larger is not one.
const MAX_RECEIPT_BYTES: usize = 4096;

/// Every `state` label value, so a state that empties is reported as 0.
pub const STATE_LABELS: [&str; 11] = [
    "ignored",
    "binding",
    "bind_abandoned",
    "binding_incomplete",
    "active",
    "draining",
    "ending",
    "completed",
    "replaced",
    "lost",
    "ended",
];

/// Label selector for objects the provider marks as Sandbox sessions.
fn selector() -> String {
    format!("{LABEL_MANAGED_BY}={MANAGED_BY},{LABEL_BINDING_VERSION}={SANDBOX_BINDING_VERSION}")
}

/// Per-namespace memory across passes. Losing it (restart) only delays
/// decisions: every timer restarts from the next observation.
#[derive(Default)]
pub struct Memory {
    /// Sandbox uid → first apiserver time its bound Pod was seen absent.
    pod_missing_since: HashMap<String, DateTime<Utc>>,
    /// Sandbox uid → first apiserver time it was seen Completed.
    completed_since: HashMap<String, DateTime<Utc>>,
    /// Pod uid → the agent container's raw termination message. Untrusted
    /// until [`evidence::from_receipt`] binds it to the session prefix and S3.
    receipts: HashMap<String, String>,
    /// (Sandbox uid, Event reason) already reported.
    reported: HashSet<(String, &'static str)>,
}

impl Memory {
    /// Record a terminated agent container's termination message. Fed by
    /// the Pod watch and by every pass, so a Pod deleted between passes
    /// (a stop, a replacement) still leaves its receipt behind.
    pub fn observe_pod(&mut self, pod: &Pod) {
        let Some(uid) = pod.metadata.uid.as_deref() else {
            return;
        };
        let message = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .and_then(|containers| containers.iter().find(|c| c.name == AGENT_CONTAINER))
            .and_then(|c| c.state.as_ref()?.terminated.as_ref()?.message.as_deref())
            .filter(|m| !m.trim().is_empty() && m.len() <= MAX_RECEIPT_BYTES);
        let Some(message) = message else {
            return;
        };
        if self.receipts.len() >= MAX_RECEIPTS && !self.receipts.contains_key(uid) {
            tracing::warn!(pod_uid = uid, "receipt memory full; dropping receipt");
            return;
        }
        self.receipts.insert(uid.to_string(), message.to_string());
    }

    /// Forget everything about Sandboxes that no longer exist, and receipts
    /// of Pods no live Sandbox is bound to.
    fn retain(&mut self, live: &HashSet<String>, bound_pods: &HashSet<String>) {
        self.pod_missing_since.retain(|uid, _| live.contains(uid));
        self.completed_since.retain(|uid, _| live.contains(uid));
        self.receipts.retain(|uid, _| bound_pods.contains(uid));
        self.reported.retain(|(uid, _)| live.contains(uid));
    }
}

/// Counts from one pass, for metrics and logs.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub states: BTreeMap<&'static str, u64>,
    pub tombstoned: u64,
    pub deleted: u64,
    pub held: u64,
    pub ignored: u64,
    pub conflicts: u64,
}

fn state_label(state: &State) -> &'static str {
    match state {
        State::Ignored(_) => "ignored",
        State::Binding { abandoned: false } => "binding",
        State::Binding { abandoned: true } => "bind_abandoned",
        State::BindingIncomplete { .. } => "binding_incomplete",
        State::Active => "active",
        State::Draining => "draining",
        State::Ending { .. } => "ending",
        State::Completed { .. } => "completed",
        State::Replaced => "replaced",
        State::Lost { .. } => "lost",
        State::Ended { .. } => "ended",
    }
}

enum ApplyError {
    Conflict,
    Other(String),
}

fn reason_is(error: &kube::Error, reason: &str) -> bool {
    matches!(error, kube::Error::Api(e) if e.reason == reason)
}

/// Record one pass in metrics.
pub fn record_pass(namespace: &str, result: &Result<Summary, String>) {
    let outcome = if result.is_ok() { "ok" } else { "error" };
    metrics::counter!("buzz_agent_manager_passes_total",
        "namespace" => namespace.to_string(), "result" => outcome)
    .increment(1);
    match result {
        Ok(summary) => {
            for state in STATE_LABELS {
                let count = summary.states.get(state).copied().unwrap_or_default();
                metrics::gauge!("buzz_agent_manager_sessions",
                    "namespace" => namespace.to_string(), "state" => state)
                .set(count as f64);
            }
            metrics::gauge!("buzz_agent_manager_checkpoint_holds",
                "namespace" => namespace.to_string())
            .set(summary.held as f64);
            metrics::gauge!("buzz_agent_manager_last_success_timestamp_seconds",
                "namespace" => namespace.to_string())
            .set(chrono::Utc::now().timestamp() as f64);
        }
        Err(error) => tracing::warn!(namespace, %error, "reconcile pass failed"),
    }
}

/// The loop for one namespace: a pass every `interval`, and the Pod watch
/// feeding [`Memory::observe_pod`] in between. Returns only on `cancel`; a
/// pass in flight finishes first (its requests are individually bounded).
pub async fn run_namespace<S: CheckpointStore>(
    reconciler: Arc<Reconciler<S>>,
    name: String,
    namespace: Namespace,
    interval: Duration,
    health: Arc<Health>,
    cancel: CancellationToken,
) {
    let pods: Api<Pod> = Api::namespaced(reconciler.client.clone(), &name);
    let mut receipts = watcher(pods, watcher::Config::default().labels(&pod_selector()))
        .default_backoff()
        .boxed();
    let mut memory = Memory::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {
                let result = reconciler.pass(&name, &namespace, &mut memory).await;
                record_pass(&name, &result);
                health.record(&name, result.is_ok());
            }
            event = receipts.next() => match event {
                Some(Ok(watcher::Event::Apply(pod) | watcher::Event::InitApply(pod)
                    | watcher::Event::Delete(pod))) => memory.observe_pod(&pod),
                Some(Ok(watcher::Event::Init | watcher::Event::InitDone)) => {}
                Some(Err(error)) => {
                    // The backoff re-establishes the watch; passes still run.
                    tracing::warn!(namespace = name, %error, "Pod watch failed; retrying");
                    metrics::counter!("buzz_agent_manager_watch_errors_total",
                        "namespace" => name.clone()).increment(1);
                }
                // The watcher stream never ends; if it did, passes carry on
                // and receipts come from passes alone.
                None => receipts = futures_util::stream::pending().boxed(),
            },
        }
    }
}

fn pod_selector() -> String {
    format!("{LABEL_MANAGED_BY}={MANAGED_BY},{LABEL_BINDING_VERSION}={SANDBOX_BINDING_VERSION}")
}

/// Reconciler for every configured namespace.
pub struct Reconciler<S> {
    pub client: Client,
    pub store: S,
    pub dry_run: bool,
}

impl<S: CheckpointStore> Reconciler<S> {
    fn sandboxes(&self, namespace: &str) -> Api<DynamicObject> {
        Api::namespaced_with(self.client.clone(), namespace, &sandbox_resource())
    }

    /// List managed Sandboxes and return the apiserver clock from the same
    /// response (quorum read: resourceVersion unset).
    async fn list_sandboxes(
        &self,
        namespace: &str,
    ) -> Result<(Vec<DynamicObject>, Option<DateTime<Utc>>), String> {
        let url = format!("/apis/agents.x-k8s.io/v1beta1/namespaces/{namespace}/sandboxes");
        let request = kube::core::Request::new(url)
            .list(&ListParams::default().labels(&selector()))
            .map_err(|e| format!("build Sandbox list: {e}"))?;
        let (parts, body) = request.into_parts();
        let response = self
            .client
            .send(http::Request::from_parts(parts, body.into()))
            .await
            .map_err(|e| format!("list Sandboxes in {namespace}: {e}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "list Sandboxes in {namespace}: HTTP {}",
                response.status()
            ));
        }
        let now = response
            .headers()
            .get(http::header::DATE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| DateTime::parse_from_rfc2822(v).ok())
            .map(|v| v.with_timezone(&Utc));
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .map_err(|e| format!("read Sandbox list: {e}"))?
            .to_bytes();
        let list: kube::core::ObjectList<DynamicObject> =
            serde_json::from_slice(&bytes).map_err(|e| format!("decode Sandbox list: {e}"))?;
        Ok((list.items, now))
    }

    /// One pass over `namespace`.
    pub async fn pass(
        &self,
        namespace: &str,
        config: &Namespace,
        memory: &mut Memory,
    ) -> Result<Summary, String> {
        let (sandboxes, now) = self.list_sandboxes(namespace).await?;
        // The apiserver clock is the only clock: a skewed local clock must
        // never age a session into a tombstone or a deletion.
        let now = now.ok_or("apiserver response carried no usable Date; skipping pass")?;
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let listed = pods
            .list(&ListParams::default().labels(&pod_selector()))
            .await
            .map_err(|e| format!("list Pods in {namespace}: {e}"))?
            .items;
        for pod in &listed {
            memory.observe_pod(pod);
        }
        let mut by_name: HashMap<String, Pod> = listed
            .into_iter()
            .filter_map(|pod| Some((pod.metadata.name.clone()?, pod)))
            .collect();
        let mut summary = Summary::default();
        let mut live = HashSet::new();
        let mut bound_pods = HashSet::new();
        for sandbox in sandboxes {
            let Some(name) = sandbox.metadata.name.clone() else {
                continue;
            };
            live.extend(sandbox.metadata.uid.clone());
            bound_pods.extend(annotation(&sandbox, INITIAL_POD).map(str::to_string));
            let pod = by_name.remove(&name);
            if let Err(error) = self
                .reconcile_one(namespace, config, sandbox, pod, now, memory, &mut summary)
                .await
            {
                tracing::warn!(namespace, sandbox = name, %error, "reconcile failed; retrying next pass");
                metrics::counter!("buzz_agent_manager_errors_total", "namespace" => namespace.to_string())
                    .increment(1);
            }
        }
        memory.retain(&live, &bound_pods);
        Ok(summary)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one call site per pass; a context struct would only rename the same values"
    )]
    async fn reconcile_one(
        &self,
        namespace: &str,
        config: &Namespace,
        mut sandbox: DynamicObject,
        mut pod: Option<Pod>,
        now: DateTime<Utc>,
        memory: &mut Memory,
        summary: &mut Summary,
    ) -> Result<(), String> {
        let name = sandbox.metadata.name.clone().unwrap_or_default();
        for _ in 0..MAX_ATTEMPTS {
            let uid = sandbox.metadata.uid.clone().unwrap_or_default();
            let missing_since = if annotation(&sandbox, INITIAL_POD).is_some() && pod.is_none() {
                Some(*memory.pod_missing_since.entry(uid.clone()).or_insert(now))
            } else {
                memory.pod_missing_since.remove(&uid);
                None
            };
            let observation = Observation::from_objects(&sandbox, pod.as_ref(), now, missing_since);
            let state = classify(&observation, &config.owner_pubkey);
            let evidence = self
                .evidence(&state, &sandbox, &observation, config, memory)
                .await;
            let hold_since = match state {
                State::Completed { .. } => {
                    *memory.completed_since.entry(uid.clone()).or_insert(now)
                }
                _ => now,
            };
            let action = plan(&state, &evidence, now, hold_since);
            match self
                .apply(namespace, &sandbox, &state, &action, now, memory, summary)
                .await
            {
                Ok(()) => {
                    *summary.states.entry(state_label(&state)).or_default() += 1;
                    return Ok(());
                }
                Err(ApplyError::Other(error)) => return Err(error),
                Err(ApplyError::Conflict) => {
                    summary.conflicts += 1;
                    metrics::counter!("buzz_agent_manager_cas_conflicts_total", "namespace" => namespace.to_string())
                        .increment(1);
                    let Some(fresh) = self
                        .sandboxes(namespace)
                        .get_opt(&name)
                        .await
                        .map_err(|e| format!("re-read Sandbox: {e}"))?
                    else {
                        return Ok(());
                    };
                    sandbox = fresh;
                    pod = Api::<Pod>::namespaced(self.client.clone(), namespace)
                        .get_opt(&name)
                        .await
                        .map_err(|e| format!("re-read Pod: {e}"))?;
                    if let Some(pod) = &pod {
                        memory.observe_pod(pod);
                    }
                }
            }
        }
        Err("Sandbox kept changing; deferring to next pass".into())
    }

    /// Checkpoint evidence for a session about to be tombstoned: its own
    /// receipt, or else the checkpoint a recovered session was restored from,
    /// so a failed restore or stop never orphans the original work.
    async fn evidence(
        &self,
        state: &State,
        sandbox: &DynamicObject,
        observation: &Observation,
        config: &Namespace,
        memory: &Memory,
    ) -> Evidence {
        if !needs_evidence(state) {
            return Evidence::Missing;
        }
        if annotation(sandbox, CHECKPOINTING) == Some(CHECKPOINTING_DISABLED) {
            return Evidence::Unconfigured;
        }
        let own = self
            .own_evidence(state, sandbox, observation, config, memory)
            .await;
        match (
            own,
            annotation(sandbox, RESTORE_CHECKPOINT),
            annotation(sandbox, RECOVERED_FROM),
        ) {
            (Evidence::Missing, Some(restore), Some(previous)) => {
                let previous_prefix = format!("{}{previous}/", config.checkpoint_prefix);
                evidence::inherited(&self.store, restore, &previous_prefix).await
            }
            (own, ..) => own,
        }
    }

    /// The session's own checkpoint. The receipt is the bound (first) Pod's
    /// termination message: read from the live Pod when it is still there,
    /// otherwise from what the watch captured. The session prefix comes from
    /// config and the Sandbox, never the Pod.
    async fn own_evidence(
        &self,
        state: &State,
        sandbox: &DynamicObject,
        observation: &Observation,
        config: &Namespace,
        memory: &Memory,
    ) -> Evidence {
        // Abandoned or never-started bindings never ran a harness.
        if matches!(
            state,
            State::Binding { .. } | State::BindingIncomplete { .. }
        ) {
            return Evidence::Missing;
        }
        let Some(initial) = observation.initial_pod.as_deref() else {
            return Evidence::Missing;
        };
        let live_message = match &observation.pod {
            Some(PodObservation {
                uid,
                container: Container::Terminated { message, .. },
                ..
            }) if uid == initial => message.as_deref(),
            _ => None,
        };
        let message = live_message.or_else(|| memory.receipts.get(initial).map(String::as_str));
        let Some(generation) = annotation(sandbox, GENERATION) else {
            return Evidence::Missing;
        };
        let session_prefix = format!("{}{generation}/", config.checkpoint_prefix);
        evidence::from_receipt(&self.store, message, &session_prefix).await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one call site per pass; a context struct would only rename the same values"
    )]
    async fn apply(
        &self,
        namespace: &str,
        sandbox: &DynamicObject,
        state: &State,
        action: &Action,
        now: DateTime<Utc>,
        memory: &mut Memory,
        summary: &mut Summary,
    ) -> Result<(), ApplyError> {
        let name = sandbox.metadata.name.as_deref().unwrap_or_default();
        match action {
            Action::None => Ok(()),
            Action::Ignore(why) => {
                summary.ignored += 1;
                self.report(
                    namespace,
                    sandbox,
                    memory,
                    "SandboxIgnored",
                    "Warning",
                    &format!(
                        "buzz-agent-manager leaves this Sandbox alone: {}",
                        why.as_str()
                    ),
                    now,
                )
                .await;
                Ok(())
            }
            Action::Hold { until } => {
                summary.held += 1;
                self.report(
                    namespace,
                    sandbox,
                    memory,
                    "CheckpointMissing",
                    "Warning",
                    &format!(
                        "session completed without a verified checkpoint; holding until {}",
                        until.to_rfc3339_opts(SecondsFormat::Secs, true)
                    ),
                    now,
                )
                .await;
                Ok(())
            }
            Action::Tombstone { reason, checkpoint } => {
                tracing::info!(
                    namespace,
                    sandbox = name,
                    state = state_label(state),
                    reason = reason.as_str(),
                    checkpoint,
                    dry_run = self.dry_run,
                    "tombstoning session"
                );
                if self.dry_run {
                    return Ok(());
                }
                let patch = tombstone_patch(sandbox, reason.as_str(), checkpoint, now);
                match self
                    .sandboxes(namespace)
                    .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                    .await
                {
                    Ok(_) => {}
                    Err(e) if reason_is(&e, "Conflict") => return Err(ApplyError::Conflict),
                    Err(e) if reason_is(&e, "NotFound") => return Ok(()),
                    Err(e) => return Err(ApplyError::Other(format!("tombstone Sandbox: {e}"))),
                }
                summary.tombstoned += 1;
                metrics::counter!("buzz_agent_manager_tombstones_total",
                    "namespace" => namespace.to_string(), "reason" => reason.as_str())
                .increment(1);
                self.report(
                    namespace,
                    sandbox,
                    memory,
                    "SessionEnded",
                    "Normal",
                    &format!(
                        "session ended ({}); checkpoint: {checkpoint}",
                        reason.as_str()
                    ),
                    now,
                )
                .await;
                Ok(())
            }
            Action::Delete => {
                tracing::info!(
                    namespace,
                    sandbox = name,
                    dry_run = self.dry_run,
                    "deleting expired tombstone"
                );
                if self.dry_run {
                    return Ok(());
                }
                let params = DeleteParams {
                    preconditions: Some(Preconditions {
                        uid: sandbox.metadata.uid.clone(),
                        resource_version: sandbox.metadata.resource_version.clone(),
                    }),
                    ..Default::default()
                };
                match self.sandboxes(namespace).delete(name, &params).await {
                    Ok(_) => {}
                    Err(e) if reason_is(&e, "Conflict") => return Err(ApplyError::Conflict),
                    Err(e) if reason_is(&e, "NotFound") => return Ok(()),
                    Err(e) => return Err(ApplyError::Other(format!("delete tombstone: {e}"))),
                }
                summary.deleted += 1;
                metrics::counter!("buzz_agent_manager_tombstones_deleted_total",
                    "namespace" => namespace.to_string())
                .increment(1);
                Ok(())
            }
        }
    }

    /// Emit one Event per (Sandbox, reason). Advisory: a failure is logged and
    /// retried on the next pass because it is not recorded as reported.
    #[expect(
        clippy::too_many_arguments,
        reason = "one call site per pass; a context struct would only rename the same values"
    )]
    async fn report(
        &self,
        namespace: &str,
        sandbox: &DynamicObject,
        memory: &mut Memory,
        reason: &'static str,
        kind: &str,
        message: &str,
        now: DateTime<Utc>,
    ) {
        let uid = sandbox.metadata.uid.clone().unwrap_or_default();
        if self.dry_run || memory.reported.contains(&(uid.clone(), reason)) {
            return;
        }
        let name = sandbox.metadata.name.clone().unwrap_or_default();
        let time = k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(now);
        let event = Event {
            metadata: ObjectMeta {
                generate_name: Some(format!("{name}.")),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            involved_object: ObjectReference {
                api_version: Some("agents.x-k8s.io/v1beta1".into()),
                kind: Some("Sandbox".into()),
                name: Some(name),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.clone()),
                ..Default::default()
            },
            reason: Some(reason.into()),
            message: Some(message.into()),
            type_: Some(kind.into()),
            source: Some(EventSource {
                component: Some("buzz-agent-manager".into()),
                ..Default::default()
            }),
            reporting_component: Some("buzz-agent-manager".into()),
            reporting_instance: Some("buzz-agent-manager".into()),
            first_timestamp: Some(time.clone()),
            last_timestamp: Some(time),
            count: Some(1),
            ..Default::default()
        };
        let events: Api<Event> = Api::namespaced(self.client.clone(), namespace);
        match events.create(&PostParams::default(), &event).await {
            Ok(_) => {
                memory.reported.insert((uid, reason));
            }
            Err(error) => tracing::warn!(namespace, reason, %error, "could not record Event"),
        }
    }
}

/// The one write that ends a session: a JSON merge patch guarded by the
/// observed resourceVersion. It only ever sets `Suspended`.
pub fn tombstone_patch(
    sandbox: &DynamicObject,
    reason: &str,
    checkpoint: &str,
    now: DateTime<Utc>,
) -> serde_json::Value {
    json!({
        "metadata": {
            "resourceVersion": sandbox.metadata.resource_version,
            "annotations": {
                (LIFECYCLE): LIFECYCLE_ENDED,
                (ENDED_REASON): reason,
                (ENDED_AT): now.to_rfc3339_opts(SecondsFormat::Secs, true),
                (CHECKPOINT): checkpoint,
            },
        },
        "spec": {"operatingMode": "Suspended"},
    })
}

#[cfg(test)]
#[path = "reconcile_tests.rs"]
mod tests;
