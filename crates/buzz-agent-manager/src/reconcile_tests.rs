//! Reconcile passes against an in-memory apiserver over the real
//! `kube::Client` path. Every test asserts on the requests actually sent.

use super::*;
use crate::evidence::tests::FakeStore;
use buzz_backend_kubernetes::lifecycle::OWNER;
use proptest::prelude::*;
use serde_json::Value;

#[path = "fake_apiserver.rs"]
mod fake;

const NS: &str = "buzz-agent-staging-dev";
const GENERATION_ID: &str = "0123456789abcdef0123456789abcdef";

fn owner() -> String {
    "a".repeat(64)
}

fn config() -> Namespace {
    Namespace {
        developer: "dev".into(),
        owner_pubkey: owner(),
        checkpoint_prefix: "dev/".into(),
    }
}

fn key() -> String {
    format!("dev/{GENERATION_ID}/6f9619ff-8b86-d011-b42d-00cf4fc964ff.tar.gz")
}

fn receipt(key: &str) -> String {
    json!({"version": 1, "checkpoint": key}).to_string()
}

fn ago(state: &fake::State, secs: i64) -> String {
    (state.now - chrono::Duration::seconds(secs)).to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// A managed, owner-matching Sandbox `age` seconds old.
fn sandbox(state: &fake::State, name: &str, age: i64, mode: &str, annotations: Value) -> Value {
    let mut all = json!({
        (OWNER): owner(),
        (GENERATION): GENERATION_ID,
        (CHECKPOINTING): "enabled",
    });
    fake::merge(&mut all, &annotations);
    json!({
        "apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox",
        "metadata": {
            "name": name, "namespace": NS, "uid": format!("sb-{name}"),
            "resourceVersion": "1", "creationTimestamp": ago(state, age),
            "labels": {(LABEL_MANAGED_BY): MANAGED_BY, (LABEL_BINDING_VERSION): "2"},
            "annotations": all,
        },
        "spec": {"operatingMode": mode},
    })
}

/// The Sandbox-owned Pod named `name`, `age` seconds old, with this agent
/// container state.
fn pod(state: &fake::State, name: &str, uid: &str, age: i64, container: Value) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": name, "namespace": NS, "uid": uid,
            "creationTimestamp": ago(state, age),
            "labels": {(LABEL_MANAGED_BY): MANAGED_BY, (LABEL_BINDING_VERSION): "2"},
            "ownerReferences": [{"apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox",
                "name": name, "uid": format!("sb-{name}"), "controller": true}],
        },
        "status": {"containerStatuses": [{"name": "agent", "image": "i", "imageID": "",
            "ready": false, "restartCount": 0, "state": container}]},
    })
}

fn terminated(message: Option<&str>) -> Value {
    let mut state = json!({"terminated": {"exitCode": 0, "finishedAt": "2027-01-15T08:00:00Z"}});
    if let Some(message) = message {
        state["terminated"]["message"] = message.into();
    }
    state
}

fn reconciler(state: &fake::Shared, store: FakeStore) -> Reconciler<FakeStore> {
    Reconciler {
        client: fake::client(state),
        store,
        dry_run: false,
    }
}

fn store_with(keys: &[String]) -> FakeStore {
    FakeStore {
        keys: keys.to_vec(),
        ..Default::default()
    }
}

/// Every non-GET request except Event creation.
fn writes(state: &fake::Shared) -> Vec<fake::Call> {
    state
        .lock()
        .unwrap()
        .calls
        .iter()
        .filter(|c| c.method != http::Method::GET && !c.path.ends_with("/events"))
        .cloned()
        .collect()
}

fn events(state: &fake::Shared) -> Vec<Value> {
    state
        .lock()
        .unwrap()
        .calls
        .iter()
        .filter(|c| c.method == http::Method::POST && c.path.ends_with("/events"))
        .map(|c| c.body.clone())
        .collect()
}

fn stored(state: &fake::Shared, name: &str) -> Value {
    state.lock().unwrap().sandboxes[name].clone()
}

fn observe(memory: &mut Memory, pod: Value) {
    memory.observe_pod(&serde_json::from_value(pod).unwrap());
}

async fn pass(reconciler: &Reconciler<FakeStore>, memory: &mut Memory) -> Result<Summary, String> {
    reconciler.pass(NS, &config(), memory).await
}

#[tokio::test]
async fn a_completed_session_with_a_verified_receipt_is_tombstoned_once() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(&s, "s", 600, "Running", json!({(INITIAL_POD): "pod-1"}));
        let p = pod(&s, "s", "pod-1", 600, terminated(Some(&receipt(&key()))));
        s.sandboxes.insert("s".into(), sb);
        s.pods.insert("s".into(), p);
    }
    let r = reconciler(&state, store_with(&[key()]));
    let mut memory = Memory::default();
    let summary = pass(&r, &mut memory).await.unwrap();
    assert_eq!(summary.tombstoned, 1);
    let sent = writes(&state);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].method, http::Method::PATCH);
    // One CAS merge patch carrying the observed resourceVersion.
    assert_eq!(sent[0].body["metadata"]["resourceVersion"], "1");
    let ended = stored(&state, "s");
    let now = state.lock().unwrap().now;
    assert_eq!(ended["spec"]["operatingMode"], "Suspended");
    let annotations = &ended["metadata"]["annotations"];
    assert_eq!(annotations[LIFECYCLE], LIFECYCLE_ENDED);
    assert_eq!(annotations[ENDED_REASON], "completed");
    assert_eq!(annotations[CHECKPOINT], key());
    // ended-at is the apiserver's clock, not ours.
    assert_eq!(
        annotations[ENDED_AT],
        now.to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    assert_eq!(events(&state).len(), 1);
    // The next pass sees an unexpired tombstone and does nothing.
    let summary = pass(&r, &mut memory).await.unwrap();
    assert_eq!(summary.tombstoned, 0);
    assert_eq!(writes(&state).len(), 1);
}

#[tokio::test]
async fn a_completed_session_without_a_checkpoint_is_held_for_24_hours() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(&s, "s", 600, "Running", json!({(INITIAL_POD): "pod-1"}));
        let mut p = pod(&s, "s", "pod-1", 600, terminated(None));
        p["status"]["containerStatuses"][0]["state"]["terminated"]["finishedAt"] =
            ago(&s, 60).into();
        s.sandboxes.insert("s".into(), sb);
        s.pods.insert("s".into(), p);
    }
    let r = reconciler(&state, FakeStore::default());
    let mut memory = Memory::default();
    let summary = pass(&r, &mut memory).await.unwrap();
    assert_eq!(summary.held, 1);
    assert!(writes(&state).is_empty());
    let warned = events(&state);
    assert_eq!(warned.len(), 1);
    assert_eq!(warned[0]["reason"], "CheckpointMissing");
    // Still held one second before the hold ends; a repeated hold does not
    // repeat its Event.
    state.lock().unwrap().now += chrono::Duration::seconds(24 * 3600 - 61);
    assert_eq!(pass(&r, &mut memory).await.unwrap().held, 1);
    assert!(writes(&state).is_empty());
    assert_eq!(events(&state).len(), 1);
    state.lock().unwrap().now += chrono::Duration::seconds(1);
    assert_eq!(pass(&r, &mut memory).await.unwrap().tombstoned, 1);
    assert_eq!(
        stored(&state, "s")["metadata"]["annotations"][CHECKPOINT],
        "missing"
    );
}

/// A recovered session whose restore failed exits without a receipt. Its
/// tombstone carries the checkpoint it was recovered from, so the developer
/// can retry recovery instead of losing the original work.
#[tokio::test]
async fn a_failed_recovery_carries_its_restore_checkpoint_forward() {
    const PREVIOUS: &str = "fedcba9876543210fedcba9876543210";
    let original = format!("dev/{PREVIOUS}/6f9619ff-8b86-d011-b42d-00cf4fc964ff.tar.gz");
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(
            &s,
            "s",
            600,
            "Running",
            json!({
                (INITIAL_POD): "pod-1",
                (RESTORE_CHECKPOINT): original,
                (RECOVERED_FROM): PREVIOUS,
            }),
        );
        let mut p = pod(&s, "s", "pod-1", 600, terminated(None));
        p["status"]["containerStatuses"][0]["state"]["terminated"]["finishedAt"] =
            ago(&s, 60).into();
        s.sandboxes.insert("s".into(), sb);
        s.pods.insert("s".into(), p);
    }
    let r = reconciler(&state, store_with(std::slice::from_ref(&original)));
    let mut memory = Memory::default();
    // Still held and reported: this session saved nothing of its own.
    assert_eq!(pass(&r, &mut memory).await.unwrap().held, 1);
    assert_eq!(events(&state)[0]["reason"], "CheckpointMissing");
    state.lock().unwrap().now += chrono::Duration::seconds(24 * 3600);
    assert_eq!(pass(&r, &mut memory).await.unwrap().tombstoned, 1);
    assert_eq!(
        stored(&state, "s")["metadata"]["annotations"][CHECKPOINT],
        original
    );
}

/// `stop` suspends the Sandbox; the controller deletes the Pod after the
/// harness wrote its receipt. The watch captured it, so the tombstone
/// records the checkpoint even though the Pod is gone.
#[tokio::test]
async fn a_stopped_session_records_the_receipt_its_deleted_pod_left_behind() {
    let state = fake::Shared::default();
    let gone_pod;
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(
            &s,
            "s",
            600,
            "Suspended",
            json!({(INITIAL_POD): "pod-1", (LIFECYCLE): "ending", (ENDED_REASON): "stopped"}),
        );
        gone_pod = pod(&s, "s", "pod-1", 600, terminated(Some(&receipt(&key()))));
        s.sandboxes.insert("s".into(), sb);
    }
    let r = reconciler(&state, store_with(&[key()]));
    let mut memory = Memory::default();
    observe(&mut memory, gone_pod);
    assert_eq!(pass(&r, &mut memory).await.unwrap().tombstoned, 1);
    let annotations = stored(&state, "s")["metadata"]["annotations"].clone();
    assert_eq!(annotations[ENDED_REASON], "stopped");
    assert_eq!(annotations[CHECKPOINT], key());
}

#[tokio::test]
async fn a_receipt_s3_does_not_confirm_is_recorded_as_missing() {
    let state = fake::Shared::default();
    let planted;
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(
            &s,
            "s",
            600,
            "Suspended",
            json!({(INITIAL_POD): "pod-1", (LIFECYCLE): "ending", (ENDED_REASON): "stopped"}),
        );
        planted = pod(&s, "s", "pod-1", 600, terminated(Some(&receipt(&key()))));
        s.sandboxes.insert("s".into(), sb);
    }
    let r = reconciler(&state, FakeStore::default());
    let mut memory = Memory::default();
    observe(&mut memory, planted);
    pass(&r, &mut memory).await.unwrap();
    assert_eq!(
        stored(&state, "s")["metadata"]["annotations"][CHECKPOINT],
        "missing"
    );
}

#[tokio::test]
async fn an_s3_outage_defers_the_tombstone() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(&s, "s", 600, "Running", json!({(INITIAL_POD): "pod-1"}));
        let p = pod(&s, "s", "pod-1", 600, terminated(Some(&receipt(&key()))));
        s.sandboxes.insert("s".into(), sb);
        s.pods.insert("s".into(), p);
    }
    let store = FakeStore {
        keys: vec![key()],
        fail: true,
        ..Default::default()
    };
    let r = reconciler(&state, store);
    pass(&r, &mut Memory::default()).await.unwrap();
    assert!(writes(&state).is_empty());
}

#[tokio::test]
async fn a_replaced_session_is_tombstoned_with_the_old_pods_receipt() {
    let state = fake::Shared::default();
    let old_pod;
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(&s, "s", 600, "Running", json!({(INITIAL_POD): "pod-1"}));
        old_pod = pod(&s, "s", "pod-1", 600, terminated(Some(&receipt(&key()))));
        let new_pod = pod(&s, "s", "pod-2", 5, json!({"running": {}}));
        s.sandboxes.insert("s".into(), sb);
        s.pods.insert("s".into(), new_pod);
    }
    let r = reconciler(&state, store_with(&[key()]));
    let mut memory = Memory::default();
    observe(&mut memory, old_pod);
    pass(&r, &mut memory).await.unwrap();
    let annotations = stored(&state, "s")["metadata"]["annotations"].clone();
    assert_eq!(annotations[ENDED_REASON], "replaced");
    assert_eq!(annotations[CHECKPOINT], key());
}

#[tokio::test]
async fn a_lost_pod_is_tombstoned_only_after_the_grace() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(&s, "s", 600, "Running", json!({(INITIAL_POD): "pod-1"}));
        s.sandboxes.insert("s".into(), sb);
    }
    let r = reconciler(&state, FakeStore::default());
    let mut memory = Memory::default();
    pass(&r, &mut memory).await.unwrap();
    state.lock().unwrap().now += chrono::Duration::seconds(59);
    pass(&r, &mut memory).await.unwrap();
    assert!(writes(&state).is_empty());
    state.lock().unwrap().now += chrono::Duration::seconds(1);
    pass(&r, &mut memory).await.unwrap();
    assert_eq!(
        stored(&state, "s")["metadata"]["annotations"][ENDED_REASON],
        "lost"
    );
}

/// A node lost without warning leaves no termination receipt. The session
/// still ends with the newest checkpoint the harness saved after a turn.
#[tokio::test]
async fn a_lost_session_records_its_newest_turn_checkpoint() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(&s, "s", 600, "Running", json!({(INITIAL_POD): "pod-1"}));
        s.sandboxes.insert("s".into(), sb);
    }
    let older = format!("dev/{GENERATION_ID}/00000000-0000-0000-0000-000000000001.tar.gz");
    let r = reconciler(&state, store_with(&[older, key()]));
    let mut memory = Memory::default();
    pass(&r, &mut memory).await.unwrap();
    state.lock().unwrap().now += chrono::Duration::seconds(60);
    pass(&r, &mut memory).await.unwrap();
    let annotations = &stored(&state, "s")["metadata"]["annotations"];
    assert_eq!(annotations[ENDED_REASON], "lost");
    assert_eq!(annotations[CHECKPOINT], key());
}

#[tokio::test]
async fn abandoned_bindings_are_tombstoned_without_probing_s3() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let unbound = sandbox(&s, "unbound", 1200, "Running", json!({}));
        let young = sandbox(&s, "young", 1199, "Running", json!({}));
        let stuck = sandbox(
            &s,
            "stuck",
            1300,
            "Running",
            json!({(INITIAL_POD): "pod-s"}),
        );
        let stuck_pod = pod(&s, "stuck", "pod-s", 1200, json!({"waiting": {}}));
        s.sandboxes.insert("unbound".into(), unbound);
        s.sandboxes.insert("young".into(), young);
        s.sandboxes.insert("stuck".into(), stuck);
        s.pods.insert("stuck".into(), stuck_pod);
    }
    let r = reconciler(&state, FakeStore::default());
    pass(&r, &mut Memory::default()).await.unwrap();
    assert_eq!(
        stored(&state, "unbound")["metadata"]["annotations"][ENDED_REASON],
        "bind-abandoned"
    );
    assert_eq!(
        stored(&state, "stuck")["metadata"]["annotations"][ENDED_REASON],
        "binding-failed"
    );
    assert!(stored(&state, "young")["metadata"]["annotations"]
        .get(LIFECYCLE)
        .is_none());
    assert!(r.store.probes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sessions_without_checkpointing_are_tombstoned_as_unconfigured() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(
            &s,
            "s",
            600,
            "Running",
            json!({(INITIAL_POD): "pod-1", (CHECKPOINTING): "disabled"}),
        );
        let p = pod(&s, "s", "pod-1", 600, terminated(None));
        s.sandboxes.insert("s".into(), sb);
        s.pods.insert("s".into(), p);
    }
    let r = reconciler(&state, FakeStore::default());
    pass(&r, &mut Memory::default()).await.unwrap();
    assert_eq!(
        stored(&state, "s")["metadata"]["annotations"][CHECKPOINT],
        "unconfigured"
    );
}

/// The CAS loses to a concurrent writer that already tombstoned the
/// session: the manager re-reads, re-classifies (Ended), and writes nothing
/// further instead of replaying its stale tombstone.
#[tokio::test]
async fn a_cas_conflict_re_reads_and_re_classifies() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(
            &s,
            "s",
            600,
            "Running",
            json!({(INITIAL_POD): "pod-1", (CHECKPOINTING): "disabled"}),
        );
        let p = pod(&s, "s", "pod-1", 600, terminated(None));
        let now = s.now.to_rfc3339_opts(SecondsFormat::Secs, true);
        s.sandboxes.insert("s".into(), sb);
        s.pods.insert("s".into(), p);
        s.race = Some((
            "s".into(),
            json!({"spec": {"operatingMode": "Suspended"},
                "metadata": {"annotations": {(LIFECYCLE): "ended", (ENDED_REASON): "stopped",
                    (ENDED_AT): now, (CHECKPOINT): "missing"}}}),
        ));
    }
    let r = reconciler(&state, FakeStore::default());
    let summary = pass(&r, &mut Memory::default()).await.unwrap();
    assert_eq!(summary.conflicts, 1);
    assert_eq!(summary.tombstoned, 0);
    assert_eq!(summary.states.get("ended"), Some(&1));
    assert_eq!(writes(&state).len(), 1, "only the losing CAS attempt");
    // The peer's tombstone stands.
    assert_eq!(
        stored(&state, "s")["metadata"]["annotations"][ENDED_REASON],
        "stopped"
    );
}

#[tokio::test]
async fn expired_tombstones_are_deleted_with_uid_and_version_preconditions() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let tombstone = |s: &fake::State, name: &str, ended_ago: i64| {
            sandbox(
                s,
                name,
                ended_ago + 600,
                "Suspended",
                json!({(LIFECYCLE): "ended", (ENDED_AT): ago(s, ended_ago),
                    (ENDED_REASON): "stopped", (CHECKPOINT): "missing"}),
            )
        };
        let old = tombstone(&s, "old", 30 * 24 * 3600);
        let recent = tombstone(&s, "recent", 30 * 24 * 3600 - 1);
        s.sandboxes.insert("old".into(), old);
        s.sandboxes.insert("recent".into(), recent);
    }
    let r = reconciler(&state, FakeStore::default());
    assert_eq!(pass(&r, &mut Memory::default()).await.unwrap().deleted, 1);
    let sent = writes(&state);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].method, http::Method::DELETE);
    assert!(sent[0].path.ends_with("/sandboxes/old"));
    assert_eq!(
        sent[0].body["preconditions"],
        json!({"uid": "sb-old", "resourceVersion": "1"})
    );
    assert!(state.lock().unwrap().sandboxes.contains_key("recent"));
}

#[tokio::test]
async fn foreign_owners_are_left_alone_and_reported_once() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let foreign = sandbox(&s, "s", 5000, "Running", json!({(OWNER): "b".repeat(64)}));
        s.sandboxes.insert("s".into(), foreign);
        // Unmarked objects are not even listed.
        let mut unmarked = sandbox(&s, "u", 5000, "Running", json!({}));
        unmarked["metadata"]["labels"] = json!({});
        s.sandboxes.insert("u".into(), unmarked);
    }
    let r = reconciler(&state, FakeStore::default());
    let mut memory = Memory::default();
    pass(&r, &mut memory).await.unwrap();
    pass(&r, &mut memory).await.unwrap();
    assert!(writes(&state).is_empty());
    let reported = events(&state);
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0]["reason"], "SandboxIgnored");
    assert_eq!(reported[0]["involvedObject"]["name"], "s");
}

#[tokio::test]
async fn without_an_apiserver_date_the_pass_writes_nothing() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        s.no_date = true;
        let sb = sandbox(&s, "s", 5000, "Running", json!({}));
        s.sandboxes.insert("s".into(), sb);
    }
    let r = reconciler(&state, FakeStore::default());
    assert!(pass(&r, &mut Memory::default()).await.is_err());
    assert!(writes(&state).is_empty());
}

#[tokio::test]
async fn dry_run_decides_but_writes_nothing() {
    let state = fake::Shared::default();
    {
        let mut s = state.lock().unwrap();
        let sb = sandbox(&s, "s", 5000, "Running", json!({}));
        s.sandboxes.insert("s".into(), sb);
    }
    let r = Reconciler {
        dry_run: true,
        ..reconciler(&state, FakeStore::default())
    };
    pass(&r, &mut Memory::default()).await.unwrap();
    assert!(writes(&state).is_empty());
    assert!(events(&state).is_empty());
}

#[test]
fn receipt_memory_is_bounded_and_pruned_to_bound_pods() {
    let s = fake::State::default();
    let mut memory = Memory::default();
    for i in 0..MAX_RECEIPTS + 10 {
        observe(
            &mut memory,
            pod(&s, "s", &format!("pod-{i}"), 0, terminated(Some("{}"))),
        );
    }
    assert_eq!(memory.receipts.len(), MAX_RECEIPTS);
    // Oversized and blank messages are not receipts.
    let mut fresh = Memory::default();
    for message in ["x".repeat(MAX_RECEIPT_BYTES + 1), " ".into()] {
        observe(
            &mut fresh,
            pod(&s, "s", "pod-x", 0, terminated(Some(&message))),
        );
    }
    assert!(fresh.receipts.is_empty());
    memory.retain(&HashSet::new(), &HashSet::from(["pod-3".to_string()]));
    assert_eq!(
        memory.receipts.keys().collect::<Vec<_>>(),
        [&"pod-3".to_string()]
    );
}

// ── Never resurrect ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Session {
    owner_matches: bool,
    mode: &'static str,
    lifecycle: Option<&'static str>,
    age: i64,
    ended_ago: Option<i64>,
    bound: bool,
    /// (uid matches the binding, Pod age, container: waiting/running/terminated)
    pod: Option<(bool, i64, u8)>,
    checkpointing: bool,
    receipt: bool,
}

fn any_session() -> impl Strategy<Value = Session> {
    (
        (
            any::<bool>(),
            prop_oneof![Just("Running"), Just("Suspended"), Just("Paused")],
            prop_oneof![
                Just(None),
                Just(Some("ending")),
                Just(Some("ended")),
                Just(Some("bogus"))
            ],
            0i64..3_000_000,
            proptest::option::of(0i64..3_000_000),
        ),
        (
            any::<bool>(),
            proptest::option::of((any::<bool>(), 0i64..3000, 0u8..3)),
            any::<bool>(),
            any::<bool>(),
        ),
    )
        .prop_map(
            |(
                (owner_matches, mode, lifecycle, age, ended_ago),
                (bound, pod, checkpointing, receipt),
            )| {
                Session {
                    owner_matches,
                    mode,
                    lifecycle,
                    age,
                    ended_ago,
                    bound,
                    pod,
                    checkpointing,
                    receipt,
                }
            },
        )
}

fn install(s: &mut fake::State, i: usize, session: &Session) {
    let name = format!("s{i}");
    let mut annotations = json!({
        (CHECKPOINTING): if session.checkpointing { "enabled" } else { "disabled" },
    });
    if !session.owner_matches {
        annotations[OWNER] = "c".repeat(64).into();
    }
    if let Some(lifecycle) = session.lifecycle {
        annotations[LIFECYCLE] = lifecycle.into();
    }
    if let Some(ended) = session.ended_ago {
        annotations[ENDED_AT] = ago(s, ended).into();
    }
    if session.bound {
        annotations[INITIAL_POD] = format!("pod-{i}-a").into();
    }
    let sb = sandbox(s, &name, session.age, session.mode, annotations);
    s.sandboxes.insert(name.clone(), sb);
    if let Some((same_uid, age, container)) = session.pod {
        let uid = if same_uid {
            format!("pod-{i}-a")
        } else {
            format!("pod-{i}-b")
        };
        let container = match container {
            0 => json!({"waiting": {}}),
            1 => json!({"running": {}}),
            _ => terminated(session.receipt.then(|| receipt(&key())).as_deref()),
        };
        let p = pod(s, &name, &uid, age, container);
        s.pods.insert(name, p);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// Over arbitrary namespaces, a pass never creates a Sandbox, never
    /// writes `Running`, only ever writes the tombstone shape to an
    /// owner-matching session that is not already ended, and deletes only
    /// Suspended `ended` tombstones past retention.
    #[test]
    fn no_pass_resurrects_or_discards_a_live_session(
        sessions in proptest::collection::vec(any_session(), 1..6),
        verified in any::<bool>(),
    ) {
        let state = fake::Shared::default();
        let before = {
            let mut s = state.lock().unwrap();
            for (i, session) in sessions.iter().enumerate() {
                install(&mut s, i, session);
            }
            s.sandboxes.clone()
        };
        let store = if verified { store_with(&[key()]) } else { FakeStore::default() };
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let r = reconciler(&state, store);
            pass(&r, &mut Memory::default()).await
        }).unwrap();
        let now = state.lock().unwrap().now;
        for call in writes(&state) {
            let name = call.path.rsplit('/').next().unwrap().to_string();
            match call.method {
                http::Method::PATCH => {
                    let body = &call.body;
                    prop_assert_eq!(&body["spec"], &json!({"operatingMode": "Suspended"}));
                    prop_assert!(body["metadata"]["resourceVersion"].is_string());
                    let annotations = body["metadata"]["annotations"].as_object().unwrap();
                    prop_assert_eq!(&annotations[LIFECYCLE], LIFECYCLE_ENDED);
                    let keys: HashSet<&str> = annotations.keys().map(String::as_str).collect();
                    prop_assert_eq!(keys, HashSet::from([LIFECYCLE, ENDED_REASON, ENDED_AT, CHECKPOINT]));
                    let original = &before[&name];
                    prop_assert_eq!(&original["metadata"]["annotations"][OWNER], &json!(owner()));
                    prop_assert_ne!(&original["metadata"]["annotations"][LIFECYCLE], "ended");
                }
                http::Method::DELETE => {
                    let original = &before[&name];
                    let annotations = &original["metadata"]["annotations"];
                    prop_assert_eq!(&original["spec"]["operatingMode"], "Suspended");
                    prop_assert_eq!(&annotations[LIFECYCLE], "ended");
                    let ended = DateTime::parse_from_rfc3339(annotations[ENDED_AT].as_str().unwrap())
                        .unwrap()
                        .with_timezone(&Utc);
                    prop_assert!(now - ended >= chrono::Duration::days(30));
                }
                ref other => prop_assert!(false, "unexpected write {} {}", other, call.path),
            }
        }
    }
}
