use super::*;
use serde_json::json;

const OWNER_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";

fn t(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
}

/// A Running, managed, owner-matching Sandbox created at t(0) with nothing else.
fn base(now: i64) -> Observation {
    Observation {
        now: t(now),
        managed: true,
        binding_version: Some("2".into()),
        owner: Some(OWNER_HEX.into()),
        created: Some(t(0)),
        deleting: false,
        operating_mode: OperatingMode::Running,
        lifecycle: Lifecycle::Live,
        ended_reason: None,
        ended_at: None,
        initial_pod: None,
        pod: None,
        pod_missing_since: None,
    }
}

fn pod(uid: &str, container: Container) -> PodObservation {
    PodObservation {
        uid: uid.into(),
        created: Some(t(0)),
        deleting: false,
        owned: true,
        container,
    }
}

fn bound(now: i64, container: Container) -> Observation {
    Observation {
        initial_pod: Some("uid-1".into()),
        pod: Some(pod("uid-1", container)),
        ..base(now)
    }
}

fn classify_one(o: &Observation) -> State {
    classify(o, OWNER_HEX)
}

#[test]
fn binding_and_bind_abandoned_split_at_the_bind_timeout() {
    assert_eq!(
        classify_one(&base(BIND_TIMEOUT_SECS - 1)),
        State::Binding { abandoned: false }
    );
    assert_eq!(
        classify_one(&base(BIND_TIMEOUT_SECS)),
        State::Binding { abandoned: true }
    );
    // An unbound Sandbox whose Pod exists is still Binding.
    let with_pod = Observation {
        pod: Some(pod("uid-1", Container::Running)),
        ..base(5)
    };
    assert_eq!(classify_one(&with_pod), State::Binding { abandoned: false });
    // No creation timestamp never ages into abandonment.
    let undated = Observation {
        created: None,
        ..base(BIND_TIMEOUT_SECS * 10)
    };
    assert_eq!(classify_one(&undated), State::Binding { abandoned: false });
}

#[test]
fn binding_incomplete_expires_on_pod_age() {
    assert_eq!(
        classify_one(&bound(BIND_TIMEOUT_SECS - 1, Container::NeverStarted)),
        State::BindingIncomplete { expired: false }
    );
    assert_eq!(
        classify_one(&bound(BIND_TIMEOUT_SECS, Container::NeverStarted)),
        State::BindingIncomplete { expired: true }
    );
}

#[test]
fn active_completed_and_replaced() {
    assert_eq!(classify_one(&bound(10, Container::Running)), State::Active);
    let terminated = Container::Terminated {
        finished_at: Some(t(5)),
        message: Some("m".into()),
    };
    assert_eq!(
        classify_one(&bound(10, terminated)),
        State::Completed {
            finished_at: Some(t(5)),
            message: Some("m".into())
        }
    );
    let replaced = Observation {
        pod: Some(pod("uid-2", Container::Running)),
        ..bound(10, Container::Running)
    };
    assert_eq!(classify_one(&replaced), State::Replaced);
}

#[test]
fn lost_is_confirmed_only_after_the_grace() {
    let missing = |now, since| Observation {
        pod: None,
        pod_missing_since: since,
        ..bound(now, Container::Running)
    };
    assert_eq!(
        classify_one(&missing(100, None)),
        State::Lost { confirmed: false }
    );
    assert_eq!(
        classify_one(&missing(100, Some(t(100 - LOST_GRACE_SECS + 1)))),
        State::Lost { confirmed: false }
    );
    assert_eq!(
        classify_one(&missing(100, Some(t(100 - LOST_GRACE_SECS)))),
        State::Lost { confirmed: true }
    );
}

#[test]
fn draining_covers_deleting_objects_and_ending_with_a_pod() {
    let mut deleting_pod = bound(10, Container::Running);
    deleting_pod.pod.as_mut().unwrap().deleting = true;
    assert_eq!(classify_one(&deleting_pod), State::Draining);
    let deleting_sandbox = Observation {
        deleting: true,
        ..bound(10, Container::Running)
    };
    assert_eq!(classify_one(&deleting_sandbox), State::Draining);
    let ending = Observation {
        lifecycle: Lifecycle::Ending,
        operating_mode: OperatingMode::Suspended,
        ..bound(10, Container::Running)
    };
    assert_eq!(classify_one(&ending), State::Draining);
    let ended_pod = Observation {
        pod: None,
        ended_reason: Some(EndedReason::Stopped),
        ..ending
    };
    assert_eq!(
        classify_one(&ended_pod),
        State::Ending {
            reason: EndedReason::Stopped
        }
    );
}

#[test]
fn ended_tombstones_expire_after_thirty_days_and_never_without_a_date() {
    let ended = |now, at| Observation {
        operating_mode: OperatingMode::Suspended,
        lifecycle: Lifecycle::Ended,
        ended_at: at,
        ..base(now)
    };
    assert_eq!(
        classify_one(&ended(ENDED_RETENTION_SECS - 1, Some(t(0)))),
        State::Ended { expired: false }
    );
    assert_eq!(
        classify_one(&ended(ENDED_RETENTION_SECS, Some(t(0)))),
        State::Ended { expired: true }
    );
    assert_eq!(
        classify_one(&ended(ENDED_RETENTION_SECS * 2, None)),
        State::Ended { expired: false }
    );
    // `ended` while Running is somebody else's edit: hands off.
    let running = Observation {
        operating_mode: OperatingMode::Running,
        ..ended(ENDED_RETENTION_SECS * 2, Some(t(0)))
    };
    assert_eq!(
        classify_one(&running),
        State::Ignored(Ignored::Inconsistent)
    );
}

#[test]
fn foreign_unmarked_and_owner_mismatched_objects_are_ignored() {
    let unmarked = Observation {
        managed: false,
        ..base(BIND_TIMEOUT_SECS * 2)
    };
    assert_eq!(classify_one(&unmarked), State::Ignored(Ignored::Unmarked));
    let bare_pod_binding = Observation {
        binding_version: Some("1".into()),
        ..base(BIND_TIMEOUT_SECS * 2)
    };
    assert_eq!(
        classify_one(&bare_pod_binding),
        State::Ignored(Ignored::Unmarked)
    );
    for owner in [None, Some("2".repeat(64))] {
        let foreign = Observation {
            owner,
            ..base(BIND_TIMEOUT_SECS * 2)
        };
        assert_eq!(
            classify_one(&foreign),
            State::Ignored(Ignored::ForeignOwner)
        );
    }
    let mut foreign_pod = bound(10, Container::Running);
    foreign_pod.pod.as_mut().unwrap().owned = false;
    assert_eq!(
        classify_one(&foreign_pod),
        State::Ignored(Ignored::ForeignPod)
    );
    let suspended = Observation {
        operating_mode: OperatingMode::Suspended,
        ..base(BIND_TIMEOUT_SECS * 2)
    };
    assert_eq!(
        classify_one(&suspended),
        State::Ignored(Ignored::NotRunning)
    );
}

#[test]
fn observation_reads_live_objects() {
    let sandbox: DynamicObject = serde_json::from_value(json!({
        "apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox",
        "metadata": {"name": "s", "uid": "sandbox-uid",
            "creationTimestamp": "2027-01-15T08:00:00Z",
            "labels": {(LABEL_MANAGED_BY): MANAGED_BY, (LABEL_BINDING_VERSION): "2"},
            "annotations": {(OWNER): OWNER_HEX, (INITIAL_POD): "pod-uid",
                (LIFECYCLE): "ending", (ENDED_REASON): "stopped"}},
        "spec": {"operatingMode": "Suspended"}
    }))
    .unwrap();
    let pod: Pod = serde_json::from_value(json!({
        "metadata": {"name": "s", "uid": "pod-uid", "ownerReferences": [{
            "apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox",
            "name": "s", "uid": "sandbox-uid", "controller": true}]},
        "status": {"containerStatuses": [{"name": "agent", "image": "i", "imageID": "",
            "ready": false, "restartCount": 0,
            "state": {"terminated": {"exitCode": 0, "message": "{}",
                "finishedAt": "2027-01-15T09:00:00Z"}}}]}
    }))
    .unwrap();
    let observed = Observation::from_objects(&sandbox, Some(&pod), t(0), None);
    assert!(observed.managed);
    assert_eq!(observed.ended_reason, Some(EndedReason::Stopped));
    let pod = observed.pod.as_ref().unwrap();
    assert!(pod.owned);
    assert!(matches!(
        &pod.container,
        Container::Terminated { message: Some(m), finished_at: Some(_) } if m == "{}"
    ));
    assert_eq!(classify_one(&observed), State::Draining);
}

#[test]
fn receipts_bind_to_the_session_prefix() {
    let prefix = "dev/0123456789abcdef0123456789abcdef/";
    let key = format!("{prefix}6f9619ff-8b86-d011-b42d-00cf4fc964ff.tar.gz");
    let receipt = json!({"version": 1, "checkpoint": key}).to_string();
    assert_eq!(receipt_key(&receipt, prefix).as_deref(), Some(key.as_str()));
    // Another generation, another developer, traversal, nesting, junk.
    for bad in [
        json!({"version": 1, "checkpoint": "dev/ffffffffffffffffffffffffffffffff/x.tar.gz"}),
        json!({"version": 1, "checkpoint": "other/0123456789abcdef0123456789abcdef/x.tar.gz"}),
        json!({"version": 1, "checkpoint": format!("{prefix}../x.tar.gz")}),
        json!({"version": 1, "checkpoint": format!("{prefix}a/b.tar.gz")}),
        json!({"version": 1, "checkpoint": prefix}),
        json!({"version": 2, "checkpoint": key}),
        json!({"version": 1, "checkpoint": key, "extra": true}),
    ] {
        assert_eq!(receipt_key(&bad.to_string(), prefix), None, "{bad}");
    }
    assert_eq!(receipt_key("not json", prefix), None);
    assert_eq!(receipt_key(&receipt, ""), None);
}

#[test]
fn reasons_round_trip_and_checkpoint_markers_are_not_keys() {
    for reason in [
        EndedReason::Stopped,
        EndedReason::Completed,
        EndedReason::Replaced,
        EndedReason::Lost,
        EndedReason::BindingFailed,
        EndedReason::BindAbandoned,
    ] {
        assert_eq!(EndedReason::parse(reason.as_str()), Some(reason));
    }
    assert_eq!(EndedReason::parse("bogus"), None);
    assert!(is_checkpoint_key("dev/g/x.tar.gz"));
    assert!(!is_checkpoint_key(CHECKPOINT_MISSING));
    assert!(!is_checkpoint_key(CHECKPOINT_UNCONFIGURED));
    assert!(!is_checkpoint_key(""));
}

#[test]
fn unknown_lifecycle_and_operating_mode_values_are_left_alone() {
    let unknown_lifecycle = Observation {
        lifecycle: Lifecycle::Unrecognized,
        ..bound(10, Container::Running)
    };
    assert_eq!(
        classify_one(&unknown_lifecycle),
        State::Ignored(Ignored::Inconsistent)
    );
    let unknown_mode = Observation {
        operating_mode: OperatingMode::Other,
        ..bound(10, Container::Running)
    };
    assert_eq!(
        classify_one(&unknown_mode),
        State::Ignored(Ignored::NotRunning)
    );
    let mut ending_foreign_pod = Observation {
        lifecycle: Lifecycle::Ending,
        operating_mode: OperatingMode::Suspended,
        ..bound(10, Container::Running)
    };
    ending_foreign_pod.pod.as_mut().unwrap().owned = false;
    assert_eq!(
        classify_one(&ending_foreign_pod),
        State::Ignored(Ignored::ForeignPod)
    );
}

#[test]
fn lifecycle_and_operating_mode_read_their_wire_spellings() {
    let object = |annotations: serde_json::Value, mode: serde_json::Value| -> DynamicObject {
        serde_json::from_value(json!({
            "apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox",
            "metadata": {"name": "s", "annotations": annotations},
            "spec": {"operatingMode": mode}
        }))
        .unwrap()
    };
    let cases = [
        (json!({}), Lifecycle::Live),
        (json!({(LIFECYCLE): "ending"}), Lifecycle::Ending),
        (json!({(LIFECYCLE): "ended"}), Lifecycle::Ended),
        (json!({(LIFECYCLE): "Ended"}), Lifecycle::Unrecognized),
    ];
    for (annotations, expected) in cases {
        assert_eq!(
            Lifecycle::of(&object(annotations, json!("Running"))),
            expected
        );
    }
    for (mode, expected) in [
        (json!("Running"), OperatingMode::Running),
        (json!("Suspended"), OperatingMode::Suspended),
        (json!("Paused"), OperatingMode::Other),
        (serde_json::Value::Null, OperatingMode::Other),
    ] {
        assert_eq!(OperatingMode::of(&object(json!({}), mode)), expected);
    }
}
