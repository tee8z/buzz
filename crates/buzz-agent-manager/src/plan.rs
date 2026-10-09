//! Pure planning: one classified session plus its checkpoint evidence becomes
//! at most one action.
//!
//! The action vocabulary is deliberately closed. There is no variant that sets
//! `operatingMode: Running` or creates a Sandbox, and [`Action::Delete`] is
//! produced only for an expired tombstone, so the reconciler cannot resurrect
//! a session or discard a live one (property-tested below).

use buzz_backend_kubernetes::lifecycle::{
    EndedReason, Ignored, State, CHECKPOINT_MISSING, CHECKPOINT_UNCONFIGURED,
    MISSING_CHECKPOINT_HOLD_SECS,
};
use chrono::{DateTime, Duration, Utc};

/// What is known about a session's checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Evidence {
    /// Receipt and S3 listing agree on this key.
    Verified(String),
    /// The session was launched without checkpoint-on-stop.
    Unconfigured,
    /// No receipt, or the receipt and S3 disagree.
    Missing,
    /// Missing, but the session was recovered from this (still present) key,
    /// which the tombstone carries forward.
    Inherited(String),
    /// S3 could not be asked; decide on a later pass.
    Unavailable,
}

impl Evidence {
    /// Tombstone `checkpoint` annotation value; `None` while undecidable.
    fn annotation(&self) -> Option<String> {
        match self {
            Self::Verified(key) | Self::Inherited(key) => Some(key.clone()),
            Self::Unconfigured => Some(CHECKPOINT_UNCONFIGURED.into()),
            Self::Missing => Some(CHECKPOINT_MISSING.into()),
            Self::Unavailable => None,
        }
    }
}

/// The single action for one session on one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Nothing to do.
    None,
    /// Leave it alone and say why (Event).
    Ignore(Ignored),
    /// Completed without a checkpoint: hold until `until` (Event + metric).
    Hold { until: DateTime<Utc> },
    /// One CAS merge patch: Suspended + lifecycle annotations.
    Tombstone {
        reason: EndedReason,
        checkpoint: String,
    },
    /// Delete an Ended tombstone past retention (uid + resourceVersion
    /// preconditions).
    Delete,
}

fn tombstone(reason: EndedReason, evidence: &Evidence) -> Action {
    match evidence.annotation() {
        Some(checkpoint) => Action::Tombstone { reason, checkpoint },
        None => Action::None,
    }
}

/// Does planning `state` depend on checkpoint evidence? Only states that
/// tombstone do; everything else is decided without asking S3.
pub fn needs_evidence(state: &State) -> bool {
    matches!(
        state,
        State::Binding { abandoned: true }
            | State::BindingIncomplete { expired: true }
            | State::Ending { .. }
            | State::Completed { .. }
            | State::Replaced
            | State::Lost { confirmed: true }
    )
}

/// Plan one session. `hold_since` is when the completed container finished
/// (falling back to when this manager first saw it completed).
pub fn plan(
    state: &State,
    evidence: &Evidence,
    now: DateTime<Utc>,
    hold_since: DateTime<Utc>,
) -> Action {
    match state {
        State::Ignored(why) => Action::Ignore(*why),
        State::Binding { abandoned: true } => tombstone(EndedReason::BindAbandoned, evidence),
        State::BindingIncomplete { expired: true } => {
            tombstone(EndedReason::BindingFailed, evidence)
        }
        State::Ending { reason } => tombstone(*reason, evidence),
        State::Replaced => tombstone(EndedReason::Replaced, evidence),
        State::Lost { confirmed: true } => tombstone(EndedReason::Lost, evidence),
        State::Completed { finished_at, .. } => match evidence {
            Evidence::Missing | Evidence::Inherited(_) => {
                let until = finished_at.unwrap_or(hold_since)
                    + Duration::seconds(MISSING_CHECKPOINT_HOLD_SECS);
                if now >= until {
                    tombstone(EndedReason::Completed, evidence)
                } else {
                    Action::Hold { until }
                }
            }
            _ => tombstone(EndedReason::Completed, evidence),
        },
        State::Ended { expired: true } => Action::Delete,
        State::Binding { abandoned: false }
        | State::BindingIncomplete { expired: false }
        | State::Active
        | State::Draining
        | State::Lost { confirmed: false }
        | State::Ended { expired: false } => Action::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    fn key() -> Evidence {
        Evidence::Verified("dev/g/x.tar.gz".into())
    }

    #[test]
    fn every_terminal_state_tombstones_with_its_reason() {
        let cases = [
            (
                State::Binding { abandoned: true },
                EndedReason::BindAbandoned,
            ),
            (
                State::BindingIncomplete { expired: true },
                EndedReason::BindingFailed,
            ),
            (State::Replaced, EndedReason::Replaced),
            (State::Lost { confirmed: true }, EndedReason::Lost),
            (
                State::Ending {
                    reason: EndedReason::Stopped,
                },
                EndedReason::Stopped,
            ),
        ];
        for (state, reason) in cases {
            assert_eq!(
                plan(&state, &Evidence::Missing, t(0), t(0)),
                Action::Tombstone {
                    reason,
                    checkpoint: CHECKPOINT_MISSING.into()
                },
                "{state:?}"
            );
            assert_eq!(
                plan(&state, &key(), t(0), t(0)),
                Action::Tombstone {
                    reason,
                    checkpoint: "dev/g/x.tar.gz".into()
                }
            );
            assert_eq!(
                plan(&state, &Evidence::Unavailable, t(0), t(0)),
                Action::None
            );
        }
    }

    #[test]
    fn completed_with_a_checkpoint_or_none_configured_tombstones_now() {
        let completed = State::Completed {
            finished_at: Some(t(0)),
            message: None,
        };
        assert_eq!(
            plan(&completed, &key(), t(1), t(1)),
            Action::Tombstone {
                reason: EndedReason::Completed,
                checkpoint: "dev/g/x.tar.gz".into()
            }
        );
        assert_eq!(
            plan(&completed, &Evidence::Unconfigured, t(1), t(1)),
            Action::Tombstone {
                reason: EndedReason::Completed,
                checkpoint: CHECKPOINT_UNCONFIGURED.into()
            }
        );
        assert_eq!(
            plan(&completed, &Evidence::Unavailable, t(1), t(1)),
            Action::None
        );
    }

    #[test]
    fn completed_without_a_checkpoint_holds_for_24_hours() {
        let completed = State::Completed {
            finished_at: Some(t(0)),
            message: None,
        };
        let until = t(MISSING_CHECKPOINT_HOLD_SECS);
        assert_eq!(
            plan(
                &completed,
                &Evidence::Missing,
                t(MISSING_CHECKPOINT_HOLD_SECS - 1),
                t(5)
            ),
            Action::Hold { until }
        );
        assert_eq!(
            plan(&completed, &Evidence::Missing, until, t(5)),
            Action::Tombstone {
                reason: EndedReason::Completed,
                checkpoint: CHECKPOINT_MISSING.into()
            }
        );
        // Without finishedAt the hold runs from first observation.
        let undated = State::Completed {
            finished_at: None,
            message: None,
        };
        assert_eq!(
            plan(&undated, &Evidence::Missing, until, t(10)),
            Action::Hold {
                until: t(10 + MISSING_CHECKPOINT_HOLD_SECS)
            }
        );
    }

    /// A recovered session that saved nothing of its own still holds, then
    /// carries its restore checkpoint forward instead of `missing`.
    #[test]
    fn completed_with_an_inherited_checkpoint_holds_then_carries_it_forward() {
        let completed = State::Completed {
            finished_at: Some(t(0)),
            message: None,
        };
        let inherited = Evidence::Inherited("dev/g/x.tar.gz".into());
        let until = t(MISSING_CHECKPOINT_HOLD_SECS);
        assert_eq!(
            plan(&completed, &inherited, t(1), t(1)),
            Action::Hold { until }
        );
        assert_eq!(
            plan(&completed, &inherited, until, t(1)),
            Action::Tombstone {
                reason: EndedReason::Completed,
                checkpoint: "dev/g/x.tar.gz".into()
            }
        );
        assert_eq!(
            plan(&State::Lost { confirmed: true }, &inherited, t(0), t(0)),
            Action::Tombstone {
                reason: EndedReason::Lost,
                checkpoint: "dev/g/x.tar.gz".into()
            }
        );
    }

    #[test]
    fn ended_tombstones_are_deleted_only_once_expired() {
        assert_eq!(
            plan(
                &State::Ended { expired: true },
                &Evidence::Missing,
                t(0),
                t(0)
            ),
            Action::Delete
        );
        assert_eq!(
            plan(&State::Ended { expired: false }, &key(), t(0), t(0)),
            Action::None
        );
    }

    fn any_state() -> impl Strategy<Value = State> {
        let reason = prop_oneof![
            Just(EndedReason::Stopped),
            Just(EndedReason::Completed),
            Just(EndedReason::Replaced),
            Just(EndedReason::Lost),
            Just(EndedReason::BindingFailed),
            Just(EndedReason::BindAbandoned),
        ];
        let ignored = prop_oneof![
            Just(Ignored::Unmarked),
            Just(Ignored::ForeignOwner),
            Just(Ignored::ForeignPod),
            Just(Ignored::NotRunning),
            Just(Ignored::Inconsistent),
        ];
        prop_oneof![
            ignored.prop_map(State::Ignored),
            any::<bool>().prop_map(|abandoned| State::Binding { abandoned }),
            any::<bool>().prop_map(|expired| State::BindingIncomplete { expired }),
            Just(State::Active),
            Just(State::Draining),
            reason.prop_map(|reason| State::Ending { reason }),
            (proptest::option::of(0i64..200_000)).prop_map(|at| State::Completed {
                finished_at: at.map(t),
                message: None,
            }),
            Just(State::Replaced),
            any::<bool>().prop_map(|confirmed| State::Lost { confirmed }),
            any::<bool>().prop_map(|expired| State::Ended { expired }),
        ]
    }

    fn any_evidence() -> impl Strategy<Value = Evidence> {
        prop_oneof![
            "[a-z]{1,8}".prop_map(Evidence::Verified),
            "[a-z]{1,8}".prop_map(Evidence::Inherited),
            Just(Evidence::Unconfigured),
            Just(Evidence::Missing),
            Just(Evidence::Unavailable),
        ]
    }

    proptest! {
        /// Never resurrect, never discard a live session: deletion only for
        /// an expired tombstone, nothing at all for live or ignored sessions,
        /// and tombstones always carry a decided checkpoint value.
        #[test]
        fn no_plan_resurrects_or_deletes_a_live_session(
            state in any_state(),
            evidence in any_evidence(),
            now in 0i64..400_000,
            since in 0i64..400_000,
        ) {
            let action = plan(&state, &evidence, t(now), t(since));
            if action == Action::Delete {
                prop_assert_eq!(&state, &State::Ended { expired: true });
            }
            if matches!(state, State::Active | State::Draining | State::Ended { .. })
                || matches!(state, State::Ignored(_))
            {
                prop_assert!(matches!(action, Action::None | Action::Delete | Action::Ignore(_)));
            }
            if let Action::Tombstone { checkpoint, .. } = &action {
                prop_assert!(!checkpoint.is_empty());
                prop_assert_ne!(&evidence, &Evidence::Unavailable);
            }
            // States decided without evidence really ignore it, so the
            // reconciler may skip the S3 probe for them.
            if !needs_evidence(&state) {
                prop_assert_eq!(&action, &plan(&state, &Evidence::Missing, t(now), t(since)));
            }
        }
    }
}
