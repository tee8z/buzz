//! Bounded, content-minimized activity records for platform log collection.
//!
//! The encrypted observer feed retains its original payload. The platform feed
//! records operation metadata and output sizes; prompts, raw results, credentials,
//! and arbitrary adapter-provided titles never enter this stream.

use std::collections::HashMap;
use std::time::Instant;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::observer::ObserverEvent;

const MAX_ACTIVE_TOOLS: usize = 256;

pub(crate) struct ActivityRecorder {
    source_id: String,
    sequence: u64,
    tools: HashMap<(String, String), (Instant, Option<String>)>,
}

impl ActivityRecorder {
    pub(crate) fn new() -> Self {
        Self {
            source_id: uuid::Uuid::new_v4().to_string(),
            sequence: 0,
            tools: HashMap::new(),
        }
    }

    pub(crate) fn record(&mut self, event: &ObserverEvent) -> Option<Value> {
        let mut details = json!({});
        let kind = match event.kind.as_str() {
            "checkpoint_started" | "checkpoint_failed" => event.kind.as_str(),
            "checkpoint_saved" => {
                details = event.payload.clone();
                event.kind.as_str()
            }
            "harness_started" | "harness_stopped" | "turn_started" | "turn_completed" => {
                if event.kind == "harness_stopped" {
                    details["unfinished_tools"] = self.tools.len().into();
                    self.tools.clear();
                } else if event.kind == "turn_completed" {
                    let before = self.tools.len();
                    self.tools.retain(|_, (_, turn)| turn != &event.turn_id);
                    details["unfinished_tools"] = (before - self.tools.len()).into();
                    if let Some(duration_ms) = event.payload["duration_ms"].as_u64() {
                        details["duration_ms"] = duration_ms.into();
                    }
                }
                event.kind.as_str()
            }
            "acp_read" if event.payload["method"] == "session/update" => {
                let update = &event.payload["params"]["update"];
                let update_kind = update["sessionUpdate"].as_str()?;
                if !matches!(update_kind, "tool_call" | "tool_call_update") {
                    return None;
                }
                let tool_id = opaque_id(update["toolCallId"].as_str().unwrap_or("missing"));
                let key = (
                    opaque_id(session_id(event).unwrap_or("missing")),
                    tool_id.clone(),
                );
                details["tool_id"] = tool_id.into();
                details["kind"] = update["kind"]
                    .as_str()
                    .filter(|value| {
                        matches!(
                            *value,
                            "read"
                                | "edit"
                                | "delete"
                                | "move"
                                | "search"
                                | "execute"
                                | "think"
                                | "fetch"
                                | "switch_mode"
                        )
                    })
                    .unwrap_or("other")
                    .into();
                let status = update["status"]
                    .as_str()
                    .filter(|value| {
                        matches!(*value, "pending" | "in_progress" | "completed" | "failed")
                    })
                    .unwrap_or("unknown");
                details["status"] = status.into();
                details["arguments"] = command_summary(&update["rawInput"]);
                details["output"] = json!({
                    "redacted": true,
                    "bytes": update.get("content").map(|v| v.to_string().len()).unwrap_or(0),
                    "exit_code": update["rawOutput"]["exitCode"].as_i64(),
                });
                if update_kind == "tool_call" {
                    if self.tools.len() < MAX_ACTIVE_TOOLS {
                        self.tools
                            .entry(key.clone())
                            .or_insert_with(|| (Instant::now(), event.turn_id.clone()));
                    } else {
                        details["timing_capacity_exceeded"] = true.into();
                    }
                }
                if matches!(status, "completed" | "failed") {
                    if let Some((start, _)) = self.tools.remove(&key) {
                        details["duration_ms"] = json!(start.elapsed().as_millis());
                    }
                    "tool_completed"
                } else if update_kind == "tool_call" {
                    "tool_started"
                } else {
                    "tool_updated"
                }
            }
            _ => return None,
        };
        self.sequence += 1;
        Some(json!({
            "schema": "buzz.activity.v1",
            "source_id": self.source_id,
            "event_id": format!("{}:{}", self.source_id, self.sequence),
            "sequence": self.sequence,
            "timestamp": event.timestamp,
            "event": kind,
            "agent_index": event.agent_index,
            "channel_id": event.channel_id.as_deref().map(opaque_id),
            "acp_session_id": session_id(event).map(opaque_id),
            "turn_id": event.turn_id.as_deref().map(opaque_id),
            "details": details,
        }))
    }
}

fn session_id(event: &ObserverEvent) -> Option<&str> {
    event
        .session_id
        .as_deref()
        .or_else(|| event.payload["params"]["sessionId"].as_str())
}

fn opaque_id(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn command_summary(input: &Value) -> Value {
    // Log only known command names. Arbitrary arguments may contain secrets,
    // SQL statements, environment assignments, or embedded shell expansions.
    let command = input["command"].as_str().unwrap_or("");
    let mut words = command.split_whitespace();
    let program = words
        .next()
        .filter(|value| {
            matches!(
                *value,
                "cargo"
                    | "git"
                    | "go"
                    | "python"
                    | "python3"
                    | "node"
                    | "npm"
                    | "pnpm"
                    | "yarn"
                    | "just"
                    | "make"
                    | "via"
                    | "gh"
                    | "psql"
                    | "sqlite3"
            )
        })
        .unwrap_or("other");
    let operation = words
        .next()
        .filter(|value| {
            matches!(
                *value,
                "build"
                    | "test"
                    | "check"
                    | "clippy"
                    | "fmt"
                    | "clone"
                    | "fetch"
                    | "commit"
                    | "push"
                    | "status"
                    | "diff"
                    | "install"
                    | "run"
                    | "api"
            )
        })
        .unwrap_or("other");
    json!({"program": program, "operation": operation, "remaining_arguments": "[redacted]"})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(payload: Value) -> ObserverEvent {
        ObserverEvent {
            seq: 1,
            timestamp: "2026-10-08T12:00:00Z".into(),
            kind: "acp_read".into(),
            agent_index: Some(0),
            channel_id: Some("channel".into()),
            session_id: Some("session".into()),
            turn_id: Some("turn".into()),
            started_at: None,
            payload,
        }
    }

    #[test]
    fn tool_records_are_correlated_and_exclude_raw_content() {
        let mut recorder = ActivityRecorder::new();
        let mut e = event(json!({"method":"session/update","params":{"update":{
            "sessionUpdate":"tool_call", "toolCallId":"secret-id", "kind":"execute",
            "title":"SECRET-TITLE", "status":"in_progress",
            "rawInput":{"command":"cargo test SECRET-ARG", "password":"SECRET-PASSWORD"},
            "content":[{"text":"SECRET-RESULT"}]
        }}}));
        let start = recorder.record(&e).unwrap();
        e.payload["params"]["update"]["sessionUpdate"] = "tool_call_update".into();
        e.payload["params"]["update"]["status"] = "completed".into();
        e.payload["params"]["update"]["rawOutput"] = json!({"exitCode":0});
        let finish = recorder.record(&e).unwrap();
        assert_eq!(start["event"], "tool_started");
        assert_eq!(finish["event"], "tool_completed");
        assert_eq!(start["details"]["tool_id"], finish["details"]["tool_id"]);
        assert_eq!(finish["sequence"], 2);
        assert!(finish["details"]["duration_ms"].is_number());
        assert_eq!(finish["details"]["output"]["exit_code"], 0);
        assert_eq!(start["details"]["arguments"]["program"], "cargo");
        for value in [start, finish] {
            let serialized = value.to_string();
            assert!(!serialized.contains("SECRET"));
            assert!(!serialized.contains("secret-id"));
        }
    }

    #[test]
    fn prompt_and_agent_text_are_never_platform_records() {
        let mut recorder = ActivityRecorder::new();
        let mut e = event(json!({"method":"session/update","params":{"update":{
            "sessionUpdate":"agent_message_chunk", "content":{"text":"SECRET"}
        }}}));
        assert!(recorder.record(&e).is_none());
        e.kind = "acp_write".into();
        e.payload = json!({"method":"session/prompt","params":{"prompt":"SECRET"}});
        assert!(recorder.record(&e).is_none());
    }

    #[test]
    fn turn_completion_reports_only_its_duration() {
        let mut recorder = ActivityRecorder::new();
        let mut completed = event(json!({"duration_ms": 4200, "prompt": "synthetic secret"}));
        completed.kind = "turn_completed".into();
        let record = recorder.record(&completed).unwrap();
        assert_eq!(record["details"]["duration_ms"], 4200);
        assert_eq!(record["details"]["unfinished_tools"], 0);
        assert!(!record.to_string().contains("synthetic secret"));
        let mut legacy = event(json!({}));
        legacy.kind = "turn_completed".into();
        assert!(recorder.record(&legacy).unwrap()["details"]
            .get("duration_ms")
            .is_none());
    }

    #[test]
    fn active_tool_tracking_is_bounded() {
        let mut recorder = ActivityRecorder::new();
        let mut last = Value::Null;
        for id in 0..=MAX_ACTIVE_TOOLS {
            last = recorder
                .record(&event(
                    json!({"method":"session/update","params":{"update":{
                        "sessionUpdate":"tool_call", "toolCallId":id.to_string()
                    }}}),
                ))
                .unwrap();
        }
        assert_eq!(recorder.tools.len(), MAX_ACTIVE_TOOLS);
        assert_eq!(last["details"]["timing_capacity_exceeded"], true);
    }

    #[derive(Clone)]
    struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_acp_read_loop_emits_only_sanitized_activity_when_enabled() {
        use tracing::instrument::WithSubscriber;
        let output = Capture(Default::default());
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
        async {
            for enabled in [false, true] {
                let script = r#"
read -r request
printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call","toolCallId":"tool-1","kind":"execute","status":"in_progress","title":"DO-NOT-LOG-TITLE","rawInput":{"command":"cargo test DO-NOT-LOG-ARG"}}}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call_update","toolCallId":"tool-1","status":"completed","content":[{"text":"DO-NOT-LOG-RESULT"}],"rawOutput":{"exitCode":0}}}}'
printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":2,"agentCapabilities":{},"authMethods":[]}}'
"#;
                let mut client = crate::acp::AcpClient::spawn(
                    "bash", &["-c".into(), script.into()], &[], false,
                ).await.unwrap();
                let observer = if enabled {
                    crate::observer::ObserverHandle::with_activity_log()
                } else {
                    crate::observer::ObserverHandle::in_process()
                };
                client.set_observer(Some(observer), 0);
                client.initialize().await.unwrap();
                client.shutdown().await;
            }
        }.with_subscriber(subscriber).await;
        let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        let records: Vec<Value> = text
            .lines()
            .filter_map(|line| {
                let envelope: Value = serde_json::from_str(line).ok()?;
                if envelope["fields"]["schema"] != "buzz.activity.v1" {
                    return None;
                }
                Some(serde_json::from_str(envelope["fields"]["record"].as_str()?).unwrap())
            })
            .collect();
        assert_eq!(
            records.len(),
            2,
            "only the enabled adapter emits platform records"
        );
        assert_eq!(records[0]["event"], "tool_started");
        assert_eq!(records[1]["event"], "tool_completed");
        assert!(!serde_json::to_string(&records)
            .unwrap()
            .contains("DO-NOT-LOG"));
    }
}
