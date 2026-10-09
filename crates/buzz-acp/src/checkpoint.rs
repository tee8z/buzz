//! Checkpoint the workspace after all adapter processes have stopped.
use std::path::Path;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::observer::{ObserverContext, ObserverHandle};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u32,
    key: String,
    sha256: String,
    repositories: u64,
    bytes: u64,
}

fn receipt(bytes: &[u8]) -> Result<serde_json::Value> {
    ensure!(bytes.len() <= 2048, "checkpoint receipt exceeds budget");
    let saved: Receipt = serde_json::from_slice(bytes).context("invalid checkpoint receipt")?;
    ensure!(
        saved.version == 1
            && saved.sha256.len() == 64
            && saved.sha256.bytes().all(|b| b.is_ascii_hexdigit())
            && saved.key.len() <= 200
            && !saved.key.contains("..")
            && saved
                .key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-/._".contains(&b)),
        "invalid checkpoint binding"
    );
    Ok(serde_json::json!({"key":saved.key,"sha256":saved.sha256,
        "repositories":saved.repositories,"bytes":saved.bytes}))
}

/// Kubernetes container termination-message path. The kubelet mounts it into
/// every container; the in-cluster manager reads the receipt back from the
/// container status (`docs/remote-agents.md` §Sandbox lifecycle).
const TERMINATION_LOG: &str = "/dev/termination-log";

pub(crate) async fn save(observer: Option<&ObserverHandle>) -> Result<()> {
    let mut command = Command::new("buzz-agent-checkpoint");
    command.arg("save");
    save_with_command(
        observer,
        command,
        Duration::from_secs(120),
        Path::new(TERMINATION_LOG),
    )
    .await
}

/// Record the saved checkpoint key as the container's termination message:
/// `{"version":1,"checkpoint":"<key>"}`. Best effort and never created: the
/// file exists only where the kubelet mounted it, so a local run (no file, or
/// an unwritable one) does nothing.
fn write_termination_receipt(path: &Path, key: &str) {
    let receipt = serde_json::json!({"version": 1, "checkpoint": key}).to_string();
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
    {
        let _ = std::io::Write::write_all(&mut file, receipt.as_bytes());
    }
}

async fn save_with_command(
    observer: Option<&ObserverHandle>,
    mut command: Command,
    timeout: Duration,
    termination_log: &Path,
) -> Result<()> {
    let emit = |kind, details| {
        if let Some(observer) = observer {
            observer.emit(kind, None, &ObserverContext::default(), details);
        }
    };
    emit("checkpoint_started", serde_json::json!({}));
    command
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    command.process_group(0);
    let saved = run_checkpoint(command, timeout).await;
    match saved {
        Ok(details) => {
            if let Some(key) = details["key"].as_str() {
                write_termination_receipt(termination_log, key);
            }
            emit("checkpoint_saved", details);
            Ok(())
        }
        Err(error) => {
            emit("checkpoint_failed", serde_json::json!({}));
            Err(error)
        }
    }
}

async fn run_checkpoint(mut command: Command, timeout: Duration) -> Result<serde_json::Value> {
    let mut child = command
        .spawn()
        .context("could not start workspace checkpoint")?;
    let stdout = child
        .stdout
        .take()
        .context("checkpoint output is missing")?;
    let result = tokio::time::timeout(timeout, async {
        let mut output = Vec::new();
        stdout.take(2049).read_to_end(&mut output).await?;
        ensure!(output.len() <= 2048, "checkpoint receipt exceeds budget");
        ensure!(child.wait().await?.success(), "checkpoint command failed");
        receipt(&output)
    })
    .await;
    #[cfg(unix)]
    if !matches!(&result, Ok(Ok(_))) {
        if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
    match result {
        Ok(Ok(details)) => Ok(details),
        _ => Err(anyhow::anyhow!(
            "checkpoint failed; retain the Sandbox workspace for explicit recovery"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NO_LOG_PATH: &str = "/nonexistent/termination-log";

    #[cfg(unix)]
    #[tokio::test]
    async fn oversized_receipt_is_rejected_before_the_helper_finishes() {
        let mut command = Command::new("sh");
        command.args(["-c", "head -c 2049 /dev/zero; sleep 60"]);
        let started = std::time::Instant::now();
        assert!(save_with_command(
            None,
            command,
            Duration::from_secs(10),
            Path::new(NO_LOG_PATH)
        )
        .await
        .is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn receipt_cannot_smuggle_raw_output_into_activity() {
        let valid = serde_json::json!({"version":1,"key":"developer/session/id.tar.gz",
            "sha256":"a".repeat(64),"repositories":2,"bytes":100});
        assert!(receipt(valid.to_string().as_bytes()).is_ok());
        let mut extra = valid.clone();
        extra["raw_output"] = "synthetic secret".into();
        assert!(receipt(extra.to_string().as_bytes()).is_err());
        let mut invalid = valid;
        invalid["key"] = "../another-developer".into();
        assert!(receipt(invalid.to_string().as_bytes()).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn helper_failures_never_emit_a_saved_checkpoint() {
        for script in ["exit 1", "printf 'not a receipt'", "sleep 60 & wait"] {
            let observer = ObserverHandle::in_process();
            let mut command = Command::new("sh");
            command.args(["-c", script]);
            assert!(save_with_command(
                Some(&observer),
                command,
                Duration::from_millis(100),
                Path::new(NO_LOG_PATH)
            )
            .await
            .is_err());
            let events = observer.snapshot();
            assert_eq!(
                events
                    .iter()
                    .map(|event| event.kind.as_str())
                    .collect::<Vec<_>>(),
                ["checkpoint_started", "checkpoint_failed"]
            );
        }
        let observer = ObserverHandle::in_process();
        let command = Command::new("/nonexistent/checkpoint-helper");
        assert!(save_with_command(
            Some(&observer),
            command,
            Duration::from_secs(1),
            Path::new(NO_LOG_PATH)
        )
        .await
        .is_err());
        assert_eq!(
            observer.snapshot().last().unwrap().kind,
            "checkpoint_failed"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn helper_success_emits_verified_receipt_after_exit() {
        let observer = ObserverHandle::in_process();
        let valid = serde_json::json!({"version":1,"key":"developer/session/id.tar.gz",
            "sha256":"a".repeat(64),"repositories":2,"bytes":100})
        .to_string();
        let mut command = Command::new("sh");
        command.args(["-c", "printf '%s' \"$1\"", "checkpoint-test", &valid]);
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("termination-log");
        std::fs::write(&log, "stale").unwrap();
        save_with_command(Some(&observer), command, Duration::from_secs(1), &log)
            .await
            .unwrap();
        let events = observer.snapshot();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "checkpoint_started");
        assert_eq!(events[1].kind, "checkpoint_saved");
        assert_eq!(events[1].payload["key"], "developer/session/id.tar.gz");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(&log).unwrap())
                .unwrap(),
            serde_json::json!({"version": 1, "checkpoint": "developer/session/id.tar.gz"})
        );
    }

    /// Local runs have no kubelet-mounted file; the receipt must never create
    /// one, and a failed checkpoint must never write one.
    #[cfg(unix)]
    #[tokio::test]
    async fn receipt_is_never_created_and_never_written_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let absent = dir.path().join("termination-log");
        let valid = serde_json::json!({"version":1,"key":"developer/session/id.tar.gz",
            "sha256":"a".repeat(64),"repositories":2,"bytes":100})
        .to_string();
        let mut command = Command::new("sh");
        command.args(["-c", "printf '%s' \"$1\"", "checkpoint-test", &valid]);
        save_with_command(None, command, Duration::from_secs(1), &absent)
            .await
            .unwrap();
        assert!(!absent.exists());
        let present = dir.path().join("present");
        std::fs::write(&present, "").unwrap();
        let mut failing = Command::new("sh");
        failing.args(["-c", "exit 1"]);
        assert!(
            save_with_command(None, failing, Duration::from_secs(1), &present)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read_to_string(&present).unwrap(), "");
    }
}
