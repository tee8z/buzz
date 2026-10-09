//! Checkpoint the workspace after all adapter processes have stopped, and in
//! the background after completed turns.
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
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

type SaveFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Background checkpoints after completed turns. A node lost without warning
/// sends no SIGTERM, so the save on stop never runs; these bound the loss to
/// the work since the last one. One save runs at a time, at most one starts
/// per `interval`, and the save on stop still runs after the adapters exit.
pub(crate) struct TurnCheckpoints {
    interval: Duration,
    start: Box<dyn FnMut() -> SaveFuture + Send>,
    last_started: Option<tokio::time::Instant>,
    running: Option<tokio::task::JoinHandle<()>>,
}

impl TurnCheckpoints {
    /// Saves through `buzz-agent-checkpoint save`; `interval` zero disables.
    pub(crate) fn new(interval: Duration, observer: Option<ObserverHandle>) -> Self {
        Self::with_start(
            interval,
            Box::new(move || {
                let observer = observer.clone();
                Box::pin(async move {
                    if let Err(error) = save(observer.as_ref()).await {
                        tracing::warn!(%error, "turn checkpoint failed");
                    }
                })
            }),
        )
    }

    fn with_start(interval: Duration, start: Box<dyn FnMut() -> SaveFuture + Send>) -> Self {
        Self {
            interval,
            start,
            last_started: None,
            running: None,
        }
    }

    /// Start a background save after a completed turn, unless one is running
    /// or the last one started less than `interval` ago.
    pub(crate) fn after_turn(&mut self) {
        if self.interval.is_zero() || self.running.as_ref().is_some_and(|t| !t.is_finished()) {
            return;
        }
        let now = tokio::time::Instant::now();
        if self
            .last_started
            .is_some_and(|at| now.duration_since(at) < self.interval)
        {
            return;
        }
        self.last_started = Some(now);
        self.running = Some(tokio::spawn((self.start)()));
    }

    /// Before the save on stop: let a running save finish within `bound`,
    /// otherwise cancel it (its helper process group is killed).
    pub(crate) async fn settle(&mut self, bound: Duration) {
        if let Some(mut task) = self.running.take() {
            if tokio::time::timeout(bound, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
    }
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
    // Kill the helper's whole process group (gitleaks, tar, aws) if the save
    // fails, times out, or is cancelled; disarmed only on success.
    #[cfg(unix)]
    let mut group = KillGroupOnDrop(child.id().and_then(|id| i32::try_from(id).ok()));
    let result = tokio::time::timeout(timeout, async {
        let mut output = Vec::new();
        stdout.take(2049).read_to_end(&mut output).await?;
        ensure!(output.len() <= 2048, "checkpoint receipt exceeds budget");
        ensure!(child.wait().await?.success(), "checkpoint command failed");
        receipt(&output)
    })
    .await;
    #[cfg(unix)]
    if matches!(&result, Ok(Ok(_))) {
        group.0 = None;
    }
    match result {
        Ok(Ok(details)) => Ok(details),
        _ => Err(anyhow::anyhow!(
            "checkpoint failed; retain the Sandbox workspace for explicit recovery"
        )),
    }
}

#[cfg(unix)]
struct KillGroupOnDrop(Option<i32>);

#[cfg(unix)]
impl Drop for KillGroupOnDrop {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
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

    fn counting(
        interval: Duration,
        hold: Duration,
    ) -> (
        TurnCheckpoints,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let started = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = started.clone();
        let checkpoints = TurnCheckpoints::with_start(
            interval,
            Box::new(move || {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(tokio::time::sleep(hold))
            }),
        );
        (checkpoints, started)
    }

    async fn settle_tasks() {
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn turn_checkpoints_are_single_flight_and_rate_limited() {
        use std::sync::atomic::Ordering::SeqCst;
        let (mut checkpoints, started) =
            counting(Duration::from_secs(600), Duration::from_secs(30));
        checkpoints.after_turn();
        settle_tasks().await;
        assert_eq!(started.load(SeqCst), 1);
        // Within the interval: no new save.
        checkpoints.after_turn();
        assert_eq!(started.load(SeqCst), 1);
        // Past the interval but a save still running: no second save.
        let (mut slow, slow_started) = counting(Duration::from_secs(10), Duration::from_secs(60));
        slow.after_turn();
        settle_tasks().await;
        tokio::time::advance(Duration::from_secs(20)).await;
        slow.after_turn();
        assert_eq!(slow_started.load(SeqCst), 1);
        // Finished and past the interval: the next turn saves again.
        tokio::time::advance(Duration::from_secs(600)).await;
        settle_tasks().await;
        checkpoints.after_turn();
        settle_tasks().await;
        assert_eq!(started.load(SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_interval_disables_turn_checkpoints() {
        let (mut checkpoints, started) = counting(Duration::ZERO, Duration::ZERO);
        checkpoints.after_turn();
        settle_tasks().await;
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn settle_waits_for_a_running_save_then_cancels_past_the_bound() {
        let (mut quick, _) = counting(Duration::from_secs(600), Duration::from_secs(5));
        quick.after_turn();
        settle_tasks().await;
        let before = tokio::time::Instant::now();
        quick.settle(Duration::from_secs(60)).await;
        assert_eq!(before.elapsed(), Duration::from_secs(5));
        let (mut stuck, _) = counting(Duration::from_secs(600), Duration::from_secs(3600));
        stuck.after_turn();
        settle_tasks().await;
        let before = tokio::time::Instant::now();
        stuck.settle(Duration::from_secs(60)).await;
        assert_eq!(before.elapsed(), Duration::from_secs(60));
        assert!(stuck.running.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_cancelled_save_kills_its_helper_process_group() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("child.pid");
        let mut command = Command::new("sh");
        command.args([
            "-c",
            &format!("sleep 300 & echo $! > {}; wait", pid_file.display()),
        ]);
        command
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped());
        command.process_group(0);
        let save = tokio::spawn(run_checkpoint(command, Duration::from_secs(600)));
        let child_pid = loop {
            if let Ok(text) = std::fs::read_to_string(&pid_file) {
                if let Ok(pid) = text.trim().parse::<i32>() {
                    break pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        save.abort();
        let _ = save.await;
        let pid = nix::unistd::Pid::from_raw(child_pid);
        let mut gone = false;
        for _ in 0..100 {
            if nix::sys::signal::kill(pid, None).is_err() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(gone, "background helper survived cancellation");
    }
}
