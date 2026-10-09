//! The manager's only listener: `/metrics`, `/healthz`, `/readyz`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;

/// Per-namespace loop progress.
pub struct Health {
    started: Instant,
    /// A loop that has not attempted a pass within this window is wedged.
    window: Duration,
    namespaces: Mutex<HashMap<String, Progress>>,
}

#[derive(Clone, Copy, Default)]
struct Progress {
    attempted: Option<Instant>,
    succeeded: Option<Instant>,
}

impl Health {
    /// `interval` is the pass interval; both probes allow three missed passes.
    pub fn new(namespaces: impl IntoIterator<Item = String>, interval: Duration) -> Self {
        Self {
            started: Instant::now(),
            window: (interval * 3).max(Duration::from_secs(120)),
            namespaces: Mutex::new(
                namespaces
                    .into_iter()
                    .map(|ns| (ns, Progress::default()))
                    .collect(),
            ),
        }
    }

    /// Record one pass.
    pub fn record(&self, namespace: &str, ok: bool) {
        if let Ok(mut namespaces) = self.namespaces.lock() {
            let progress = namespaces.entry(namespace.to_string()).or_default();
            let now = Instant::now();
            progress.attempted = Some(now);
            if ok {
                progress.succeeded = Some(now);
            }
        }
    }

    fn check(&self, pick: impl Fn(&Progress) -> Option<Instant>) -> bool {
        let Ok(namespaces) = self.namespaces.lock() else {
            return false;
        };
        let fresh = |at: Option<Instant>| at.is_some_and(|at| at.elapsed() <= self.window);
        namespaces
            .values()
            .all(|progress| fresh(pick(progress)) || self.started.elapsed() <= self.window)
    }

    /// Every namespace loop is still running passes.
    pub fn live(&self) -> bool {
        self.check(|p| p.attempted)
    }

    /// Every namespace had a successful pass recently.
    pub fn ready(&self) -> bool {
        let Ok(namespaces) = self.namespaces.lock() else {
            return false;
        };
        namespaces
            .values()
            .all(|p| p.succeeded.is_some_and(|at| at.elapsed() <= self.window))
    }
}

#[derive(Clone)]
struct App {
    health: Arc<Health>,
    metrics: PrometheusHandle,
}

/// The router served on `--listen`.
pub fn router(health: Arc<Health>, metrics: PrometheusHandle) -> Router {
    Router::new()
        .route(
            "/metrics",
            get(|State(app): State<App>| async move { app.metrics.render() }),
        )
        .route(
            "/healthz",
            get(|State(app): State<App>| async move {
                if app.health.live() {
                    (StatusCode::OK, "ok")
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "reconcile loop stalled")
                }
            }),
        )
        .route(
            "/readyz",
            get(|State(app): State<App>| async move {
                if app.health.ready() {
                    (StatusCode::OK, "ok")
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "no recent successful pass")
                }
            }),
        )
        .with_state(App { health, metrics })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_needs_a_success_in_every_namespace() {
        let health = Health::new(["a".to_string(), "b".to_string()], Duration::from_secs(30));
        assert!(health.live(), "a fresh process is live within its window");
        assert!(!health.ready());
        health.record("a", true);
        assert!(!health.ready(), "namespace b has not succeeded");
        health.record("b", false);
        assert!(!health.ready());
        health.record("b", true);
        assert!(health.ready());
    }

    #[tokio::test]
    async fn serves_metrics_and_probes() {
        use tower::ServiceExt;
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let health = Arc::new(Health::new(["a".to_string()], Duration::from_secs(30)));
        let app = router(Arc::clone(&health), handle);
        let status = |path: &'static str| {
            let app = app.clone();
            async move {
                app.oneshot(
                    axum::http::Request::get(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
            }
        };
        assert_eq!(status("/metrics").await, StatusCode::OK);
        assert_eq!(status("/healthz").await, StatusCode::OK);
        assert_eq!(status("/readyz").await, StatusCode::SERVICE_UNAVAILABLE);
        health.record("a", true);
        assert_eq!(status("/readyz").await, StatusCode::OK);
        assert_eq!(status("/other").await, StatusCode::NOT_FOUND);
    }
}
