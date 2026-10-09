//! Checkpoint evidence: the harness's termination-message receipt, confirmed
//! against an S3 `ListObjectsV2` of the session's own prefix.
//!
//! Both must agree before a key is recorded on a tombstone. The receipt alone
//! is written by the (untrusted) Pod; the listing alone cannot say which
//! object the harness meant. The prefix comes from the manager's config and
//! the Sandbox's generation annotation — never from the Pod — so a receipt for
//! another developer or generation is refused before S3 is asked.

use std::future::Future;
use std::time::Duration;

use buzz_backend_kubernetes::lifecycle::{is_session_key, receipt_key};

use crate::plan::Evidence;

/// Bound on listing pages per probe (1000 keys each). A session writes one
/// checkpoint per stop, so a prefix this large is already anomalous.
const MAX_PAGES: usize = 10;

/// Read-only view of the checkpoint bucket. A trait because S3 is a real
/// external boundary: production uses [`S3Store`], tests an in-memory bucket.
pub trait CheckpointStore: Send + Sync {
    /// Does `key` exist under `prefix`?
    fn contains(
        &self,
        prefix: &str,
        key: &str,
    ) -> impl Future<Output = Result<bool, String>> + Send;
}

/// S3 store using `ListObjectsV2` only (IAM: `s3:ListBucket`).
pub struct S3Store {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3Store {
    /// Build from the ambient AWS credential chain (IRSA in-cluster).
    pub async fn new(bucket: &str, region: &str) -> Self {
        // Bounded so one slow probe cannot stall a namespace pass (and with
        // it shutdown) for longer than a few attempts.
        let timeouts = aws_config::timeout::TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .operation_attempt_timeout(Duration::from_secs(10))
            .operation_timeout(Duration::from_secs(25))
            .build();
        let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .timeout_config(timeouts)
            .load()
            .await;
        Self {
            client: aws_sdk_s3::Client::new(&shared),
            bucket: bucket.to_string(),
        }
    }
}

impl CheckpointStore for S3Store {
    async fn contains(&self, prefix: &str, key: &str) -> Result<bool, String> {
        let mut token: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let page = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_continuation_token(token.take())
                .send()
                .await
                .map_err(|e| {
                    format!(
                        "list checkpoints: {}",
                        aws_sdk_s3::error::DisplayErrorContext(e)
                    )
                })?;
            if page
                .contents()
                .iter()
                .any(|object| object.key() == Some(key))
            {
                return Ok(true);
            }
            match page.next_continuation_token() {
                Some(next) if page.is_truncated() == Some(true) => token = Some(next.to_string()),
                _ => return Ok(false),
            }
        }
        Err(format!(
            "checkpoint prefix {prefix} exceeds the listing bound"
        ))
    }
}

/// Evidence for a terminated container's `message` within `session_prefix`
/// (`<developer prefix><generation>/`).
pub async fn from_receipt<S: CheckpointStore>(
    store: &S,
    message: Option<&str>,
    session_prefix: &str,
) -> Evidence {
    let Some(key) = message.and_then(|m| receipt_key(m, session_prefix)) else {
        return Evidence::Missing;
    };
    probe(store, session_prefix, key, Evidence::Verified).await
}

/// Evidence for a recovered session that left no verified checkpoint of its
/// own: the checkpoint it was restored from, if that object still exists
/// under the previous generation's prefix. `restore` comes from the
/// provider-written Sandbox annotation, never the Pod, so carrying it forward
/// keeps the original work recoverable after a failed restore or stop.
pub async fn inherited<S: CheckpointStore>(
    store: &S,
    restore: &str,
    previous_prefix: &str,
) -> Evidence {
    if !is_session_key(restore, previous_prefix) {
        return Evidence::Missing;
    }
    probe(
        store,
        previous_prefix,
        restore.to_string(),
        Evidence::Inherited,
    )
    .await
}

async fn probe<S: CheckpointStore>(
    store: &S,
    prefix: &str,
    key: String,
    found: fn(String) -> Evidence,
) -> Evidence {
    match store.contains(prefix, &key).await {
        Ok(true) => found(key),
        Ok(false) => Evidence::Missing,
        Err(error) => {
            tracing::warn!(%error, "checkpoint probe failed; deferring decision");
            metrics::counter!("buzz_agent_manager_s3_errors_total").increment(1);
            Evidence::Unavailable
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::Mutex;

    /// In-memory bucket recording every probe.
    #[derive(Default)]
    pub struct FakeStore {
        pub keys: Vec<String>,
        pub fail: bool,
        pub probes: Mutex<Vec<String>>,
    }

    impl CheckpointStore for FakeStore {
        async fn contains(&self, prefix: &str, key: &str) -> Result<bool, String> {
            self.probes.lock().unwrap().push(prefix.to_string());
            if self.fail {
                return Err("synthetic outage".into());
            }
            Ok(self.keys.iter().any(|k| k == key && k.starts_with(prefix)))
        }
    }

    const PREFIX: &str = "dev/0123456789abcdef0123456789abcdef/";

    fn receipt(key: &str) -> String {
        serde_json::json!({"version": 1, "checkpoint": key}).to_string()
    }

    #[tokio::test]
    async fn receipt_and_listing_must_agree() {
        let key = format!("{PREFIX}6f9619ff-8b86-d011-b42d-00cf4fc964ff.tar.gz");
        let store = FakeStore {
            keys: vec![key.clone()],
            ..Default::default()
        };
        assert_eq!(
            from_receipt(&store, Some(&receipt(&key)), PREFIX).await,
            Evidence::Verified(key.clone())
        );
        // Receipt names an object S3 does not have.
        let other = format!("{PREFIX}00000000-0000-0000-0000-000000000000.tar.gz");
        assert_eq!(
            from_receipt(&store, Some(&receipt(&other)), PREFIX).await,
            Evidence::Missing
        );
        // No receipt, even though S3 has a key.
        assert_eq!(from_receipt(&store, None, PREFIX).await, Evidence::Missing);
        assert_eq!(
            from_receipt(&store, Some("garbage"), PREFIX).await,
            Evidence::Missing
        );
    }

    /// A compromised Pod pointing at another generation's (existing) object
    /// is refused without even asking S3.
    #[tokio::test]
    async fn foreign_generation_receipts_are_refused_before_probing() {
        let foreign = "dev/ffffffffffffffffffffffffffffffff/x.tar.gz".to_string();
        let store = FakeStore {
            keys: vec![foreign.clone()],
            ..Default::default()
        };
        assert_eq!(
            from_receipt(&store, Some(&receipt(&foreign)), PREFIX).await,
            Evidence::Missing
        );
        assert!(store.probes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_s3_outage_defers_rather_than_records_missing() {
        let key = format!("{PREFIX}x.tar.gz");
        let store = FakeStore {
            keys: vec![key.clone()],
            fail: true,
            ..Default::default()
        };
        assert_eq!(
            from_receipt(&store, Some(&receipt(&key)), PREFIX).await,
            Evidence::Unavailable
        );
        assert_eq!(store.probes.lock().unwrap().as_slice(), [PREFIX]);
    }

    #[tokio::test]
    async fn a_restore_key_is_inherited_only_from_its_previous_generation() {
        let key = format!("{PREFIX}6f9619ff-8b86-d011-b42d-00cf4fc964ff.tar.gz");
        let store = FakeStore {
            keys: vec![key.clone()],
            ..Default::default()
        };
        assert_eq!(
            inherited(&store, &key, PREFIX).await,
            Evidence::Inherited(key.clone())
        );
        // Expired or deleted since the recovery.
        let gone = format!("{PREFIX}00000000-0000-0000-0000-000000000000.tar.gz");
        assert_eq!(inherited(&store, &gone, PREFIX).await, Evidence::Missing);
        // Annotation pointing outside the recovered-from generation.
        let probes = store.probes.lock().unwrap().len();
        let other = "dev/ffffffffffffffffffffffffffffffff/";
        assert_eq!(inherited(&store, &key, other).await, Evidence::Missing);
        assert_eq!(store.probes.lock().unwrap().len(), probes);
        let outage = FakeStore {
            fail: true,
            ..Default::default()
        };
        assert_eq!(
            inherited(&outage, &key, PREFIX).await,
            Evidence::Unavailable
        );
    }
}
