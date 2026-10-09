//! Manager configuration: a JSON file mounted from the Terraform-managed
//! ConfigMap `buzz-agent-manager/agent-manager-config` (key `config.json`).
//!
//! The file is the only source of the namespaces the manager may touch and of
//! each namespace's expected owner. The manager has no cluster-scoped RBAC, so
//! it never discovers namespaces on its own.

use std::collections::BTreeMap;

use serde::Deserialize;

/// The only config schema version this binary accepts.
pub const CONFIG_VERSION: u32 = 1;

/// Parsed and validated configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    /// Bucket holding workspace checkpoints (`ListBucket` only).
    pub checkpoint_bucket: String,
    /// AWS region of the bucket.
    pub checkpoint_region: String,
    /// Namespace name to its developer binding.
    pub namespaces: BTreeMap<String, Namespace>,
}

/// One developer namespace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Namespace {
    pub developer: String,
    /// Owner pubkey (64 lowercase hex) every managed Sandbox must carry.
    pub owner_pubkey: String,
    /// Developer checkpoint prefix, e.g. `tee8z/`.
    pub checkpoint_prefix: String,
}

fn is_dns_label(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn is_owner(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Same shape the image's checkpoint helper enforces:
/// `[a-z0-9][a-z0-9-]{0,31}/`.
fn is_checkpoint_prefix(value: &str) -> bool {
    let Some(body) = value.strip_suffix('/') else {
        return false;
    };
    (1..=32).contains(&body.len())
        && body
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && body
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn is_bucket(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
        && value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && value
            .bytes()
            .last()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

impl Config {
    /// Parse and validate. Every refusal names the offending field.
    pub fn parse(text: &str) -> Result<Self, String> {
        let config: Self =
            serde_json::from_str(text).map_err(|e| format!("invalid manager config: {e}"))?;
        if config.version != CONFIG_VERSION {
            return Err(format!(
                "unsupported config version {} (expected {CONFIG_VERSION})",
                config.version
            ));
        }
        if !is_bucket(&config.checkpoint_bucket) {
            return Err("checkpoint_bucket is not a valid S3 bucket name".into());
        }
        if config.checkpoint_region.trim().is_empty() {
            return Err("checkpoint_region is required".into());
        }
        if config.namespaces.is_empty() {
            return Err("namespaces must list at least one developer namespace".into());
        }
        for (name, namespace) in &config.namespaces {
            if !is_dns_label(name) {
                return Err(format!("namespace {name:?} is not a valid namespace name"));
            }
            if namespace.developer.trim().is_empty() {
                return Err(format!("namespaces.{name}.developer is required"));
            }
            if !is_owner(&namespace.owner_pubkey) {
                return Err(format!(
                    "namespaces.{name}.owner_pubkey must be 64 lowercase hex characters"
                ));
            }
            if !is_checkpoint_prefix(&namespace.checkpoint_prefix) {
                return Err(format!(
                    "namespaces.{name}.checkpoint_prefix must match [a-z0-9][a-z0-9-]{{0,31}}/"
                ));
            }
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid() -> serde_json::Value {
        json!({
            "version": 1,
            "checkpoint_bucket": "voltage-staging-agent-checkpoints-498537461460",
            "checkpoint_region": "us-west-2",
            "namespaces": {"buzz-agent-staging-tee8z": {
                "developer": "tee8z", "owner_pubkey": "a".repeat(64), "checkpoint_prefix": "tee8z/"
            }}
        })
    }

    #[test]
    fn parses_the_terraform_contract() {
        let config = Config::parse(&valid().to_string()).unwrap();
        assert_eq!(
            config.namespaces["buzz-agent-staging-tee8z"].developer,
            "tee8z"
        );
    }

    #[test]
    fn refuses_each_invalid_field() {
        const NS: &str = "/namespaces/buzz-agent-staging-tee8z";
        let cases = [
            ("/version", json!(2)),
            ("/checkpoint_bucket", json!("Bad_Bucket")),
            ("/checkpoint_region", json!(" ")),
            ("/namespaces", json!({})),
            ("/extra", json!(true)),
            (
                "/namespaces",
                json!({"Bad_NS": valid()["namespaces"]["buzz-agent-staging-tee8z"]}),
            ),
            ("/owner_pubkey", json!("A".repeat(64))),
            ("/checkpoint_prefix", json!("tee8z")),
            ("/checkpoint_prefix", json!("a/b/")),
            ("/developer", json!(" ")),
        ];
        for (field, bad) in cases {
            let mut value = valid();
            let pointer = if value.pointer(field).is_some() || field == "/extra" {
                field.to_string()
            } else {
                format!("{NS}{field}")
            };
            match value.pointer_mut(&pointer) {
                Some(slot) => *slot = bad,
                None => value["extra"] = bad,
            }
            assert!(Config::parse(&value.to_string()).is_err(), "{pointer}");
        }
    }
}
