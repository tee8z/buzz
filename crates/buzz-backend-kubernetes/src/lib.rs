//! Library surface of the Kubernetes provider: the Agent Sandbox session
//! lifecycle shared with the in-cluster `buzz-agent-manager`.
//!
//! Deliberately small. The provider binary ships inside the desktop app, so
//! anything a consumer of this library needs (S3, HTTP servers, metrics) must
//! live in that consumer, never here.

pub mod lifecycle;
