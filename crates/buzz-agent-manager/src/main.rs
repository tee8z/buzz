//! `buzz-agent-manager`: the in-cluster lifecycle manager for Buzz Agent
//! Sandbox sessions (`docs/remote-agents.md` §Sandbox lifecycle).
//!
//! Phase 1 has one subcommand, `reconcile`: classify every managed Sandbox in
//! the configured developer namespaces, tombstone sessions that have ended,
//! and delete tombstones once their 30-day checkpoint retention has passed.
//! It never starts or resumes a session; only an explicit desktop recovery
//! does that.

mod config;
mod evidence;
mod plan;
mod reconcile;
mod server;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::evidence::S3Store;
use crate::reconcile::{run_namespace, Memory, Reconciler};
use crate::server::Health;

/// How long in-flight passes get to finish after SIGTERM before the process
/// exits anyway (inside the default 30s Pod termination grace).
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

#[derive(Parser)]
#[command(name = "buzz-agent-manager", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Reconcile Agent Sandbox session lifecycles.
    Reconcile(ReconcileArgs),
}

#[derive(Args)]
struct ReconcileArgs {
    /// Path to the JSON config (ConfigMap `agent-manager-config`, key `config.json`).
    #[arg(
        long,
        env = "BUZZ_AGENT_MANAGER_CONFIG",
        default_value = "/etc/buzz-agent-manager/config.json"
    )]
    config: PathBuf,
    /// Address for /metrics, /healthz, and /readyz (the only listener).
    #[arg(
        long,
        env = "BUZZ_AGENT_MANAGER_LISTEN",
        default_value = "0.0.0.0:9090"
    )]
    listen: SocketAddr,
    /// Seconds between passes over each namespace.
    #[arg(
        long,
        env = "BUZZ_AGENT_MANAGER_INTERVAL_SECONDS",
        default_value_t = 30,
        value_parser = clap::value_parser!(u64).range(5..=3600)
    )]
    interval_seconds: u64,
    /// Log decisions without writing anything to the cluster.
    #[arg(long, env = "BUZZ_AGENT_MANAGER_DRY_RUN")]
    dry_run: bool,
    /// Run one pass per namespace, print a summary, and exit (no listener).
    #[arg(long)]
    once: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Explicit, as in buzz-backend-kubernetes: the workspace unifies ring and
    // aws-lc-rs, which leaves rustls unable to auto-select a provider.
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    match Cli::parse().command {
        Command::Reconcile(args) => reconcile(args).await,
    }
}

async fn reconcile(args: ReconcileArgs) -> Result<()> {
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("read config {}", args.config.display()))?;
    let config = Config::parse(&text).map_err(anyhow::Error::msg)?;
    let client = kube::Client::try_default()
        .await
        .context("build in-cluster Kubernetes client")?;
    let store = S3Store::new(&config.checkpoint_bucket, &config.checkpoint_region).await;
    let reconciler = Arc::new(Reconciler {
        client,
        store,
        dry_run: args.dry_run,
    });
    if args.once {
        return once(&reconciler, &config).await;
    }

    let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .context("install metrics recorder")?;
    let interval = Duration::from_secs(args.interval_seconds);
    let health = Arc::new(Health::new(config.namespaces.keys().cloned(), interval));
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    tracing::info!(listen = %args.listen, namespaces = config.namespaces.len(),
        dry_run = args.dry_run, "buzz-agent-manager reconcile started");

    let cancel = CancellationToken::new();
    let mut loops = tokio::task::JoinSet::new();
    for (name, namespace) in config.namespaces.clone() {
        loops.spawn(run_namespace(
            Arc::clone(&reconciler),
            name,
            namespace,
            interval,
            Arc::clone(&health),
            cancel.clone(),
        ));
    }
    let server = axum::serve(listener, server::router(health, metrics))
        .with_graceful_shutdown(cancel.clone().cancelled_owned());
    let mut server = tokio::spawn(std::future::IntoFuture::into_future(server));
    // Any component ending on its own is a failure: the loops run until
    // cancelled and the listener until shutdown.
    let outcome = tokio::select! {
        () = shutdown() => {
            tracing::info!("shutting down");
            Ok(())
        }
        result = &mut server => match result {
            Ok(Ok(())) => Err(anyhow::anyhow!("metrics listener stopped")),
            Ok(Err(error)) => Err(anyhow::Error::new(error).context("metrics listener failed")),
            Err(error) => Err(anyhow::Error::new(error).context("metrics listener panicked")),
        },
        Some(result) = loops.join_next() => match result {
            Ok(()) => Err(anyhow::anyhow!("a namespace loop stopped")),
            Err(error) => Err(anyhow::Error::new(error).context("a namespace loop panicked")),
        },
    };
    cancel.cancel();
    let drained = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while loops.join_next().await.is_some() {}
        let _ = (&mut server).await;
    })
    .await;
    if drained.is_err() {
        tracing::warn!("shutdown grace elapsed; abandoning in-flight passes");
        loops.abort_all();
        server.abort();
    }
    outcome
}

async fn once(reconciler: &Reconciler<S3Store>, config: &Config) -> Result<()> {
    let mut failed = false;
    let mut report = BTreeMap::new();
    for (name, namespace) in &config.namespaces {
        let mut memory = Memory::default();
        match reconciler.pass(name, namespace, &mut memory).await {
            Ok(summary) => {
                report.insert(name.clone(), format!("{summary:?}"));
            }
            Err(error) => {
                failed = true;
                report.insert(name.clone(), format!("error: {error}"));
            }
        }
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    anyhow::ensure!(!failed, "at least one namespace pass failed");
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_is_well_formed_and_defaults_match_the_deployment_contract() {
        Cli::command().debug_assert();
        let cli = Cli::try_parse_from(["buzz-agent-manager", "reconcile"]).unwrap();
        let Command::Reconcile(args) = cli.command;
        assert_eq!(
            args.config,
            PathBuf::from("/etc/buzz-agent-manager/config.json")
        );
        assert_eq!(args.listen, "0.0.0.0:9090".parse().unwrap());
        assert_eq!(args.interval_seconds, 30);
        assert!(!args.dry_run && !args.once);
        assert!(Cli::try_parse_from([
            "buzz-agent-manager",
            "reconcile",
            "--interval-seconds",
            "1"
        ])
        .is_err());
        assert!(Cli::try_parse_from(["buzz-agent-manager", "api"]).is_err());
    }
}
