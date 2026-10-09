import {
  fromRawManagedAgent,
  invokeTauri,
  type RawManagedAgent,
} from "@/shared/api/tauri";
import type {
  ManagedAgent,
  ManagedAgentRuntimeStatus,
} from "@/shared/api/types";
import type { RemoteAgentSession } from "./remoteAgentSession";

export async function startManagedAgent(
  pubkey: string,
  options?: {
    /** Tenant scope captured by the caller before its first await; the
     * backend fails closed before any spawn/deploy side effect when the
     * active community no longer matches. */
    expectedRelayUrl?: string;
    /** Signer identity captured with the relay scope; the backend fails
     * closed when the active workspace identity no longer matches. */
    expectedSignerPubkey?: string;
    /** Unix-seconds replay floor for a publish-first mention send: the
     * spawned harness's first REQ replays at least back to this moment, so
     * the already-published triggering message lands in its window however
     * long the spawn takes. Local spawns receive it as process env; provider
     * deploys carry it in the payload's launch.policy_env. */
    replayFloorUnix?: number;
    sessionScope?: RemoteAgentSession;
  },
): Promise<ManagedAgent> {
  const response = await invokeTauri<RawManagedAgent>("start_managed_agent", {
    pubkey,
    expectedRelayUrl: options?.expectedRelayUrl ?? null,
    expectedSignerPubkey: options?.expectedSignerPubkey ?? null,
    replayFloorUnix: options?.replayFloorUnix ?? null,
    sessionScope: options?.sessionScope ?? null,
  });
  return fromRawManagedAgent(response);
}

/** How the user chose to recover an ended thread session. */
export type RemoteRecoveryMode = "checkpoint" | "fresh";

/** Tenant scope a recovery prompt captured; the backend fails closed on change. */
export type RemoteSessionTenantScope = {
  expectedRelayUrl: string;
  expectedSignerPubkey: string;
};

/** Session state the provider reports after `stop`. */
export type RemoteSessionStopState = "ending" | "ended" | "absent";

/** Ends a thread's remote session with the provider's `stop` operation. */
export async function endRemoteAgentSession(
  pubkey: string,
  sessionScope: RemoteAgentSession,
  scope: RemoteSessionTenantScope,
): Promise<RemoteSessionStopState> {
  return invokeTauri<RemoteSessionStopState>("end_remote_agent_session", {
    pubkey,
    sessionScope,
    expectedRelayUrl: scope.expectedRelayUrl,
    expectedSignerPubkey: scope.expectedSignerPubkey,
  });
}

/**
 * Recovers a thread's ended remote session. Call only from an explicit user
 * action: the mode applies to this one deploy and is never stored.
 */
export async function recoverRemoteAgentSession(
  pubkey: string,
  sessionScope: RemoteAgentSession,
  mode: RemoteRecoveryMode,
  scope: RemoteSessionTenantScope,
): Promise<ManagedAgent> {
  const response = await invokeTauri<RawManagedAgent>(
    "recover_remote_agent_session",
    {
      pubkey,
      sessionScope,
      mode,
      expectedRelayUrl: scope.expectedRelayUrl,
      expectedSignerPubkey: scope.expectedSignerPubkey,
    },
  );
  return fromRawManagedAgent(response);
}

/** Whether the agent's provider advertises `stop`, so sessions can be ended and recovered. */
export async function remoteAgentSessionSupportsLifecycle(
  pubkey: string,
): Promise<boolean> {
  return invokeTauri<boolean>("remote_agent_session_supports_lifecycle", {
    pubkey,
  });
}

export async function stopManagedAgent(pubkey: string): Promise<ManagedAgent> {
  const response = await invokeTauri<RawManagedAgent>("stop_managed_agent", {
    pubkey,
  });
  return fromRawManagedAgent(response);
}

export async function setManagedAgentStartOnAppLaunch(
  pubkey: string,
  startOnAppLaunch: boolean,
): Promise<ManagedAgent> {
  const response = await invokeTauri<RawManagedAgent>(
    "set_managed_agent_start_on_app_launch",
    {
      pubkey,
      startOnAppLaunch,
    },
  );
  return fromRawManagedAgent(response);
}

export async function setManagedAgentAutoRestart(
  pubkey: string,
  autoRestartOnConfigChange: boolean,
): Promise<ManagedAgent> {
  const response = await invokeTauri<RawManagedAgent>(
    "set_managed_agent_auto_restart",
    {
      pubkey,
      autoRestartOnConfigChange,
    },
  );
  return fromRawManagedAgent(response);
}

export async function listManagedAgentRuntimes(): Promise<
  ManagedAgentRuntimeStatus[]
> {
  return invokeTauri<ManagedAgentRuntimeStatus[]>(
    "list_managed_agent_runtimes",
  );
}

export async function startManagedAgentRuntime(
  pubkey: string,
  relayUrl: string,
): Promise<ManagedAgentRuntimeStatus> {
  return invokeTauri("start_managed_agent_runtime", { pubkey, relayUrl });
}

export async function stopManagedAgentRuntime(
  pubkey: string,
  relayUrl: string,
): Promise<ManagedAgentRuntimeStatus> {
  return invokeTauri("stop_managed_agent_runtime", { pubkey, relayUrl });
}

export async function restartManagedAgentRuntime(
  pubkey: string,
  relayUrl: string,
): Promise<ManagedAgentRuntimeStatus> {
  return invokeTauri("restart_managed_agent_runtime", { pubkey, relayUrl });
}

export async function putManagedAgentRuntimeLifecycle(
  outerPubkey: string,
  payload: unknown,
): Promise<ManagedAgentRuntimeStatus> {
  return invokeTauri("put_managed_agent_runtime_lifecycle", {
    outerPubkey,
    payload,
  });
}

export async function reconcileManagedAgentRuntimes(
  communities: readonly { relayUrl: string }[],
): Promise<ManagedAgentRuntimeStatus[]> {
  return invokeTauri("reconcile_managed_agent_runtimes", { communities });
}
