import type { ManagedAgent, RelayEvent } from "@/shared/api/types";
import type { RemoteAgentSession } from "@/shared/api/remoteAgentSession";
import { getThreadReference } from "./threading";

export type PublishedMessage = Pick<RelayEvent, "id" | "tags">;
// biome-ignore lint/suspicious/noConfusingVoidType: legacy send callbacks return Promise<void> without a published event.
export type PublishedMessageResult = PublishedMessage | void;

export function usesThreadSandbox(agent: ManagedAgent): boolean {
  return (
    agent.backend?.type === "provider" &&
    agent.backend.id === "kubernetes" &&
    agent.backend.config.sandbox === true
  );
}

export function publishedRemoteSession(
  message: PublishedMessageResult,
  channelId: string | null,
): RemoteAgentSession | undefined {
  if (!message || !channelId) return undefined;
  // Matches the harness: a reply's root (or lone parent), else the message.
  const threadRoot = (
    getThreadReference(message.tags).rootId ?? message.id
  ).toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(threadRoot)) return undefined;
  return { channelId, threadRoot };
}

/** The explicit action a thread-session error asks the user for. */
export type RemoteSessionRecoveryKind = "recover" | "end-first";

/**
 * Classifies a provider error for a thread session. The provider protocol has
 * no error codes, so this matches its stable phrases: a live session that
 * must be ended before recovery is `end-first`; an ended, ending, or
 * unrecoverable session that only an explicit recovery can replace is
 * `recover`. Anything else is not a lifecycle error.
 */
export function remoteSessionRecoveryKind(
  error: string,
): RemoteSessionRecoveryKind | null {
  const message = error.toLowerCase();
  if (message.includes("end it first") || message.includes("end the session")) {
    return "end-first";
  }
  if (
    message.includes("explicit recovery is required") ||
    message.includes("recover it explicitly once it has ended") ||
    message.includes("session ended") ||
    message.includes("start fresh instead")
  ) {
    return "recover";
  }
  return null;
}

/**
 * Whether checkpoint recovery is still worth offering after `error`. The
 * provider reports a session without a verified checkpoint, or no ended
 * session at all, by telling the user to start fresh.
 */
export function remoteSessionOffersCheckpoint(error: string): boolean {
  const message = error.toLowerCase();
  return !(
    message.includes("no verified checkpoint") ||
    message.includes("no ended session exists")
  );
}
