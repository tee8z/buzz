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
