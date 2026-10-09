import type { RemoteAgentSession } from "@/shared/api/remoteAgentSession";
import { normalizePubkey } from "@/shared/lib/pubkey";

/**
 * A thread-session start that failed with a lifecycle error. The thread panel
 * offers the explicit recovery actions for it. Only the failure is kept: the
 * user's recovery choice is sent with one command and never stored.
 */
export type RemoteSessionRecoveryPrompt = {
  agentPubkey: string;
  agentName: string;
  session: RemoteAgentSession;
  /** Tenant scope of the failed start; the recovery command asserts it. */
  expectedRelayUrl: string;
  expectedSignerPubkey: string;
  error: string;
};

/** Bounds the store: one entry per failed (tenant, agent, thread). */
export const MAX_REMOTE_SESSION_RECOVERY_PROMPTS = 32;

let prompts: readonly RemoteSessionRecoveryPrompt[] = [];
const listeners = new Set<() => void>();

/** Identity of a prompt: the tenant, the agent, and the thread. */
export function remoteSessionRecoveryPromptKey(
  prompt: Pick<
    RemoteSessionRecoveryPrompt,
    "agentPubkey" | "session" | "expectedRelayUrl" | "expectedSignerPubkey"
  >,
): string {
  return [
    prompt.expectedRelayUrl.trim(),
    normalizePubkey(prompt.expectedSignerPubkey),
    normalizePubkey(prompt.agentPubkey),
    prompt.session.channelId,
    prompt.session.threadRoot,
  ].join("\u0000");
}

/** Pure update: replace any prompt with the same key, newest last, bounded. */
export function withRemoteSessionRecoveryPrompt(
  current: readonly RemoteSessionRecoveryPrompt[],
  prompt: RemoteSessionRecoveryPrompt,
): readonly RemoteSessionRecoveryPrompt[] {
  const key = remoteSessionRecoveryPromptKey(prompt);
  return [
    ...current.filter(
      (existing) => remoteSessionRecoveryPromptKey(existing) !== key,
    ),
    prompt,
  ].slice(-MAX_REMOTE_SESSION_RECOVERY_PROMPTS);
}

/** Pure selection: the prompts for one thread. */
export function remoteSessionRecoveryPromptsForThread(
  current: readonly RemoteSessionRecoveryPrompt[],
  session: RemoteAgentSession,
): RemoteSessionRecoveryPrompt[] {
  return current.filter(
    (prompt) =>
      prompt.session.channelId === session.channelId &&
      prompt.session.threadRoot === session.threadRoot,
  );
}

function setPrompts(next: readonly RemoteSessionRecoveryPrompt[]): void {
  if (next === prompts) return;
  prompts = next;
  for (const listener of listeners) listener();
}

/** Records a failed thread-session start so its thread can offer recovery. */
export function recordRemoteSessionRecoveryPrompt(
  prompt: RemoteSessionRecoveryPrompt,
): void {
  setPrompts(withRemoteSessionRecoveryPrompt(prompts, prompt));
}

/** Drops a prompt after its session started, recovered, or was dismissed. */
export function clearRemoteSessionRecoveryPrompt(
  prompt: Parameters<typeof remoteSessionRecoveryPromptKey>[0],
): void {
  const key = remoteSessionRecoveryPromptKey(prompt);
  const next = prompts.filter(
    (existing) => remoteSessionRecoveryPromptKey(existing) !== key,
  );
  if (next.length !== prompts.length) setPrompts(next);
}

export function subscribeRemoteSessionRecoveryPrompts(
  listener: () => void,
): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function getRemoteSessionRecoveryPrompts(): readonly RemoteSessionRecoveryPrompt[] {
  return prompts;
}
