import * as React from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { managedAgentsQueryKey } from "@/features/agents/hooks";
import { matchesDetachedToastScope } from "@/features/messages/lib/detachedToastScope";
import {
  type RemoteSessionRecoveryKind,
  remoteSessionOffersCheckpoint,
  remoteSessionRecoveryKind,
} from "@/features/messages/lib/remoteAgentSession";
import {
  clearRemoteSessionRecoveryPrompt,
  getRemoteSessionRecoveryPrompts,
  type RemoteSessionRecoveryPrompt,
  remoteSessionRecoveryPromptKey,
  remoteSessionRecoveryPromptsForThread,
  subscribeRemoteSessionRecoveryPrompts,
} from "@/features/messages/lib/remoteSessionRecoveryPrompts";
import type { RemoteAgentSession } from "@/shared/api/remoteAgentSession";
import {
  endRemoteAgentSession,
  type RemoteRecoveryMode,
  type RemoteSessionStopState,
  recoverRemoteAgentSession,
  remoteAgentSessionSupportsLifecycle,
} from "@/shared/api/tauriManagedAgents";
import { Alert, AlertDescription } from "@/shared/ui/alert";
import { Button } from "@/shared/ui/button";
import { getErrorMessage } from "./useMentionSendFlow.helpers";

type PromptView = {
  kind: RemoteSessionRecoveryKind;
  offersCheckpoint: boolean;
  message: string;
};

const END_OUTCOMES: Record<RemoteSessionStopState, PromptView> = {
  ending: {
    kind: "recover",
    offersCheckpoint: true,
    message:
      "The session is ending. Recover it once it has saved its checkpoint.",
  },
  ended: {
    kind: "recover",
    offersCheckpoint: true,
    message: "The session has ended.",
  },
  absent: {
    kind: "recover",
    offersCheckpoint: false,
    message: "No session exists for this thread.",
  },
};

function RemoteSessionRecoveryBanner({
  prompt,
}: {
  prompt: RemoteSessionRecoveryPrompt;
}) {
  const queryClient = useQueryClient();
  const supportsLifecycle = useQuery({
    queryKey: ["remote-agent-session-lifecycle", prompt.agentPubkey],
    queryFn: () => remoteAgentSessionSupportsLifecycle(prompt.agentPubkey),
    staleTime: 60_000,
  });
  // The latest action's outcome. Local to this prompt and dropped on unmount;
  // the recovery mode itself is never kept anywhere.
  const [outcome, setOutcome] = React.useState<PromptView | null>(null);
  const [busy, setBusy] = React.useState(false);

  const initialKind = remoteSessionRecoveryKind(prompt.error);
  if (!initialKind || supportsLifecycle.data !== true) return null;
  const view: PromptView = outcome ?? {
    kind: initialKind,
    offersCheckpoint: remoteSessionOffersCheckpoint(prompt.error),
    message: prompt.error,
  };
  const tenant = {
    expectedRelayUrl: prompt.expectedRelayUrl,
    expectedSignerPubkey: prompt.expectedSignerPubkey,
  };

  const run = async (action: () => Promise<PromptView | null>) => {
    setBusy(true);
    try {
      const next = await action();
      if (next) setOutcome(next);
    } catch (error) {
      // Keep the actions available: a failed attempt must not strand the
      // user. A new lifecycle error retargets them (for example, recovery of
      // a live session asks to end it first).
      const message = getErrorMessage(error, "The action failed.");
      setOutcome({
        kind: remoteSessionRecoveryKind(message) ?? view.kind,
        offersCheckpoint:
          view.offersCheckpoint && remoteSessionOffersCheckpoint(message),
        message,
      });
    } finally {
      setBusy(false);
    }
  };

  const recover = (mode: RemoteRecoveryMode) =>
    run(async () => {
      await recoverRemoteAgentSession(
        prompt.agentPubkey,
        prompt.session,
        mode,
        tenant,
      );
      clearRemoteSessionRecoveryPrompt(prompt);
      void queryClient.invalidateQueries({ queryKey: managedAgentsQueryKey });
      toast.success(
        mode === "checkpoint"
          ? `${prompt.agentName} is restoring its workspace.`
          : `${prompt.agentName} is starting a fresh workspace.`,
      );
      return null;
    });

  const end = () =>
    run(async () => {
      const state = await endRemoteAgentSession(
        prompt.agentPubkey,
        prompt.session,
        tenant,
      );
      return END_OUTCOMES[state];
    });

  return (
    <Alert
      aria-busy={busy}
      className="pointer-events-auto mb-2"
      data-testid="remote-session-recovery"
      variant="destructive"
    >
      <AlertDescription>
        {`${prompt.agentName}: ${view.message}`}
      </AlertDescription>
      <div className="mt-2 flex flex-wrap gap-2">
        {view.kind === "end-first" ? (
          <Button
            disabled={busy}
            onClick={() => void end()}
            size="xs"
            type="button"
            variant="outline"
          >
            End session
          </Button>
        ) : (
          <>
            {view.offersCheckpoint ? (
              <Button
                disabled={busy}
                onClick={() => void recover("checkpoint")}
                size="xs"
                type="button"
                variant="outline"
              >
                Recover from checkpoint
              </Button>
            ) : null}
            <Button
              disabled={busy}
              onClick={() => void recover("fresh")}
              size="xs"
              type="button"
              variant="outline"
            >
              Start fresh
            </Button>
          </>
        )}
        <Button
          disabled={busy}
          onClick={() => clearRemoteSessionRecoveryPrompt(prompt)}
          size="xs"
          type="button"
          variant="ghost"
        >
          Dismiss
        </Button>
      </div>
    </Alert>
  );
}

/**
 * Explicit end/recover actions for thread-sandbox starts in this thread that
 * failed with a session lifecycle error. Shown only for the tenant that is on
 * screen and only when the agent's provider advertises `stop`.
 */
export function RemoteSessionRecoveryBanners({
  session,
}: {
  session: RemoteAgentSession | undefined;
}) {
  const prompts = React.useSyncExternalStore(
    subscribeRemoteSessionRecoveryPrompts,
    getRemoteSessionRecoveryPrompts,
  );
  if (!session) return null;
  const visible = remoteSessionRecoveryPromptsForThread(
    prompts,
    session,
  ).filter((prompt) =>
    matchesDetachedToastScope(
      prompt.expectedRelayUrl,
      prompt.expectedSignerPubkey,
    ),
  );
  if (visible.length === 0) return null;
  return (
    <>
      {visible.map((prompt) => (
        <RemoteSessionRecoveryBanner
          key={remoteSessionRecoveryPromptKey(prompt)}
          prompt={prompt}
        />
      ))}
    </>
  );
}
