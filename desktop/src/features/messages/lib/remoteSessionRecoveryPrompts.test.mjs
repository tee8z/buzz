import assert from "node:assert/strict";
import { test } from "node:test";
import {
  clearRemoteSessionRecoveryPrompt,
  getRemoteSessionRecoveryPrompts,
  MAX_REMOTE_SESSION_RECOVERY_PROMPTS,
  recordRemoteSessionRecoveryPrompt,
  remoteSessionRecoveryPromptsForThread,
  subscribeRemoteSessionRecoveryPrompts,
  withRemoteSessionRecoveryPrompt,
} from "./remoteSessionRecoveryPrompts.ts";

const channelId = "f9ee507e-d03c-4570-baa6-02951c38e1cd";

function prompt(overrides = {}) {
  return {
    agentPubkey: "a".repeat(64),
    agentName: "Agent",
    session: { channelId, threadRoot: "b".repeat(64) },
    expectedRelayUrl: "wss://relay.example",
    expectedSignerPubkey: "c".repeat(64),
    error: "Sandbox session ended; explicit recovery is required",
    ...overrides,
  };
}

test("a newer failure for the same tenant, agent, and thread replaces the older one", () => {
  const first = prompt();
  const second = prompt({
    agentPubkey: "A".repeat(64),
    error: "Sandbox session is ending; recover it explicitly once it has ended",
  });
  const next = withRemoteSessionRecoveryPrompt(
    withRemoteSessionRecoveryPrompt([], first),
    second,
  );
  assert.deepEqual(next, [second]);

  const otherTenant = prompt({ expectedSignerPubkey: "d".repeat(64) });
  assert.equal(withRemoteSessionRecoveryPrompt(next, otherTenant).length, 2);
});

test("the prompt store stays bounded, keeping the newest failures", () => {
  let prompts = [];
  for (
    let index = 0;
    index <= MAX_REMOTE_SESSION_RECOVERY_PROMPTS;
    index += 1
  ) {
    prompts = withRemoteSessionRecoveryPrompt(
      prompts,
      prompt({
        session: {
          channelId,
          threadRoot: index.toString(16).padStart(64, "0"),
        },
      }),
    );
  }
  assert.equal(prompts.length, MAX_REMOTE_SESSION_RECOVERY_PROMPTS);
  assert.equal(prompts[0].session.threadRoot, "1".padStart(64, "0"));
});

test("a thread selects only its own prompts", () => {
  const here = prompt();
  const otherThread = prompt({
    session: { channelId, threadRoot: "e".repeat(64) },
  });
  const otherChannel = prompt({
    session: {
      channelId: "00000000-0000-4000-8000-000000000000",
      threadRoot: "b".repeat(64),
    },
  });
  assert.deepEqual(
    remoteSessionRecoveryPromptsForThread(
      [here, otherThread, otherChannel],
      here.session,
    ),
    [here],
  );
});

test("recording and clearing notify subscribers; clearing matches the start's scope", () => {
  let notifications = 0;
  const unsubscribe = subscribeRemoteSessionRecoveryPrompts(() => {
    notifications += 1;
  });
  const failed = prompt();
  recordRemoteSessionRecoveryPrompt(failed);
  assert.deepEqual(getRemoteSessionRecoveryPrompts(), [failed]);
  // A later successful start carries no error or name, only its scope.
  clearRemoteSessionRecoveryPrompt({
    agentPubkey: failed.agentPubkey.toUpperCase(),
    session: failed.session,
    expectedRelayUrl: ` ${failed.expectedRelayUrl} `,
    expectedSignerPubkey: failed.expectedSignerPubkey,
  });
  assert.deepEqual(getRemoteSessionRecoveryPrompts(), []);
  clearRemoteSessionRecoveryPrompt(failed);
  assert.equal(notifications, 2, "clearing an absent prompt is silent");
  unsubscribe();
});
