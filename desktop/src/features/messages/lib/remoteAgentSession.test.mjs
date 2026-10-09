import assert from "node:assert/strict";
import { test } from "node:test";
import {
  publishedRemoteSession,
  usesThreadSandbox,
} from "./remoteAgentSession.ts";

test("published top-level and nested replies select their canonical workspace", () => {
  const channelId = "f9ee507e-d03c-4570-baa6-02951c38e1cd";
  const root = "a".repeat(64);
  const expected = { channelId, threadRoot: root };
  assert.deepEqual(
    publishedRemoteSession({ id: root, tags: [] }, channelId),
    expected,
  );
  assert.deepEqual(
    publishedRemoteSession(
      {
        id: "b".repeat(64),
        tags: [
          ["e", root.toUpperCase(), "", "root"],
          ["e", "c".repeat(64), "", "reply"],
        ],
      },
      channelId,
    ),
    expected,
  );
  assert.equal(publishedRemoteSession(undefined, channelId), undefined);
  assert.equal(publishedRemoteSession({ id: root, tags: [] }, null), undefined);
  assert.equal(
    publishedRemoteSession({ id: "invalid", tags: [] }, channelId),
    undefined,
  );
});

test("only an explicitly configured Kubernetes Sandbox uses thread launches", () => {
  assert.equal(
    usesThreadSandbox({
      backend: {
        type: "provider",
        id: "kubernetes",
        config: { sandbox: true },
      },
    }),
    true,
  );
  assert.equal(
    usesThreadSandbox({
      backend: { type: "provider", id: "kubernetes", config: {} },
    }),
    false,
  );
  assert.equal(
    usesThreadSandbox({
      backend: { type: "provider", id: "other", config: { sandbox: true } },
    }),
    false,
  );
  assert.equal(usesThreadSandbox({ backend: { type: "local" } }), false);
});
