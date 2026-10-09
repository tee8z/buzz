import assert from "node:assert/strict";
import { test } from "node:test";
import {
  setup,
  deferred,
  KEY,
  TEXT,
} from "./useMentionSendFlow.test-support.mjs";

const CHANNEL = "11111111-2222-3333-4444-555555555555";
const ROOT = "c".repeat(64);

for (const reply of [false, true]) {
  test(`sandbox wake waits for publication and uses the ${reply ? "parent" : "new"} thread`, async () => {
    const s = await setup();
    s.dismiss();
    s.options.channelId = CHANNEL;
    s.options.mentions.memberPubkeys = new Set([KEY]);
    s.query.data = [
      {
        pubkey: KEY,
        name: "RemoteScout",
        status: "deployed",
        backend: {
          type: "provider",
          id: "kubernetes",
          config: { sandbox: true },
        },
      },
    ];
    const publish = deferred();
    s.options.onSendRef.current = () => publish.promise;
    s.rerender();
    let pending;
    await s.act(async () => {
      pending = s.result.current.sendMessageWithMentionFlow({
        capturedChannelId: CHANNEL,
        pendingImeta: [],
        trimmed: TEXT,
      });
    });
    assert.equal(
      s.events("add").length,
      0,
      "a pending message cannot launch a workspace",
    );
    await s.act(async () => {
      publish.resolve({
        id: reply ? "d".repeat(64) : ROOT,
        tags: reply
          ? [
              ["e", ROOT, "", "root"],
              ["e", "e".repeat(64), "", "reply"],
            ]
          : [],
      });
      await pending;
    });
    const starts = s.events("add");
    assert.equal(
      starts.length,
      1,
      "deployed status must still resolve this thread's workspace",
    );
    assert.deepEqual(starts[0][1].sessionScope, {
      channelId: CHANNEL,
      threadRoot: ROOT,
    });
    assert.equal(starts[0][1].pubkey, KEY);
  });
}

for (const outcome of ["rejected", "missing event"]) {
  test(`sandbox refuses a launch when publication is ${outcome}`, async () => {
    const s = await setup();
    s.dismiss();
    s.options.mentions.memberPubkeys = new Set([KEY]);
    s.query.data = [
      {
        pubkey: KEY,
        name: "RemoteScout",
        status: "deployed",
        backend: {
          type: "provider",
          id: "kubernetes",
          config: { sandbox: true },
        },
      },
    ];
    s.options.onSendRef.current = async () => {
      if (outcome === "rejected") throw new Error("relay refused publication");
    };
    s.rerender();
    await s.act(async () => {
      await s.result.current.sendMessageWithMentionFlow({
        capturedChannelId: CHANNEL,
        pendingImeta: [],
        trimmed: TEXT,
      });
    });
    assert.equal(s.events("add").length, 0);
    assert.equal(s.events("error").length, 1, "failed launch must be visible");
  });
}
