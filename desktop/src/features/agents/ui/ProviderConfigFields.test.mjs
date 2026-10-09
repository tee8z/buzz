import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { coerceConfigValues } from "./ProviderConfigFields.tsx";

const schema = {
  properties: {
    inactivity_seconds: { type: "integer" },
    threshold: { type: "number" },
    label: { type: "string" },
  },
};

describe("coerceConfigValues", () => {
  it("parses public scheduling objects and preserves invalid input for validation", () => {
    const nested = { properties: { pod_options: { type: "object" } } };
    assert.deepEqual(
      coerceConfigValues(
        { pod_options: '{"node_selector":{"workload":"agents"}}' },
        nested,
      ),
      { pod_options: { node_selector: { workload: "agents" } } },
    );
    assert.deepEqual(coerceConfigValues({ pod_options: "" }, nested), {});
    assert.deepEqual(coerceConfigValues({ pod_options: "invalid" }, nested), {
      pod_options: "invalid",
    });
  });
  it("omits cleared numeric fields without losing explicit zero", () => {
    assert.deepEqual(
      coerceConfigValues(
        { inactivity_seconds: "", threshold: "0", label: "" },
        schema,
      ),
      { threshold: 0, label: "" },
    );
  });

  it("preserves nonempty invalid numeric input for provider validation", () => {
    assert.deepEqual(
      coerceConfigValues({ inactivity_seconds: "not-a-number" }, schema),
      { inactivity_seconds: "not-a-number" },
    );
  });
});
