import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { REDACTED_EVENT_VALUE, redactOpenClawAgentResponse } from "../redaction.js";
import { redactSensitive, redactHttpRequestBody } from "../middleware/redact-sensitive.js";

describe("OpenClaw public credential projections", () => {
  it("redacts malformed CAS bodies and runtime token aliases without depending on body validation", () => {
    const value = randomUUID();
    expect(JSON.stringify(redactHttpRequestBody({ value }, "/api/agents/fixture/gateway-auth-token-binding")).includes(value)).toBe(false);
    expect(JSON.stringify(redactSensitive({ adapterConfig: { token: value, sibling: { type: "secret_ref", secretId: value } } })).includes(value)).toBe(false);
  });
  it("redacts CAS value and trimmed legacy credentials from HTTP denial/error payloads", () => {
    const values = Array.from({ length: 4 }, () => randomUUID());
    const payload = { expectedUpdatedAt: new Date(0).toISOString(), value: values[0],
      adapterConfig: { headers: { " X-OpenClaw-Token ": values[1], " X-OpenClaw-Auth ": values[2], " Authorization ": values[3] } } };
    const encoded = JSON.stringify(redactSensitive(payload));
    expect(values.some((value) => encoded.includes(value))).toBe(false);
    expect(redactSensitive({ value: "public metric" })).toEqual({ value: "public metric" });
  });
  it("redacts usable references and legacy credentials in agents and every revision snapshot", () => {
    const secretId = randomUUID();
    const legacy = randomUUID();
    const agent = { adapterType: "openclaw_gateway", adapterConfig: {
      authToken: { type: "secret_ref", secretId, version: "latest" },
      password: { type: "secret_ref", secretId, version: "latest" },
      devicePrivateKeyPem: { type: "secret_ref", secretId, version: "latest" },
      headers: { " Authorization ": legacy, "X-Sibling": "keep" },
    }, runtimeConfig: { modelProfiles: { cheap: { adapterConfig: { authToken: { type: "secret_ref", secretId, version: "latest" }, devicePrivateKeyPem: legacy } } } } };
    const projected = redactOpenClawAgentResponse({ agent, revisions: [{ beforeConfig: agent, afterConfig: agent }] });
    const encoded = JSON.stringify(projected);
    expect(encoded.includes(secretId)).toBe(false);
    expect(encoded.includes(legacy)).toBe(false);
    expect(encoded.includes(REDACTED_EVENT_VALUE)).toBe(true);
    expect(encoded.includes('"X-Sibling":"keep"')).toBe(true);
    expect(agent.adapterConfig.authToken.secretId === secretId).toBe(true);
  });

  it("projects partial historical snapshots using the owning gateway adapter type", () => {
    const secretId = randomUUID();
    const result = redactOpenClawAgentResponse({ beforeConfig: { runtimeConfig: { modelProfiles: { cheap: { adapterConfig: { password: { type: "secret_ref", secretId, version: "latest" } } } } } } }, "openclaw_gateway");
    expect(JSON.stringify(result).includes(secretId)).toBe(false);
  });

  it("does not alter other adapters or Date serialization", () => {
    const value = { adapterType: "process", adapterConfig: { value: "keep" }, updatedAt: new Date(0) };
    expect(redactOpenClawAgentResponse(value)).toEqual(value);
  });
});
