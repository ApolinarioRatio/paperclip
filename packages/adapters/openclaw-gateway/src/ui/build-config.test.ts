import { describe, expect, it } from "vitest";
import type { CreateConfigValues } from "@paperclipai/adapter-utils";
import { buildOpenClawGatewayConfig } from "./build-config.js";

function baseValues(): CreateConfigValues {
  return {
    adapterType: "openclaw_gateway",
    cwd: "",
    promptTemplate: "",
    model: "",
    thinkingEffort: "",
    chrome: false,
    dangerouslySkipPermissions: false,
    search: false,
    fastMode: false,
    dangerouslyBypassSandbox: false,
    command: "",
    args: "",
    extraArgs: "",
    envVars: "",
    envBindings: {},
    url: "wss://gateway.example/ws",
    bootstrapPrompt: "",
    maxTurnsPerRun: 0,
    heartbeatEnabled: false,
    intervalSec: 0,
  };
}

describe("buildOpenClawGatewayConfig", () => {
  it("applies the documented timeout defaults when unset (timeoutSec=120, waitTimeoutMs=120000)", () => {
    const config = buildOpenClawGatewayConfig(baseValues());
    expect(config.timeoutSec).toBe(120);
    expect(config.waitTimeoutMs).toBe(120000);
  });

  it("preserves explicit timeout values when provided", () => {
    const config = buildOpenClawGatewayConfig({
      ...baseValues(),
      timeoutSec: 45,
      waitTimeoutMs: 9000,
    });
    expect(config.timeoutSec).toBe(45);
    expect(config.waitTimeoutMs).toBe(9000);
  });

  it("applies the documented identity defaults when unset", () => {
    const config = buildOpenClawGatewayConfig(baseValues());
    expect(config.sessionKeyStrategy).toBe("issue");
    expect(config.role).toBe("operator");
    expect(config.scopes).toEqual(["operator.admin"]);
  });

  it("keeps gateway authentication canonical and removes legacy token headers", () => {
    const config = buildOpenClawGatewayConfig({
      ...baseValues(),
      authToken: "synthetic-gateway-token",
      headersJson: JSON.stringify({
        "X-OpenClaw-Token": "legacy-token",
        "x-OPENclaw-auth": "legacy-auth",
        Authorization: "Bearer legacy-bearer",
        "x-sibling-header": "preserve-me",
      }),
    });

    expect(config.authToken).toBe("synthetic-gateway-token");
    expect(config.headers).toEqual({ "x-sibling-header": "preserve-me" });
    expect(JSON.stringify(config)).not.toContain("legacy-token");
    expect(JSON.stringify(config)).not.toContain("legacy-auth");
    expect(JSON.stringify(config)).not.toContain("legacy-bearer");
  });

  it("promotes legacy header input into the canonical authToken field", () => {
    const config = buildOpenClawGatewayConfig({
      ...baseValues(),
      headersJson: JSON.stringify({
        "X-OpenClaw-Auth": "legacy-auth-input",
        "x-sibling-header": "preserve-me",
      }),
    });

    expect(config.authToken).toBe("legacy-auth-input");
    expect(config.headers).toEqual({ "x-sibling-header": "preserve-me" });
  });
});
