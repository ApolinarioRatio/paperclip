import { unprocessable } from "../errors.js";

export function isOpenClawCredentialHeader(key: string): boolean {
  return ["x-openclaw-token", "x-openclaw-auth", "authorization"].includes(key.trim().toLowerCase());
}

export function hasOpenClawCredentialInput(adapterType: unknown, config: unknown): boolean {
  if (adapterType !== "openclaw_gateway" || typeof config !== "object" || config === null || Array.isArray(config)) return false;
  const record = config as Record<string, unknown>;
  if (["authToken", "token", "password", "devicePrivateKeyPem"].some((key) => Object.prototype.hasOwnProperty.call(record, key))) return true;
  const headers = record.headers;
  return typeof headers === "object" && headers !== null && !Array.isArray(headers)
    && Object.keys(headers).some(isOpenClawCredentialHeader);
}

export function hasOpenClawRuntimeCredentialInput(adapterType: unknown, runtimeConfig: unknown): boolean {
  if (adapterType !== "openclaw_gateway" || !runtimeConfig || typeof runtimeConfig !== "object") return false;
  const profiles = (runtimeConfig as Record<string, unknown>).modelProfiles;
  if (!profiles || typeof profiles !== "object") return false;
  return Object.values(profiles).some((profile) => profile && typeof profile === "object"
    && hasOpenClawCredentialInput(adapterType, (profile as Record<string, unknown>).adapterConfig));
}

/** Gateway profiles inherit one canonical binding; never accept credential overrides. */
export function normalizeOpenClawRuntimeCredentials(
  runtimeConfig: Record<string, unknown>,
  canonicalConfig: Record<string, unknown>,
  historical = false,
): Record<string, unknown> {
  const hasCanonicalToken = Boolean(canonicalConfig.authToken || canonicalConfig.password);
  const profiles = runtimeConfig.modelProfiles;
  if (!profiles || typeof profiles !== "object" || Array.isArray(profiles)) return runtimeConfig;
  const normalized = { ...profiles } as Record<string, unknown>;
  for (const [key, value] of Object.entries(normalized)) {
    if (!value || typeof value !== "object" || Array.isArray(value)) continue;
    const profile = value as Record<string, unknown>;
    if (!hasOpenClawCredentialInput("openclaw_gateway", profile.adapterConfig)) continue;
    const config = { ...(profile.adapterConfig as Record<string, unknown>) };
    const hasSharedCredential = ["authToken", "token", "password"].some((field) => Object.prototype.hasOwnProperty.call(config, field))
      || Boolean(config.headers && typeof config.headers === "object" && Object.keys(config.headers).some(isOpenClawCredentialHeader));
    if (hasSharedCredential && !hasCanonicalToken && !historical) {
      throw unprocessable("OpenClaw model profiles must inherit the canonical authToken binding");
    }
    delete config.authToken;
    delete config.token;
    delete config.password;
    if (Object.prototype.hasOwnProperty.call(config, "devicePrivateKeyPem")) {
      if (!canonicalConfig.devicePrivateKeyPem && !historical) throw unprocessable("OpenClaw model profiles must inherit the canonical device identity");
      delete config.devicePrivateKeyPem;
      if (!canonicalConfig.devicePrivateKeyPem) config.devicePrivateKeyPem = "***REDACTED***";
    }
    if (config.headers && typeof config.headers === "object" && !Array.isArray(config.headers)) {
      config.headers = Object.fromEntries(Object.entries(config.headers).filter(([header]) => !isOpenClawCredentialHeader(header)));
    }
    // Historical profile-only values cannot be restored as credentials. Keep a
    // structural marker so rollback fails closed instead of silently activating.
    if (hasSharedCredential && !hasCanonicalToken) config.authToken = "***REDACTED***";
    normalized[key] = { ...profile, adapterConfig: config };
  }
  return { ...runtimeConfig, modelProfiles: normalized };
}
