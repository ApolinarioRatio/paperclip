import { companyPortabilityService } from "../services/company-portability.js";
import * as instructionsModule from "../services/agent-instructions.js";
import express from "express";
import request from "supertest";
import * as liveEvents from "../services/live-events.js";
import { approvalService } from "../services/approvals.js";
import { accessRoutes } from "../routes/access.js";
import { randomUUID, createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { eq, sql } from "drizzle-orm";
import {
  activityLog, agents, agentConfigRevisions, approvals, invites, joinRequests, companies, companySecrets,
  companySecretVersions, companySecretBindings, createDb,
} from "@paperclipai/db";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { agentService } from "../services/agents.js";
import { secretService } from "../services/secrets.js";
import { REDACTED_EVENT_VALUE, redactOpenClawAgentResponse } from "../redaction.js";

const support = await getEmbeddedPostgresTestSupport();
const describeDb = support.supported ? describe : describe.skip;
const legacyNames = [" X-OpenClaw-Token ", "\tX-OPENCLAW-AUTH\t", " Authorization "];

describeDb("OpenClaw credential persistence invariant", () => {
  let db: ReturnType<typeof createDb>;
  let stop: () => Promise<void>;
  let keyDir: string;
  const oldKeyFile = process.env.PAPERCLIP_SECRETS_MASTER_KEY_FILE;

  beforeAll(async () => {
    keyDir = mkdtempSync(path.join(os.tmpdir(), "paperclip-openclaw-invariant-"));
    process.env.PAPERCLIP_SECRETS_MASTER_KEY_FILE = path.join(keyDir, "master.key");
    const fixture = await startEmbeddedPostgresTestDatabase("openclaw-invariant");
    stop = fixture.cleanup;
    db = createDb(fixture.connectionString);
  }, 30_000);

  afterAll(async () => {
    await stop?.();
    if (oldKeyFile === undefined) delete process.env.PAPERCLIP_SECRETS_MASTER_KEY_FILE;
    else process.env.PAPERCLIP_SECRETS_MASTER_KEY_FILE = oldKeyFile;
    if (keyDir) rmSync(keyDir, { recursive: true, force: true });
  });

  async function company() {
    const id = randomUUID();
    await db.insert(companies).values({ id, name: id, issuePrefix: id.slice(0, 8), requireBoardApprovalForNewAgents: false });
    return id;
  }

  function config(value = randomUUID()) {
    return {
      url: "wss://gateway.example.invalid",
      headers: { [legacyNames[0]!]: value, [legacyNames[1]!]: randomUUID(), [legacyNames[2]!]: randomUUID(), " X-Sibling ": "keep" },
      sibling: { nested: true },
    };
  }

  // Legacy fixtures must bypass the service whose invariant is under test.
  async function legacyAgent(status = "idle", adapterConfig: Record<string, unknown> = config()) {
    const companyId = await company();
    const [row] = await db.insert(agents).values({ companyId, name: randomUUID(), adapterType: "openclaw_gateway", status, adapterConfig }).returning();
    return row!;
  }

  async function history(row: typeof agents.$inferSelect) {
    for (let index = 0; index < 3; index++) {
      await db.insert(agentConfigRevisions).values({
        companyId: row.companyId, agentId: row.id, changedKeys: ["adapterConfig"],
        beforeConfig: { ...row, adapterConfig: config() },
        afterConfig: { ...row, adapterConfig: { ...config(), authToken: randomUUID() } },
      });
    }
  }

  function assertSafeConfig(value: unknown) {
    const c = value as Record<string, unknown>;
    expect((c.authToken as Record<string, unknown>)?.type).toBe("secret_ref");
    expect(Object.keys((c.headers ?? {}) as object).some((key) =>
      ["x-openclaw-token", "x-openclaw-auth", "authorization"].includes(key.trim().toLowerCase()),
    )).toBe(false);
    expect(c.sibling).toEqual({ nested: true });
    expect(c.headers).toEqual({ " X-Sibling ": "keep" });
  }

  async function assertAllSafe(agentId: string) {
    const row = await agentService(db).getById(agentId);
    assertSafeConfig(row!.adapterConfig);
    const revisions = await db.select().from(agentConfigRevisions).where(eq(agentConfigRevisions.agentId, agentId));
    for (const revision of revisions) {
      assertSafeConfig(revision.beforeConfig.adapterConfig);
      assertSafeConfig(revision.afterConfig.adapterConfig);
    }
    return { row: row!, revisions };
  }

  async function state(companyId: string) {
    return Promise.all([
      db.select().from(agents).where(eq(agents.companyId, companyId)),
      db.select().from(agentConfigRevisions).where(eq(agentConfigRevisions.companyId, companyId)),
      db.select().from(companySecrets).where(eq(companySecrets.companyId, companyId)),
      db.select().from(companySecretBindings).where(eq(companySecretBindings.companyId, companyId)),
      db.select().from(activityLog).where(eq(activityLog.companyId, companyId)),
      db.select().from(companySecretVersions),
    ]).then((tables) => tables.map((rows) => rows.sort((left, right) => left.id.localeCompare(right.id))));
  }

  it.each(legacyNames)("create migrates trimmed/case-insensitive legacy header %s and attributes managed secrets", async (header) => {
    const companyId = await company();
    const value = randomUUID();
    const row = await agentService(db).create(companyId, {
      name: randomUUID(), adapterType: "openclaw_gateway",
      adapterConfig: { ...config(), headers: { [header]: header.trim().toLowerCase() === "authorization" ? `Bearer ${value}` : value, " X-Sibling ": "keep" } },
    }, { actor: { userId: "fixture-owner" } });
    assertSafeConfig(row.adapterConfig);
    const resolved = await secretService(db).resolveAdapterConfigForRuntime(companyId, row.adapterConfig,
      { consumerType: "agent", consumerId: row.id }, { adapterType: "openclaw_gateway" });
    expect(resolved.config.authToken === value).toBe(true);
    const secrets = await db.select().from(companySecrets).where(eq(companySecrets.companyId, companyId));
    expect(secrets.every((secret) => secret.createdByUserId === "fixture-owner")).toBe(true);
  });

  it.each(["explicit config", "config omitted"])("generic update scrubs current and every historical snapshot: %s", async (mode) => {
    const row = await legacyAgent();
    await history(row);
    const patch = mode === "explicit config" ? { adapterConfig: config() } : { title: "updated" };
    await agentService(db).update(row.id, patch, { recordRevision: { createdByUserId: "fixture-owner" } });
    const result = await assertAllSafe(row.id);
    expect(result.revisions.length).toBe(4);
  });

  it("CAS atomically scrubs directly seeded history and rejects stale retries with zero writes", async () => {
    const row = await legacyAgent();
    await history(row);
    const value = randomUUID();
    await agentService(db).updateGatewayAuthTokenBindingCas(row.id, {
      expectedUpdatedAt: row.updatedAt.toISOString(), value, actor: { userId: "fixture-owner" },
    });
    const result = await assertAllSafe(row.id);
    expect(result.revisions.length).toBe(4);
    const resolved = await secretService(db).resolveAdapterConfigForRuntime(row.companyId, result.row.adapterConfig,
      { consumerType: "agent", consumerId: row.id }, { adapterType: "openclaw_gateway" });
    expect(resolved.config.authToken === value).toBe(true);
    const audit = await db.select().from(activityLog).where(eq(activityLog.companyId, row.companyId));
    expect(audit.filter((entry) => entry.action === "agent.gateway_auth_token_bound")).toEqual([
      expect.objectContaining({ actorType: "user", actorId: "fixture-owner", entityType: "agent", entityId: row.id,
        details: { bindingConfigPath: "authToken", legacyHeaderRemoved: true } }),
    ]);
    expect(JSON.stringify(audit).includes(value)).toBe(false);
    const credentials = await db.select().from(companySecrets).where(eq(companySecrets.companyId, row.companyId));
    expect(credentials.length).toBeGreaterThan(0);
    expect(credentials.every((credential) => credential.createdByUserId === "fixture-owner")).toBe(true);
    const baseline = await state(row.companyId);
    await expect(agentService(db).updateGatewayAuthTokenBindingCas(row.id, {
      expectedUpdatedAt: row.updatedAt.toISOString(), value: randomUUID(), actor: { userId: "fixture-owner" },
    })).rejects.toMatchObject({ status: 409 });
    expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
  });

  it("injected revision-write failure rolls back current config, history, secrets, versions, bindings and activity", async () => {
    const row = await legacyAgent();
    await history(row);
    const baseline = await state(row.companyId);
    await db.execute(sql`CREATE FUNCTION fixture_reject_revision() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture revision rejected'; END $$`);
    await db.execute(sql`CREATE TRIGGER fixture_reject_revision BEFORE INSERT ON agent_config_revisions FOR EACH ROW EXECUTE FUNCTION fixture_reject_revision()`);
    try {
      await expect(agentService(db).updateGatewayAuthTokenBindingCas(row.id, {
        expectedUpdatedAt: row.updatedAt.toISOString(), value: randomUUID(), actor: { userId: "fixture-owner" },
      })).rejects.toBeTruthy();
      expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
      await expect(agentService(db).update(row.id, { title: "Generic injected failure" }, { recordRevision: { source: "fixture_failure" } })).rejects.toThrow("OpenClaw credential persistence failed");
      expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
    } finally {
      await db.execute(sql`DROP TRIGGER fixture_reject_revision ON agent_config_revisions`);
      await db.execute(sql`DROP FUNCTION fixture_reject_revision()`);
    }
  });

  it("competing CAS writers have one winner and one zero-write conflict", async () => {
    const row = await legacyAgent();
    const results = await Promise.allSettled([randomUUID(), randomUUID()].map((value) =>
      agentService(db).updateGatewayAuthTokenBindingCas(row.id, { expectedUpdatedAt: row.updatedAt.toISOString(), value, actor: {} }),
    ));
    expect(results.filter((r) => r.status === "fulfilled").length).toBe(1);
    expect(results.filter((r) => r.status === "rejected" && r.reason.status === 409).length).toBe(1);
    expect((await assertAllSafe(row.id)).revisions.length).toBe(1);
  });

  it("an injected create failure leaves no managed secret, version, binding, agent or activity", async () => {
    const companyId = await company();
    const baseline = await state(companyId);
    await db.execute(sql`CREATE FUNCTION fixture_reject_agent() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture agent rejected'; END $$`);
    await db.execute(sql`CREATE TRIGGER fixture_reject_agent BEFORE INSERT ON agents FOR EACH ROW EXECUTE FUNCTION fixture_reject_agent()`);
    try {
      await expect(agentService(db).create(companyId, { name: randomUUID(), adapterType: "openclaw_gateway", adapterConfig: config() })).rejects.toBeTruthy();
      expect(JSON.stringify(await state(companyId)) === JSON.stringify(baseline)).toBe(true);
    } finally {
      await db.execute(sql`DROP TRIGGER fixture_reject_agent ON agents`);
      await db.execute(sql`DROP FUNCTION fixture_reject_agent()`);
    }
  });

  it("a redacted response marker preserves the existing canonical binding during editing", async () => {
    const row = await legacyAgent();
    await agentService(db).update(row.id, { title: "first edit" });
    const prior = await agentService(db).getById(row.id);
    await agentService(db).update(row.id, { adapterConfig: { ...prior!.adapterConfig, authToken: REDACTED_EVENT_VALUE } });
    const next = await agentService(db).getById(row.id);
    expect(JSON.stringify(next!.adapterConfig.authToken) === JSON.stringify(prior!.adapterConfig.authToken)).toBe(true);
  });

  it("approval activation with omitted adapter config scrubs legacy current and history", async () => {
    const row = await legacyAgent("pending_approval");
    await history(row);
    await agentService(db).activatePendingApproval(row.id, undefined, { userId: "fixture-approver" });
    expect((await assertAllSafe(row.id)).row.status).toBe("idle");
    const secrets = await db.select().from(companySecrets).where(eq(companySecrets.companyId, row.companyId));
    expect(secrets.every((secret) => secret.createdByUserId === "fixture-approver")).toBe(true);
  });

  it("rollback, restore/import and configuration-copy cannot reintroduce legacy plaintext; canonical wins", async () => {
    const value = randomUUID();
    const row = await legacyAgent("idle", { ...config(), authToken: value });
    const [revision] = await db.insert(agentConfigRevisions).values({
      companyId: row.companyId, agentId: row.id, changedKeys: ["adapterConfig"],
      beforeConfig: { ...row }, afterConfig: { ...row },
    }).returning();
    await agentService(db).rollbackConfigRevision(row.id, revision!.id, { userId: "fixture-owner" });
    const { row: safe } = await assertAllSafe(row.id);
    const resolved = await secretService(db).resolveAdapterConfigForRuntime(row.companyId, safe.adapterConfig,
      { consumerType: "agent", consumerId: row.id }, { adapterType: "openclaw_gateway" });
    expect(resolved.config.authToken === value).toBe(true);
    const copied = await agentService(db).create(row.companyId, { name: randomUUID(), adapterType: "openclaw_gateway", adapterConfig: row.adapterConfig });
    assertSafeConfig(copied.adapterConfig);
    await agentService(db).update(copied.id, { adapterConfig: row.adapterConfig });
    assertSafeConfig((await agentService(db).getById(copied.id))!.adapterConfig);
  });
  it.each(legacyNames)("direct legacy wrapped header survives migration without plaintext: %s", async (header) => {
    const value = randomUUID();
    const row = await legacyAgent("idle", { headers: { [header]: { value: header.trim().toLowerCase() === "authorization" ? `Bearer ${value}` : value } } });
    const updated = await agentService(db).update(row.id, { title: "Migrate wrapped legacy" });
    const resolved = await secretService(db).resolveAdapterConfigForRuntime(row.companyId, updated!.adapterConfig, { consumerType: "agent", consumerId: row.id }, { adapterType: "openclaw_gateway" });
    expect(resolved.config.authToken === value).toBe(true);
    expect(JSON.stringify(updated!.adapterConfig).includes(value)).toBe(false);
  });

  it.each(["token", "password", "devicePrivateKeyPem"])("manages runtime credential alias/field with attributed binding: %s", async (field) => {
    const companyId = await company();
    const value = randomUUID();
    const row = await agentService(db).create(companyId, { name: randomUUID(), adapterType: "openclaw_gateway", adapterConfig: { [field]: value } }, { actor: { userId: "fixture-owner" } });
    const key = field === "token" ? "authToken" : field;
    expect((row.adapterConfig[key] as Record<string, unknown>)?.type).toBe("secret_ref");
    expect(Object.prototype.hasOwnProperty.call(row.adapterConfig, "token")).toBe(false);
    const resolved = await secretService(db).resolveAdapterConfigForRuntime(companyId, row.adapterConfig, { consumerType: "agent", consumerId: row.id }, { adapterType: "openclaw_gateway" });
    expect(resolved.config[key] === value).toBe(true);
    await agentService(db).update(row.id, { adapterConfig: { [key]: REDACTED_EVENT_VALUE } });
    expect(((await agentService(db).getById(row.id))!.adapterConfig[key] as any).secretId === (row.adapterConfig[key] as any).secretId).toBe(true);
  });

  it("profiles inherit canonical password and device identity; unsafe profile-only credentials fail without writes", async () => {
    const companyId = await company();
    const password = randomUUID();
    const identity = randomUUID();
    const override = randomUUID();
    const runtimeConfig = { modelProfiles: { cheap: { adapterConfig: { password: override, devicePrivateKeyPem: override, headers: { " Authorization ": override, "X-Sibling": "keep" }, model: "fixture-model" } } } };
    const row = await agentService(db).create(companyId, { name: randomUUID(), adapterType: "openclaw_gateway", adapterConfig: { password, devicePrivateKeyPem: identity }, runtimeConfig });
    const profile = (row.runtimeConfig.modelProfiles as typeof runtimeConfig.modelProfiles).cheap.adapterConfig;
    expect(JSON.stringify(profile).includes(override)).toBe(false);
    const resolved = await secretService(db).resolveAdapterConfigForRuntime(companyId, { ...row.adapterConfig, ...profile }, { consumerType: "agent", consumerId: row.id }, { adapterType: "openclaw_gateway" });
    expect(resolved.config.password === password && resolved.config.devicePrivateKeyPem === identity).toBe(true);
    expect(resolved.config.model).toBe("fixture-model");
    const baseline = await state(companyId);
    await expect(agentService(db).create(companyId, { name: randomUUID(), adapterType: "openclaw_gateway", adapterConfig: {}, runtimeConfig })).rejects.toMatchObject({ status: 422 });
    expect(JSON.stringify(await state(companyId)) === JSON.stringify(baseline)).toBe(true);
  });

  it("scrubs partial profile-only and already-redacted password history without blocking a safe CAS", async () => {
    const row = await legacyAgent();
    const legacy = randomUUID();
    await db.insert(agentConfigRevisions).values({ companyId: row.companyId, agentId: row.id, changedKeys: ["runtimeConfig"],
      beforeConfig: { adapterType: "openclaw_gateway", runtimeConfig: { modelProfiles: { cheap: { adapterConfig: { password: legacy, devicePrivateKeyPem: legacy } } } } },
      afterConfig: { adapterType: "openclaw_gateway", adapterConfig: { password: REDACTED_EVENT_VALUE } },
    });
    await agentService(db).updateGatewayAuthTokenBindingCas(row.id, { expectedUpdatedAt: row.updatedAt.toISOString(), value: randomUUID(), actor: { userId: "fixture-owner" } });
    const revisions = await db.select().from(agentConfigRevisions).where(eq(agentConfigRevisions.agentId, row.id));
    expect(JSON.stringify(revisions).includes(legacy)).toBe(false);
    const baseline = await state(row.companyId);
    await expect(agentService(db).rollbackConfigRevision(row.id, revisions[0]!.id, { userId: "fixture-owner" })).rejects.toMatchObject({ status: 422 });
    expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
  });

  it("approval failure rolls back resolution, activation and credentials with no precommit publications", async () => {
    const row = await legacyAgent("pending_approval");
    await history(row);
    const [approval] = await db.insert(approvals).values({ companyId: row.companyId, type: "hire_agent", status: "pending", payload: { agentId: row.id, adapterType: "openclaw_gateway" } }).returning();
    const baseline = await state(row.companyId);
    const publication = vi.spyOn(liveEvents, "publishLiveEvent");
    await db.execute(sql`CREATE FUNCTION fixture_reject_activation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture activation rejected'; END $$`);
    await db.execute(sql`CREATE TRIGGER fixture_reject_activation BEFORE UPDATE ON agents FOR EACH ROW EXECUTE FUNCTION fixture_reject_activation()`);
    try {
      await expect(approvalService(db).approve(approval!.id, "fixture-owner")).rejects.toThrow("OpenClaw approval persistence failed");
      expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
      expect((await db.select().from(approvals).where(eq(approvals.id, approval!.id)))[0]!.status).toBe("pending");
      expect(publication).not.toHaveBeenCalled();
    } finally {
      await db.execute(sql`DROP TRIGGER fixture_reject_activation ON agents`);
      await db.execute(sql`DROP FUNCTION fixture_reject_activation()`);
      publication.mockRestore();
    }
    const publishedStates: string[] = [];
    const committed = vi.spyOn(liveEvents, "publishLiveEvent").mockImplementation(() => { publishedStates.push("published"); return undefined as never; });
    try {
      await approvalService(db).approve(approval!.id, "fixture-owner");
      expect((await db.select().from(approvals).where(eq(approvals.id, approval!.id)))[0]!.status).toBe("approved");
      expect((await agentService(db).getById(row.id))!.status).toBe("idle");
      // The release binding synchronizer has no binding-created audit publication;
      // preserve that contract instead of importing master's newer audit behavior.
      expect(publishedStates).toEqual([]);
    } finally { committed.mockRestore(); }
  });

  async function inviteApp(companyId: string, authorized = false) {
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => { (req as any).actor = authorized
      ? { type: "board", userId: "fixture-owner", source: "local_implicit", isInstanceAdmin: true, companyIds: [companyId] }
      : { type: "none", companyIds: [] }; next(); });
    app.use("/api", accessRoutes(db, { deploymentMode: "authenticated", deploymentExposure: "private", bindHost: "127.0.0.1", allowedHostnames: [] }));
    app.use((error: any, _req: any, res: any, _next: any) => res.status(error.status ?? 500).json({ error: "fixture request rejected" }));
    return app;
  }

  it("invite insert failure rolls back managed defaults and invite state without activity", async () => {
    const companyId = await company();
    const token = randomUUID();
    const [invite] = await db.insert(invites).values({ companyId, tokenHash: createHash("sha256").update(token).digest("hex"), expiresAt: new Date(Date.now() + 60_000) }).returning();
    const baseline = await state(companyId);
    await db.execute(sql`CREATE FUNCTION fixture_reject_join() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture join rejected'; END $$`);
    await db.execute(sql`CREATE TRIGGER fixture_reject_join BEFORE INSERT ON join_requests FOR EACH ROW EXECUTE FUNCTION fixture_reject_join()`);
    try {
      const response = await request(await inviteApp(companyId)).post(`/api/invites/${token}/accept`).send({ requestType: "agent", agentName: "Fixture", adapterType: "openclaw_gateway", agentDefaultsPayload: { url: "ws://127.0.0.1:18789", authToken: randomUUID(), disableDeviceAuth: true } });
      expect(response.status).toBe(409);
      expect(JSON.stringify(await state(companyId)) === JSON.stringify(baseline)).toBe(true);
      expect((await db.select().from(invites).where(eq(invites.id, invite!.id)))[0]!.acceptedAt).toBe(null);
    } finally {
      await db.execute(sql`DROP TRIGGER fixture_reject_join ON join_requests`);
      await db.execute(sql`DROP FUNCTION fixture_reject_join()`);
    }
  });

  it("approved invite replay requires update permission before join, secret, agent or activity writes", async () => {
    const row = await legacyAgent();
    const token = randomUUID();
    const [invite] = await db.insert(invites).values({ companyId: row.companyId, tokenHash: createHash("sha256").update(token).digest("hex"), acceptedAt: new Date(), expiresAt: new Date(Date.now() + 60_000) }).returning();
    const [join] = await db.insert(joinRequests).values({ companyId: row.companyId, inviteId: invite!.id, requestType: "agent", status: "approved", createdAgentId: row.id, adapterType: "openclaw_gateway", requestIp: "127.0.0.1", agentName: "Fixture", agentDefaultsPayload: { ...row.adapterConfig, disableDeviceAuth: true } }).returning();
    const baseline = await state(row.companyId);
    const response = await request(await inviteApp(row.companyId)).post(`/api/invites/${token}/accept`).send({ requestType: "agent", adapterType: "openclaw_gateway", agentDefaultsPayload: { authToken: randomUUID() } });
    expect(response.status).toBe(403);
    expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
    expect(JSON.stringify((await db.select().from(joinRequests).where(eq(joinRequests.id, join!.id)))[0]) === JSON.stringify(join)).toBe(true);
  });

  it("authorized replay rolls back join and agent together on failure, then reuses one canonical managed binding", async () => {
    const row = await legacyAgent();
    const token = randomUUID();
    const replacement = randomUUID();
    const [invite] = await db.insert(invites).values({ companyId: row.companyId, tokenHash: createHash("sha256").update(token).digest("hex"), acceptedAt: new Date(), expiresAt: new Date(Date.now() + 60_000) }).returning();
    const [join] = await db.insert(joinRequests).values({ companyId: row.companyId, inviteId: invite!.id, requestType: "agent", status: "approved", createdAgentId: row.id, adapterType: "openclaw_gateway", requestIp: "127.0.0.1", agentName: "Fixture", agentDefaultsPayload: { ...row.adapterConfig, disableDeviceAuth: true } }).returning();
    const baseline = await state(row.companyId);
    const app = await inviteApp(row.companyId, true);
    const replay = () => request(app).post(`/api/invites/${token}/accept`).send({ requestType: "agent", adapterType: "openclaw_gateway", agentDefaultsPayload: { authToken: replacement } });
    await db.execute(sql`CREATE FUNCTION fixture_reject_replay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture replay rejected'; END $$`);
    await db.execute(sql`CREATE TRIGGER fixture_reject_replay BEFORE UPDATE ON agents FOR EACH ROW EXECUTE FUNCTION fixture_reject_replay()`);
    const publication = vi.spyOn(liveEvents, "publishLiveEvent");
    try {
      expect((await replay()).status).toBe(409);
      expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
      expect(JSON.stringify((await db.select().from(joinRequests).where(eq(joinRequests.id, join!.id)))[0]) === JSON.stringify(join)).toBe(true);
      expect(publication).not.toHaveBeenCalled();
    } finally {
      await db.execute(sql`DROP TRIGGER fixture_reject_replay ON agents`);
      await db.execute(sql`DROP FUNCTION fixture_reject_replay()`);
      publication.mockRestore();
    }
    const response = await replay();
    expect(response.status).toBe(202);
    const current = (await agentService(db).getById(row.id))!;
    const storedJoin = (await db.select().from(joinRequests).where(eq(joinRequests.id, join!.id)))[0]!;
    const ref = current.adapterConfig.authToken as any;
    expect(ref.secretId === (storedJoin.agentDefaultsPayload!.authToken as any).secretId).toBe(true);
    const resolved = await secretService(db).resolveAdapterConfigForRuntime(row.companyId, current.adapterConfig, { consumerType: "agent", consumerId: row.id }, { adapterType: "openclaw_gateway" });
    expect(resolved.config.authToken === replacement).toBe(true);
    expect(JSON.stringify(response.body).includes(ref.secretId)).toBe(false);
    expect(JSON.stringify(response.body).includes(replacement)).toBe(false);
    const managed = await db.select().from(companySecrets).where(eq(companySecrets.companyId, row.companyId));
    expect(managed.length).toBe(2);
    expect(managed.every((secret) => secret.createdByUserId === "fixture-owner")).toBe(true);
  });

  it("throwing postcommit subscribers cannot turn a committed CAS into a failure receipt", async () => {
    const row = await legacyAgent();
    const publication = vi.spyOn(liveEvents, "publishLiveEvent").mockImplementation(() => { throw new Error("fixture subscriber failure"); });
    try {
      const result = await agentService(db).updateGatewayAuthTokenBindingCas(row.id, { expectedUpdatedAt: row.updatedAt.toISOString(), value: randomUUID(), actor: { userId: "fixture-owner" } });
      expect(result?.id).toBe(row.id);
      expect(publication).toHaveBeenCalled();
      expect((await db.select().from(agentConfigRevisions).where(eq(agentConfigRevisions.agentId, row.id))).length).toBe(1);
    } finally { publication.mockRestore(); }
  });

  it("approval payload creation and resubmission normalize inside their write transactions", async () => {
    const companyId = await company();
    const baseline = await state(companyId);
    const payload = { adapterType: "openclaw_gateway", adapterConfig: { authToken: randomUUID() } };
    await db.execute(sql`CREATE FUNCTION fixture_reject_approval() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture approval rejected'; END $$`);
    await db.execute(sql`CREATE TRIGGER fixture_reject_approval BEFORE INSERT ON approvals FOR EACH ROW EXECUTE FUNCTION fixture_reject_approval()`);
    try {
      await expect(approvalService(db).create(companyId, { type: "hire_agent", payload, requestedByUserId: "fixture-owner" })).rejects.toThrow("OpenClaw approval persistence failed");
      expect(JSON.stringify(await state(companyId)) === JSON.stringify(baseline)).toBe(true);
    } finally {
      await db.execute(sql`DROP TRIGGER fixture_reject_approval ON approvals`);
      await db.execute(sql`DROP FUNCTION fixture_reject_approval()`);
    }
    const created = await approvalService(db).create(companyId, { type: "hire_agent", status: "revision_requested", payload, requestedByUserId: "fixture-owner" });
    expect(((created.payload.adapterConfig as any).authToken as any).type).toBe("secret_ref");
    const resubmitted = await approvalService(db).resubmit(created.id, { adapterType: "openclaw_gateway", adapterConfig: { password: randomUUID() } }, { userId: "fixture-editor" });
    expect(((resubmitted.payload.adapterConfig as any).password as any).type).toBe("secret_ref");
    const managed = await db.select().from(companySecrets).where(eq(companySecrets.companyId, companyId));
    expect(managed.some((secret) => secret.createdByUserId === "fixture-editor")).toBe(true);
  });

  it.each(["incoming", "stored"])("legacy type-omitted approval resubmit infers gateway and rolls back failed normalization writes: %s", async (source) => {
    const row = await legacyAgent("pending_approval");
    const value = randomUUID();
    const [approval] = await db.insert(approvals).values({ companyId: row.companyId, type: "hire_agent", status: "revision_requested",
      requestedByUserId: "fixture-requester", payload: { agentId: row.id, adapterConfig: { authToken: value } },
    }).returning();
    const payload = source === "incoming" ? { agentId: row.id, adapterConfig: { authToken: value } } : undefined;
    const baseline = await state(row.companyId);
    await db.execute(sql`CREATE FUNCTION fixture_reject_resubmit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture resubmit rejected'; END $$`);
    await db.execute(sql`CREATE TRIGGER fixture_reject_resubmit BEFORE UPDATE ON approvals FOR EACH ROW EXECUTE FUNCTION fixture_reject_resubmit()`);
    try {
      await expect(approvalService(db).resubmit(approval!.id, payload, { userId: "fixture-editor" })).rejects.toThrow("OpenClaw approval persistence failed");
      expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
      expect(JSON.stringify(await approvalService(db).getById(approval!.id)) === JSON.stringify(approval)).toBe(true);
    } finally {
      await db.execute(sql`DROP TRIGGER fixture_reject_resubmit ON approvals`);
      await db.execute(sql`DROP FUNCTION fixture_reject_resubmit()`);
    }
    const result = await approvalService(db).resubmit(approval!.id, payload, { userId: "fixture-editor" });
    expect(result.payload.adapterType).toBe("openclaw_gateway");
    expect(((result.payload.adapterConfig as any).authToken as any).type).toBe("secret_ref");
    expect(JSON.stringify(result.payload).includes(value)).toBe(false);
    const managed = await db.select().from(companySecrets).where(eq(companySecrets.companyId, row.companyId));
    expect(managed).toHaveLength(1);
    expect(managed[0]!.createdByUserId).toBe("fixture-editor");
  });

  it("type-only gateway transition keeps credentials in secure history, never the new response or exported files", async () => {
    const values = [randomUUID(), randomUUID(), randomUUID()];
    const row = await legacyAgent("idle", { ...config(), authToken: values[0], password: values[1], devicePrivateKeyPem: values[2] });
    const [priorRevision] = await db.insert(agentConfigRevisions).values({ companyId: row.companyId, agentId: row.id, changedKeys: ["adapterConfig"], beforeConfig: { ...row }, afterConfig: { ...row } }).returning();
    const switched = await agentService(db).update(row.id, { adapterType: "process" }, { recordRevision: { source: "fixture_transition", createdByUserId: "fixture-editor" } });
    expect(switched!.adapterType).toBe("process");
    for (const key of ["authToken", "password", "devicePrivateKeyPem"]) expect(Object.prototype.hasOwnProperty.call(switched!.adapterConfig, key)).toBe(false);
    expect(switched!.adapterConfig.sibling).toEqual({ nested: true });
    const managed = await db.select().from(companySecrets).where(eq(companySecrets.companyId, row.companyId));
    expect(managed.length).toBeGreaterThanOrEqual(3);
    const revisions = await db.select().from(agentConfigRevisions).where(eq(agentConfigRevisions.agentId, row.id));
    const receipt = JSON.stringify(redactOpenClawAgentResponse({ agent: switched, revisions }));
    expect(managed.some((secret) => receipt.includes(secret.id))).toBe(false);
    expect(values.some((value) => receipt.includes(value))).toBe(false);
    const instructions = vi.spyOn(instructionsModule, "agentInstructionsService").mockReturnValue({ exportFiles: async () => ({ files: { "AGENTS.md": "Fixture instructions" }, entryFile: "AGENTS.md", warnings: [] }) } as any);
    try {
      const exported = await companyPortabilityService(db).exportBundle(row.companyId, { include: { company: false, agents: true, projects: false, issues: false } });
      const artifact = JSON.stringify(exported);
      expect(managed.some((secret) => artifact.includes(secret.id))).toBe(false);
      expect(values.some((value) => artifact.includes(value))).toBe(false);
    } finally { instructions.mockRestore(); }
    await agentService(db).rollbackConfigRevision(row.id, priorRevision!.id, { userId: "fixture-editor" });
    const restored = (await agentService(db).getById(row.id))!;
    expect(restored.adapterType).toBe("openclaw_gateway");
    const resolved = await secretService(db).resolveAdapterConfigForRuntime(row.companyId, restored.adapterConfig, { consumerType: "agent", consumerId: row.id }, { adapterType: "openclaw_gateway" });
    expect(resolved.config.authToken === values[0] && resolved.config.password === values[1] && resolved.config.devicePrivateKeyPem === values[2]).toBe(true);
  });

  it("does not remove distinct explicit credentials belonging to a replacement non-gateway adapter", async () => {
    const row = await legacyAgent();
    const value = randomUUID();
    const switched = await agentService(db).update(row.id, { adapterType: "process", adapterConfig: { password: value, sibling: "replacement" } });
    expect(switched!.adapterConfig.password === value).toBe(true);
    expect(switched!.adapterConfig.sibling).toBe("replacement");
  });

  it("type-only gateway approval activation uses the same atomic scrub and safe transition history", async () => {
    const value = randomUUID();
    const row = await legacyAgent("pending_approval", { ...config(), authToken: value });
    const baseline = await state(row.companyId);
    await db.execute(sql`CREATE FUNCTION fixture_reject_activation_revision() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture activation revision rejected'; END $$`);
    await db.execute(sql`CREATE TRIGGER fixture_reject_activation_revision BEFORE INSERT ON agent_config_revisions FOR EACH ROW EXECUTE FUNCTION fixture_reject_activation_revision()`);
    try {
      await expect(agentService(db).activatePendingApproval(row.id, { adapterType: "process" }, { userId: "fixture-editor" })).rejects.toThrow("OpenClaw credential persistence failed");
      expect(JSON.stringify(await state(row.companyId)) === JSON.stringify(baseline)).toBe(true);
    } finally {
      await db.execute(sql`DROP TRIGGER fixture_reject_activation_revision ON agent_config_revisions`);
      await db.execute(sql`DROP FUNCTION fixture_reject_activation_revision()`);
    }
    const activated = await agentService(db).activatePendingApproval(row.id, { adapterType: "process" }, { userId: "fixture-editor" });
    expect(activated.agent.adapterType).toBe("process");
    expect(activated.agent.status).toBe("idle");
    expect(activated.agent.adapterConfig.authToken).toBeUndefined();
    const revisions = await db.select().from(agentConfigRevisions).where(eq(agentConfigRevisions.agentId, row.id));
    expect(revisions).toHaveLength(1);
    expect(revisions[0]!.beforeConfig.adapterType).toBe("openclaw_gateway");
    expect(((revisions[0]!.beforeConfig.adapterConfig as any).authToken as any).type).toBe("secret_ref");
    const managed = await db.select().from(companySecrets).where(eq(companySecrets.companyId, row.companyId));
    expect(managed).toHaveLength(1);
    const receipt = JSON.stringify(redactOpenClawAgentResponse({ agent: activated.agent, revisions }));
    expect(receipt.includes(value) || receipt.includes(managed[0]!.id)).toBe(false);
  });

});
