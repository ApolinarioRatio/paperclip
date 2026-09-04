import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  agentWakeupRequests,
  agents,
  companies,
  createDb,
  heartbeatRuns,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import {
  admitLiveRun,
  promoteScheduledRetryWithAdmission,
} from "../services/live-run-admission.js";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

describeEmbeddedPostgres("transactional per-agent live-run admission", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-live-run-admission-");
    db = createDb(tempDb.connectionString);
  }, 20_000);

  afterEach(async () => {
    await db.delete(heartbeatRuns);
    await db.delete(agentWakeupRequests);
    await db.delete(agents);
    await db.delete(companies);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedAgent(maxLiveRuns: number | null) {
    const companyId = randomUUID();
    const agentId = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Admission test company",
      issuePrefix: `A${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      status: "active",
      defaultResponsibleUserId: "test-user",
      requireBoardApprovalForNewAgents: false,
    });
    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "Admission Agent",
      role: "engineer",
      status: "idle",
      adapterType: "process",
      adapterConfig: {},
      runtimeConfig: {
        heartbeat: maxLiveRuns === null ? {} : { maxLiveRuns },
      },
      permissions: {},
    });
    return { companyId, agentId };
  }

  function admit(companyId: string, agentId: string, taskKey: string) {
    return db.transaction((tx) => admitLiveRun({
      tx,
      agent: { id: agentId, companyId },
      wakeupRequest: {
        source: "automation",
        triggerDetail: "system",
        reason: "admission_test",
        payload: { taskKey },
        requestedByActorType: "system",
        requestedByActorId: null,
      },
      run: {
        invocationSource: "automation",
        triggerDetail: "system",
        contextSnapshot: { taskKey },
        responsibleUserId: "test-user",
      },
      coalescing: {
        taskKey,
        mergeContextSnapshot: (existing, incoming) => ({
          ...(existing && typeof existing === "object" ? existing : {}),
          ...incoming,
        }),
      },
    }));
  }

  it("serializes competing distinct scopes so one slot admits once", async () => {
    const { companyId, agentId } = await seedAgent(1);

    const results = await Promise.all([
      admit(companyId, agentId, "issue:a"),
      admit(companyId, agentId, "issue:b"),
    ]);

    expect(results.map((result) => result.kind).sort()).toEqual(["queued", "skipped"]);
    const runs = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.agentId, agentId));
    const wakes = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.agentId, agentId));
    expect(runs).toHaveLength(1);
    expect(wakes.filter((wake) => wake.status === "skipped")).toHaveLength(1);
  });

  it("coalesces competing same-scope wakes before applying the cap", async () => {
    const { companyId, agentId } = await seedAgent(1);

    const results = await Promise.all([
      admit(companyId, agentId, "issue:a"),
      admit(companyId, agentId, "issue:a"),
    ]);

    expect(results.map((result) => result.kind).sort()).toEqual(["coalesced", "queued"]);
    const runs = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.agentId, agentId));
    const wakes = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.agentId, agentId));
    expect(runs).toHaveLength(1);
    expect(wakes.filter((wake) => wake.status === "coalesced")).toHaveLength(1);
  });

  it("coalesces into an existing scope even when live capacity is full", async () => {
    const { companyId, agentId } = await seedAgent(1);
    const first = await admit(companyId, agentId, "issue:a");
    expect(first.kind).toBe("queued");

    const second = await admit(companyId, agentId, "issue:a");

    expect(second.kind).toBe("coalesced");
    expect("run" in second ? second.run.id : null).toBe("run" in first ? first.run.id : null);
  });

  it("keeps different agents on independent admission lanes", async () => {
    const first = await seedAgent(1);
    const second = await seedAgent(1);

    const results = await Promise.all([
      admit(first.companyId, first.agentId, "issue:a"),
      admit(second.companyId, second.agentId, "issue:b"),
    ]);

    expect(results.map((result) => result.kind)).toEqual(["queued", "queued"]);
  });

  it("serializes unlimited agents without imposing a cap", async () => {
    const { companyId, agentId } = await seedAgent(null);

    const results = await Promise.all([
      admit(companyId, agentId, "issue:a"),
      admit(companyId, agentId, "issue:b"),
    ]);

    expect(results.map((result) => result.kind)).toEqual(["queued", "queued"]);
    expect(await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.agentId, agentId)))
      .toHaveLength(2);
  });

  it("keeps a deferred wake durable at capacity and promotes it after a slot frees", async () => {
    const { companyId, agentId } = await seedAgent(1);
    const live = await admit(companyId, agentId, "issue:live");
    if (live.kind !== "queued") throw new Error("test setup failed to queue live run");
    const [deferredWake] = await db.insert(agentWakeupRequests).values({
      companyId,
      agentId,
      source: "automation",
      triggerDetail: "system",
      reason: "issue_execution_deferred",
      payload: { issueId: "issue:deferred" },
      status: "deferred_issue_execution",
      requestedByActorType: "system",
    }).returning();

    const promoteDeferred = () => db.transaction((tx) => admitLiveRun({
      tx,
      agent: { id: agentId, companyId },
      existingWakeupRequestId: deferredWake.id,
      preserveExistingWakeAtCapacity: true,
      wakeupRequest: {
        source: "automation",
        triggerDetail: "system",
        reason: "issue_execution_promoted",
        payload: { issueId: "issue:deferred" },
        requestedByActorType: "system",
        requestedByActorId: null,
      },
      run: {
        invocationSource: "automation",
        triggerDetail: "system",
        contextSnapshot: { taskKey: "issue:deferred", issueId: "issue:deferred" },
        responsibleUserId: "test-user",
      },
      coalescing: {
        taskKey: "issue:deferred",
        mergeContextSnapshot: (existing, incoming) => ({
          ...(existing && typeof existing === "object" ? existing : {}),
          ...incoming,
        }),
      },
    }));

    const blocked = await promoteDeferred();
    expect(blocked.kind).toBe("deferred_at_capacity");
    expect((await db.select().from(agentWakeupRequests)
      .where(eq(agentWakeupRequests.id, deferredWake.id)))[0]?.status)
      .toBe("deferred_issue_execution");

    await db.update(heartbeatRuns).set({ status: "succeeded" }).where(eq(heartbeatRuns.id, live.run.id));
    const promoted = await promoteDeferred();
    expect(promoted.kind).toBe("queued");
    expect(promoted.wakeupRequest.id).toBe(deferredWake.id);
    expect(promoted.wakeupRequest.status).toBe("queued");
  });

  it("keeps a due retry durable at capacity and promotes it after a slot frees", async () => {
    const { companyId, agentId } = await seedAgent(1);
    const live = await admit(companyId, agentId, "issue:live");
    if (live.kind !== "queued") throw new Error("test setup failed to queue live run");
    const [retryWake] = await db.insert(agentWakeupRequests).values({
      companyId,
      agentId,
      source: "automation",
      triggerDetail: "system",
      reason: "retry_test",
      status: "claimed",
      requestedByActorType: "system",
    }).returning();
    const [retryRun] = await db.insert(heartbeatRuns).values({
      companyId,
      agentId,
      invocationSource: "automation",
      triggerDetail: "system",
      status: "scheduled_retry",
      wakeupRequestId: retryWake.id,
      contextSnapshot: { taskKey: "issue:retry" },
      scheduledRetryAt: new Date(Date.now() - 1_000),
      responsibleUserId: "test-user",
    }).returning();
    await db.update(agentWakeupRequests)
      .set({ runId: retryRun.id })
      .where(eq(agentWakeupRequests.id, retryWake.id));
    expect(retryRun.wakeupRequestId).toBe(retryWake.id);

    const blocked = await db.transaction((tx) => promoteScheduledRetryWithAdmission({
      tx,
      agent: { id: agentId, companyId },
      runId: retryRun.id,
    }));
    expect(blocked.kind).toBe("at_capacity");
    expect((await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, retryRun.id)))[0]?.status)
      .toBe("scheduled_retry");
    const durableWake = (await db.select().from(agentWakeupRequests)
      .where(eq(agentWakeupRequests.id, retryWake.id)))[0];
    expect(durableWake?.status).toBe("claimed");
    expect(durableWake?.reason).toBe("retry_test");

    await db.update(heartbeatRuns).set({ status: "succeeded" }).where(eq(heartbeatRuns.id, live.run.id));
    const promoted = await db.transaction((tx) => promoteScheduledRetryWithAdmission({
      tx,
      agent: { id: agentId, companyId },
      runId: retryRun.id,
    }));
    expect(promoted.kind).toBe("promoted");
    expect(promoted.run?.status).toBe("queued");
  });

  it("admits only available slots when due retries compete", async () => {
    const { companyId, agentId } = await seedAgent(1);
    const retryIds: string[] = [];
    for (const taskKey of ["issue:retry-a", "issue:retry-b"]) {
      const [wake] = await db.insert(agentWakeupRequests).values({
        companyId,
        agentId,
        source: "automation",
        triggerDetail: "system",
        reason: "retry_test",
        status: "claimed",
        requestedByActorType: "system",
      }).returning();
      const [run] = await db.insert(heartbeatRuns).values({
        companyId,
        agentId,
        invocationSource: "automation",
        triggerDetail: "system",
        status: "scheduled_retry",
        wakeupRequestId: wake.id,
        contextSnapshot: { taskKey },
        scheduledRetryAt: new Date(Date.now() - 1_000),
        responsibleUserId: "test-user",
      }).returning();
      await db.update(agentWakeupRequests).set({ runId: run.id }).where(eq(agentWakeupRequests.id, wake.id));
      retryIds.push(run.id);
    }

    const results = await Promise.all(retryIds.map((runId) => db.transaction((tx) =>
      promoteScheduledRetryWithAdmission({
        tx,
        agent: { id: agentId, companyId },
        runId,
      }))));

    expect(results.map((result) => result.kind).sort()).toEqual(["at_capacity", "promoted"]);
    const runs = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.agentId, agentId));
    expect(runs.filter((run) => run.status === "queued")).toHaveLength(1);
    expect(runs.filter((run) => run.status === "scheduled_retry")).toHaveLength(1);
  });
});
