import { and, asc, eq, inArray, lte, sql } from "drizzle-orm";
import {
  agentWakeupRequests,
  agents,
  companies,
  heartbeatRuns,
  type Db,
} from "@paperclipai/db";

export type LiveRunAdmissionTransaction = Parameters<Parameters<Db["transaction"]>[0]>[0];

type AgentAdmissionSeed = Pick<
  typeof agents.$inferSelect,
  "id" | "companyId"
>;

type WakeupRequestSeed = Omit<
  typeof agentWakeupRequests.$inferInsert,
  "id" | "companyId" | "agentId" | "runId" | "status"
>;

type HeartbeatRunSeed = Omit<
  typeof heartbeatRuns.$inferInsert,
  "id" | "companyId" | "agentId" | "wakeupRequestId" | "status"
>;

type CoalescingOptions = {
  taskKey: string | null;
  allowQueued?: boolean;
  allowRunning?: boolean;
  allowScheduledRetry?: boolean;
  excludeRunIds?: string[];
  canCoalesce?: (run: typeof heartbeatRuns.$inferSelect) => boolean;
  mergeContextSnapshot: (existing: unknown, incoming: Record<string, unknown>) => Record<string, unknown>;
};

export type AdmitLiveRunInput = {
  tx: LiveRunAdmissionTransaction;
  agent: AgentAdmissionSeed;
  wakeupRequest: WakeupRequestSeed;
  run: HeartbeatRunSeed & { contextSnapshot: Record<string, unknown> };
  existingWakeupRequestId?: string | null;
  preserveExistingWakeAtCapacity?: false;
  coalescing: CoalescingOptions;
  now?: Date;
};

type PreserveExistingWakeAdmissionInput = Omit<AdmitLiveRunInput, "preserveExistingWakeAtCapacity"> & {
  existingWakeupRequestId: string;
  preserveExistingWakeAtCapacity: true;
};

export type LiveRunAdmissionResult =
  | { kind: "queued"; run: typeof heartbeatRuns.$inferSelect; wakeupRequest: typeof agentWakeupRequests.$inferSelect }
  | { kind: "coalesced"; run: typeof heartbeatRuns.$inferSelect; wakeupRequest: typeof agentWakeupRequests.$inferSelect }
  | { kind: "skipped"; observed: number; limit: number; wakeupRequest: typeof agentWakeupRequests.$inferSelect };

type PreserveExistingWakeAdmissionResult = LiveRunAdmissionResult
  | { kind: "deferred_at_capacity"; observed: number; limit: number; wakeupRequest: typeof agentWakeupRequests.$inferSelect };

function readObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

export function liveRunTaskKey(contextSnapshot: unknown) {
  const context = readObject(contextSnapshot);
  for (const key of ["taskKey", "taskId", "issueId"] as const) {
    const value = context[key];
    if (typeof value === "string" && value.trim().length > 0) return value.trim();
  }
  return null;
}

export function parseMaxLiveRuns(runtimeConfig: unknown) {
  const heartbeat = readObject(readObject(runtimeConfig).heartbeat);
  const value = heartbeat.maxLiveRuns;
  return typeof value === "number" && Number.isInteger(value) && value > 0
    ? value
    : null;
}

function sameTaskScope(left: string | null, right: string | null) {
  return (left ?? null) === (right ?? null);
}

function heartbeatSkipPayload(payload: unknown, observed: number, limit: number) {
  return {
    ...readObject(payload),
    heartbeatSkip: {
      reason: "heartbeat.live_run_limit",
      observed,
      limit,
    },
  };
}

function requireWakeupRequest(
  wakeupRequest: typeof agentWakeupRequests.$inferSelect | undefined,
  wakeupRequestId: string,
) {
  if (!wakeupRequest) {
    throw new Error(`Live-run admission wakeup request ${wakeupRequestId} no longer exists`);
  }
  return wakeupRequest;
}

/**
 * The only path for creating a fresh queued heartbeat run.
 *
 * The agent row is the admission mutex. Under that lock the primitive first
 * coalesces an equivalent live scope, then counts queued/running runs, then
 * records either a skipped request or one linked request/run pair.
 */
export function admitLiveRun(input: PreserveExistingWakeAdmissionInput): Promise<PreserveExistingWakeAdmissionResult>;
export function admitLiveRun(input: AdmitLiveRunInput): Promise<LiveRunAdmissionResult>;
export async function admitLiveRun(
  input: AdmitLiveRunInput | PreserveExistingWakeAdmissionInput,
): Promise<PreserveExistingWakeAdmissionResult> {
  const now = input.now ?? new Date();
  const companyExists = await input.tx
    .select({ id: companies.id })
    .from(companies)
    .where(eq(companies.id, input.agent.companyId))
    .for("key share")
    .then((rows) => Boolean(rows[0]));
  if (!companyExists) throw new Error("Live-run admission company no longer exists");

  const lockedAgent = await input.tx
    .select({
      id: agents.id,
      companyId: agents.companyId,
      runtimeConfig: agents.runtimeConfig,
    })
    .from(agents)
    .where(and(eq(agents.id, input.agent.id), eq(agents.companyId, input.agent.companyId)))
    .for("update")
    .then((rows) => rows[0] ?? null);
  if (!lockedAgent) throw new Error("Live-run admission target agent no longer exists");

  const activeRuns = await input.tx
    .select()
    .from(heartbeatRuns)
    .where(and(
      eq(heartbeatRuns.companyId, lockedAgent.companyId),
      eq(heartbeatRuns.agentId, lockedAgent.id),
      inArray(heartbeatRuns.status, ["queued", "running", "scheduled_retry"]),
    ))
    .orderBy(
      sql`case ${heartbeatRuns.status} when 'queued' then 0 when 'scheduled_retry' then 1 else 2 end`,
      asc(heartbeatRuns.createdAt),
      asc(heartbeatRuns.id),
    );

  const excluded = new Set(input.coalescing.excludeRunIds ?? []);
  const allowedStatuses = [
    ...(input.coalescing.allowQueued === false ? [] : ["queued" as const]),
    ...(input.coalescing.allowRunning === false ? [] : ["running" as const]),
    ...(input.coalescing.allowScheduledRetry === false ? [] : ["scheduled_retry" as const]),
  ];
  for (const coalescedTarget of activeRuns) {
    if (excluded.has(coalescedTarget.id)) continue;
    if (!allowedStatuses.includes(coalescedTarget.status as typeof allowedStatuses[number])) continue;
    if (!sameTaskScope(liveRunTaskKey(coalescedTarget.contextSnapshot), input.coalescing.taskKey)) continue;
    if (!(input.coalescing.canCoalesce?.(coalescedTarget) ?? true)) continue;
    const mergedRun = await input.tx
      .update(heartbeatRuns)
      .set({
        contextSnapshot: input.coalescing.mergeContextSnapshot(
          coalescedTarget.contextSnapshot,
          input.run.contextSnapshot,
        ),
        updatedAt: now,
      })
      .where(and(
        eq(heartbeatRuns.id, coalescedTarget.id),
        inArray(heartbeatRuns.status, allowedStatuses),
      ))
      .returning()
      .then((rows) => rows[0]);
    if (!mergedRun) continue;

    const coalescedValues = {
      ...input.wakeupRequest,
      status: "coalesced" as const,
      coalescedCount: 1,
      runId: mergedRun.id,
      finishedAt: now,
      updatedAt: now,
    };
    const wakeupRequest = input.existingWakeupRequestId
      ? await input.tx
        .update(agentWakeupRequests)
        .set(coalescedValues)
        .where(and(
          eq(agentWakeupRequests.id, input.existingWakeupRequestId),
          eq(agentWakeupRequests.companyId, lockedAgent.companyId),
          eq(agentWakeupRequests.agentId, lockedAgent.id),
        ))
        .returning()
        .then((rows) => requireWakeupRequest(rows[0], input.existingWakeupRequestId!))
      : await input.tx
        .insert(agentWakeupRequests)
        .values({
          companyId: lockedAgent.companyId,
          agentId: lockedAgent.id,
          ...coalescedValues,
        })
        .returning()
        .then((rows) => rows[0]);

    return { kind: "coalesced", run: mergedRun, wakeupRequest };
  }

  const [{ observed }] = await input.tx
    .select({ observed: sql<number>`count(*)::int` })
    .from(heartbeatRuns)
    .where(and(
      eq(heartbeatRuns.companyId, lockedAgent.companyId),
      eq(heartbeatRuns.agentId, lockedAgent.id),
      inArray(heartbeatRuns.status, ["queued", "running"]),
    ));
  const limit = parseMaxLiveRuns(lockedAgent.runtimeConfig);
  if (limit !== null && observed >= limit) {
    if (input.preserveExistingWakeAtCapacity) {
      const durableWakeupRequest = await input.tx
        .select()
        .from(agentWakeupRequests)
        .where(and(
          eq(agentWakeupRequests.id, input.existingWakeupRequestId),
          eq(agentWakeupRequests.companyId, lockedAgent.companyId),
          eq(agentWakeupRequests.agentId, lockedAgent.id),
        ))
        .for("update")
        .then((rows) => requireWakeupRequest(rows[0], input.existingWakeupRequestId));
      return {
        kind: "deferred_at_capacity",
        observed,
        limit,
        wakeupRequest: durableWakeupRequest,
      };
    }
    const skippedValues = {
      ...input.wakeupRequest,
      reason: "heartbeat.live_run_limit",
      payload: heartbeatSkipPayload(input.wakeupRequest.payload, observed, limit),
      status: "skipped" as const,
      finishedAt: now,
      updatedAt: now,
    };
    const wakeupRequest = input.existingWakeupRequestId
      ? await input.tx
        .update(agentWakeupRequests)
        .set(skippedValues)
        .where(and(
          eq(agentWakeupRequests.id, input.existingWakeupRequestId),
          eq(agentWakeupRequests.companyId, lockedAgent.companyId),
          eq(agentWakeupRequests.agentId, lockedAgent.id),
        ))
        .returning()
        .then((rows) => requireWakeupRequest(rows[0], input.existingWakeupRequestId!))
      : await input.tx
        .insert(agentWakeupRequests)
        .values({
          companyId: lockedAgent.companyId,
          agentId: lockedAgent.id,
          ...skippedValues,
        })
        .returning()
        .then((rows) => rows[0]);
    return { kind: "skipped", observed, limit, wakeupRequest };
  }

  const queuedWakeValues = {
    ...input.wakeupRequest,
    status: "queued" as const,
    runId: null,
    claimedAt: null,
    finishedAt: null,
    error: null,
    updatedAt: now,
  };
  const wakeupRequest = input.existingWakeupRequestId
    ? await input.tx
      .update(agentWakeupRequests)
      .set(queuedWakeValues)
      .where(and(
        eq(agentWakeupRequests.id, input.existingWakeupRequestId),
        eq(agentWakeupRequests.companyId, lockedAgent.companyId),
        eq(agentWakeupRequests.agentId, lockedAgent.id),
      ))
      .returning()
      .then((rows) => requireWakeupRequest(rows[0], input.existingWakeupRequestId!))
    : await input.tx
      .insert(agentWakeupRequests)
      .values({
        companyId: lockedAgent.companyId,
        agentId: lockedAgent.id,
        ...queuedWakeValues,
      })
      .returning()
      .then((rows) => rows[0]);

  const queuedRun = await input.tx
    .insert(heartbeatRuns)
    .values({
      companyId: lockedAgent.companyId,
      agentId: lockedAgent.id,
      ...input.run,
      status: "queued",
      wakeupRequestId: wakeupRequest.id,
      updatedAt: now,
    })
    .returning()
    .then((rows) => rows[0]);

  const linkedWakeupRequest = await input.tx
    .update(agentWakeupRequests)
    .set({ runId: queuedRun.id, updatedAt: now })
    .where(and(
      eq(agentWakeupRequests.id, wakeupRequest.id),
      eq(agentWakeupRequests.companyId, lockedAgent.companyId),
      eq(agentWakeupRequests.agentId, lockedAgent.id),
    ))
    .returning()
    .then((rows) => requireWakeupRequest(rows[0], wakeupRequest.id));

  return { kind: "queued", run: queuedRun, wakeupRequest: linkedWakeupRequest };
}

export async function promoteScheduledRetryWithAdmission(input: {
  tx: LiveRunAdmissionTransaction;
  agent: AgentAdmissionSeed;
  runId: string;
  now?: Date;
}) {
  const now = input.now ?? new Date();
  const lockedAgent = await input.tx
    .select({ id: agents.id, companyId: agents.companyId, runtimeConfig: agents.runtimeConfig })
    .from(agents)
    .where(and(eq(agents.id, input.agent.id), eq(agents.companyId, input.agent.companyId)))
    .for("update")
    .then((rows) => rows[0] ?? null);
  if (!lockedAgent) return { kind: "not_promoted" as const, run: null };

  const dueRun = await input.tx
    .select()
    .from(heartbeatRuns)
    .where(and(
      eq(heartbeatRuns.id, input.runId),
      eq(heartbeatRuns.companyId, lockedAgent.companyId),
      eq(heartbeatRuns.agentId, lockedAgent.id),
      eq(heartbeatRuns.status, "scheduled_retry"),
    ))
    .for("update")
    .then((rows) => rows[0] ?? null);
  if (!dueRun) return { kind: "not_promoted" as const, run: null };

  const [{ observed }] = await input.tx
    .select({ observed: sql<number>`count(*)::int` })
    .from(heartbeatRuns)
    .where(and(
      eq(heartbeatRuns.companyId, lockedAgent.companyId),
      eq(heartbeatRuns.agentId, lockedAgent.id),
      inArray(heartbeatRuns.status, ["queued", "running"]),
    ));
  const limit = parseMaxLiveRuns(lockedAgent.runtimeConfig);
  if (limit !== null && observed >= limit) {
    return { kind: "at_capacity" as const, run: dueRun, observed, limit };
  }

  const promoted = await input.tx
    .update(heartbeatRuns)
    .set({ status: "queued", updatedAt: now })
    .where(and(
      eq(heartbeatRuns.id, dueRun.id),
      eq(heartbeatRuns.status, "scheduled_retry"),
      lte(heartbeatRuns.scheduledRetryAt, now),
    ))
    .returning()
    .then((rows) => rows[0] ?? null);
  if (promoted?.wakeupRequestId) {
    await input.tx
      .update(agentWakeupRequests)
      .set({ status: "queued", finishedAt: null, error: null, updatedAt: now })
      .where(and(
        eq(agentWakeupRequests.id, promoted.wakeupRequestId),
        eq(agentWakeupRequests.companyId, lockedAgent.companyId),
        eq(agentWakeupRequests.agentId, lockedAgent.id),
      ))
      .returning()
      .then((rows) => requireWakeupRequest(rows[0], promoted.wakeupRequestId!));
  }
  return promoted
    ? { kind: "promoted" as const, run: promoted }
    : { kind: "not_promoted" as const, run: null };
}
