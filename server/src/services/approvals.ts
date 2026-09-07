import { and, asc, eq, inArray, sql } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { approvalComments, approvals } from "@paperclipai/db";
import { HttpError, notFound, unprocessable } from "../errors.js";
import { redactCurrentUserText } from "../log-redaction.js";
import { secretService } from "./secrets.js";
import { agentService } from "./agents.js";
import { budgetService } from "./budgets.js";
import { notifyHireApproved } from "./hire-hook.js";
import { instanceSettingsService } from "./instance-settings.js";

import { publishGatewayActivities, type ActivityPublication } from "./activity-log.js";

type ApprovalTransaction = { notificationDb: Db; publications: ActivityPublication[]; afterCommit: Array<() => void> };

export function approvalService(db: Db, transaction?: ApprovalTransaction) {
  const agentsSvc = agentService(db, transaction?.publications);
  const budgets = budgetService(db);
  const instanceSettings = instanceSettingsService(db);
  const canResolveStatuses = new Set(["pending", "revision_requested"]);
  const resolvableStatuses = Array.from(canResolveStatuses);
  type ApprovalRecord = typeof approvals.$inferSelect;
  type ResolutionResult = { approval: ApprovalRecord; applied: boolean };

  function redactApprovalComment<T extends { body: string }>(comment: T, censorUsernameInLogs: boolean): T {
    return {
      ...comment,
      body: redactCurrentUserText(comment.body, { enabled: censorUsernameInLogs }),
    };
  }

  async function reconcileApprovedBuiltInAgent(companyId: string, payload: Record<string, unknown>) {
    const sourceBuiltInAgentKey = typeof payload.sourceBuiltInAgentKey === "string" ? payload.sourceBuiltInAgentKey : null;
    if (!sourceBuiltInAgentKey) return;
    const { builtInAgentService } = await import("./built-in-agents.js");
    await builtInAgentService(db).ensure(companyId, sourceBuiltInAgentKey);
  }

  async function getExistingApproval(id: string) {
    const existing = await db
      .select()
      .from(approvals)
      .where(eq(approvals.id, id))
      .then((rows) => rows[0] ?? null);
    if (!existing) throw notFound("Approval not found");
    return existing;
  }

  async function resolveApproval(
    id: string,
    targetStatus: "approved" | "rejected",
    decidedByUserId: string,
    decisionNote: string | null | undefined,
    prior?: ApprovalRecord,
  ): Promise<ResolutionResult> {
    const existing = prior ?? await getExistingApproval(id);
    if (!canResolveStatuses.has(existing.status)) {
      if (existing.status === targetStatus) {
        return { approval: existing, applied: false };
      }
      throw unprocessable(
        `Only pending or revision requested approvals can be ${targetStatus === "approved" ? "approved" : "rejected"}`,
      );
    }

    const now = new Date();
    const updated = await db
      .update(approvals)
      .set({
        status: targetStatus,
        decidedByUserId,
        decisionNote: decisionNote ?? null,
        decidedAt: now,
        updatedAt: now,
      })
      .where(and(eq(approvals.id, id), inArray(approvals.status, resolvableStatuses)))
      .returning()
      .then((rows) => rows[0] ?? null);

    if (updated) {
      return { approval: updated, applied: true };
    }

    const latest = await getExistingApproval(id);
    if (latest.status === targetStatus) {
      return { approval: latest, applied: false };
    }

    throw unprocessable(
      `Only pending or revision requested approvals can be ${targetStatus === "approved" ? "approved" : "rejected"}`,
    );
  }

  return {
    list: (companyId: string, status?: string) => {
      const conditions = [eq(approvals.companyId, companyId)];
      if (status) conditions.push(eq(approvals.status, status));
      return db.select().from(approvals).where(and(...conditions));
    },

    getById: (id: string) =>
      db
        .select()
        .from(approvals)
        .where(eq(approvals.id, id))
        .then((rows) => rows[0] ?? null),

    findOpenHireApprovalForAgent: async (companyId: string, agentId: string) => {
      const rows = await db
        .select()
        .from(approvals)
        .where(
          and(
            eq(approvals.companyId, companyId),
            eq(approvals.type, "hire_agent"),
            inArray(approvals.status, resolvableStatuses),
            sql`${approvals.payload} ->> 'agentId' = ${agentId}`,
          ),
        );
      return rows[0] ?? null;
    },

    create: async (companyId: string, data: Omit<typeof approvals.$inferInsert, "companyId">) => {
      const payload = data.payload as Record<string, unknown>;
      const payloadAgent = data.type === "hire_agent" && payload.adapterType === undefined && typeof payload.agentId === "string"
        ? await agentsSvc.getById(payload.agentId) : null;
      const gateway = data.type === "hire_agent" && (payload.adapterType ?? payloadAgent?.adapterType) === "openclaw_gateway";
      const persist = async (scopedDb: Db) => {
        const normalizedPayload = gateway ? await secretService(scopedDb).normalizeHireApprovalPayloadForPersistence(companyId, payload, {
          adapterType: "openclaw_gateway", actor: { userId: data.requestedByUserId, agentId: data.requestedByAgentId },
        }) : payload;
        return scopedDb.insert(approvals).values({ ...data, payload: normalizedPayload, companyId }).returning().then((rows) => rows[0]);
      };
      if (!gateway) return persist(db);
      return db.transaction((tx) => persist(tx as unknown as Db)).catch((error: unknown) => {
        if (error instanceof HttpError) throw error;
        throw new Error("OpenClaw approval persistence failed");
      });
    },

    // Cancel an open (pending/revision_requested) approval without a board
    // decision — e.g. when its paired agent is terminated during duplicate
    // cleanup. Idempotent: a no-op on already-resolved approvals.
    cancel: async (id: string, reason?: string | null) => {
      const now = new Date();
      const updated = await db
        .update(approvals)
        .set({
          status: "cancelled",
          decisionNote: reason ?? null,
          decidedAt: now,
          updatedAt: now,
        })
        .where(and(eq(approvals.id, id), inArray(approvals.status, resolvableStatuses)))
        .returning()
        .then((rows) => rows[0] ?? null);
      return updated;
    },

    approve: async (id: string, decidedByUserId: string, decisionNote?: string | null): Promise<ResolutionResult> => {
      let prior: ApprovalRecord | undefined;
      if (!transaction) {
        const candidate = await getExistingApproval(id);
        prior = candidate;
        const payload = candidate.payload as Record<string, unknown>;
        const pendingAgent = candidate.type === "hire_agent" && typeof payload.agentId === "string"
          ? await agentsSvc.getById(payload.agentId) : null;
        if (candidate.type === "hire_agent" && (payload.adapterType === "openclaw_gateway" || pendingAgent?.adapterType === "openclaw_gateway")) {
          const context: ApprovalTransaction = { notificationDb: db, publications: [], afterCommit: [] };
          const result = await db.transaction((tx) => approvalService(tx as unknown as Db, context).approve(id, decidedByUserId, decisionNote))
            .catch((error: unknown) => {
              if (error instanceof HttpError) throw error;
              throw new Error("OpenClaw approval persistence failed");
            });
          publishGatewayActivities(context.publications);
          for (const notify of context.afterCommit) notify();
          return result;
        }
      }
      const { approval: updated, applied } = await resolveApproval(
        id,
        "approved",
        decidedByUserId,
        decisionNote,
        prior,
      );

      let hireApprovedAgentId: string | null = null;
      const now = new Date();
      if (applied && updated.type === "hire_agent") {
        if (transaction) {
          updated.payload = await secretService(db).normalizeHireApprovalPayloadForPersistence(updated.companyId, updated.payload, {
            adapterType: "openclaw_gateway", actor: { userId: decidedByUserId },
          });
          await db.update(approvals).set({ payload: updated.payload }).where(eq(approvals.id, updated.id));
        }
        const payload = updated.payload as Record<string, unknown>;
        const payloadAgentId = typeof payload.agentId === "string" ? payload.agentId : null;
        if (payloadAgentId) {
          await agentsSvc.activatePendingApproval(payloadAgentId, payload, { userId: decidedByUserId });
          await reconcileApprovedBuiltInAgent(updated.companyId, payload);
          hireApprovedAgentId = payloadAgentId;
        } else {
          const created = await agentsSvc.create(updated.companyId, {
            name: String(payload.name ?? "New Agent"),
            role: String(payload.role ?? "general"),
            title: typeof payload.title === "string" ? payload.title : null,
            reportsTo: typeof payload.reportsTo === "string" ? payload.reportsTo : null,
            capabilities: typeof payload.capabilities === "string" ? payload.capabilities : null,
            adapterType: String(payload.adapterType ?? "process"),
            adapterConfig:
              typeof payload.adapterConfig === "object" && payload.adapterConfig !== null
                ? (payload.adapterConfig as Record<string, unknown>)
                : {},
            budgetMonthlyCents:
              typeof payload.budgetMonthlyCents === "number" ? payload.budgetMonthlyCents : 0,
            metadata:
              typeof payload.metadata === "object" && payload.metadata !== null
                ? (payload.metadata as Record<string, unknown>)
                : null,
            status: "idle",
            spentMonthlyCents: 0,
            permissions: undefined,
            lastHeartbeatAt: null,
            runtimeConfig: typeof payload.runtimeConfig === "object" && payload.runtimeConfig !== null ? payload.runtimeConfig as Record<string, unknown> : {},
          }, ...(payload.adapterType === "openclaw_gateway" ? [{ actor: { userId: decidedByUserId } }] as const : []));
          hireApprovedAgentId = created?.id ?? null;
        }
        if (hireApprovedAgentId) {
          const budgetMonthlyCents =
            typeof payload.budgetMonthlyCents === "number" ? payload.budgetMonthlyCents : 0;
          if (budgetMonthlyCents > 0) {
            await budgets.upsertPolicy(
              updated.companyId,
              {
                scopeType: "agent",
                scopeId: hireApprovedAgentId,
                amount: budgetMonthlyCents,
                windowKind: "calendar_month_utc",
              },
              decidedByUserId,
            );
          }
          const approvedAgentId = hireApprovedAgentId;
          const notify = () => { void notifyHireApproved(transaction?.notificationDb ?? db, {
            companyId: updated.companyId,
            agentId: approvedAgentId,
            source: "approval",
            sourceId: id,
            approvedAt: now,
          }).catch(() => {}); };
          if (transaction) transaction.afterCommit.push(notify);
          else notify();
        }
      }

      return { approval: updated, applied };
    },

    reject: async (id: string, decidedByUserId: string, decisionNote?: string | null) => {
      const { approval: updated, applied } = await resolveApproval(
        id,
        "rejected",
        decidedByUserId,
        decisionNote,
      );

      if (applied && updated.type === "hire_agent") {
        const payload = updated.payload as Record<string, unknown>;
        const payloadAgentId = typeof payload.agentId === "string" ? payload.agentId : null;
        if (payloadAgentId) {
          await agentsSvc.terminate(payloadAgentId);
        }
      }

      return { approval: updated, applied };
    },

    requestRevision: async (id: string, decidedByUserId: string, decisionNote?: string | null) => {
      const existing = await getExistingApproval(id);
      if (existing.status !== "pending") {
        throw unprocessable("Only pending approvals can request revision");
      }

      const now = new Date();
      return db
        .update(approvals)
        .set({
          status: "revision_requested",
          decidedByUserId,
          decisionNote: decisionNote ?? null,
          decidedAt: now,
          updatedAt: now,
        })
        .where(eq(approvals.id, id))
        .returning()
        .then((rows) => rows[0]);
    },

    resubmit: async (id: string, payload?: Record<string, unknown>, actor?: { userId?: string | null; agentId?: string | null }) => {
      const existing = await getExistingApproval(id);
      if (existing.status !== "revision_requested") {
        throw unprocessable("Only revision requested approvals can be resubmitted");
      }

      const adapterType = payload?.adapterType ?? existing.payload.adapterType;
      const linkedAgentId = payload?.agentId ?? existing.payload.agentId;
      const linkedAgent = existing.type === "hire_agent" && adapterType === undefined && typeof linkedAgentId === "string"
        ? await agentsSvc.getById(linkedAgentId) : null;
      const gateway = existing.type === "hire_agent" && (adapterType ?? linkedAgent?.adapterType) === "openclaw_gateway";
      const persist = async (scopedDb: Db) => {
        const normalizedPayload = gateway ? await secretService(scopedDb).normalizeHireApprovalPayloadForPersistence(existing.companyId, payload ?? existing.payload, {
          adapterType: "openclaw_gateway", actor: actor ?? { userId: existing.requestedByUserId, agentId: existing.requestedByAgentId },
        }) : payload ?? existing.payload;
        return scopedDb.update(approvals).set({
          status: "pending", payload: normalizedPayload, decisionNote: null, decidedByUserId: null, decidedAt: null, updatedAt: new Date(),
        }).where(eq(approvals.id, id)).returning().then((rows) => rows[0]);
      };
      if (!gateway) return persist(db);
      return db.transaction((tx) => persist(tx as unknown as Db)).catch((error: unknown) => {
        if (error instanceof HttpError) throw error;
        throw new Error("OpenClaw approval persistence failed");
      });
    },

    listComments: async (approvalId: string) => {
      const existing = await getExistingApproval(approvalId);
      const { censorUsernameInLogs } = await instanceSettings.getGeneral();
      return db
        .select()
        .from(approvalComments)
        .where(
          and(
            eq(approvalComments.approvalId, approvalId),
            eq(approvalComments.companyId, existing.companyId),
          ),
        )
        .orderBy(asc(approvalComments.createdAt))
        .then((comments) => comments.map((comment) => redactApprovalComment(comment, censorUsernameInLogs)));
    },

    addComment: async (
      approvalId: string,
      body: string,
      actor: { agentId?: string; userId?: string },
    ) => {
      const existing = await getExistingApproval(approvalId);
      const currentUserRedactionOptions = {
        enabled: (await instanceSettings.getGeneral()).censorUsernameInLogs,
      };
      const redactedBody = redactCurrentUserText(body, currentUserRedactionOptions);
      return db
        .insert(approvalComments)
        .values({
          companyId: existing.companyId,
          approvalId,
          authorAgentId: actor.agentId ?? null,
          authorUserId: actor.userId ?? null,
          body: redactedBody,
        })
        .returning()
        .then((rows) => redactApprovalComment(rows[0], currentUserRedactionOptions.enabled));
    },
  };
}
