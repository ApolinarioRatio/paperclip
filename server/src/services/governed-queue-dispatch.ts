import { createHash } from "node:crypto";
import { and, count, eq, inArray, isNull, ne } from "drizzle-orm";
import {
  agentWakeupRequests,
  agents,
  approvals,
  companies,
  governedIssueControlCutover,
  heartbeatRuns,
  issueApprovals,
  issueRelations,
  issues,
  type Db,
} from "@paperclipai/db";
import { conflict, preconditionFailed, unprocessable } from "../errors.js";
import { evaluateAgentInvokabilityFromDb } from "./agent-invokability.js";
import { persistActivity, type ActivityPublication } from "./activity-log.js";
import { budgetService } from "./budgets.js";
import {
  assertGovernedAssignmentCapacity,
  hasGovernedSingleActiveAssignmentPolicy,
} from "./governed-queue-assignment.js";
import {
  activateGovernedIssueControl,
  lockGovernedIssueLane,
  resolveGovernedVerifierPool,
} from "./governed-issue-separation.js";
import { issueTreeControlService } from "./issue-tree-control.js";

const LIVE_WAKE_STATUSES = ["queued", "deferred_issue_execution", "claimed"] as const;
const LIVE_RUN_STATUSES = ["queued", "running"] as const;
const SERVER_MAX_DISPATCHES = 3;
const MAX_AUTHORIZATION_TTL_MS = 15 * 60 * 1000;
const MARKER_PREFIX = "AUTO-QUEUE:";

async function assertIssueCreatedAfterGovernedCutover(dbOrTx: Db, issue: typeof issues.$inferSelect) {
  const cutover = await dbOrTx
    .select({ activatedAt: governedIssueControlCutover.activatedAt })
    .from(governedIssueControlCutover)
    .where(eq(governedIssueControlCutover.id, "singleton"))
    .then((rows) => rows[0] ?? null);
  if (!cutover || issue.createdAt < cutover.activatedAt) {
    throw conflict("Governed dispatch requires an issue created after the immutable control cutover", {
      code: "governed_issue_predates_cutover",
      issueCreatedAt: issue.createdAt.toISOString(),
      cutoverActivatedAt: cutover?.activatedAt.toISOString() ?? null,
    });
  }
}

function readObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function readNonEmptyString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function normalizedMarkerDescription(description: string | null, approvalMarker: string) {
  const retained = (description ?? "")
    .split("\n")
    .filter((line) => !line.trimStart().startsWith(MARKER_PREFIX))
    .join("\n")
    .trimEnd();
  return `${retained}${retained ? "\n\n" : ""}${MARKER_PREFIX} ${approvalMarker}`;
}

export function governedQueueScopeDigest(issue: {
  id: string;
  companyId: string;
  projectId: string | null;
  parentId: string | null;
  goalId: string | null;
  identifier: string | null;
  title: string;
  description: string | null;
  priority: string;
  targetAgentId: string;
  operationId: string;
  policyDigest: string;
  approvalMarker: string;
  expiresAt: string;
  maxDispatches: number;
}) {
  const canonical = {
    id: issue.id,
    companyId: issue.companyId,
    projectId: issue.projectId,
    parentId: issue.parentId,
    goalId: issue.goalId,
    identifier: issue.identifier,
    title: issue.title,
    description: issue.description,
    priority: issue.priority,
    targetAgentId: issue.targetAgentId,
    operationId: issue.operationId,
    policyDigest: issue.policyDigest,
    reviewRequired: true,
    approvalMarker: issue.approvalMarker,
    expiresAt: issue.expiresAt,
    maxDispatches: issue.maxDispatches,
  };
  return createHash("sha256").update(JSON.stringify(canonical)).digest("hex");
}

export type GovernedQueueDispatchInput = {
  issueId: string;
  companyId: string;
  expectedUpdatedAt: string;
  approvalId: string;
  approvalMarker: string;
  targetAgentId: string;
  authorityAgentId: string;
  maxDispatches: number;
  expiresAt: string;
  idempotencyKey: string;
  responsibleUserId: string;
  sessionIdBefore: string | null;
  now?: Date;
};

export type GovernedQueueApprovalRequest = Pick<
  GovernedQueueDispatchInput,
  | "issueId"
  | "companyId"
  | "expectedUpdatedAt"
  | "approvalMarker"
  | "targetAgentId"
  | "authorityAgentId"
  | "maxDispatches"
  | "expiresAt"
  | "idempotencyKey"
  | "now"
>;

export async function prepareGovernedQueueApprovalPayload(
  db: Db,
  input: GovernedQueueApprovalRequest,
  basePayload: Record<string, unknown>,
) {
  const now = input.now ?? new Date();
  const expectedUpdatedAt = new Date(input.expectedUpdatedAt);
  const expiresAt = new Date(input.expiresAt);
  const approvalMarker = readNonEmptyString(input.approvalMarker);
  const callerIdempotencyKey = readNonEmptyString(input.idempotencyKey);
  if (Number.isNaN(expectedUpdatedAt.getTime())) {
    throw preconditionFailed("Invalid expectedUpdatedAt", { code: "invalid_issue_version" });
  }
  if (Number.isNaN(expiresAt.getTime()) || expiresAt <= now) {
    throw preconditionFailed("Queue authorization must expire in the future", {
      code: "invalid_queue_expiry",
    });
  }
  if (expiresAt.getTime() - now.getTime() > MAX_AUTHORIZATION_TTL_MS) {
    throw preconditionFailed("Queue authorization lifetime exceeds the server maximum", {
      code: "queue_expiry_exceeds_server_limit",
      maxTtlSeconds: MAX_AUTHORIZATION_TTL_MS / 1000,
    });
  }
  if (!approvalMarker || approvalMarker.includes("\n") || approvalMarker.length > 200) {
    throw unprocessable("approvalMarker must be a single non-empty line of at most 200 characters");
  }
  if (!callerIdempotencyKey || callerIdempotencyKey.length > 200) {
    throw unprocessable("idempotencyKey must contain 1 to 200 characters");
  }
  if (!Number.isInteger(input.maxDispatches) || input.maxDispatches < 1 || input.maxDispatches > SERVER_MAX_DISPATCHES) {
    throw unprocessable(`maxDispatches must be between 1 and ${SERVER_MAX_DISPATCHES}`);
  }

  return db.transaction(async (tx) => {
    await lockGovernedIssueLane(tx as unknown as Db, input.companyId, input.issueId);
    const issue = await tx.select().from(issues)
      .where(and(eq(issues.id, input.issueId), eq(issues.companyId, input.companyId)))
      .for("update")
      .then((rows) => rows[0] ?? null);
    if (!issue) throw conflict("Issue does not belong to company", { code: "issue_company_mismatch" });
    await assertIssueCreatedAfterGovernedCutover(tx as unknown as Db, issue);
    const target = await tx.select({ id: agents.id, companyId: agents.companyId }).from(agents)
      .where(and(eq(agents.id, input.targetAgentId), eq(agents.companyId, input.companyId)))
      .then((rows) => rows[0] ?? null);
    if (!target) throw conflict("Target agent does not belong to company", { code: "target_company_mismatch" });
    if (issue.updatedAt.getTime() !== expectedUpdatedAt.getTime()) {
      throw preconditionFailed("Issue version no longer matches", { code: "stale_issue_version" });
    }
    if (issue.status !== "todo" || issue.assigneeAgentId !== null || issue.assigneeUserId !== null) {
      throw conflict("Issue is no longer an unassigned todo", { code: "issue_not_claimable" });
    }
    if (issue.startedAt !== null || issue.checkoutRunId !== null || issue.executionRunId !== null) {
      throw conflict("Governed dispatch cannot establish a complete builder set for previously executed work", {
        code: "governed_prior_builder_history_unresolved",
      });
    }
    const verifierPool = await resolveGovernedVerifierPool(tx as unknown as Db, {
      companyId: input.companyId,
      issueId: input.issueId,
      executionPolicy: issue.executionPolicy,
      additionalBuilderAgentIds: [input.targetAgentId],
    });
    const serverIdempotencyKey = `governed_queue:${input.authorityAgentId}:${callerIdempotencyKey}`;
    const markedDescription = normalizedMarkerDescription(issue.description, approvalMarker);
    const scopeDigest = governedQueueScopeDigest({
      ...issue,
      description: markedDescription,
      targetAgentId: input.targetAgentId,
      operationId: serverIdempotencyKey,
      policyDigest: verifierPool.policyDigest,
      approvalMarker,
      expiresAt: expiresAt.toISOString(),
      maxDispatches: input.maxDispatches,
    });
    return {
      ...basePayload,
      queueDispatchRequest: {
        expectedUpdatedAt: expectedUpdatedAt.toISOString(),
        approvalMarker,
        targetAgentId: input.targetAgentId,
        expiresAt: expiresAt.toISOString(),
        idempotencyKey: callerIdempotencyKey,
        maxDispatches: input.maxDispatches,
      },
      queueDispatchScope: {
        schemaVersion: 1,
        issueId: input.issueId,
        companyId: input.companyId,
        targetAgentId: input.targetAgentId,
        operationId: serverIdempotencyKey,
        issueUpdatedAt: expectedUpdatedAt.toISOString(),
        policyDigest: verifierPool.policyDigest,
        reviewRequired: true,
        scopeDigest,
        issuedAt: now.toISOString(),
      },
    };
  });
}

export function governedQueueDispatchService(db: Db) {
  return {
    dispatch: async (input: GovernedQueueDispatchInput) => {
      const now = input.now ?? new Date();
      const expectedUpdatedAt = new Date(input.expectedUpdatedAt);
      const expiresAt = new Date(input.expiresAt);
      const approvalMarker = readNonEmptyString(input.approvalMarker);
      const callerIdempotencyKey = readNonEmptyString(input.idempotencyKey);
      const responsibleUserId = readNonEmptyString(input.responsibleUserId);
      if (Number.isNaN(expectedUpdatedAt.getTime())) {
        throw preconditionFailed("Invalid expectedUpdatedAt", { code: "invalid_issue_version" });
      }
      if (Number.isNaN(expiresAt.getTime())) {
        throw preconditionFailed("Queue authorization expiry is invalid", { code: "invalid_queue_expiry" });
      }
      if (!approvalMarker || approvalMarker.includes("\n") || approvalMarker.length > 200) {
        throw unprocessable("approvalMarker must be a single non-empty line of at most 200 characters");
      }
      if (!callerIdempotencyKey || callerIdempotencyKey.length > 200) {
        throw unprocessable("idempotencyKey must contain 1 to 200 characters");
      }
      if (!responsibleUserId) {
        throw unprocessable("A responsible user is required for governed queue dispatch");
      }
      if (!Number.isInteger(input.maxDispatches) || input.maxDispatches < 1 || input.maxDispatches > SERVER_MAX_DISPATCHES) {
        throw unprocessable(`maxDispatches must be between 1 and ${SERVER_MAX_DISPATCHES}`);
      }

      const serverIdempotencyKey = `governed_queue:${input.authorityAgentId}:${callerIdempotencyKey}`;
      const publications: ActivityPublication[] = [];
      const result = await db.transaction(async (tx) => {
        const company = await tx
          .select({ id: companies.id, status: companies.status })
          .from(companies)
          .where(eq(companies.id, input.companyId))
          .for("update")
          .then((rows) => rows[0] ?? null);
        if (!company || company.status !== "active") {
          throw conflict("Company is not active", { code: "company_not_active" });
        }
        await lockGovernedIssueLane(tx as unknown as Db, input.companyId, input.issueId);

        const existingWake = await tx
          .select()
          .from(agentWakeupRequests)
          .where(and(
            eq(agentWakeupRequests.companyId, input.companyId),
            eq(agentWakeupRequests.idempotencyKey, serverIdempotencyKey),
          ))
          .orderBy(agentWakeupRequests.createdAt)
          .limit(1)
          .then((rows) => rows[0] ?? null);
        if (existingWake) {
          const payload = readObject(existingWake.payload);
          if (
            payload.issueId !== input.issueId
            || payload.approvalId !== input.approvalId
            || payload.authorityAgentId !== input.authorityAgentId
            || payload.targetAgentId !== input.targetAgentId
            || payload.approvalMarker !== approvalMarker
            || payload.expectedUpdatedAt !== expectedUpdatedAt.toISOString()
            || payload.expiresAt !== expiresAt.toISOString()
            || payload.maxDispatches !== input.maxDispatches
          ) {
            throw conflict("Idempotency key is already bound to a different dispatch", {
              code: "queue_idempotency_mismatch",
            });
          }
          const existingRun = existingWake.runId
            ? await tx.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, existingWake.runId)).then((rows) => rows[0] ?? null)
            : null;
          const existingIssue = await tx.select().from(issues).where(eq(issues.id, input.issueId)).then((rows) => rows[0] ?? null);
          if (!existingRun || !existingIssue) {
            throw conflict("Existing governed dispatch is incomplete", { code: "queue_dispatch_incomplete" });
          }
          return {
            idempotent: true as const,
            issue: existingIssue,
            run: existingRun,
            wakeupRequest: existingWake,
            publications,
          };
        }

        if (expiresAt <= now) {
          throw preconditionFailed("Queue authorization must expire in the future", { code: "invalid_queue_expiry" });
        }
        if (expiresAt.getTime() - now.getTime() > MAX_AUTHORIZATION_TTL_MS) {
          throw preconditionFailed("Queue authorization lifetime exceeds the server maximum", {
            code: "queue_expiry_exceeds_server_limit",
            maxTtlSeconds: MAX_AUTHORIZATION_TTL_MS / 1000,
          });
        }

        const target = await tx
          .select()
          .from(agents)
          .where(and(eq(agents.id, input.targetAgentId), eq(agents.companyId, input.companyId)))
          .for("update")
          .then((rows) => rows[0] ?? null);
        if (!target || target.status !== "idle") {
          throw conflict("Target agent is not active and idle", { code: "target_not_idle" });
        }
        if (!hasGovernedSingleActiveAssignmentPolicy(target.runtimeConfig)) {
          throw conflict("Target agent has not enabled the governed single-assignment queue policy", {
            code: "target_queue_policy_disabled",
          });
        }
        const heartbeatPolicy = readObject(readObject(target.runtimeConfig).heartbeat);
        const wakeOnDemand =
          heartbeatPolicy.wakeOnDemand
          ?? heartbeatPolicy.wakeOnAssignment
          ?? heartbeatPolicy.wakeOnOnDemand
          ?? heartbeatPolicy.wakeOnAutomation;
        if (
          heartbeatPolicy.enabled === true
          || wakeOnDemand !== true
          || heartbeatPolicy.maxConcurrentRuns !== 1
        ) {
          throw conflict(
            "Target must disable timer heartbeats and explicitly enable on-demand concurrency 1",
            {
              code: "target_queue_runtime_policy_mismatch",
              timerEnabled: heartbeatPolicy.enabled === true,
              wakeOnDemand,
              maxConcurrentRuns: heartbeatPolicy.maxConcurrentRuns ?? null,
            },
          );
        }
        await assertGovernedAssignmentCapacity(tx as unknown as Db, {
          companyId: input.companyId,
          agentId: input.targetAgentId,
          excludeIssueId: input.issueId,
        });

        const issue = await tx
          .select()
          .from(issues)
          .where(and(eq(issues.id, input.issueId), eq(issues.companyId, input.companyId)))
          .for("update")
          .then((rows) => rows[0] ?? null);
        if (!issue) throw conflict("Issue does not belong to company", { code: "issue_company_mismatch" });
        await assertIssueCreatedAfterGovernedCutover(tx as unknown as Db, issue);
        if (issue.updatedAt.getTime() !== expectedUpdatedAt.getTime()) {
          throw preconditionFailed("Issue version no longer matches", { code: "stale_issue_version" });
        }
        if (issue.status !== "todo" || issue.assigneeAgentId !== null || issue.assigneeUserId !== null) {
          throw conflict("Issue is no longer an unassigned todo", { code: "issue_not_claimable" });
        }
        if (issue.startedAt !== null || issue.checkoutRunId !== null || issue.executionRunId !== null) {
          throw conflict("Governed dispatch cannot establish a complete builder set for previously executed work", {
            code: "governed_prior_builder_history_unresolved",
          });
        }
        const verifierPool = await resolveGovernedVerifierPool(tx as unknown as Db, {
          companyId: input.companyId,
          issueId: input.issueId,
          executionPolicy: issue.executionPolicy,
          additionalBuilderAgentIds: [input.targetAgentId],
        });

        const unresolvedBlocker = await tx
          .select({ id: issues.id })
          .from(issueRelations)
          .innerJoin(issues, eq(issueRelations.issueId, issues.id))
          .where(and(
            eq(issueRelations.companyId, input.companyId),
            eq(issueRelations.relatedIssueId, input.issueId),
            eq(issueRelations.type, "blocks"),
            ne(issues.status, "done"),
          ))
          .limit(1)
          .then((rows) => rows[0] ?? null);
        if (unresolvedBlocker) {
          throw conflict("Issue has an unresolved blocker", {
            code: "issue_has_unresolved_blocker",
            blockerIssueId: unresolvedBlocker.id,
          });
        }

        const txDb = tx as unknown as Db;
        const invokability = await evaluateAgentInvokabilityFromDb(txDb, target);
        if (!invokability.invokable) {
          throw conflict(invokability.message, {
            code: "target_not_invokable",
            status: target.status,
            reason: invokability.reason,
            invalidOrgChain: invokability.invalidOrgChain,
            ...invokability.details,
          });
        }
        const budgetBlock = await budgetService(txDb).getInvocationBlock(
          input.companyId,
          input.targetAgentId,
          { issueId: input.issueId, projectId: issue.projectId },
        );
        if (budgetBlock) {
          throw conflict(budgetBlock.reason, {
            code: "target_budget_blocked",
            scopeType: budgetBlock.scopeType,
            scopeId: budgetBlock.scopeId,
          });
        }
        const activePauseHold = await issueTreeControlService(txDb)
          .getActivePauseHoldGate(input.companyId, input.issueId);
        if (activePauseHold) {
          throw conflict("Issue is under an active subtree pause hold", {
            code: "issue_tree_hold_active",
            holdId: activePauseHold.holdId,
            rootIssueId: activePauseHold.rootIssueId,
          });
        }

        const approval = await tx
          .select()
          .from(approvals)
          .where(and(eq(approvals.id, input.approvalId), eq(approvals.companyId, input.companyId)))
          .for("update")
          .then((rows) => rows[0] ?? null);
        const linkedApproval = approval
          ? await tx
              .select({
                approvalId: issueApprovals.approvalId,
                linkedByAgentId: issueApprovals.linkedByAgentId,
              })
              .from(issueApprovals)
              .where(and(
                eq(issueApprovals.companyId, input.companyId),
                eq(issueApprovals.issueId, input.issueId),
                eq(issueApprovals.approvalId, input.approvalId),
              ))
              .then((rows) => rows[0] ?? null)
          : null;
        if (!approval || !linkedApproval || approval.status !== "pending") {
          throw conflict("Approval is not a linked pending authorization", { code: "approval_not_dispatchable" });
        }
        if (approval.createdAt > now) {
          throw preconditionFailed("Approval creation time is in the future", {
            code: "queue_approval_age_invalid",
          });
        }
        if (now.getTime() - approval.createdAt.getTime() > MAX_AUTHORIZATION_TTL_MS) {
          throw preconditionFailed("Queue approval is older than the server maximum", {
            code: "queue_approval_expired",
            maxTtlSeconds: MAX_AUTHORIZATION_TTL_MS / 1000,
          });
        }
        if (expiresAt.getTime() - approval.createdAt.getTime() > MAX_AUTHORIZATION_TTL_MS) {
          throw preconditionFailed("Queue approval authorization window exceeds the server maximum", {
            code: "queue_approval_window_exceeds_server_limit",
            maxTtlSeconds: MAX_AUTHORIZATION_TTL_MS / 1000,
          });
        }
        if (approval.type !== "governed_queue_dispatch") {
          throw conflict("Approval is not a governed queue authorization", {
            code: "queue_approval_type_invalid",
          });
        }
        if (
          approval.requestedByAgentId !== input.authorityAgentId
          || linkedApproval.linkedByAgentId !== input.authorityAgentId
        ) {
          throw conflict("Authenticated authority does not own the linked approval", {
            code: "queue_authority_mismatch",
          });
        }

        const markedDescription = normalizedMarkerDescription(issue.description, approvalMarker);
        const scopeDigest = governedQueueScopeDigest({
          ...issue,
          description: markedDescription,
          targetAgentId: input.targetAgentId,
          operationId: serverIdempotencyKey,
          policyDigest: verifierPool.policyDigest,
          approvalMarker,
          expiresAt: expiresAt.toISOString(),
          maxDispatches: input.maxDispatches,
        });
        const approvalPayload = readObject(approval.payload);
        const issuedRequest = readObject(approvalPayload.queueDispatchRequest);
        const issuedScope = readObject(approvalPayload.queueDispatchScope);
        if (issuedScope.schemaVersion !== 1 || issuedScope.reviewRequired !== true) {
          throw conflict("Governed queue approval lacks a server-issued scope", {
            code: "queue_approval_scope_missing",
          });
        }
        if (
          issuedRequest.expectedUpdatedAt !== expectedUpdatedAt.toISOString()
          || issuedRequest.approvalMarker !== approvalMarker
          || issuedRequest.targetAgentId !== input.targetAgentId
          || issuedRequest.expiresAt !== expiresAt.toISOString()
          || issuedRequest.idempotencyKey !== callerIdempotencyKey
          || issuedRequest.maxDispatches !== input.maxDispatches
          || issuedScope.issueId !== input.issueId
          || issuedScope.companyId !== input.companyId
          || issuedScope.targetAgentId !== input.targetAgentId
          || issuedScope.operationId !== serverIdempotencyKey
          || issuedScope.issueUpdatedAt !== expectedUpdatedAt.toISOString()
          || issuedScope.policyDigest !== verifierPool.policyDigest
          || issuedScope.scopeDigest !== scopeDigest
        ) {
          throw preconditionFailed("Governed queue approval scope no longer matches durable state", {
            code: "queue_approval_scope_mismatch",
          });
        }

        const [liveRunCount, orphanWakeCount] = await Promise.all([
          tx
            .select({ value: count() })
            .from(heartbeatRuns)
            .where(and(
              eq(heartbeatRuns.companyId, input.companyId),
              inArray(heartbeatRuns.status, [...LIVE_RUN_STATUSES]),
            ))
            .then((rows) => Number(rows[0]?.value ?? 0)),
          tx
            .select({ value: count() })
            .from(agentWakeupRequests)
            .where(and(
              eq(agentWakeupRequests.companyId, input.companyId),
              inArray(agentWakeupRequests.status, [...LIVE_WAKE_STATUSES]),
              isNull(agentWakeupRequests.runId),
            ))
            .then((rows) => Number(rows[0]?.value ?? 0)),
        ]);
        if (liveRunCount + orphanWakeCount >= input.maxDispatches) {
          throw conflict("Governed queue dispatch capacity is full", {
            code: "max_dispatches_reached",
            observed: liveRunCount + orphanWakeCount,
            limit: input.maxDispatches,
            scope: "governed_queue_path",
          });
        }

        const updatedIssue = await tx
          .update(issues)
          .set({
            description: markedDescription,
            assigneeAgentId: input.targetAgentId,
            assigneeUserId: null,
            status: "in_progress",
            startedAt: issue.startedAt ?? now,
            updatedAt: now,
          })
          .where(and(
            eq(issues.id, input.issueId),
            eq(issues.companyId, input.companyId),
            eq(issues.status, "todo"),
            isNull(issues.assigneeAgentId),
            isNull(issues.assigneeUserId),
            eq(issues.updatedAt, issue.updatedAt),
          ))
          .returning()
          .then((rows) => rows[0] ?? null);
        if (!updatedIssue) {
          throw preconditionFailed("Issue changed while dispatch was evaluated", {
            code: "queue_dispatch_compare_and_swap_failed",
          });
        }

        const queueAuthorization = {
          approvalMarker,
          authorityAgentId: input.authorityAgentId,
          targetAgentId: input.targetAgentId,
          scopeDigest,
          policyDigest: verifierPool.policyDigest,
          reviewRequired: true,
          expiresAt: expiresAt.toISOString(),
          maxDispatches: input.maxDispatches,
          idempotencyKey: serverIdempotencyKey,
        };
        const updatedApproval = await tx
          .update(approvals)
          .set({
            status: "approved",
            payload: { ...readObject(approval.payload), queueDispatch: queueAuthorization },
            decisionNote: "Governed queue dispatch authorized atomically by the authority agent.",
            decidedAt: now,
            updatedAt: now,
          })
          .where(and(eq(approvals.id, input.approvalId), eq(approvals.status, "pending")))
          .returning()
          .then((rows) => rows[0] ?? null);
        if (!updatedApproval) {
          throw preconditionFailed("Approval changed while dispatch was evaluated", {
            code: "queue_approval_compare_and_swap_failed",
          });
        }

        const payload = {
          issueId: input.issueId,
          taskId: input.issueId,
          approvalId: input.approvalId,
          approvalMarker,
          expectedUpdatedAt: expectedUpdatedAt.toISOString(),
          authorityAgentId: input.authorityAgentId,
          targetAgentId: input.targetAgentId,
          scopeDigest,
          policyDigest: verifierPool.policyDigest,
          reviewRequired: true,
          expiresAt: expiresAt.toISOString(),
          maxDispatches: input.maxDispatches,
        };
        const wakeupRequest = await tx
          .insert(agentWakeupRequests)
          .values({
            companyId: input.companyId,
            agentId: input.targetAgentId,
            source: "automation",
            triggerDetail: "system",
            reason: "governed_queue_dispatch",
            payload,
            status: "queued",
            requestedByActorType: "agent",
            requestedByActorId: input.authorityAgentId,
            idempotencyKey: serverIdempotencyKey,
            updatedAt: now,
          })
          .returning()
          .then((rows) => rows[0]);
        const run = await tx
          .insert(heartbeatRuns)
          .values({
            companyId: input.companyId,
            agentId: input.targetAgentId,
            invocationSource: "automation",
            triggerDetail: "system",
            status: "queued",
            responsibleUserId,
            wakeupRequestId: wakeupRequest.id,
            contextSnapshot: {
              ...payload,
              wakeReason: "governed_queue_dispatch",
              taskKey: `issue:${input.issueId}`,
            },
            sessionIdBefore: input.sessionIdBefore,
          })
          .returning()
          .then((rows) => rows[0]);
        const linkedWakeupRequest = await tx
          .update(agentWakeupRequests)
          .set({ runId: run.id, updatedAt: now })
          .where(eq(agentWakeupRequests.id, wakeupRequest.id))
          .returning()
          .then((rows) => rows[0]);

        await activateGovernedIssueControl(tx as unknown as Db, {
          companyId: input.companyId,
          issueId: input.issueId,
          builderAgentId: input.targetAgentId,
          approvalId: input.approvalId,
          sourceRunId: run.id,
          policyDigest: verifierPool.policyDigest,
          firstAssignedAt: now,
        });

        const { publication } = await persistActivity(tx as unknown as Db, {
          companyId: input.companyId,
          actorType: "agent",
          actorId: input.authorityAgentId,
          agentId: input.authorityAgentId,
          runId: run.id,
          action: "issue.governed_queue_dispatched",
          entityType: "issue",
          entityId: input.issueId,
          issueId: input.issueId,
          responsibleUserIdOverride: responsibleUserId,
          details: {
            approvalId: input.approvalId,
            targetAgentId: input.targetAgentId,
            scopeDigest,
            maxDispatches: input.maxDispatches,
            queueCapScope: "governed_queue_path",
          },
        });
        publications.push(publication);

        return {
          idempotent: false as const,
          issue: updatedIssue,
          run,
          wakeupRequest: linkedWakeupRequest,
          publications,
        };
      });
      return result;
    },
  };
}
