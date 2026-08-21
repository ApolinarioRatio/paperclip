import { createHash } from "node:crypto";
import { and, eq, inArray, sql } from "drizzle-orm";
import {
  agents,
  companyMemberships,
  governedIssueBuilderHistory,
  issueExecutionDecisions,
  issues,
  type Db,
} from "@paperclipai/db";
import type { IssueExecutionPolicy } from "@paperclipai/shared";
import { conflict } from "../errors.js";
import {
  normalizeIssueExecutionPolicy,
  parseIssueExecutionState,
} from "./issue-execution-policy.js";

const ELIGIBLE_VERIFIER_STATUSES = ["active", "idle", "running"] as const;
const VERIFICATION_RESERVATION_TTL_MS = 24 * 60 * 60 * 1000;

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, canonicalize(entry)]),
  );
}

export function governedReviewPolicyDigest(rawPolicy: unknown) {
  const policy = normalizeIssueExecutionPolicy(rawPolicy);
  const reviewStages = policy?.stages.filter((stage) => stage.type === "review") ?? [];
  if (reviewStages.length === 0) {
    throw conflict("Governed queue work requires an explicit review stage", {
      code: "governed_review_policy_missing",
    });
  }
  const canonicalPolicy = {
    schemaVersion: 1,
    reviewStages: reviewStages.map((stage) => ({
      id: stage.id,
      type: stage.type,
      approvalsNeeded: stage.approvalsNeeded,
      participants: stage.participants.map((participant) => ({
        id: participant.id,
        type: participant.type,
        agentId: participant.agentId ?? null,
        userId: participant.userId ?? null,
      })),
    })),
  };
  return {
    policy: policy as IssueExecutionPolicy,
    reviewStages,
    digest: createHash("sha256")
      .update(JSON.stringify(canonicalize(canonicalPolicy)))
      .digest("hex"),
  };
}

export async function lockGovernedIssueLane(
  dbOrTx: Db,
  companyId: string,
  issueId: string,
) {
  await dbOrTx.execute(
    sql`select pg_advisory_xact_lock(hashtextextended(${`governed-issue:${companyId}:${issueId}`}, 0))`,
  );
}

export async function lockGovernedIssueDecisionLane(
  dbOrTx: Db,
  input: {
    companyId: string;
    issueId: string;
    expected: typeof issues.$inferSelect;
  },
) {
  await lockGovernedIssueLane(dbOrTx, input.companyId, input.issueId);
  const locked = await dbOrTx.select().from(issues)
    .where(and(eq(issues.id, input.issueId), eq(issues.companyId, input.companyId)))
    .for("update")
    .then((rows) => rows[0] ?? null);
  if (!locked) {
    throw conflict("Governed issue no longer exists", { code: "governed_issue_missing" });
  }
  const expected = input.expected;
  const protectedStateMatches =
    locked.updatedAt.getTime() === expected.updatedAt.getTime()
    && locked.status === expected.status
    && locked.assigneeAgentId === expected.assigneeAgentId
    && locked.assigneeUserId === expected.assigneeUserId
    && JSON.stringify(canonicalize(locked.executionPolicy)) === JSON.stringify(canonicalize(expected.executionPolicy))
    && JSON.stringify(canonicalize(locked.executionState)) === JSON.stringify(canonicalize(expected.executionState));
  if (!protectedStateMatches) {
    throw conflict("Governed issue changed before its decision lane was locked", {
      code: "stale_issue_version",
      expectedUpdatedAt: expected.updatedAt.toISOString(),
      actualUpdatedAt: locked.updatedAt.toISOString(),
    });
  }
  return locked;
}

export async function resolveGovernedVerifierPool(
  dbOrTx: Db,
  input: {
    companyId: string;
    issueId: string;
    executionPolicy: unknown;
    additionalBuilderAgentIds?: string[];
  },
) {
  const policy = governedReviewPolicyDigest(input.executionPolicy);
  const configuredAgentIds = Array.from(new Set(
    policy.reviewStages.flatMap((stage) =>
      stage.participants
        .filter((participant) => participant.type === "agent" && participant.agentId)
        .map((participant) => participant.agentId!),
    ),
  ));
  const configuredUserIds = Array.from(new Set(
    policy.reviewStages.flatMap((stage) =>
      stage.participants
        .filter((participant) => participant.type === "user" && participant.userId)
        .map((participant) => participant.userId!),
    ),
  ));
  const historicalBuilders = await dbOrTx
    .select({ agentId: governedIssueBuilderHistory.agentId })
    .from(governedIssueBuilderHistory)
    .where(and(
      eq(governedIssueBuilderHistory.companyId, input.companyId),
      eq(governedIssueBuilderHistory.issueId, input.issueId),
      eq(governedIssueBuilderHistory.kind, "builder"),
    ));
  const excluded = new Set([
    ...historicalBuilders.map((row) => row.agentId),
    ...(input.additionalBuilderAgentIds ?? []),
  ]);
  const candidateIds = configuredAgentIds.filter((agentId) => !excluded.has(agentId));
  const eligibleRows = candidateIds.length === 0
    ? []
    : await dbOrTx
        .select({ id: agents.id })
        .from(agents)
        .where(and(
          eq(agents.companyId, input.companyId),
          inArray(agents.id, candidateIds),
          inArray(agents.status, [...ELIGIBLE_VERIFIER_STATUSES]),
        ));
  const eligibleVerifierAgentIds = eligibleRows.map((row) => row.id).sort();
  const eligibleUserRows = configuredUserIds.length === 0
    ? []
    : await dbOrTx
        .select({ userId: companyMemberships.principalId })
        .from(companyMemberships)
        .where(and(
          eq(companyMemberships.companyId, input.companyId),
          eq(companyMemberships.principalType, "user"),
          eq(companyMemberships.status, "active"),
          inArray(companyMemberships.principalId, configuredUserIds),
        ));
  const eligibleVerifierUserIds = eligibleUserRows.map((row) => row.userId).sort();
  if (eligibleVerifierAgentIds.length === 0 && eligibleVerifierUserIds.length === 0) {
    throw conflict("No independent verifier is structurally eligible for governed queue work", {
      code: "governed_verifier_pool_empty",
    });
  }
  return {
    policy: policy.policy,
    policyDigest: policy.digest,
    eligibleVerifierAgentIds,
    eligibleVerifierUserIds,
  };
}

export async function activateGovernedIssueControl(
  dbOrTx: Db,
  input: {
    companyId: string;
    issueId: string;
    builderAgentId: string;
    approvalId: string;
    sourceRunId: string | null;
    policyDigest: string;
    firstAssignedAt: Date;
  },
) {
  const inserted = await dbOrTx
    .insert(governedIssueBuilderHistory)
    .values({
      companyId: input.companyId,
      issueId: input.issueId,
      agentId: input.builderAgentId,
      kind: "builder",
      state: "recorded",
      source: "governed_queue_dispatch",
      sourceRunId: input.sourceRunId,
      approvalId: input.approvalId,
      reviewRequiredSnapshot: true,
      policyDigest: input.policyDigest,
      firstAssignedAt: input.firstAssignedAt,
    })
    .onConflictDoNothing()
    .returning()
    .then((rows) => rows[0] ?? null);
  if (!inserted) {
    throw conflict("Governed builder history already contains the dispatch worker", {
      code: "governed_builder_history_conflict",
      issueId: input.issueId,
      agentId: input.builderAgentId,
    });
  }
  return inserted;
}

async function governedBuilderRows(dbOrTx: Db, companyId: string, issueId: string) {
  return dbOrTx
    .select()
    .from(governedIssueBuilderHistory)
    .where(and(
      eq(governedIssueBuilderHistory.companyId, companyId),
      eq(governedIssueBuilderHistory.issueId, issueId),
      eq(governedIssueBuilderHistory.kind, "builder"),
    ));
}

async function activeVerificationReservation(dbOrTx: Db, companyId: string, issueId: string) {
  return dbOrTx
    .select()
    .from(governedIssueBuilderHistory)
    .where(and(
      eq(governedIssueBuilderHistory.companyId, companyId),
      eq(governedIssueBuilderHistory.issueId, issueId),
      eq(governedIssueBuilderHistory.kind, "verification_reservation"),
      eq(governedIssueBuilderHistory.state, "active"),
    ))
    .limit(1)
    .then((rows) => rows[0] ?? null);
}

async function assertVerifierAgentEligible(
  dbOrTx: Db,
  input: { companyId: string; agentId: string },
) {
  const agent = await dbOrTx
    .select({ id: agents.id, status: agents.status })
    .from(agents)
    .where(and(
      eq(agents.companyId, input.companyId),
      eq(agents.id, input.agentId),
      inArray(agents.status, [...ELIGIBLE_VERIFIER_STATUSES]),
    ))
    .then((rows) => rows[0] ?? null);
  if (!agent) {
    throw conflict("Verification participant is not an active same-company agent", {
      code: "governed_verifier_not_eligible",
      agentId: input.agentId,
    });
  }
}

async function assertVerifierUserEligible(
  dbOrTx: Db,
  input: { companyId: string; userId: string },
) {
  const membership = await dbOrTx
    .select({ id: companyMemberships.id })
    .from(companyMemberships)
    .where(and(
      eq(companyMemberships.companyId, input.companyId),
      eq(companyMemberships.principalType, "user"),
      eq(companyMemberships.principalId, input.userId),
      eq(companyMemberships.status, "active"),
    ))
    .then((rows) => rows[0] ?? null);
  if (!membership) {
    throw conflict("Verification participant is not an active same-company user", {
      code: "governed_verifier_not_eligible",
      userId: input.userId,
    });
  }
}

function reviewStageAllowsPrincipal(
  policy: IssueExecutionPolicy,
  stageId: string,
  principal: { type: "agent"; agentId: string } | { type: "user"; userId: string },
) {
  return policy.stages.some((stage) =>
    stage.id === stageId
    && stage.type === "review"
    && stage.participants.some((participant) =>
      participant.type === principal.type
      && (principal.type === "agent"
        ? participant.agentId === principal.agentId
        : participant.userId === principal.userId)));
}

export async function recordGovernedBuilderIfControlled(
  dbOrTx: Db,
  input: {
    companyId: string;
    issueId: string;
    agentId: string;
    source: string;
    sourceRunId?: string | null;
    currentExecutionState?: unknown;
    firstAssignedAt?: Date;
  },
) {
  const builders = await governedBuilderRows(dbOrTx, input.companyId, input.issueId);
  const control = builders.find((row) => row.approvalId !== null) ?? null;
  if (!control) return { controlled: false as const };
  const activeReservation = await activeVerificationReservation(dbOrTx, input.companyId, input.issueId);
  if (activeReservation?.agentId === input.agentId) {
    throw conflict("The active verifier cannot take build work on the same issue", {
      code: "governed_verifier_cannot_build",
      agentId: input.agentId,
    });
  }
  const state = parseIssueExecutionState(input.currentExecutionState);
  if (
    state?.status === "pending"
    && state.currentStageType === "review"
    && state.currentParticipant?.type === "agent"
    && state.currentParticipant.agentId === input.agentId
  ) {
    throw conflict("The active verifier cannot take build work on the same issue", {
      code: "governed_verifier_cannot_build",
      agentId: input.agentId,
    });
  }
  await dbOrTx
    .insert(governedIssueBuilderHistory)
    .values({
      companyId: input.companyId,
      issueId: input.issueId,
      agentId: input.agentId,
      kind: "builder",
      state: "recorded",
      source: input.source,
      sourceRunId: input.sourceRunId ?? null,
      approvalId: null,
      reviewRequiredSnapshot: true,
      policyDigest: control.policyDigest,
      firstAssignedAt: input.firstAssignedAt ?? new Date(),
    })
    .onConflictDoNothing();
  return { controlled: true as const, policyDigest: control.policyDigest };
}

export async function authorizeGovernedIssueMutation(
  dbOrTx: Db,
  input: {
    current: typeof issues.$inferSelect;
    patch: Partial<typeof issues.$inferInsert>;
    actorAgentId?: string | null;
    actorUserId?: string | null;
    source: string;
    sourceRunId?: string | null;
    now?: Date;
  },
) {
  const now = input.now ?? new Date();
  const builders = await governedBuilderRows(dbOrTx, input.current.companyId, input.current.id);
  const control = builders.find((row) => row.approvalId !== null) ?? null;
  if (!control) return { controlled: false as const };
  let activeReservation: Awaited<ReturnType<typeof activeVerificationReservation>> | null = await activeVerificationReservation(
    dbOrTx,
    input.current.companyId,
    input.current.id,
  );

  const nextPolicyRaw = input.patch.executionPolicy === undefined
    ? input.current.executionPolicy
    : input.patch.executionPolicy;
  let policy: ReturnType<typeof governedReviewPolicyDigest>;
  try {
    policy = governedReviewPolicyDigest(nextPolicyRaw);
  } catch {
    throw conflict("Governed review policy removal requires the exception lane", {
      code: "governed_review_policy_change_requires_exception",
    });
  }
  if (policy.digest !== control.policyDigest) {
    throw conflict("Governed review policy changes require the exception lane", {
      code: "governed_review_policy_change_requires_exception",
      expectedPolicyDigest: control.policyDigest,
      observedPolicyDigest: policy.digest,
    });
  }

  const currentState = parseIssueExecutionState(input.current.executionState);
  const nextState = input.patch.executionState === undefined
    ? currentState
    : parseIssueExecutionState(input.patch.executionState);
  const nextStatus = input.patch.status ?? input.current.status;
  const nextAssigneeAgentId = input.patch.assigneeAgentId === undefined
    ? input.current.assigneeAgentId
    : input.patch.assigneeAgentId;
  const nextAssigneeUserId = input.patch.assigneeUserId === undefined
    ? input.current.assigneeUserId
    : input.patch.assigneeUserId;
  if (nextAssigneeUserId && (nextStatus === "todo" || nextStatus === "in_progress")) {
    throw conflict("Governed builder work is agent-only", {
      code: "governed_user_builder_unsupported",
      userId: nextAssigneeUserId,
    });
  }
  if (
    activeReservation
    && nextAssigneeAgentId
    && activeReservation.agentId === nextAssigneeAgentId
    && (nextStatus === "todo" || nextStatus === "in_progress")
  ) {
    throw conflict("A reserved verifier cannot become a builder", {
      code: "governed_verifier_cannot_build",
      reservationId: activeReservation.id,
    });
  }

  const leavingActiveReview =
    input.current.status === "in_review"
    && currentState?.status === "pending"
    && currentState.currentStageType === "review"
    && (
      nextStatus !== "in_review"
      || nextState?.status !== "pending"
      || nextState.currentStageId !== currentState.currentStageId
    );
  if (leavingActiveReview) {
    const participant = currentState.currentParticipant;
    const actorMatches = participant?.type === "agent"
      ? input.actorAgentId === participant.agentId
      : participant?.type === "user" && input.actorUserId === participant.userId;
    if (!participant || !actorMatches) {
      throw conflict("Only the reserved verifier can complete the governed review", {
        code: "governed_verifier_actor_mismatch",
      });
    }
    const decisionId = nextState?.lastDecisionId ?? null;
    const persistedDecision = decisionId
      ? await dbOrTx
          .select()
          .from(issueExecutionDecisions)
          .where(and(
            eq(issueExecutionDecisions.id, decisionId),
            eq(issueExecutionDecisions.companyId, input.current.companyId),
            eq(issueExecutionDecisions.issueId, input.current.id),
            eq(issueExecutionDecisions.stageId, currentState.currentStageId!),
          ))
          .then((rows) => rows[0] ?? null)
      : null;
    if (
      !persistedDecision
      || persistedDecision.reservationId !== activeReservation?.id
      || persistedDecision.outcome !== nextState?.lastDecisionOutcome
      || (participant.type === "agent"
        ? persistedDecision.actorAgentId !== participant.agentId
        : persistedDecision.actorUserId !== participant.userId)
    ) {
      throw conflict("Governed verification completion requires its persisted decision", {
        code: "governed_verification_decision_missing",
      });
    }
    const principalAllowed = participant.type === "agent" && participant.agentId
      ? reviewStageAllowsPrincipal(policy.policy, currentState.currentStageId!, {
          type: "agent",
          agentId: participant.agentId,
        })
      : participant.type === "user" && participant.userId
        ? reviewStageAllowsPrincipal(policy.policy, currentState.currentStageId!, {
            type: "user",
            userId: participant.userId,
          })
        : false;
    if (!principalAllowed) {
      throw conflict("The reserved verifier is no longer eligible under the governed review policy", {
        code: "governed_verifier_not_eligible",
      });
    }
    if (participant.type === "agent" && participant.agentId) {
      if (builders.some((row) => row.agentId === participant.agentId)) {
        throw conflict("A builder cannot complete verification for its own issue", {
          code: "governed_builder_cannot_verify",
          agentId: participant.agentId,
        });
      }
      await assertVerifierAgentEligible(dbOrTx, {
        companyId: input.current.companyId,
        agentId: participant.agentId,
      });
    } else if (participant.type === "user" && participant.userId) {
      await assertVerifierUserEligible(dbOrTx, {
        companyId: input.current.companyId,
        userId: participant.userId,
      });
    }
    const reservationMatches =
      activeReservation?.stageId === currentState.currentStageId
      && activeReservation.policyDigest === control.policyDigest
      && activeReservation.reservationExpiresAt !== null
      && activeReservation.reservationExpiresAt > now
      && (participant.type === "agent"
        ? activeReservation.agentId === participant.agentId && activeReservation.userId === null
        : activeReservation.userId === participant.userId && activeReservation.agentId === null);
    if (
      !reservationMatches
      || currentState.verificationReservationId !== activeReservation?.id
      || !currentState.verificationReservedAt
      || !currentState.verificationExpiresAt
      || currentState.verificationPolicyDigest !== control.policyDigest
      || new Date(currentState.verificationExpiresAt) <= now
    ) {
      throw conflict("The governed verification reservation is absent, stale, or expired", {
        code: "governed_verification_reservation_invalid",
      });
    }
    const changesRequested = nextState?.lastDecisionOutcome === "changes_requested";
    await dbOrTx
      .update(governedIssueBuilderHistory)
      .set({
        state: changesRequested ? "released" : "completed",
        resolvedAt: now,
        resolution: changesRequested ? "changes_requested" : "approved",
      })
      .where(and(
        eq(governedIssueBuilderHistory.id, activeReservation!.id),
        eq(governedIssueBuilderHistory.state, "active"),
      ));
    activeReservation = null;
  }

  if (nextStatus === "in_review") {
    if (
      !nextState
      || nextState.status !== "pending"
      || nextState.currentStageType !== "review"
      || !nextState.currentStageId
      || !nextState.currentParticipant
    ) {
      throw conflict("Governed work cannot enter review without a resolvable verifier reservation", {
        code: "governed_verification_reservation_missing",
      });
    }
    const participant = nextState.currentParticipant;
    const principal = participant.type === "agent" && participant.agentId
      ? { type: "agent" as const, agentId: participant.agentId }
      : participant.type === "user" && participant.userId
        ? { type: "user" as const, userId: participant.userId }
        : null;
    if (!principal) {
      throw conflict("Governed verification requires a server-resolved principal", {
        code: "governed_verification_reservation_missing",
      });
    }
    if (participant.type === "agent" && participant.agentId) {
      if (builders.some((row) => row.agentId === participant.agentId)) {
        throw conflict("A builder cannot reserve verification for its own issue", {
          code: "governed_builder_cannot_verify",
          agentId: participant.agentId,
        });
      }
      await assertVerifierAgentEligible(dbOrTx, {
        companyId: input.current.companyId,
        agentId: participant.agentId,
      });
    } else if (participant.type === "user" && participant.userId) {
      await assertVerifierUserEligible(dbOrTx, {
        companyId: input.current.companyId,
        userId: participant.userId,
      });
    }
    if (!reviewStageAllowsPrincipal(policy.policy, nextState.currentStageId, principal)) {
      throw conflict("The selected verifier is not eligible under the governed review policy", {
        code: "governed_verifier_not_eligible",
      });
    }

    if (activeReservation?.reservationExpiresAt && activeReservation.reservationExpiresAt <= now) {
      await dbOrTx
        .update(governedIssueBuilderHistory)
        .set({ state: "expired", resolvedAt: now, resolution: "expired" })
        .where(and(
          eq(governedIssueBuilderHistory.id, activeReservation.id),
          eq(governedIssueBuilderHistory.state, "active"),
        ));
      activeReservation = null;
    }
    const preserveReservation =
      activeReservation?.stageId === nextState.currentStageId
      && activeReservation.policyDigest === control.policyDigest
      && (principal.type === "agent"
        ? activeReservation.agentId === principal.agentId && activeReservation.userId === null
        : activeReservation.userId === principal.userId && activeReservation.agentId === null);
    if (activeReservation && !preserveReservation) {
      await dbOrTx
        .update(governedIssueBuilderHistory)
        .set({ state: "released", resolvedAt: now, resolution: "stage_replaced" })
        .where(and(
          eq(governedIssueBuilderHistory.id, activeReservation.id),
          eq(governedIssueBuilderHistory.state, "active"),
        ));
      activeReservation = null;
    }
    if (!activeReservation) {
      const reservationExpiresAt = new Date(now.getTime() + VERIFICATION_RESERVATION_TTL_MS);
      activeReservation = await dbOrTx
        .insert(governedIssueBuilderHistory)
        .values({
          companyId: input.current.companyId,
          issueId: input.current.id,
          agentId: principal.type === "agent" ? principal.agentId : null,
          userId: principal.type === "user" ? principal.userId : null,
          kind: "verification_reservation",
          state: "active",
          stageId: nextState.currentStageId,
          source: `verification_reservation:${input.source}`,
          sourceRunId: input.sourceRunId ?? null,
          approvalId: null,
          reviewRequiredSnapshot: true,
          policyDigest: control.policyDigest,
          reservationExpiresAt,
          firstAssignedAt: now,
        })
        .onConflictDoNothing()
        .returning()
        .then((rows) => rows[0] ?? null);
      if (!activeReservation) {
        throw conflict("Another verifier already holds the active reservation", {
          code: "governed_verification_reservation_conflict",
        });
      }
    }
    input.patch.executionState = {
      ...nextState,
      verificationReservedAt: activeReservation.createdAt.toISOString(),
      verificationExpiresAt: activeReservation.reservationExpiresAt!.toISOString(),
      verificationPolicyDigest: control.policyDigest,
      verificationReservationId: activeReservation.id,
    } as unknown as Record<string, unknown>;
  }

  if (
    nextAssigneeAgentId
    && (nextStatus === "todo" || nextStatus === "in_progress")
  ) {
    await recordGovernedBuilderIfControlled(dbOrTx, {
      companyId: input.current.companyId,
      issueId: input.current.id,
      agentId: nextAssigneeAgentId,
      source: input.source,
      sourceRunId: input.sourceRunId ?? null,
      currentExecutionState: input.current.executionState,
      firstAssignedAt: now,
    });
  }

  if (nextStatus === "done" && !leavingActiveReview) {
    const approvedReviews = await dbOrTx
      .select({
        actorAgentId: issueExecutionDecisions.actorAgentId,
        reservationId: issueExecutionDecisions.reservationId,
      })
      .from(issueExecutionDecisions)
      .where(and(
        eq(issueExecutionDecisions.companyId, input.current.companyId),
        eq(issueExecutionDecisions.issueId, input.current.id),
        eq(issueExecutionDecisions.stageType, "review"),
        eq(issueExecutionDecisions.outcome, "approved"),
      ));
    const independentlyVerified = approvedReviews.some((decision) =>
      decision.reservationId !== null
      && (decision.actorAgentId === null
        || !builders.some((row) => row.agentId === decision.actorAgentId)));
    if (!independentlyVerified) {
      throw conflict("Governed work cannot complete without independent verification", {
        code: "governed_verification_required",
      });
    }
  }

  // The matching trigger GUC blocks accidental direct application writes. It is
  // not a PostgreSQL privilege boundary: a session using the same database role
  // can set a custom GUC. Canonical authority remains this locked server path,
  // the append-retained ledger, persisted decisions, and database constraints.
  await dbOrTx.execute(
    sql`select set_config('paperclip.governed_issue_authority', ${input.current.id}, true)`,
  );
  return { controlled: true as const, policyDigest: control.policyDigest };
}
