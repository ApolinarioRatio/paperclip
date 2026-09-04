import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  activityLog,
  agentWakeupRequests,
  agents,
  approvals,
  companyMemberships,
  companies,
  createDb,
  governedIssueBuilderHistory,
  governedIssueControlCutover,
  heartbeatRuns,
  issueApprovals,
  issueExecutionDecisions,
  issueRelations,
  issues,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { agentService } from "../services/agents.js";
import {
  governedQueueDispatchService,
  prepareGovernedQueueApprovalPayload,
} from "../services/governed-queue-dispatch.js";
import {
  applyIssueExecutionPolicyTransition,
  normalizeIssueExecutionPolicy,
  parseIssueExecutionState,
} from "../services/issue-execution-policy.js";
import { issueService } from "../services/issues.js";
import { issueTreeControlService } from "../services/issue-tree-control.js";
import { lockGovernedIssueDecisionLane } from "../services/governed-issue-separation.js";
import { admitLiveRun } from "../services/live-run-admission.js";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

describeEmbeddedPostgres("governedQueueDispatchService", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-governed-queue-");
    db = createDb(tempDb.connectionString);
  }, 20_000);

  afterEach(async () => {
    // Governed history is append-retained by a row trigger. Test teardown uses
    // TRUNCATE so each isolated fixture can still remove its disposable ledger.
    await db.execute(sql`truncate table issue_execution_decisions, governed_issue_builder_history`);
    await db.delete(activityLog);
    await db.delete(heartbeatRuns);
    await db.delete(agentWakeupRequests);
    await db.delete(issueApprovals);
    await db.delete(approvals);
    await db.delete(issueRelations);
    await db.delete(issues);
    await db.delete(agents);
    await db.delete(companyMemberships);
    await db.delete(companies);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedDispatchFixture(options: { userVerifier?: boolean } = {}) {
    const companyId = randomUUID();
    const authorityAgentId = randomUUID();
    const targetAgentId = randomUUID();
    const verifierAgentId = randomUUID();
    const issueId = randomUUID();
    const approvalId = randomUUID();
    const updatedAt = new Date("2026-08-21T01:00:00.000Z");

    await db.insert(companies).values({
      id: companyId,
      name: "RatioCore",
      issuePrefix: `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      status: "active",
      defaultResponsibleUserId: "alex",
      requireBoardApprovalForNewAgents: false,
    });
    if (options.userVerifier) {
      await db.insert(companyMemberships).values({
        companyId,
        principalType: "user",
        principalId: "alex",
        status: "active",
      });
    }
    await db.insert(agents).values([
      {
        id: authorityAgentId,
        companyId,
        name: "Manny",
        role: "chief_of_staff",
        status: "idle",
        adapterType: "hermes_local",
        adapterConfig: { wakeOnDemand: true, maxConcurrentRuns: 1 },
        runtimeConfig: {},
        permissions: {},
      },
      {
        id: targetAgentId,
        companyId,
        name: "QwenCoder",
        role: "engineer",
        status: "idle",
        adapterType: "hermes_local",
        adapterConfig: { wakeOnDemand: true, maxConcurrentRuns: 1 },
        runtimeConfig: {
          governedQueue: { singleActiveAssignment: true },
          heartbeat: { enabled: false, intervalSec: 0, wakeOnDemand: true, maxConcurrentRuns: 1 },
        },
        permissions: {},
      },
      {
        id: verifierAgentId,
        companyId,
        name: "IndependentVerifier",
        role: "reviewer",
        status: "idle",
        adapterType: "hermes_local",
        adapterConfig: { wakeOnDemand: true, maxConcurrentRuns: 1 },
        runtimeConfig: {},
        permissions: {},
      },
    ]);
    await db.insert(issues).values({
      id: issueId,
      companyId,
      issueNumber: 1,
      identifier: "RATA-1",
      title: "Implement bounded queue dispatch",
      description: "Approved autonomous implementation work.",
      status: "todo",
      priority: "high",
      responsibleUserId: "alex",
      executionPolicy: {
        mode: "normal",
        commentRequired: true,
        stages: [{
          id: randomUUID(),
          type: "review",
          approvalsNeeded: 1,
          participants: options.userVerifier
            ? [{ id: randomUUID(), type: "user", userId: "alex" }]
            : [{ id: randomUUID(), type: "agent", agentId: verifierAgentId }],
        }],
      },
      updatedAt,
    });
    const approvalPayload = await prepareGovernedQueueApprovalPayload(db, {
      companyId,
      issueId,
      expectedUpdatedAt: updatedAt.toISOString(),
      approvalMarker: "RATA-1:alex-approved",
      targetAgentId,
      authorityAgentId,
      maxDispatches: 3,
      expiresAt: "2026-08-21T01:15:00.000Z",
      idempotencyKey: "rata-1-qwen-v1",
      now: new Date("2026-08-21T01:04:00.000Z"),
    }, { purpose: "governed_queue_dispatch" });
    await db.insert(approvals).values({
      id: approvalId,
      companyId,
      type: "governed_queue_dispatch",
      status: "pending",
      requestedByAgentId: authorityAgentId,
      payload: approvalPayload,
      createdAt: new Date("2026-08-21T01:04:00.000Z"),
      updatedAt: new Date("2026-08-21T01:04:00.000Z"),
    });
    await db.insert(issueApprovals).values({
      companyId,
      issueId,
      approvalId,
      linkedByAgentId: authorityAgentId,
    });

    return {
      approvalId,
      authorityAgentId,
      companyId,
      issueId,
      targetAgentId,
      verifierAgentId,
      verifierUserId: options.userVerifier ? "alex" : null,
      updatedAt,
    };
  }

  function dispatchInput(fixture: Awaited<ReturnType<typeof seedDispatchFixture>>) {
    return {
      ...fixture,
      expectedUpdatedAt: fixture.updatedAt.toISOString(),
      approvalMarker: "RATA-1:alex-approved",
      expiresAt: "2026-08-21T01:15:00.000Z",
      idempotencyKey: "rata-1-qwen-v1",
      maxDispatches: 3,
      now: new Date("2026-08-21T01:05:00.000Z"),
      responsibleUserId: "alex",
      sessionIdBefore: null,
    };
  }

  async function executionPatch(
    issueId: string,
    actorAgentId: string,
    requestedStatus: "done" | "in_progress",
    commentBody = "Independent review completed.",
  ) {
    const current = await db.select().from(issues).where(eq(issues.id, issueId)).then((rows) => rows[0]);
    const policy = normalizeIssueExecutionPolicy(current.executionPolicy)!;
    const transition = applyIssueExecutionPolicyTransition({
      issue: current,
      policy,
      previousPolicy: policy,
      requestedStatus,
      requestedAssigneePatch: {},
      actor: { agentId: actorAgentId },
      commentBody,
    });
    return {
      status: requestedStatus,
      ...transition.patch,
      actorAgentId,
    };
  }

  async function completeExecutionWithDecision(issueId: string, actorAgentId: string) {
    const current = await db.select().from(issues).where(eq(issues.id, issueId)).then((rows) => rows[0]);
    const policy = normalizeIssueExecutionPolicy(current.executionPolicy)!;
    const transition = applyIssueExecutionPolicyTransition({
      issue: current,
      policy,
      previousPolicy: policy,
      requestedStatus: "done",
      requestedAssigneePatch: {},
      actor: { agentId: actorAgentId },
      commentBody: "Independent review completed.",
    });
    if (!transition.decision || !transition.patch.executionState) {
      throw new Error("Expected a review decision transition");
    }
    const decisionId = randomUUID();
    const executionState = {
      ...(transition.patch.executionState as Record<string, unknown>),
      lastDecisionId: decisionId,
      lastDecisionOutcome: transition.decision.outcome,
    };
    return db.transaction(async (tx) => {
      await tx.insert(issueExecutionDecisions).values({
        id: decisionId,
        companyId: current.companyId,
        issueId,
        stageId: transition.decision!.stageId,
        stageType: transition.decision!.stageType,
        actorAgentId,
        actorUserId: null,
        outcome: transition.decision!.outcome,
        body: transition.decision!.body,
        createdByRunId: null,
        reservationId: parseIssueExecutionState(current.executionState)?.verificationReservationId ?? null,
      });
      return issueService(db).update(issueId, {
        status: "done",
        ...transition.patch,
        executionState,
        actorAgentId,
      }, tx);
    });
  }

  it("atomically approves, claims, audits, and reserves a runnable heartbeat", async () => {
    const fixture = await seedDispatchFixture();
    const result = await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));

    expect(result.idempotent).toBe(false);
    expect(result.issue.status).toBe("in_progress");
    expect(result.issue.assigneeAgentId).toBe(fixture.targetAgentId);
    expect(result.issue.description).toContain("AUTO-QUEUE: RATA-1:alex-approved");
    expect(result.wakeupRequest.status).toBe("queued");
    expect(result.wakeupRequest.runId).toBe(result.run.id);
    expect(result.run.status).toBe("queued");
    expect(result.run.contextSnapshot).toMatchObject({
      issueId: fixture.issueId,
      taskId: fixture.issueId,
      approvalId: fixture.approvalId,
      authorityAgentId: fixture.authorityAgentId,
      targetAgentId: fixture.targetAgentId,
      maxDispatches: 3,
    });

    const approval = await db.select().from(approvals).where(eq(approvals.id, fixture.approvalId)).then((rows) => rows[0]);
    expect(approval.status).toBe("approved");
    expect(approval.payload).toMatchObject({
      queueDispatch: {
        approvalMarker: "RATA-1:alex-approved",
        authorityAgentId: fixture.authorityAgentId,
        targetAgentId: fixture.targetAgentId,
        maxDispatches: 3,
      },
    });

    const audit = await db.select().from(activityLog).where(eq(activityLog.entityId, fixture.issueId));
    expect(audit).toHaveLength(1);
    expect(audit[0]).toMatchObject({
      actorType: "agent",
      actorId: fixture.authorityAgentId,
      agentId: fixture.authorityAgentId,
      action: "issue.governed_queue_dispatched",
      runId: result.run.id,
    });
    const builders = await db
      .select()
      .from(governedIssueBuilderHistory)
      .where(eq(governedIssueBuilderHistory.issueId, fixture.issueId));
    expect(builders).toEqual([
      expect.objectContaining({
        companyId: fixture.companyId,
        issueId: fixture.issueId,
        agentId: fixture.targetAgentId,
        approvalId: fixture.approvalId,
        source: "governed_queue_dispatch",
        reviewRequiredSnapshot: true,
      }),
    ]);
  });

  it("fails closed when the review policy has no verifier outside the ever-builder set", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(issues).set({
      executionPolicy: {
        mode: "normal",
        commentRequired: true,
        stages: [{
          id: randomUUID(),
          type: "review",
          approvalsNeeded: 1,
          participants: [{ id: randomUUID(), type: "agent", agentId: fixture.targetAgentId }],
        }],
      },
    }).where(eq(issues.id, fixture.issueId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 409, details: { code: "governed_verifier_pool_empty" } });
    expect(await db.select().from(governedIssueBuilderHistory)).toHaveLength(0);
    expect(await db.select().from(heartbeatRuns)).toHaveLength(0);
  });

  it("fails closed when the task has no review policy", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(issues).set({ executionPolicy: null }).where(eq(issues.id, fixture.issueId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 409, details: { code: "governed_review_policy_missing" } });
    expect(await db.select().from(governedIssueBuilderHistory)).toHaveLength(0);
  });

  it("fails closed when prior execution makes the prospective builder set incomplete", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(issues)
      .set({ startedAt: new Date("2026-08-21T00:30:00.000Z") })
      .where(eq(issues.id, fixture.issueId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({
        status: 409,
        details: { code: "governed_prior_builder_history_unresolved" },
      });
    expect(await db.select().from(governedIssueBuilderHistory)).toHaveLength(0);
  });

  it("reserves an independent verifier on review entry and re-checks it at completion", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const svc = issueService(db);

    const submitted = await svc.update(
      fixture.issueId,
      await executionPatch(fixture.issueId, fixture.targetAgentId, "done"),
    );
    expect(submitted).toMatchObject({
      status: "in_review",
      assigneeAgentId: fixture.verifierAgentId,
    });
    expect(parseIssueExecutionState(submitted!.executionState)).toMatchObject({
      status: "pending",
      currentStageType: "review",
      currentParticipant: { type: "agent", agentId: fixture.verifierAgentId },
      verificationPolicyDigest: expect.stringMatching(/^[0-9a-f]{64}$/),
      verificationReservedAt: expect.any(String),
      verificationExpiresAt: expect.any(String),
    });
    const activeReservation = await db.select().from(governedIssueBuilderHistory)
      .where(and(
        eq(governedIssueBuilderHistory.issueId, fixture.issueId),
        eq(governedIssueBuilderHistory.kind, "verification_reservation"),
        eq(governedIssueBuilderHistory.state, "active"),
      ));
    expect(activeReservation).toEqual([
      expect.objectContaining({
        agentId: fixture.verifierAgentId,
        userId: null,
        reservationExpiresAt: expect.any(Date),
      }),
    ]);
    await expect(db.update(governedIssueBuilderHistory)
      .set({ state: "completed" })
      .where(eq(governedIssueBuilderHistory.id, activeReservation[0]!.id)))
      .rejects.toMatchObject({ cause: { code: "23514" } });

    await expect(svc.update(
      fixture.issueId,
      await executionPatch(fixture.issueId, fixture.verifierAgentId, "done"),
    )).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_verification_decision_missing" },
    });

    const unboundDecisionId = randomUUID();
    const reviewState = parseIssueExecutionState(submitted!.executionState)!;
    await db.insert(issueExecutionDecisions).values({
      id: unboundDecisionId,
      companyId: fixture.companyId,
      issueId: fixture.issueId,
      stageId: reviewState.currentStageId!,
      stageType: "review",
      actorAgentId: fixture.verifierAgentId,
      actorUserId: null,
      outcome: "approved",
      body: "Prior unbound decision must not satisfy this reservation.",
      createdByRunId: null,
      reservationId: null,
    });
    const unboundPatch = await executionPatch(fixture.issueId, fixture.verifierAgentId, "done");
    await expect(svc.update(fixture.issueId, {
      ...unboundPatch,
      executionState: {
        ...((unboundPatch as Record<string, unknown>).executionState as Record<string, unknown>),
        lastDecisionId: unboundDecisionId,
        lastDecisionOutcome: "approved",
      },
    })).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_verification_decision_missing" },
    });

    const completed = await completeExecutionWithDecision(fixture.issueId, fixture.verifierAgentId);
    expect(completed).toMatchObject({ status: "done" });
    const completedReservation = await db.select().from(governedIssueBuilderHistory)
      .where(and(
        eq(governedIssueBuilderHistory.issueId, fixture.issueId),
        eq(governedIssueBuilderHistory.kind, "verification_reservation"),
      ));
    expect(completedReservation).toEqual([
      expect.objectContaining({ state: "completed", resolution: "approved" }),
    ]);
  });

  it("resolves and reserves an active same-company user verifier", async () => {
    const fixture = await seedDispatchFixture({ userVerifier: true });
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    await issueService(db).update(
      fixture.issueId,
      await executionPatch(fixture.issueId, fixture.targetAgentId, "done"),
    );

    const reservation = await db.select().from(governedIssueBuilderHistory)
      .where(and(
        eq(governedIssueBuilderHistory.issueId, fixture.issueId),
        eq(governedIssueBuilderHistory.kind, "verification_reservation"),
        eq(governedIssueBuilderHistory.state, "active"),
      ));
    expect(reservation).toEqual([
      expect.objectContaining({ agentId: null, userId: "alex" }),
    ]);
    await expect(issueService(db).update(fixture.issueId, {
      status: "in_progress",
      assigneeAgentId: null,
      assigneeUserId: "alex",
      actorUserId: "alex",
    })).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_user_builder_unsupported", userId: "alex" },
    });
  });

  it("serializes competing reservation-bound decisions without deadlocking", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const reserved = await issueService(db).update(
      fixture.issueId,
      await executionPatch(fixture.issueId, fixture.targetAgentId, "done"),
    );
    if (!reserved) throw new Error("Expected governed review reservation");

    const attempt = (decisionId: string) => db.transaction(async (tx) => {
      const locked = await lockGovernedIssueDecisionLane(tx as unknown as typeof db, {
        companyId: fixture.companyId,
        issueId: fixture.issueId,
        expected: reserved,
      });
      const policy = normalizeIssueExecutionPolicy(locked.executionPolicy)!;
      const transition = applyIssueExecutionPolicyTransition({
        issue: locked,
        policy,
        previousPolicy: policy,
        requestedStatus: "done",
        requestedAssigneePatch: {},
        actor: { agentId: fixture.verifierAgentId },
        commentBody: "Concurrent independent approval.",
      });
      if (!transition.decision || !transition.patch.executionState) {
        throw new Error("Expected governed review decision");
      }
      const reservationId = parseIssueExecutionState(locked.executionState)?.verificationReservationId;
      await tx.insert(issueExecutionDecisions).values({
        id: decisionId,
        companyId: fixture.companyId,
        issueId: fixture.issueId,
        stageId: transition.decision.stageId,
        stageType: transition.decision.stageType,
        actorAgentId: fixture.verifierAgentId,
        actorUserId: null,
        outcome: transition.decision.outcome,
        body: transition.decision.body,
        createdByRunId: null,
        reservationId,
      });
      return issueService(db).update(fixture.issueId, {
        status: "done",
        ...transition.patch,
        executionState: {
          ...(transition.patch.executionState as Record<string, unknown>),
          lastDecisionId: decisionId,
          lastDecisionOutcome: transition.decision.outcome,
        },
        actorAgentId: fixture.verifierAgentId,
      }, tx);
    });

    const results = await Promise.allSettled([attempt(randomUUID()), attempt(randomUUID())]);
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    const rejected = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
    expect(rejected?.reason).toMatchObject({
      status: 409,
      details: { code: "stale_issue_version" },
    });
  });

  it("blocks reassignment laundering when a later builder is selected as verifier", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const svc = issueService(db);
    await svc.update(fixture.issueId, {
      assigneeAgentId: fixture.verifierAgentId,
      actorAgentId: fixture.authorityAgentId,
    });
    await svc.update(fixture.issueId, {
      assigneeAgentId: fixture.targetAgentId,
      actorAgentId: fixture.authorityAgentId,
    });

    await expect(svc.update(
      fixture.issueId,
      await executionPatch(fixture.issueId, fixture.targetAgentId, "done"),
    )).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_builder_cannot_verify", agentId: fixture.verifierAgentId },
    });
    const builders = await db.select().from(governedIssueBuilderHistory)
      .where(and(
        eq(governedIssueBuilderHistory.issueId, fixture.issueId),
        eq(governedIssueBuilderHistory.kind, "builder"),
      ));
    expect(new Set(builders.map((row) => row.agentId))).toEqual(
      new Set([fixture.targetAgentId, fixture.verifierAgentId]),
    );
  });

  it("blocks the reserved verifier from adopting build work", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const svc = issueService(db);
    await svc.update(
      fixture.issueId,
      await executionPatch(fixture.issueId, fixture.targetAgentId, "done"),
    );

    await expect(svc.update(fixture.issueId, {
      status: "in_progress",
      assigneeAgentId: fixture.verifierAgentId,
      actorAgentId: fixture.verifierAgentId,
    })).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_verifier_cannot_build" },
    });
  });

  it("blocks direct completion and review-policy downgrade after governed dispatch", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const svc = issueService(db);

    await expect(svc.update(fixture.issueId, {
      status: "done",
      actorAgentId: fixture.targetAgentId,
    })).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_verification_required" },
    });
    await expect(svc.update(fixture.issueId, {
      executionPolicy: null,
      actorAgentId: fixture.authorityAgentId,
    })).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_review_policy_change_requires_exception" },
    });
  });

  it("serializes builder assignment against verifier reservation on the same task", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const svc = issueService(db);
    const reservePatch = await executionPatch(fixture.issueId, fixture.targetAgentId, "done");

    const results = await Promise.allSettled([
      svc.update(fixture.issueId, {
        assigneeAgentId: fixture.verifierAgentId,
        actorAgentId: fixture.authorityAgentId,
      }),
      svc.update(fixture.issueId, reservePatch),
    ]);
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);

    const issue = await db.select().from(issues).where(eq(issues.id, fixture.issueId)).then((rows) => rows[0]);
    const builders = await db.select().from(governedIssueBuilderHistory)
      .where(and(
        eq(governedIssueBuilderHistory.issueId, fixture.issueId),
        eq(governedIssueBuilderHistory.kind, "builder"),
      ));
    const verifierIsBuilder = builders.some((row) => row.agentId === fixture.verifierAgentId);
    const state = parseIssueExecutionState(issue.executionState);
    expect(
      (issue.status === "in_progress" && issue.assigneeAgentId === fixture.verifierAgentId && verifierIsBuilder && !state)
      || (
        issue.status === "in_review"
        && issue.assigneeAgentId === fixture.verifierAgentId
        && !verifierIsBuilder
        && state?.verificationPolicyDigest
      ),
    ).toBeTruthy();
  });

  it("rejects direct database writes that bypass governed issue authority", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));

    let bypassError: unknown = null;
    try {
      await db.update(issues)
        .set({ status: "done", updatedAt: new Date("2026-08-21T01:06:00.000Z") })
        .where(eq(issues.id, fixture.issueId));
    } catch (error) {
      bypassError = error;
    }
    expect(bypassError).toBeTruthy();
    expect((bypassError as { cause?: { code?: string; message?: string } }).cause).toMatchObject({
      code: "42501",
      message: "governed issue mutation must pass through assignment/verification authority",
    });
    let policyBypassError: unknown = null;
    try {
      await db.update(issues)
        .set({ executionPolicy: null, updatedAt: new Date("2026-08-21T01:06:01.000Z") })
        .where(eq(issues.id, fixture.issueId));
    } catch (error) {
      policyBypassError = error;
    }
    expect((policyBypassError as { cause?: { code?: string; message?: string } }).cause).toMatchObject({
      code: "42501",
      message: "governed issue mutation must pass through assignment/verification authority",
    });
    const issue = await db.select().from(issues).where(eq(issues.id, fixture.issueId)).then((rows) => rows[0]);
    expect(issue.status).toBe("in_progress");
    expect(issue.executionPolicy).not.toBeNull();
  });

  it("rejects governed activation for issues that predate the immutable cutover", async () => {
    const fixture = await seedDispatchFixture();
    const cutover = await db.select().from(governedIssueControlCutover)
      .where(eq(governedIssueControlCutover.id, "singleton"))
      .then((rows) => rows[0]!);
    await db.update(issues)
      .set({ createdAt: new Date(cutover.activatedAt.getTime() - 1) })
      .where(eq(issues.id, fixture.issueId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({
        status: 409,
        details: { code: "governed_issue_predates_cutover" },
      });
  });

  it("rejects user principals from governed builder statuses", async () => {
    const fixture = await seedDispatchFixture({ userVerifier: true });
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    await issueService(db).update(
      fixture.issueId,
      await executionPatch(fixture.issueId, fixture.targetAgentId, "done"),
    );

    await expect(issueService(db).update(fixture.issueId, {
      status: "in_progress",
      actorUserId: "alex",
    })).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_user_builder_unsupported", userId: "alex" },
    });
  });

  it("database-enforces immutable builder history and one active verifier reservation", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const builder = await db.select().from(governedIssueBuilderHistory)
      .where(and(
        eq(governedIssueBuilderHistory.issueId, fixture.issueId),
        eq(governedIssueBuilderHistory.kind, "builder"),
      ))
      .then((rows) => rows[0]!);

    await expect(db.update(governedIssueBuilderHistory)
      .set({ source: "tampered" })
      .where(eq(governedIssueBuilderHistory.id, builder.id)))
      .rejects.toMatchObject({ cause: { code: "42501", message: "governed builder history is immutable" } });
    await expect(db.delete(governedIssueBuilderHistory)
      .where(eq(governedIssueBuilderHistory.id, builder.id)))
      .rejects.toMatchObject({ cause: { code: "42501", message: "governed issue ledger rows are append-retained" } });
    await expect(issueService(db).remove(fixture.issueId))
      .rejects.toMatchObject({
        status: 409,
        details: { code: "governed_issue_deletion_unsupported", issueId: fixture.issueId },
      });
    await expect(db.delete(issues).where(eq(issues.id, fixture.issueId)))
      .rejects.toMatchObject({ cause: { code: "42501", message: "governed issue ledger rows are append-retained" } });
    await expect(agentService(db).remove(fixture.targetAgentId))
      .rejects.toMatchObject({
        status: 409,
        details: { code: "governed_issue_history_retention_required", issueId: fixture.issueId },
      });

    await issueService(db).update(
      fixture.issueId,
      await executionPatch(fixture.issueId, fixture.targetAgentId, "done"),
    );
    const state = parseIssueExecutionState(
      await db.select({ executionState: issues.executionState }).from(issues)
        .where(eq(issues.id, fixture.issueId)).then((rows) => rows[0]!.executionState),
    )!;
    await expect(db.insert(governedIssueBuilderHistory).values({
      companyId: fixture.companyId,
      issueId: fixture.issueId,
      userId: "second-verifier",
      kind: "verification_reservation",
      state: "active",
      stageId: state.currentStageId!,
      source: "test_competing_reservation",
      reviewRequiredSnapshot: true,
      policyDigest: builder.policyDigest,
      reservationExpiresAt: new Date("2026-08-22T01:00:00.000Z"),
    })).rejects.toMatchObject({ cause: { code: "23505" } });
  });

  it("fails closed when issue-tree control tries to mutate governed status", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const tree = issueTreeControlService(db);
    const hold = await tree.createHold(fixture.companyId, fixture.issueId, {
      mode: "cancel",
      reason: "governed cancellation requires an exception lane",
      actor: { actorType: "user", actorId: "board-user", userId: "board-user" },
    });

    await expect(tree.cancelIssueStatusesForHold(fixture.companyId, fixture.issueId, hold.hold.id))
      .rejects.toMatchObject({
        status: 409,
        details: {
          code: "governed_issue_tree_mutation_unsupported",
          issueId: fixture.issueId,
          operation: "cancel",
        },
      });
  });

  it("rejects a pending approval that lacks its server-issued immutable scope", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(approvals)
      .set({ payload: { purpose: "governed_queue_dispatch" } })
      .where(eq(approvals.id, fixture.approvalId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 409, details: { code: "queue_approval_scope_missing" } });
    const approval = await db.select().from(approvals)
      .where(eq(approvals.id, fixture.approvalId)).then((rows) => rows[0]);
    expect(approval.status).toBe("pending");
  });

  it("rejects bulk import of multiple active assignments to one governed worker", async () => {
    const fixture = await seedDispatchFixture();
    const imported = (index: number) => ({
      id: randomUUID(),
      ref: `import-${index}`,
      projectId: null,
      projectWorkspaceId: null,
      title: `Imported active issue ${index}`,
      description: null,
      assigneeAgentId: fixture.targetAgentId,
      status: "in_progress" as const,
      priority: "medium" as const,
      billingCode: null,
      assigneeAdapterOverrides: null,
      executionWorkspaceSettings: null,
      labelIds: [],
      monitorNotes: null,
      monitorScheduledBy: null,
    });

    await expect(issueService(db).importIssues(fixture.companyId, [imported(1), imported(2)]))
      .rejects.toMatchObject({
        status: 409,
        details: { code: "target_has_execution_load", incomingAssignmentCount: 2 },
      });
    const rows = await db.select().from(issues).where(eq(issues.companyId, fixture.companyId));
    expect(rows).toHaveLength(1);
  });

  it("returns the exact existing dispatch on an idempotent replay", async () => {
    const fixture = await seedDispatchFixture();
    const service = governedQueueDispatchService(db);
    const first = await service.dispatch(dispatchInput(fixture));
    const replay = await service.dispatch(dispatchInput(fixture));

    expect(replay.idempotent).toBe(true);
    expect(replay.run.id).toBe(first.run.id);
    expect(replay.wakeupRequest.id).toBe(first.wakeupRequest.id);
    expect(await db.select().from(heartbeatRuns)).toHaveLength(1);
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(1);
    expect(await db.select().from(activityLog)).toHaveLength(1);
  });

  it("returns an existing idempotent dispatch even after its authorization window expires", async () => {
    const fixture = await seedDispatchFixture();
    const service = governedQueueDispatchService(db);
    const first = await service.dispatch(dispatchInput(fixture));
    const replay = await service.dispatch({
      ...dispatchInput(fixture),
      now: new Date("2026-08-21T02:00:00.000Z"),
    });

    expect(replay.idempotent).toBe(true);
    expect(replay.run.id).toBe(first.run.id);
    expect(await db.select().from(heartbeatRuns)).toHaveLength(1);
  });

  it.each([
    { field: "approvalMarker", value: "RATA-1:different-marker" },
    { field: "expectedUpdatedAt", value: "2026-08-21T00:59:00.000Z" },
    { field: "expiresAt", value: "2026-08-21T01:14:00.000Z" },
    { field: "maxDispatches", value: 2 },
  ])("rejects an idempotency replay with changed $field", async ({ field, value }) => {
    const fixture = await seedDispatchFixture();
    const service = governedQueueDispatchService(db);
    await service.dispatch(dispatchInput(fixture));

    await expect(service.dispatch({
      ...dispatchInput(fixture),
      [field]: value,
    })).rejects.toMatchObject({
      status: 409,
      details: { code: "queue_idempotency_mismatch" },
    });
    expect(await db.select().from(heartbeatRuns)).toHaveLength(1);
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(1);
    expect(await db.select().from(activityLog)).toHaveLength(1);
  });

  it("fails stale issue CAS without approving, assigning, waking, or auditing", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(issues).set({ title: "Concurrent edit", updatedAt: new Date("2026-08-21T01:01:00.000Z") })
      .where(eq(issues.id, fixture.issueId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 412, details: { code: "stale_issue_version" } });

    const issue = await db.select().from(issues).where(eq(issues.id, fixture.issueId)).then((rows) => rows[0]);
    const approval = await db.select().from(approvals).where(eq(approvals.id, fixture.approvalId)).then((rows) => rows[0]);
    expect(issue).toMatchObject({ title: "Concurrent edit", status: "todo", assigneeAgentId: null });
    expect(approval.status).toBe("pending");
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
    expect(await db.select().from(heartbeatRuns)).toHaveLength(0);
    expect(await db.select().from(activityLog)).toHaveLength(0);
  });

  it("rejects an authority agent that does not own the linked approval", async () => {
    const fixture = await seedDispatchFixture();
    const wrongAuthorityAgentId = randomUUID();
    await db.insert(agents).values({
      id: wrongAuthorityAgentId,
      companyId: fixture.companyId,
      name: "Wrong coordinator",
      role: "manager",
      status: "idle",
      adapterType: "hermes_local",
      adapterConfig: { wakeOnDemand: true },
      runtimeConfig: {},
      permissions: {},
    });

    await expect(governedQueueDispatchService(db).dispatch({
      ...dispatchInput(fixture),
      authorityAgentId: wrongAuthorityAgentId,
    })).rejects.toMatchObject({ status: 409, details: { code: "queue_authority_mismatch" } });
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
  });

  it("rejects a target agent that is not idle", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(agents).set({ status: "running" }).where(eq(agents.id, fixture.targetAgentId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 409, details: { code: "target_not_idle" } });
  });

  it("rejects a target without the governed single-assignment policy", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(agents).set({ runtimeConfig: {} }).where(eq(agents.id, fixture.targetAgentId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 409, details: { code: "target_queue_policy_disabled" } });
  });

  it("rejects atomically when the target agent live-run cap is full", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(agents).set({
      runtimeConfig: {
        governedQueue: { singleActiveAssignment: true },
        heartbeat: {
          enabled: false,
          intervalSec: 0,
          wakeOnDemand: true,
          maxConcurrentRuns: 1,
          maxLiveRuns: 1,
        },
      },
    }).where(eq(agents.id, fixture.targetAgentId));
    await db.insert(heartbeatRuns).values({
      companyId: fixture.companyId,
      agentId: fixture.targetAgentId,
      invocationSource: "automation",
      triggerDetail: "system",
      status: "running",
      responsibleUserId: "alex",
      contextSnapshot: { taskKey: "issue:other" },
    });

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({
        status: 409,
        details: { code: "heartbeat_live_run_limit", observed: 1, limit: 1 },
      });

    const issue = await db.select().from(issues).where(eq(issues.id, fixture.issueId)).then((rows) => rows[0]);
    const approval = await db.select().from(approvals).where(eq(approvals.id, fixture.approvalId)).then((rows) => rows[0]);
    expect(issue).toMatchObject({ status: "todo", assigneeAgentId: null });
    expect(approval.status).toBe("pending");
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
    expect(await db.select().from(heartbeatRuns)).toHaveLength(1);
  });

  it("never exceeds the target cap when governed dispatch competes with an ordinary wake", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(agents).set({
      runtimeConfig: {
        governedQueue: { singleActiveAssignment: true },
        heartbeat: {
          enabled: false,
          intervalSec: 0,
          wakeOnDemand: true,
          maxConcurrentRuns: 1,
          maxLiveRuns: 1,
        },
      },
    }).where(eq(agents.id, fixture.targetAgentId));

    const [ordinary, governed] = await Promise.allSettled([
      db.transaction((tx) => admitLiveRun({
        tx,
        agent: { id: fixture.targetAgentId, companyId: fixture.companyId },
        wakeupRequest: {
          source: "automation",
          triggerDetail: "system",
          reason: "ordinary_wake",
          payload: { taskKey: "issue:ordinary" },
          requestedByActorType: "system",
          requestedByActorId: null,
        },
        run: {
          invocationSource: "automation",
          triggerDetail: "system",
          responsibleUserId: "alex",
          contextSnapshot: { taskKey: "issue:ordinary" },
        },
        coalescing: {
          taskKey: "issue:ordinary",
          mergeContextSnapshot: (existing, incoming) => ({
            ...(existing && typeof existing === "object" ? existing : {}),
            ...incoming,
          }),
        },
      })),
      governedQueueDispatchService(db).dispatch(dispatchInput(fixture)),
    ]);

    const liveRuns = await db.select().from(heartbeatRuns)
      .where(and(
        eq(heartbeatRuns.agentId, fixture.targetAgentId),
        eq(heartbeatRuns.status, "queued"),
      ));
    expect(liveRuns).toHaveLength(1);
    if (ordinary.status !== "fulfilled") throw ordinary.reason;
    if (ordinary.value.kind === "queued") {
      expect(governed.status).toBe("rejected");
    } else {
      expect(ordinary.value.kind).toBe("skipped");
      expect(governed.status).toBe("fulfilled");
    }
  });

  it.each([
    {
      name: "timer heartbeats enabled",
      heartbeat: { enabled: true, intervalSec: 60, wakeOnDemand: true, maxConcurrentRuns: 1 },
    },
    {
      name: "on-demand wake disabled",
      heartbeat: { enabled: false, intervalSec: 0, wakeOnDemand: false, maxConcurrentRuns: 1 },
    },
    {
      name: "concurrency exceeds one",
      heartbeat: { enabled: false, intervalSec: 0, wakeOnDemand: true, maxConcurrentRuns: 2 },
    },
  ])("rejects a target with $name", async ({ heartbeat }) => {
    const fixture = await seedDispatchFixture();
    await db
      .update(agents)
      .set({ runtimeConfig: { governedQueue: { singleActiveAssignment: true }, heartbeat } })
      .where(eq(agents.id, fixture.targetAgentId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({
        status: 409,
        details: { code: "target_queue_runtime_policy_mismatch" },
      });
  });

  it("rejects a generic board approval as queue authority", async () => {
    const fixture = await seedDispatchFixture();
    await db
      .update(approvals)
      .set({ type: "request_board_approval" })
      .where(eq(approvals.id, fixture.approvalId));

    await expect(
      governedQueueDispatchService(db).dispatch(dispatchInput(fixture)),
    ).rejects.toMatchObject({ status: 409, details: { code: "queue_approval_type_invalid" } });
  });

  it("rejects an issue already assigned to a user", async () => {
    const fixture = await seedDispatchFixture();
    await db.update(issues).set({ assigneeUserId: "alex" }).where(eq(issues.id, fixture.issueId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 409, details: { code: "issue_not_claimable" } });
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
  });

  it("counts unrelated live runs against the governed queue-path cap", async () => {
    const fixture = await seedDispatchFixture();
    await db.insert(heartbeatRuns).values([0, 1, 2].map(() => ({
      companyId: fixture.companyId,
      agentId: fixture.authorityAgentId,
      invocationSource: "manual",
      status: "running",
      responsibleUserId: "alex",
    })));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 409, details: { code: "max_dispatches_reached", observed: 3, limit: 3 } });
    const issue = await db.select().from(issues).where(eq(issues.id, fixture.issueId)).then((rows) => rows[0]);
    expect(issue).toMatchObject({ status: "todo", assigneeAgentId: null });
  });

  it("ignores unrelated deferred wake requests when enforcing the governed queue-path cap", async () => {
    const fixture = await seedDispatchFixture();
    await db.insert(agentWakeupRequests).values([0, 1, 2].map((index) => ({
      companyId: fixture.companyId,
      agentId: fixture.authorityAgentId,
      source: index % 2 === 0 ? "assignment" : "automation",
      reason: "issue_execution_deferred",
      status: "deferred_issue_execution",
    })));

    const result = await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));

    expect(result.idempotent).toBe(false);
    expect(result.run.status).toBe("queued");
    expect(await db.select().from(heartbeatRuns)).toHaveLength(1);
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(4);
  });

  it("counts orphan governed queue wake requests against the governed queue-path cap", async () => {
    const fixture = await seedDispatchFixture();
    await db.insert(agentWakeupRequests).values([0, 1, 2].map(() => ({
      companyId: fixture.companyId,
      agentId: fixture.authorityAgentId,
      source: "automation",
      reason: "governed_queue_dispatch",
      status: "queued",
    })));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({ status: 409, details: { code: "max_dispatches_reached", observed: 3, limit: 3 } });
    const issue = await db.select().from(issues).where(eq(issues.id, fixture.issueId)).then((rows) => rows[0]);
    expect(issue).toMatchObject({ status: "todo", assigneeAgentId: null });
  });

  it("rejects authorization lifetimes above the server-owned fifteen-minute ceiling", async () => {
    const fixture = await seedDispatchFixture();
    await expect(governedQueueDispatchService(db).dispatch({
      ...dispatchInput(fixture),
      expiresAt: "2026-08-21T01:20:01.000Z",
    })).rejects.toMatchObject({
      status: 412,
      details: { code: "queue_expiry_exceeds_server_limit", maxTtlSeconds: 900 },
    });
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
  });

  it("rejects an approval older than the server-owned fifteen-minute ceiling", async () => {
    const fixture = await seedDispatchFixture();
    await db
      .update(approvals)
      .set({
        createdAt: new Date("2026-08-21T00:49:59.000Z"),
        updatedAt: new Date("2026-08-21T00:49:59.000Z"),
      })
      .where(eq(approvals.id, fixture.approvalId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({
        status: 412,
        details: { code: "queue_approval_expired", maxTtlSeconds: 900 },
      });
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
  });

  it("rejects an approval whose creation-to-expiry window exceeds fifteen minutes", async () => {
    const fixture = await seedDispatchFixture();
    await db
      .update(approvals)
      .set({
        createdAt: new Date("2026-08-21T00:59:00.000Z"),
        updatedAt: new Date("2026-08-21T00:59:00.000Z"),
      })
      .where(eq(approvals.id, fixture.approvalId));

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({
        status: 412,
        details: { code: "queue_approval_window_exceeds_server_limit", maxTtlSeconds: 900 },
      });
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
  });

  it("rejects a todo that has an unresolved blocker", async () => {
    const fixture = await seedDispatchFixture();
    const blockerIssueId = randomUUID();
    await db.insert(issues).values({
      id: blockerIssueId,
      companyId: fixture.companyId,
      title: "Unresolved prerequisite",
      status: "todo",
      priority: "high",
    });
    await db.insert(issueRelations).values({
      companyId: fixture.companyId,
      issueId: blockerIssueId,
      relatedIssueId: fixture.issueId,
      type: "blocks",
      createdByAgentId: fixture.authorityAgentId,
    });

    await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture)))
      .rejects.toMatchObject({
        status: 409,
        details: { code: "issue_has_unresolved_blocker", blockerIssueId },
      });
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
  });

  it("prevents ordinary issue updates from adding a second active card to a governed queue worker", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const secondIssueId = randomUUID();
    await db.insert(issues).values({
      id: secondIssueId,
      companyId: fixture.companyId,
      title: "Second active card",
      status: "todo",
      priority: "medium",
    });

    await expect(issueService(db).update(secondIssueId, {
      assigneeAgentId: fixture.targetAgentId,
      status: "in_progress",
    })).rejects.toMatchObject({ status: 409, details: { code: "target_has_execution_load" } });
    const second = await db.select().from(issues).where(eq(issues.id, secondIssueId)).then((rows) => rows[0]);
    expect(second).toMatchObject({ status: "todo", assigneeAgentId: null });
  });

  it("prevents ordinary issue creation from adding a second active card", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));

    await expect(issueService(db).create(fixture.companyId, {
      title: "Created second active card",
      status: "todo",
      priority: "medium",
      assigneeAgentId: fixture.targetAgentId,
    })).rejects.toMatchObject({ status: 409, details: { code: "target_has_execution_load" } });
  });

  it("prevents ordinary checkout from adding a second active card", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const secondIssueId = randomUUID();
    await db.insert(issues).values({
      id: secondIssueId,
      companyId: fixture.companyId,
      title: "Checkout second active card",
      status: "todo",
      priority: "medium",
    });

    await expect(issueService(db).checkout(secondIssueId, fixture.targetAgentId, ["todo"], null))
      .rejects.toMatchObject({ status: 409, details: { code: "target_has_execution_load" } });
  });

  it("prevents an assigned waiting card from reactivating beside another active card", async () => {
    const fixture = await seedDispatchFixture();
    await governedQueueDispatchService(db).dispatch(dispatchInput(fixture));
    const waitingIssue = await db
      .insert(issues)
      .values({
        companyId: fixture.companyId,
        title: "Waiting review card",
        status: "in_review",
        priority: "medium",
        assigneeAgentId: fixture.targetAgentId,
      })
      .returning()
      .then((rows) => rows[0]!);

    await expect(issueService(db).update(waitingIssue.id, { status: "todo" }))
      .rejects.toMatchObject({ status: 409, details: { code: "target_has_execution_load" } });
    const persisted = await db
      .select()
      .from(issues)
      .where(eq(issues.id, waitingIssue.id))
      .then((rows) => rows[0]!);
    expect(persisted.status).toBe("in_review");
  });

  it("rejects enabling governed single-assignment policy on an over-capacity agent", async () => {
    const fixture = await seedDispatchFixture();
    const legacyAgent = await db
      .insert(agents)
      .values({
        companyId: fixture.companyId,
        name: "Legacy parallel worker",
        role: "engineer",
        status: "idle",
        adapterType: "hermes_local",
        adapterConfig: {},
        runtimeConfig: {},
        permissions: {},
      })
      .returning()
      .then((rows) => rows[0]!);
    await db.insert(issues).values([
      {
        companyId: fixture.companyId,
        title: "Legacy active one",
        status: "todo",
        priority: "medium",
        assigneeAgentId: legacyAgent.id,
      },
      {
        companyId: fixture.companyId,
        title: "Legacy active two",
        status: "in_progress",
        priority: "medium",
        assigneeAgentId: legacyAgent.id,
      },
    ]);

    await expect(agentService(db).update(legacyAgent.id, {
      runtimeConfig: { governedQueue: { singleActiveAssignment: true } },
    })).rejects.toMatchObject({
      status: 409,
      details: { code: "governed_policy_activation_over_capacity", activeAssignmentCount: 2 },
    });
    const persisted = await db
      .select()
      .from(agents)
      .where(eq(agents.id, legacyAgent.id))
      .then((rows) => rows[0]!);
    expect(persisted.runtimeConfig).toEqual({});
  });

  it("serializes a governed dispatch against an ordinary assignment transaction", async () => {
    const fixture = await seedDispatchFixture();
    const secondIssueId = randomUUID();
    await db.insert(issues).values({
      id: secondIssueId,
      companyId: fixture.companyId,
      title: "Ordinary competing assignment",
      status: "todo",
      priority: "medium",
    });

    const results = await Promise.allSettled([
      governedQueueDispatchService(db).dispatch(dispatchInput(fixture)),
      issueService(db).update(secondIssueId, {
        assigneeAgentId: fixture.targetAgentId,
        status: "in_progress",
      }),
    ]);

    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
    const assigned = await db.select().from(issues).where(eq(issues.assigneeAgentId, fixture.targetAgentId));
    expect(assigned).toHaveLength(1);
  });

  it("serializes competing dispatch transactions so exactly one target assignment wins", async () => {
    const first = await seedDispatchFixture();
    const secondIssueId = randomUUID();
    const secondApprovalId = randomUUID();
    const secondUpdatedAt = new Date("2026-08-21T01:00:00.000Z");
    await db.insert(issues).values({
      id: secondIssueId,
      companyId: first.companyId,
      issueNumber: 2,
      identifier: "RATA-2",
      title: "Competing dispatch",
      status: "todo",
      priority: "high",
      responsibleUserId: "alex",
      executionPolicy: {
        mode: "normal",
        commentRequired: true,
        stages: [{
          id: randomUUID(),
          type: "review",
          approvalsNeeded: 1,
          participants: [{ id: randomUUID(), type: "agent", agentId: first.verifierAgentId }],
        }],
      },
      updatedAt: secondUpdatedAt,
    });
    const secondApprovalPayload = await prepareGovernedQueueApprovalPayload(db, {
      companyId: first.companyId,
      issueId: secondIssueId,
      expectedUpdatedAt: secondUpdatedAt.toISOString(),
      approvalMarker: "RATA-2:alex-approved",
      targetAgentId: first.targetAgentId,
      authorityAgentId: first.authorityAgentId,
      maxDispatches: 3,
      expiresAt: "2026-08-21T01:15:00.000Z",
      idempotencyKey: "rata-2-qwen-v1",
      now: new Date("2026-08-21T01:04:00.000Z"),
    }, { purpose: "governed_queue_dispatch" });
    await db.insert(approvals).values({
      id: secondApprovalId,
      companyId: first.companyId,
      type: "governed_queue_dispatch",
      status: "pending",
      requestedByAgentId: first.authorityAgentId,
      payload: secondApprovalPayload,
    });
    await db.insert(issueApprovals).values({
      companyId: first.companyId,
      issueId: secondIssueId,
      approvalId: secondApprovalId,
      linkedByAgentId: first.authorityAgentId,
    });

    const results = await Promise.allSettled([
      governedQueueDispatchService(db).dispatch(dispatchInput(first)),
      governedQueueDispatchService(db).dispatch({
        ...dispatchInput(first),
        issueId: secondIssueId,
        approvalId: secondApprovalId,
        expectedUpdatedAt: secondUpdatedAt.toISOString(),
        approvalMarker: "RATA-2:alex-approved",
        idempotencyKey: "rata-2-qwen-v1",
      }),
    ]);

    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
    const assigned = await db.select().from(issues).where(eq(issues.assigneeAgentId, first.targetAgentId));
    expect(assigned).toHaveLength(1);
  });

  it("CAS roundtrip: dispatch succeeds using the ms-precision updatedAt returned by issueService.create", async () => {
    // Seed infrastructure (company + agents) but NOT the issue.
    const fixture = await seedDispatchFixture();

    // Create the issue via the service — caller does not supply updatedAt.
    // Before the fix PostgreSQL's defaultNow() stores microsecond precision
    // (e.g. 10:00:00.123456) while the JS Date returned to the caller is
    // truncated to milliseconds (10:00:00.123).  The SQL CAS WHERE clause then
    // compares the ms value against the stored μs value and finds no match,
    // raising queue_dispatch_compare_and_swap_failed with no intervening write.
    const createdIssue = await issueService(db).create(fixture.companyId, {
      title: "CAS precision regression issue",
      status: "todo",
      priority: "high",
      responsibleUserId: "alex",
      executionPolicy: {
        mode: "normal",
        commentRequired: true,
        stages: [{
          id: randomUUID(),
          type: "review",
          approvalsNeeded: 1,
          participants: [{ id: randomUUID(), type: "agent", agentId: fixture.verifierAgentId }],
        }],
      },
    });

    // Simulate exactly what the API serialises to the caller and what the
    // caller then echoes back as expectedUpdatedAt — toISOString() is ms only.
    const msUpdatedAt = createdIssue.updatedAt instanceof Date
      ? createdIssue.updatedAt.toISOString()
      : new Date(createdIssue.updatedAt as string).toISOString();

    const casApprovalId = randomUUID();
    const dispatchNow = new Date("2026-08-22T12:04:00.000Z");
    const expiresAt = "2026-08-22T12:15:00.000Z";

    const approvalPayload = await prepareGovernedQueueApprovalPayload(db, {
      companyId: fixture.companyId,
      issueId: createdIssue.id,
      expectedUpdatedAt: msUpdatedAt,
      approvalMarker: "CAS-PRECISION:alex-approved",
      targetAgentId: fixture.targetAgentId,
      authorityAgentId: fixture.authorityAgentId,
      maxDispatches: 1,
      expiresAt,
      idempotencyKey: "cas-precision-regression-v1",
      now: dispatchNow,
    }, { purpose: "governed_queue_dispatch" });

    await db.insert(approvals).values({
      id: casApprovalId,
      companyId: fixture.companyId,
      type: "governed_queue_dispatch",
      status: "pending",
      requestedByAgentId: fixture.authorityAgentId,
      payload: approvalPayload,
      createdAt: dispatchNow,
      updatedAt: dispatchNow,
    });
    await db.insert(issueApprovals).values({
      companyId: fixture.companyId,
      issueId: createdIssue.id,
      approvalId: casApprovalId,
      linkedByAgentId: fixture.authorityAgentId,
    });

    const result = await governedQueueDispatchService(db).dispatch({
      issueId: createdIssue.id,
      companyId: fixture.companyId,
      expectedUpdatedAt: msUpdatedAt,
      approvalId: casApprovalId,
      approvalMarker: "CAS-PRECISION:alex-approved",
      targetAgentId: fixture.targetAgentId,
      authorityAgentId: fixture.authorityAgentId,
      maxDispatches: 1,
      expiresAt,
      idempotencyKey: "cas-precision-regression-v1",
      now: new Date("2026-08-22T12:05:00.000Z"),
      responsibleUserId: "alex",
      sessionIdBefore: null,
    });

    expect(result.idempotent).toBe(false);
    expect(result.issue.status).toBe("in_progress");
    expect(result.issue.assigneeAgentId).toBe(fixture.targetAgentId);
  });

  it("rolls back every mutation when the in-transaction audit insert fails", async () => {
    const fixture = await seedDispatchFixture();
    await db.execute(sql`
      create function paperclip_test_fail_governed_queue_audit() returns trigger language plpgsql as $$
      begin
        if new.action = 'issue.governed_queue_dispatched' then
          raise exception 'forced audit failure';
        end if;
        return new;
      end $$
    `);
    await db.execute(sql`
      create trigger paperclip_test_fail_governed_queue_audit
      before insert on activity_log
      for each row execute function paperclip_test_fail_governed_queue_audit()
    `);
    try {
      await expect(governedQueueDispatchService(db).dispatch(dispatchInput(fixture))).rejects.toThrow();
    } finally {
      await db.execute(sql`drop trigger if exists paperclip_test_fail_governed_queue_audit on activity_log`);
      await db.execute(sql`drop function if exists paperclip_test_fail_governed_queue_audit()`);
    }

    const issue = await db.select().from(issues).where(eq(issues.id, fixture.issueId)).then((rows) => rows[0]);
    const approval = await db.select().from(approvals).where(eq(approvals.id, fixture.approvalId)).then((rows) => rows[0]);
    expect(issue).toMatchObject({ status: "todo", assigneeAgentId: null, description: "Approved autonomous implementation work." });
    expect(approval.status).toBe("pending");
    expect(await db.select().from(agentWakeupRequests)).toHaveLength(0);
    expect(await db.select().from(heartbeatRuns)).toHaveLength(0);
  });
});
