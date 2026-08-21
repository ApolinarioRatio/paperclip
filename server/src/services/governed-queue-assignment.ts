import { and, count, eq, inArray, ne, sql } from "drizzle-orm";
import { agents, issues, type Db } from "@paperclipai/db";
import { conflict } from "../errors.js";

const ACTIVE_ASSIGNMENT_STATUSES = ["todo", "in_progress"] as const;

function readObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

export function hasGovernedSingleActiveAssignmentPolicy(runtimeConfig: unknown) {
  return readObject(readObject(runtimeConfig).governedQueue).singleActiveAssignment === true;
}

export async function lockGovernedAssignmentLane(
  dbOrTx: Db,
  companyId: string,
  agentId: string,
) {
  await dbOrTx.execute(
    sql`select pg_advisory_xact_lock(hashtextextended(${`governed-queue-agent:${companyId}:${agentId}`}, 0))`,
  );
}

export async function assertGovernedAssignmentCapacity(
  dbOrTx: Db,
  input: {
    companyId: string;
    agentId: string;
    excludeIssueId?: string | null;
    incomingAssignmentCount?: number;
  },
) {
  await lockGovernedAssignmentLane(dbOrTx, input.companyId, input.agentId);
  const target = await dbOrTx
    .select({ runtimeConfig: agents.runtimeConfig })
    .from(agents)
    .where(and(eq(agents.id, input.agentId), eq(agents.companyId, input.companyId)))
    .then((rows) => rows[0] ?? null);
  if (!target || !hasGovernedSingleActiveAssignmentPolicy(target.runtimeConfig)) {
    return { enforced: false as const };
  }
  const incomingAssignmentCount = input.incomingAssignmentCount ?? 1;
  if (!Number.isInteger(incomingAssignmentCount) || incomingAssignmentCount < 1) {
    throw new Error("incomingAssignmentCount must be a positive integer");
  }
  if (incomingAssignmentCount > 1) {
    throw conflict("Governed agent cannot receive multiple active assignments in one operation", {
      code: "target_has_execution_load",
      incomingAssignmentCount,
    });
  }

  const activeAssignment = await dbOrTx
    .select({ id: issues.id })
    .from(issues)
    .where(and(
      eq(issues.companyId, input.companyId),
      eq(issues.assigneeAgentId, input.agentId),
      inArray(issues.status, [...ACTIVE_ASSIGNMENT_STATUSES]),
      input.excludeIssueId ? ne(issues.id, input.excludeIssueId) : undefined,
    ))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (activeAssignment) {
    throw conflict("Target agent already has active assignment load", {
      code: "target_has_execution_load",
      issueId: activeAssignment.id,
    });
  }
  return { enforced: true as const };
}

export async function assertGovernedPolicyActivationCapacity(
  dbOrTx: Db,
  input: { companyId: string; agentId: string },
) {
  await lockGovernedAssignmentLane(dbOrTx, input.companyId, input.agentId);
  const activeAssignmentCount = await dbOrTx
    .select({ value: count() })
    .from(issues)
    .where(and(
      eq(issues.companyId, input.companyId),
      eq(issues.assigneeAgentId, input.agentId),
      inArray(issues.status, [...ACTIVE_ASSIGNMENT_STATUSES]),
    ))
    .then((rows) => Number(rows[0]?.value ?? 0));
  if (activeAssignmentCount > 1) {
    throw conflict("Cannot enable governed single assignment while the agent has multiple active issues", {
      code: "governed_policy_activation_over_capacity",
      activeAssignmentCount,
    });
  }
}
