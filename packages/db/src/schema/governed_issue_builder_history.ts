import { sql } from "drizzle-orm";
import { boolean, check, index, pgTable, text, timestamp, uniqueIndex, uuid } from "drizzle-orm/pg-core";
import { companies } from "./companies.js";
import { issues } from "./issues.js";

/**
 * Immutable, server-authoritative ever-builder set for governed queue issues.
 *
 * Agent ids intentionally are not foreign keys: deleting or recreating an
 * agent must not erase historical participation and enable self-verification.
 */
export const governedIssueBuilderHistory = pgTable(
  "governed_issue_builder_history",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    companyId: uuid("company_id").notNull().references(() => companies.id),
    issueId: uuid("issue_id").notNull().references(() => issues.id, { onDelete: "cascade" }),
    agentId: uuid("agent_id"),
    userId: text("user_id"),
    kind: text("kind").notNull().default("builder"),
    state: text("state").notNull().default("recorded"),
    stageId: uuid("stage_id"),
    source: text("source").notNull(),
    sourceRunId: uuid("source_run_id"),
    approvalId: uuid("approval_id"),
    reviewRequiredSnapshot: boolean("review_required_snapshot").notNull().default(true),
    policyDigest: text("policy_digest").notNull(),
    reservationExpiresAt: timestamp("reservation_expires_at", { withTimezone: true }),
    resolvedAt: timestamp("resolved_at", { withTimezone: true }),
    resolution: text("resolution"),
    firstAssignedAt: timestamp("first_assigned_at", { withTimezone: true }).notNull().defaultNow(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    companyIssueIdx: index("governed_issue_builder_history_company_issue_idx").on(
      table.companyId,
      table.issueId,
    ),
    issueAgentUq: uniqueIndex("governed_issue_builder_history_issue_agent_uq")
      .on(table.issueId, table.agentId)
      .where(sql`${table.kind} = 'builder'`),
    activeReservationUq: uniqueIndex("governed_issue_builder_history_active_reservation_uq")
      .on(table.issueId)
      .where(sql`${table.kind} = 'verification_reservation' and ${table.state} = 'active'`),
    issueKindStateIdx: index("governed_issue_builder_history_issue_kind_state_idx")
      .on(table.companyId, table.issueId, table.kind, table.state),
    policyDigestCheck: check(
      "governed_issue_builder_history_policy_digest_check",
      sql`${table.policyDigest} ~ '^[0-9a-f]{64}$'`,
    ),
    reviewRequiredCheck: check(
      "governed_issue_builder_history_review_required_check",
      sql`${table.reviewRequiredSnapshot} = true`,
    ),
    kindStateCheck: check(
      "governed_issue_builder_history_kind_state_check",
      sql`(${table.kind} = 'builder' and ${table.state} = 'recorded' and ${table.stageId} is null and ${table.agentId} is not null and ${table.userId} is null)
        or (${table.kind} = 'verification_reservation' and ${table.state} in ('active', 'completed', 'released', 'expired') and ${table.stageId} is not null
          and ((${table.agentId} is not null and ${table.userId} is null) or (${table.agentId} is null and ${table.userId} is not null)))`,
    ),
    reservationExpiryCheck: check(
      "governed_issue_builder_history_reservation_expiry_check",
      sql`(${table.kind} = 'builder' and ${table.reservationExpiresAt} is null)
        or (${table.kind} = 'verification_reservation' and ${table.reservationExpiresAt} is not null)`,
    ),
    resolutionCheck: check(
      "governed_issue_builder_history_resolution_check",
      sql`(${table.kind} = 'builder' and ${table.resolvedAt} is null and ${table.resolution} is null)
        or (${table.kind} = 'verification_reservation' and (
          (${table.state} = 'active' and ${table.resolvedAt} is null and ${table.resolution} is null)
          or (${table.state} = 'completed' and ${table.resolvedAt} is not null and ${table.resolution} = 'approved')
          or (${table.state} = 'expired' and ${table.resolvedAt} is not null and ${table.resolution} = 'expired')
          or (${table.state} = 'released' and ${table.resolvedAt} is not null and ${table.resolution} in ('changes_requested', 'stage_replaced'))
        ))`,
    ),
  }),
);
