import { check, pgTable, text, timestamp } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

/**
 * Immutable database-application cutover for prospective governed issue control.
 * Existing issues predate this row and are intentionally ineligible because
 * their complete historical builder set cannot be reconstructed safely.
 */
export const governedIssueControlCutover = pgTable(
  "governed_issue_control_cutover",
  {
    id: text("id").primaryKey(),
    activatedAt: timestamp("activated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    singletonCheck: check(
      "governed_issue_control_cutover_singleton_check",
      sql`${table.id} = 'singleton'`,
    ),
  }),
);
