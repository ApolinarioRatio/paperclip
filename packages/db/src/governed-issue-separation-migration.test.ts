import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import postgres from "postgres";
import { applyPendingMigrations } from "./client.js";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./test-embedded-postgres.js";

const support = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = support.supported ? describe : describe.skip;
const cleanups: Array<() => Promise<void>> = [];

const rollbackSql = `
BEGIN;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM governed_issue_builder_history) THEN
    RAISE EXCEPTION 'rollback denied: governed issue evidence exists';
  END IF;
END $$;
DELETE FROM drizzle.__drizzle_migrations WHERE created_at = 1787292913812;
DROP TRIGGER governed_issue_authority_guard ON issues;
DROP FUNCTION paperclip_enforce_governed_issue_authority();
DROP TRIGGER governed_issue_ledger_retention_guard ON governed_issue_builder_history;
DROP FUNCTION paperclip_preserve_governed_issue_ledger();
DROP TRIGGER governed_issue_cutover_retention_guard ON governed_issue_control_cutover;
DROP FUNCTION paperclip_preserve_governed_issue_cutover();
ALTER TABLE issue_execution_decisions DROP CONSTRAINT issue_execution_decisions_reservation_id_governed_issue_builder_history_id_fk;
DROP INDEX issue_execution_decisions_reservation_uq;
ALTER TABLE issue_execution_decisions DROP COLUMN reservation_id;
DROP TABLE governed_issue_builder_history;
DROP TABLE governed_issue_control_cutover;
COMMIT;
`;

async function createTempDatabase() {
  const temp = await startEmbeddedPostgresTestDatabase("paperclip-governed-migration-");
  cleanups.push(temp.cleanup);
  return temp.connectionString;
}

afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.();
});

describeEmbeddedPostgres("governed issue separation migration", () => {
  it("applies cleanly, rolls back only while empty, and reapplies", async () => {
    const connectionString = await createTempDatabase();
    const sql = postgres(connectionString, { max: 1, onnotice: () => {} });
    try {
      const installed = await sql.unsafe<Array<{ ledger: string | null; cutover: string | null }>>(
        `SELECT to_regclass('public.governed_issue_builder_history')::text AS ledger,
                to_regclass('public.governed_issue_control_cutover')::text AS cutover`,
      );
      expect(installed[0]).toEqual({
        ledger: "governed_issue_builder_history",
        cutover: "governed_issue_control_cutover",
      });

      await sql.unsafe(rollbackSql);
      const removed = await sql.unsafe<Array<{ ledger: string | null; cutover: string | null }>>(
        `SELECT to_regclass('public.governed_issue_builder_history')::text AS ledger,
                to_regclass('public.governed_issue_control_cutover')::text AS cutover`,
      );
      expect(removed[0]).toEqual({ ledger: null, cutover: null });

      await applyPendingMigrations(connectionString);
      const reapplied = await sql.unsafe<Array<{ ledger: string | null; cutover: string | null; cutover_rows: number }>>(
        `SELECT to_regclass('public.governed_issue_builder_history')::text AS ledger,
                to_regclass('public.governed_issue_control_cutover')::text AS cutover,
                (SELECT count(*)::int FROM governed_issue_control_cutover) AS cutover_rows`,
      );
      expect(reapplied[0]).toEqual({
        ledger: "governed_issue_builder_history",
        cutover: "governed_issue_control_cutover",
        cutover_rows: 1,
      });
    } finally {
      await sql.end();
    }
  }, 30_000);

  it("blocks rollback and cutover mutation after governed evidence exists", async () => {
    const connectionString = await createTempDatabase();
    const sql = postgres(connectionString, { max: 1, onnotice: () => {} });
    const companyId = randomUUID();
    const issueId = randomUUID();
    try {
      await sql.unsafe(
        `INSERT INTO companies (id, name, issue_prefix, status)
         VALUES ($1, 'RatioCore', 'RATA', 'active')`,
        [companyId],
      );
      await sql.unsafe(
        `INSERT INTO issues (id, company_id, title, status, priority, issue_number, identifier)
         VALUES ($1, $2, 'Governed migration evidence', 'in_progress', 'high', 1, 'RATA-1')`,
        [issueId, companyId],
      );
      await sql.unsafe(
        `INSERT INTO governed_issue_builder_history
           (company_id, issue_id, agent_id, kind, state, source, review_required_snapshot, policy_digest, approval_id)
         VALUES ($1, $2, $3, 'builder', 'recorded', 'migration-test', true, $4, $5)`,
        [companyId, issueId, randomUUID(), "a".repeat(64), randomUUID()],
      );

      await expect(sql.unsafe(
        `DO $$ BEGIN
           IF EXISTS (SELECT 1 FROM governed_issue_builder_history) THEN
             RAISE EXCEPTION 'rollback denied: governed issue evidence exists';
           END IF;
         END $$;`,
      )).rejects.toMatchObject({ code: "P0001" });
      await expect(sql.unsafe(
        `UPDATE governed_issue_control_cutover SET activated_at = clock_timestamp() WHERE id = 'singleton'`,
      )).rejects.toMatchObject({ code: "42501" });
    } finally {
      await sql.end();
    }
  }, 30_000);
});
