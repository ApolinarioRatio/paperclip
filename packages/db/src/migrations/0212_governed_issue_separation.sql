-- Prospective-only: the cutover row is created at migration application time.
-- Issues created before that instant are ineligible because historical assignment
-- churn cannot prove their complete ever-builder set.
--
-- Guarded pre-activation rollback (aborts before evidence loss if any ledger row exists):
--   BEGIN;
--   DO $$ BEGIN
--     IF EXISTS (SELECT 1 FROM governed_issue_builder_history) THEN
--       RAISE EXCEPTION 'rollback denied: governed issue evidence exists';
--     END IF;
--   END $$;
--   DELETE FROM drizzle.__drizzle_migrations WHERE created_at = 1787292913812;
--   DROP TRIGGER governed_issue_authority_guard ON issues;
--   DROP FUNCTION paperclip_enforce_governed_issue_authority();
--   DROP TRIGGER governed_issue_ledger_retention_guard ON governed_issue_builder_history;
--   DROP FUNCTION paperclip_preserve_governed_issue_ledger();
--   DROP TRIGGER governed_issue_cutover_retention_guard ON governed_issue_control_cutover;
--   DROP FUNCTION paperclip_preserve_governed_issue_cutover();
--   ALTER TABLE issue_execution_decisions DROP CONSTRAINT issue_execution_decisions_reservation_id_governed_issue_builder_history_id_fk;
--   DROP INDEX issue_execution_decisions_reservation_uq;
--   ALTER TABLE issue_execution_decisions DROP COLUMN reservation_id;
--   DROP TABLE governed_issue_builder_history;
--   DROP TABLE governed_issue_control_cutover;
--   COMMIT;
CREATE TABLE "governed_issue_builder_history" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"company_id" uuid NOT NULL,
	"issue_id" uuid NOT NULL,
	"agent_id" uuid,
	"user_id" text,
	"kind" text DEFAULT 'builder' NOT NULL,
	"state" text DEFAULT 'recorded' NOT NULL,
	"stage_id" uuid,
	"source" text NOT NULL,
	"source_run_id" uuid,
	"approval_id" uuid,
	"review_required_snapshot" boolean DEFAULT true NOT NULL,
	"policy_digest" text NOT NULL,
	"reservation_expires_at" timestamp with time zone,
	"resolved_at" timestamp with time zone,
	"resolution" text,
	"first_assigned_at" timestamp with time zone DEFAULT now() NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "governed_issue_builder_history_policy_digest_check" CHECK ("governed_issue_builder_history"."policy_digest" ~ '^[0-9a-f]{64}$'),
	CONSTRAINT "governed_issue_builder_history_review_required_check" CHECK ("governed_issue_builder_history"."review_required_snapshot" = true),
	CONSTRAINT "governed_issue_builder_history_kind_state_check" CHECK (("governed_issue_builder_history"."kind" = 'builder' and "governed_issue_builder_history"."state" = 'recorded' and "governed_issue_builder_history"."stage_id" is null and "governed_issue_builder_history"."agent_id" is not null and "governed_issue_builder_history"."user_id" is null)
        or ("governed_issue_builder_history"."kind" = 'verification_reservation' and "governed_issue_builder_history"."state" in ('active', 'completed', 'released', 'expired') and "governed_issue_builder_history"."stage_id" is not null
          and (("governed_issue_builder_history"."agent_id" is not null and "governed_issue_builder_history"."user_id" is null) or ("governed_issue_builder_history"."agent_id" is null and "governed_issue_builder_history"."user_id" is not null)))),
	CONSTRAINT "governed_issue_builder_history_reservation_expiry_check" CHECK (("governed_issue_builder_history"."kind" = 'builder' and "governed_issue_builder_history"."reservation_expires_at" is null)
        or ("governed_issue_builder_history"."kind" = 'verification_reservation' and "governed_issue_builder_history"."reservation_expires_at" is not null)),
	CONSTRAINT "governed_issue_builder_history_resolution_check" CHECK (("governed_issue_builder_history"."kind" = 'builder' and "governed_issue_builder_history"."resolved_at" is null and "governed_issue_builder_history"."resolution" is null)
        or ("governed_issue_builder_history"."kind" = 'verification_reservation' and (
          ("governed_issue_builder_history"."state" = 'active' and "governed_issue_builder_history"."resolved_at" is null and "governed_issue_builder_history"."resolution" is null)
          or ("governed_issue_builder_history"."state" = 'completed' and "governed_issue_builder_history"."resolved_at" is not null and "governed_issue_builder_history"."resolution" = 'approved')
          or ("governed_issue_builder_history"."state" = 'expired' and "governed_issue_builder_history"."resolved_at" is not null and "governed_issue_builder_history"."resolution" = 'expired')
          or ("governed_issue_builder_history"."state" = 'released' and "governed_issue_builder_history"."resolved_at" is not null and "governed_issue_builder_history"."resolution" in ('changes_requested', 'stage_replaced'))
        )))
);
--> statement-breakpoint
CREATE TABLE "governed_issue_control_cutover" (
	"id" text PRIMARY KEY NOT NULL,
	"activated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "governed_issue_control_cutover_singleton_check" CHECK ("governed_issue_control_cutover"."id" = 'singleton')
);
--> statement-breakpoint
INSERT INTO "governed_issue_control_cutover" ("id", "activated_at") VALUES ('singleton', clock_timestamp());--> statement-breakpoint
ALTER TABLE "issue_execution_decisions" ADD COLUMN "reservation_id" uuid;--> statement-breakpoint
ALTER TABLE "governed_issue_builder_history" ADD CONSTRAINT "governed_issue_builder_history_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "governed_issue_builder_history" ADD CONSTRAINT "governed_issue_builder_history_issue_id_issues_id_fk" FOREIGN KEY ("issue_id") REFERENCES "public"."issues"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "governed_issue_builder_history_company_issue_idx" ON "governed_issue_builder_history" USING btree ("company_id","issue_id");--> statement-breakpoint
CREATE UNIQUE INDEX "governed_issue_builder_history_issue_agent_uq" ON "governed_issue_builder_history" USING btree ("issue_id","agent_id") WHERE "governed_issue_builder_history"."kind" = 'builder';--> statement-breakpoint
CREATE UNIQUE INDEX "governed_issue_builder_history_active_reservation_uq" ON "governed_issue_builder_history" USING btree ("issue_id") WHERE "governed_issue_builder_history"."kind" = 'verification_reservation' and "governed_issue_builder_history"."state" = 'active';--> statement-breakpoint
CREATE INDEX "governed_issue_builder_history_issue_kind_state_idx" ON "governed_issue_builder_history" USING btree ("company_id","issue_id","kind","state");--> statement-breakpoint
ALTER TABLE "issue_execution_decisions" ADD CONSTRAINT "issue_execution_decisions_reservation_id_governed_issue_builder_history_id_fk" FOREIGN KEY ("reservation_id") REFERENCES "public"."governed_issue_builder_history"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "issue_execution_decisions_reservation_uq" ON "issue_execution_decisions" USING btree ("reservation_id");--> statement-breakpoint
CREATE FUNCTION "paperclip_preserve_governed_issue_cutover"() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'governed issue control cutover is immutable'
    USING ERRCODE = '42501';
END;
$$;--> statement-breakpoint
CREATE TRIGGER "governed_issue_cutover_retention_guard"
BEFORE UPDATE OR DELETE ON "governed_issue_control_cutover"
FOR EACH ROW EXECUTE FUNCTION "paperclip_preserve_governed_issue_cutover"();--> statement-breakpoint
CREATE FUNCTION "paperclip_preserve_governed_issue_ledger"() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'governed issue ledger rows are append-retained' USING ERRCODE = '42501';
  END IF;
  IF OLD.kind = 'builder' THEN
    RAISE EXCEPTION 'governed builder history is immutable' USING ERRCODE = '42501';
  END IF;
  IF OLD.kind IS DISTINCT FROM NEW.kind
    OR OLD.company_id IS DISTINCT FROM NEW.company_id
    OR OLD.issue_id IS DISTINCT FROM NEW.issue_id
    OR OLD.agent_id IS DISTINCT FROM NEW.agent_id
    OR OLD.user_id IS DISTINCT FROM NEW.user_id
    OR OLD.stage_id IS DISTINCT FROM NEW.stage_id
    OR OLD.source IS DISTINCT FROM NEW.source
    OR OLD.source_run_id IS DISTINCT FROM NEW.source_run_id
    OR OLD.approval_id IS DISTINCT FROM NEW.approval_id
    OR OLD.review_required_snapshot IS DISTINCT FROM NEW.review_required_snapshot
    OR OLD.policy_digest IS DISTINCT FROM NEW.policy_digest
    OR OLD.reservation_expires_at IS DISTINCT FROM NEW.reservation_expires_at
    OR OLD.first_assigned_at IS DISTINCT FROM NEW.first_assigned_at
    OR OLD.created_at IS DISTINCT FROM NEW.created_at
    OR OLD.state <> 'active'
    OR NEW.state NOT IN ('completed', 'released', 'expired') THEN
    RAISE EXCEPTION 'governed verifier reservation identity and terminal history are immutable' USING ERRCODE = '42501';
  END IF;
  RETURN NEW;
END;
$$;--> statement-breakpoint
CREATE TRIGGER "governed_issue_ledger_retention_guard"
BEFORE UPDATE OR DELETE ON "governed_issue_builder_history"
FOR EACH ROW EXECUTE FUNCTION "paperclip_preserve_governed_issue_ledger"();--> statement-breakpoint
-- Accidental-bypass guard only. A same-role PostgreSQL session can set a custom
-- GUC; canonical authority is the locked server path plus durable constraints.
CREATE FUNCTION "paperclip_enforce_governed_issue_authority"() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM governed_issue_builder_history history
    WHERE history.issue_id = OLD.id AND history.company_id = OLD.company_id
      AND history.approval_id IS NOT NULL
  ) AND (
    NEW.assignee_agent_id IS DISTINCT FROM OLD.assignee_agent_id
    OR NEW.assignee_user_id IS DISTINCT FROM OLD.assignee_user_id
    OR NEW.status IS DISTINCT FROM OLD.status
    OR NEW.execution_policy IS DISTINCT FROM OLD.execution_policy
    OR NEW.execution_state IS DISTINCT FROM OLD.execution_state
  ) AND current_setting('paperclip.governed_issue_authority', true) IS DISTINCT FROM OLD.id::text THEN
    RAISE EXCEPTION 'governed issue mutation must pass through assignment/verification authority' USING ERRCODE = '42501';
  END IF;
  RETURN NEW;
END;
$$;--> statement-breakpoint
CREATE TRIGGER "governed_issue_authority_guard"
BEFORE UPDATE OF "assignee_agent_id", "assignee_user_id", "status", "execution_policy", "execution_state"
ON "issues" FOR EACH ROW EXECUTE FUNCTION "paperclip_enforce_governed_issue_authority"();