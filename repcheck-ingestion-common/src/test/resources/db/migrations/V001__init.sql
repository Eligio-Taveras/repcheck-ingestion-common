-- Test schema for ingestion-common DB-backed specs.
-- Applied automatically by SqlMigrationRunner when DockerPostgresSpec starts the AlloyDB Omni container.
--
-- This is NOT the production schema — it is a minimal subset shaped to exercise the generic
-- DoobieEntityRepository and the WorkflowStateUpdater against real Postgres semantics
-- (ON CONFLICT, transactional updates, primary keys).
--
-- Two tables:
--   1. members            — representative entity for EntityRepositorySpec / PlaceholderCreatorSpec.
--                           natural_key is the primary key (rather than member_id) so a placeholder
--                           with member_id = 0 can be inserted multiple times under different keys
--                           without colliding on the surrogate column.
--   2. workflow_run_steps — execution-state table for WorkflowStateUpdaterSpec, matching the columns
--                           the production code reads/writes.

CREATE TABLE IF NOT EXISTS members (
    member_id           BIGINT       NOT NULL DEFAULT 0,
    natural_key         VARCHAR(255) NOT NULL,
    first_name          TEXT,
    last_name           TEXT,
    direct_order_name   TEXT,
    inverted_order_name TEXT,
    honorific_name      TEXT,
    birth_year          TEXT,
    current_party       TEXT,
    state               TEXT,
    district            INTEGER,
    image_url           TEXT,
    image_attribution   TEXT,
    official_url        TEXT,
    update_date         TEXT,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ,
    PRIMARY KEY (natural_key)
);

CREATE TABLE IF NOT EXISTS workflow_run_steps (
    workflow_run_id  UUID         NOT NULL,
    step_name        VARCHAR(255) NOT NULL,
    status           VARCHAR(32)  NOT NULL,
    pipeline_run_id  UUID,
    retry_count      INTEGER      NOT NULL DEFAULT 0,
    max_retries      INTEGER      NOT NULL DEFAULT 3,
    original_message TEXT,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    error_message    TEXT,
    created_at       TIMESTAMPTZ  NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (workflow_run_id, step_name)
);
