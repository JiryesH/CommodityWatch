"""Add quarantined observations table for ingest QA."""

from __future__ import annotations

from alembic import op


revision = "0002_quarantined_observations"
down_revision = "0001_initial_platform_schema"
branch_labels = None
depends_on = None


DDL = """
CREATE TABLE IF NOT EXISTS quarantined_observations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingest_run_id uuid NOT NULL REFERENCES ingest_runs(id) ON DELETE CASCADE,
    indicator_id uuid REFERENCES indicators(id) ON DELETE CASCADE,
    source_id uuid REFERENCES sources(id),
    period_end_at timestamptz,
    value_native double precision,
    unit_native_code text REFERENCES unit_definitions(code),
    lower_bound double precision,
    upper_bound double precision,
    reason text NOT NULL,
    artifact_uri text,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quarantined_observations_ingest_run
    ON quarantined_observations (ingest_run_id, created_at DESC);
"""


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS idx_quarantined_observations_ingest_run;
        DROP TABLE IF EXISTS quarantined_observations;
        """
    )
