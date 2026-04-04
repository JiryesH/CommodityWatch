"""Add DemandWatch foundation tables and shared observation view."""

from __future__ import annotations

from alembic import op


revision = "0003_demandwatch_foundation"
down_revision = "0002_quarantined_observations"
branch_labels = None
depends_on = None


DDL = """
CREATE TABLE demand_verticals (
    code text PRIMARY KEY,
    name text NOT NULL UNIQUE,
    commodity_code text NOT NULL REFERENCES commodities(code),
    sector text NOT NULL CHECK (
        sector IN ('energy', 'metals', 'agriculture', 'macro', 'weather', 'cross_commodity')
    ),
    nav_label text,
    short_label text,
    description text,
    display_order integer NOT NULL DEFAULT 0,
    active boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_demand_verticals_active_order
    ON demand_verticals (active, display_order);

CREATE TABLE demand_series (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    indicator_id uuid NOT NULL UNIQUE REFERENCES indicators(id) ON DELETE CASCADE,
    vertical_code text NOT NULL REFERENCES demand_verticals(code),
    release_definition_id uuid REFERENCES release_definitions(id),
    indicator_tier text NOT NULL CHECK (
        indicator_tier IN (
            't1_direct',
            't2_throughput',
            't3_trade',
            't4_end_use',
            't5_leading',
            't6_macro',
            't7_weather'
        )
    ),
    coverage_status text NOT NULL CHECK (
        coverage_status IN ('live', 'planned', 'needs_verification', 'blocked')
    ),
    display_order integer NOT NULL DEFAULT 0,
    notes text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_demand_series_vertical_status_order
    ON demand_series (vertical_code, coverage_status, display_order);

CREATE INDEX idx_demand_series_release_status
    ON demand_series (release_definition_id, coverage_status);

CREATE VIEW demand_observations AS
SELECT
    ds.id AS demand_series_id,
    ds.vertical_code,
    ds.indicator_tier,
    ds.coverage_status,
    ds.display_order,
    ds.notes AS coverage_note,
    ds.metadata AS demand_metadata,
    i.id AS indicator_id,
    i.code AS series_code,
    i.name AS series_name,
    i.description AS series_description,
    i.measure_family,
    i.frequency,
    i.commodity_code,
    i.geography_code,
    i.source_series_key,
    i.metadata AS indicator_metadata,
    o.id AS observation_id,
    o.period_start_at,
    o.period_end_at,
    o.release_id AS source_release_id,
    o.release_date,
    o.vintage_at,
    o.observation_kind,
    o.value_native,
    o.unit_native_code,
    o.value_canonical,
    o.unit_canonical_code,
    o.revision_sequence,
    o.is_latest,
    o.qa_status,
    s.id AS source_id,
    s.slug AS source_slug,
    s.name AS source_name,
    s.source_type,
    s.legal_status AS source_legal_status,
    COALESCE(sr.source_url, rd.metadata ->> 'landing_url', i.metadata ->> 'source_url', s.homepage_url) AS source_url,
    jsonb_build_object(
        'source_name', s.name,
        'source_type', s.source_type,
        'legal_status', s.legal_status,
        'attribution_text', s.attribution_text,
        'docs_url', s.docs_url,
        'release_slug', rd.slug,
        'release_name', rd.name,
        'release_kind', rd.release_kind,
        'release_metadata', COALESCE(rd.metadata, '{}'::jsonb),
        'series_metadata', ds.metadata,
        'indicator_metadata', i.metadata
    ) AS source_metadata
FROM demand_series ds
JOIN indicators i
    ON i.id = ds.indicator_id
LEFT JOIN observations o
    ON o.indicator_id = i.id
LEFT JOIN source_releases sr
    ON sr.id = o.release_id
LEFT JOIN release_definitions rd
    ON rd.id = COALESCE(ds.release_definition_id, sr.release_definition_id)
LEFT JOIN sources s
    ON s.id = i.source_id;
"""


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    op.execute(
        """
        DROP VIEW IF EXISTS demand_observations;
        DROP INDEX IF EXISTS idx_demand_series_release_status;
        DROP INDEX IF EXISTS idx_demand_series_vertical_status_order;
        DROP TABLE IF EXISTS demand_series;
        DROP INDEX IF EXISTS idx_demand_verticals_active_order;
        DROP TABLE IF EXISTS demand_verticals;
        """
    )
