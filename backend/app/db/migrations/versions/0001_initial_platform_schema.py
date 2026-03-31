"""Initial Contango platform schema."""

from __future__ import annotations

from alembic import op


revision = "0001_initial_platform_schema"
down_revision = None
branch_labels = None
depends_on = None


DDL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE app_modules (
    code text PRIMARY KEY CHECK (
        code IN (
            'headlinewatch',
            'pricewatch',
            'calendarwatch',
            'supplywatch',
            'demandwatch',
            'inventorywatch',
            'weatherwatch'
        )
    ),
    name text NOT NULL UNIQUE
);

CREATE TABLE commodities (
    code text PRIMARY KEY,
    name text NOT NULL,
    sector text NOT NULL CHECK (
        sector IN ('energy', 'metals', 'agriculture', 'macro', 'weather', 'cross_commodity')
    ),
    parent_code text REFERENCES commodities(code),
    is_active boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE geographies (
    code text PRIMARY KEY,
    name text NOT NULL,
    geo_type text NOT NULL CHECK (
        geo_type IN (
            'country',
            'region',
            'market',
            'storage_region',
            'exchange_region',
            'basin',
            'grid_region',
            'global',
            'custom'
        )
    ),
    iso2 char(2),
    iso3 char(3),
    parent_code text REFERENCES geographies(code),
    is_active boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE unit_definitions (
    code text PRIMARY KEY,
    name text NOT NULL,
    dimension text NOT NULL,
    symbol text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE commodity_unit_conventions (
    commodity_code text NOT NULL REFERENCES commodities(code),
    measure_family text NOT NULL CHECK (
        measure_family IN (
            'stock',
            'flow',
            'capacity',
            'utilisation',
            'price',
            'weather',
            'balance',
            'macro',
            'signal',
            'spread',
            'other'
        )
    ),
    canonical_unit_code text NOT NULL REFERENCES unit_definitions(code),
    notes text,
    PRIMARY KEY (commodity_code, measure_family)
);

CREATE TABLE sources (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    slug text NOT NULL UNIQUE,
    name text NOT NULL,
    source_type text NOT NULL CHECK (
        source_type IN ('api', 'rss', 'html', 'pdf', 'csv', 'json', 'ftp', 'manual', 'webhook')
    ),
    legal_status text NOT NULL CHECK (
        legal_status IN (
            'public_domain',
            'cc_by',
            'press_release',
            'exchange_public',
            'public_registered',
            'needs_verification',
            'off_limits'
        )
    ),
    homepage_url text,
    docs_url text,
    default_timezone text,
    attribution_text text,
    rate_limit_notes text,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE release_definitions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES sources(id),
    slug text NOT NULL UNIQUE,
    name text NOT NULL,
    release_kind text NOT NULL CHECK (
        release_kind IN (
            'data_release',
            'report',
            'calendar_event',
            'press_release',
            'weather_product',
            'earnings'
        )
    ),
    module_code text REFERENCES app_modules(code),
    commodity_code text REFERENCES commodities(code),
    geography_code text REFERENCES geographies(code),
    cadence text NOT NULL,
    schedule_timezone text NOT NULL,
    schedule_rule text NOT NULL,
    default_local_time time,
    is_calendar_driven boolean NOT NULL DEFAULT false,
    active boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ingest_artifacts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES sources(id),
    storage_uri text NOT NULL,
    content_type text,
    sha256 text,
    http_status integer,
    size_bytes bigint,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE source_releases (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES sources(id),
    release_definition_id uuid REFERENCES release_definitions(id),
    release_key text NOT NULL,
    release_name text NOT NULL,
    scheduled_at timestamptz,
    released_at timestamptz,
    period_start_at timestamptz,
    period_end_at timestamptz,
    release_timezone text,
    source_url text,
    status text NOT NULL CHECK (
        status IN ('scheduled', 'observed', 'late', 'failed', 'cancelled', 'superseded')
    ),
    primary_artifact_id uuid REFERENCES ingest_artifacts(id),
    notes text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_id, release_key)
);

CREATE TABLE ingest_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_name text NOT NULL,
    source_id uuid REFERENCES sources(id),
    release_definition_id uuid REFERENCES release_definitions(id),
    source_release_id uuid REFERENCES source_releases(id),
    run_mode text NOT NULL CHECK (run_mode IN ('live', 'retry', 'backfill', 'manual')),
    status text NOT NULL CHECK (status IN ('running', 'success', 'partial', 'failed')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    fetched_items integer NOT NULL DEFAULT 0,
    inserted_rows integer NOT NULL DEFAULT 0,
    updated_rows integer NOT NULL DEFAULT 0,
    quarantined_rows integer NOT NULL DEFAULT 0,
    error_text text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE indicators (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code text NOT NULL UNIQUE,
    name text NOT NULL,
    description text,
    measure_family text NOT NULL CHECK (
        measure_family IN (
            'stock',
            'flow',
            'capacity',
            'utilisation',
            'price',
            'weather',
            'balance',
            'macro',
            'signal',
            'spread',
            'other'
        )
    ),
    frequency text NOT NULL CHECK (
        frequency IN (
            'intraday',
            'hourly',
            'daily',
            'weekly',
            'monthly',
            'quarterly',
            'annual',
            'marketing_year',
            'irregular',
            'event'
        )
    ),
    commodity_code text REFERENCES commodities(code),
    geography_code text REFERENCES geographies(code),
    source_id uuid REFERENCES sources(id),
    source_series_key text,
    native_unit_code text REFERENCES unit_definitions(code),
    canonical_unit_code text REFERENCES unit_definitions(code),
    default_observation_kind text NOT NULL CHECK (
        default_observation_kind IN ('actual', 'estimate', 'forecast', 'proxy', 'derived', 'anomaly', 'signal')
    ),
    publication_lag interval,
    seasonal_profile text,
    is_seasonal boolean NOT NULL DEFAULT false,
    is_derived boolean NOT NULL DEFAULT false,
    formula text,
    visibility_tier text NOT NULL DEFAULT 'public' CHECK (
        visibility_tier IN ('public', 'free', 'premium', 'internal')
    ),
    active boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE indicator_modules (
    indicator_id uuid NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,
    module_code text NOT NULL REFERENCES app_modules(code),
    is_primary boolean NOT NULL DEFAULT false,
    PRIMARY KEY (indicator_id, module_code)
);

CREATE UNIQUE INDEX uq_indicator_primary_module
    ON indicator_modules (indicator_id)
    WHERE is_primary;

CREATE TABLE indicator_dependencies (
    derived_indicator_id uuid NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,
    source_indicator_id uuid NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,
    dependency_role text NOT NULL,
    transform_notes text,
    PRIMARY KEY (derived_indicator_id, source_indicator_id),
    CHECK (derived_indicator_id <> source_indicator_id)
);

CREATE TABLE observations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    indicator_id uuid NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,
    period_start_at timestamptz NOT NULL,
    period_end_at timestamptz NOT NULL,
    release_id uuid REFERENCES source_releases(id),
    release_date timestamptz,
    vintage_at timestamptz NOT NULL,
    observation_kind text NOT NULL CHECK (
        observation_kind IN ('actual', 'estimate', 'forecast', 'proxy', 'derived', 'anomaly', 'signal')
    ),
    value_native numeric(24,8) NOT NULL,
    unit_native_code text NOT NULL REFERENCES unit_definitions(code),
    value_canonical numeric(24,8) NOT NULL,
    unit_canonical_code text NOT NULL REFERENCES unit_definitions(code),
    currency_code text,
    is_latest boolean NOT NULL DEFAULT true,
    revision_sequence integer NOT NULL DEFAULT 1 CHECK (revision_sequence >= 1),
    supersedes_observation_id uuid REFERENCES observations(id),
    qa_status text NOT NULL DEFAULT 'passed' CHECK (
        qa_status IN ('passed', 'quarantined', 'manual_review', 'rejected')
    ),
    source_item_ref text,
    provenance_note text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (period_start_at <= period_end_at)
);

CREATE UNIQUE INDEX uq_observations_vintage
    ON observations (indicator_id, period_start_at, period_end_at, observation_kind, vintage_at);

CREATE UNIQUE INDEX uq_observations_latest
    ON observations (indicator_id, period_start_at, period_end_at, observation_kind)
    WHERE is_latest;

CREATE TABLE seasonal_ranges (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    indicator_id uuid NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,
    profile_name text NOT NULL,
    period_type text NOT NULL CHECK (
        period_type IN ('week_of_year', 'month_of_year', 'day_of_year', 'marketing_year_month', 'gas_year_week')
    ),
    period_index integer NOT NULL,
    sample_size integer NOT NULL DEFAULT 0,
    range_start_year integer,
    range_end_year integer,
    p10 numeric(24,8),
    p25 numeric(24,8),
    p50 numeric(24,8),
    p75 numeric(24,8),
    p90 numeric(24,8),
    mean_value numeric(24,8),
    stddev_value numeric(24,8),
    computed_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (indicator_id, profile_name, period_type, period_index)
);

CREATE TABLE module_snapshot_cache (
    module_code text NOT NULL REFERENCES app_modules(code),
    snapshot_key text NOT NULL,
    as_of timestamptz NOT NULL,
    payload jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    generated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (module_code, snapshot_key)
);

CREATE TABLE app_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text NOT NULL UNIQUE,
    event_type text NOT NULL,
    producer_module_code text NOT NULL REFERENCES app_modules(code),
    aggregate_type text NOT NULL,
    aggregate_id uuid,
    commodity_code text REFERENCES commodities(code),
    geography_code text REFERENCES geographies(code),
    indicator_id uuid REFERENCES indicators(id),
    observation_id uuid REFERENCES observations(id),
    source_release_id uuid REFERENCES source_releases(id),
    status text NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'processing', 'processed', 'failed', 'discarded')
    ),
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    available_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    error_text text
);

CREATE TABLE headlines (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dedupe_key text NOT NULL UNIQUE,
    source_id uuid REFERENCES sources(id),
    source_release_id uuid REFERENCES source_releases(id),
    trigger_event_id uuid REFERENCES app_events(id),
    title text NOT NULL,
    summary text,
    body text,
    url text NOT NULL,
    source_label text NOT NULL,
    published_at timestamptz NOT NULL,
    headline_type text NOT NULL CHECK (
        headline_type IN ('external', 'auto_trigger', 'editorial', 'system')
    ),
    sentiment text CHECK (sentiment IN ('bullish', 'bearish', 'neutral') OR sentiment IS NULL),
    module_origin text REFERENCES app_modules(code),
    commodity_code text REFERENCES commodities(code),
    geography_code text REFERENCES geographies(code),
    visibility_tier text NOT NULL DEFAULT 'public' CHECK (
        visibility_tier IN ('public', 'free', 'premium', 'internal')
    ),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE headline_indicator_links (
    headline_id uuid NOT NULL REFERENCES headlines(id) ON DELETE CASCADE,
    indicator_id uuid NOT NULL REFERENCES indicators(id) ON DELETE CASCADE,
    observation_id uuid REFERENCES observations(id),
    relation_type text NOT NULL,
    PRIMARY KEY (headline_id, indicator_id, relation_type)
);

CREATE TABLE calendar_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dedupe_key text NOT NULL UNIQUE,
    release_definition_id uuid REFERENCES release_definitions(id),
    source_release_id uuid REFERENCES source_releases(id),
    title text NOT NULL,
    event_type text NOT NULL CHECK (
        event_type IN ('data_release', 'report', 'earnings', 'weather_window', 'holiday', 'manual')
    ),
    module_code text REFERENCES app_modules(code),
    commodity_code text REFERENCES commodities(code),
    geography_code text REFERENCES geographies(code),
    starts_at timestamptz NOT NULL,
    ends_at timestamptz,
    scheduled_timezone text,
    status text NOT NULL CHECK (
        status IN ('scheduled', 'confirmed', 'occurred', 'cancelled', 'delayed', 'pending_review')
    ),
    source_url text,
    source_label text,
    redistribution_ok boolean NOT NULL DEFAULT false,
    notes text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE calendar_event_changes (
    id bigserial PRIMARY KEY,
    calendar_event_id uuid NOT NULL REFERENCES calendar_events(id) ON DELETE CASCADE,
    field_name text NOT NULL,
    old_value text,
    new_value text,
    detected_at timestamptz NOT NULL DEFAULT now(),
    requires_review boolean NOT NULL DEFAULT false
);

CREATE TABLE calendar_review_items (
    id bigserial PRIMARY KEY,
    calendar_event_id uuid NOT NULL REFERENCES calendar_events(id) ON DELETE CASCADE,
    reason text NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'approved', 'rejected')
    ),
    resolution_notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz
);

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email text NOT NULL,
    password_hash text NOT NULL,
    full_name text,
    plan_code text NOT NULL DEFAULT 'free' CHECK (
        plan_code IN ('free', 'pro', 'admin')
    ),
    account_status text NOT NULL DEFAULT 'active' CHECK (
        account_status IN ('active', 'past_due', 'cancelled', 'disabled')
    ),
    timezone text NOT NULL DEFAULT 'UTC',
    email_verified_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    last_login_at timestamptz
);

CREATE UNIQUE INDEX uq_users_email_lower ON users ((lower(email)));

CREATE TABLE user_sessions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_token_hash text NOT NULL UNIQUE,
    csrf_token_hash text NOT NULL UNIQUE,
    ip_address inet,
    user_agent text,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz
);

CREATE TABLE subscriptions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider text NOT NULL CHECK (provider IN ('stripe')),
    plan_code text NOT NULL CHECK (plan_code IN ('free', 'pro')),
    stripe_customer_id text UNIQUE,
    stripe_subscription_id text UNIQUE,
    checkout_session_id text,
    status text NOT NULL CHECK (
        status IN ('trialing', 'active', 'past_due', 'cancelled', 'incomplete', 'unpaid')
    ),
    current_period_start timestamptz,
    current_period_end timestamptz,
    cancel_at_period_end boolean NOT NULL DEFAULT false,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE billing_webhook_events (
    id bigserial PRIMARY KEY,
    provider text NOT NULL CHECK (provider IN ('stripe')),
    provider_event_id text NOT NULL UNIQUE,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('received', 'processed', 'failed')),
    received_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    error_text text
);

CREATE INDEX idx_indicators_filter
    ON indicators (commodity_code, geography_code, frequency, active);

CREATE INDEX idx_indicator_modules_module
    ON indicator_modules (module_code, indicator_id);

CREATE INDEX idx_observations_indicator_period_latest
    ON observations (indicator_id, period_end_at DESC)
    WHERE is_latest;

CREATE INDEX idx_observations_indicator_vintage
    ON observations (indicator_id, period_end_at DESC, vintage_at DESC);

CREATE INDEX idx_observations_release
    ON observations (release_id);

CREATE INDEX idx_source_releases_schedule
    ON source_releases (scheduled_at DESC);

CREATE INDEX idx_source_releases_observed
    ON source_releases (released_at DESC);

CREATE INDEX idx_seasonal_ranges_lookup
    ON seasonal_ranges (indicator_id, profile_name, period_type, period_index);

CREATE INDEX idx_headlines_published
    ON headlines (published_at DESC);

CREATE INDEX idx_headlines_commodity_published
    ON headlines (commodity_code, published_at DESC);

CREATE INDEX idx_calendar_events_start
    ON calendar_events (starts_at ASC);

CREATE INDEX idx_calendar_events_module_start
    ON calendar_events (module_code, starts_at ASC);

CREATE INDEX idx_app_events_pending
    ON app_events (status, available_at ASC)
    WHERE status IN ('pending', 'failed');

CREATE INDEX idx_ingest_runs_recent
    ON ingest_runs (started_at DESC);
"""


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS billing_webhook_events;
        DROP TABLE IF EXISTS subscriptions;
        DROP TABLE IF EXISTS user_sessions;
        DROP TABLE IF EXISTS users;
        DROP TABLE IF EXISTS calendar_review_items;
        DROP TABLE IF EXISTS calendar_event_changes;
        DROP TABLE IF EXISTS calendar_events;
        DROP TABLE IF EXISTS headline_indicator_links;
        DROP TABLE IF EXISTS headlines;
        DROP TABLE IF EXISTS app_events;
        DROP TABLE IF EXISTS module_snapshot_cache;
        DROP TABLE IF EXISTS seasonal_ranges;
        DROP TABLE IF EXISTS observations;
        DROP TABLE IF EXISTS indicator_dependencies;
        DROP TABLE IF EXISTS indicator_modules;
        DROP TABLE IF EXISTS indicators;
        DROP TABLE IF EXISTS ingest_runs;
        DROP TABLE IF EXISTS source_releases;
        DROP TABLE IF EXISTS ingest_artifacts;
        DROP TABLE IF EXISTS release_definitions;
        DROP TABLE IF EXISTS sources;
        DROP TABLE IF EXISTS commodity_unit_conventions;
        DROP TABLE IF EXISTS unit_definitions;
        DROP TABLE IF EXISTS geographies;
        DROP TABLE IF EXISTS commodities;
        DROP TABLE IF EXISTS app_modules;
        """
    )
