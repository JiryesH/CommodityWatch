import { z } from "zod";

export const apiErrorSchema = z
  .object({
    detail: z.union([z.string(), z.array(z.any())]).optional(),
    error: z.string().optional(),
    message: z.string().optional(),
  })
  .passthrough();

export const indicatorListItemSchema = z
  .object({
    id: z.string(),
    code: z.string(),
    name: z.string(),
    description: z.string().nullable().optional(),
    modules: z.array(z.string()),
    commodity_code: z.string().nullable(),
    geography_code: z.string().nullable(),
    measure_family: z.string(),
    frequency: z.string(),
    native_unit: z.string().nullable(),
    canonical_unit: z.string().nullable(),
    is_seasonal: z.boolean(),
    is_derived: z.boolean(),
    visibility_tier: z.string(),
    latest_release_at: z.string().datetime().nullable(),
  })
  .passthrough();

export const indicatorListResponseSchema = z
  .object({
    items: z.array(indicatorListItemSchema),
    next_cursor: z.string().nullable(),
  })
  .passthrough();

export const indicatorLatestResponseSchema = z
  .object({
    indicator: z
      .object({
        id: z.string(),
        code: z.string(),
      })
      .passthrough(),
    latest: z
      .object({
        period_end_at: z.string().datetime(),
        release_date: z.string().datetime().nullable(),
        value: z.number(),
        unit: z.string(),
        change_from_prior_abs: z.number().nullable(),
        change_from_prior_pct: z.number().nullable(),
        deviation_from_seasonal_abs: z.number().nullable(),
        deviation_from_seasonal_zscore: z.number().nullable(),
        revision_sequence: z.number(),
      })
      .passthrough(),
  })
  .passthrough();

export const seriesPointSchema = z
  .object({
    period_start_at: z.string().datetime(),
    period_end_at: z.string().datetime(),
    release_date: z.string().datetime().nullable(),
    vintage_at: z.string().datetime(),
    value: z.number(),
    unit: z.string(),
    observation_kind: z.string(),
    revision_sequence: z.number(),
  })
  .passthrough();

export const seasonalRangePointSchema = z
  .object({
    period_index: z.number(),
    p10: z.number().nullable().optional(),
    p25: z.number().nullable().optional(),
    p50: z.number().nullable().optional(),
    p75: z.number().nullable().optional(),
    p90: z.number().nullable().optional(),
    mean: z.number().nullable().optional(),
    stddev: z.number().nullable().optional(),
  })
  .passthrough();

export const indicatorDataResponseSchema = z
  .object({
    indicator: z
      .object({
        id: z.string(),
        code: z.string(),
        name: z.string(),
        description: z.string().nullable().optional(),
        modules: z.array(z.string()),
        commodity_code: z.string().nullable(),
        geography_code: z.string().nullable(),
        frequency: z.string(),
        measure_family: z.string(),
        unit: z.string().nullable(),
        period_type: z.string().nullable().optional(),
        marketing_year_start_month: z.number().nullable().optional(),
        is_seasonal: z.boolean().optional(),
      })
      .passthrough(),
    series: z.array(seriesPointSchema),
    seasonal_range: z.array(seasonalRangePointSchema),
    metadata: z
      .object({
        latest_release_id: z.string().nullable().optional(),
        latest_release_at: z.string().datetime().nullable().optional(),
        source_url: z.string().url().nullable().optional(),
        source_label: z.string().nullable().optional(),
      })
      .passthrough(),
  })
  .passthrough();

export const snapshotCardSchema = z
  .object({
    indicator_id: z.string(),
    code: z.string(),
    name: z.string(),
    description: z.string().nullable().optional(),
    commodity_code: z.string().nullable(),
    geography_code: z.string().nullable(),
    frequency: z.string().nullable().optional(),
    source_url: z.string().url().nullable().optional(),
    period_type: z.string().nullable().optional(),
    marketing_year_start_month: z.number().nullable().optional(),
    is_seasonal: z.boolean().optional(),
    latest_value: z.number(),
    unit: z.string(),
    change_abs: z.number().nullable(),
    deviation_abs: z.number().nullable(),
    signal: z.enum(["tightening", "loosening", "expanding", "contracting", "neutral"]).catch("neutral"),
    sparkline: z.array(z.number()).default([]),
    last_updated_at: z.string().datetime(),
    stale: z.boolean().default(false),
    source_label: z.string().optional(),
  })
  .passthrough();

export const snapshotResponseSchema = z
  .object({
    module: z.string(),
    generated_at: z.string().datetime(),
    expires_at: z.string().datetime(),
    cards: z.array(snapshotCardSchema),
  })
  .passthrough();

export type RawIndicatorListResponse = z.infer<typeof indicatorListResponseSchema>;
export type RawIndicatorDataResponse = z.infer<typeof indicatorDataResponseSchema>;
export type RawIndicatorLatestResponse = z.infer<typeof indicatorLatestResponseSchema>;
export type RawSnapshotResponse = z.infer<typeof snapshotResponseSchema>;
