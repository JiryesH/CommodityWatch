"use client";

import { keepPreviousData, useQuery } from "@tanstack/react-query";

import {
  fetchIndicatorData,
  fetchIndicatorLatest,
  fetchIndicators,
  fetchInventorySnapshot,
  type IndicatorDataParams,
  type IndicatorFilters,
  type InventorySnapshotParams,
} from "@/lib/api/inventory";

export function useInventorySnapshot(params: InventorySnapshotParams = {}) {
  return useQuery({
    queryKey: ["inventory-snapshot", params],
    queryFn: () => fetchInventorySnapshot(params),
    staleTime: 2 * 60 * 1000,
    placeholderData: keepPreviousData,
  });
}

export function useIndicatorData(indicatorId: string, params: IndicatorDataParams = {}) {
  return useQuery({
    queryKey: ["indicator-data", indicatorId, params],
    queryFn: () => fetchIndicatorData(indicatorId, params),
    enabled: Boolean(indicatorId),
    staleTime: 5 * 60 * 1000,
    placeholderData: keepPreviousData,
  });
}

export function useIndicatorLatest(indicatorId: string) {
  return useQuery({
    queryKey: ["indicator-latest", indicatorId],
    queryFn: () => fetchIndicatorLatest(indicatorId),
    enabled: Boolean(indicatorId),
    staleTime: 5 * 60 * 1000,
  });
}

export function useIndicators(filters: IndicatorFilters = {}) {
  return useQuery({
    queryKey: ["indicators", filters],
    queryFn: () => fetchIndicators(filters),
    staleTime: 30 * 60 * 1000,
    placeholderData: keepPreviousData,
  });
}
