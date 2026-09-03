// Tiny typed API client matching curation-review/API.md.
// All calls are relative to /api (Vite proxies to http://127.0.0.1:8787).

import type {
  Boundary,
  EntryDetail,
  EntryReaches,
  Entry,
  GeoJSON,
  ItemSearchResult,
  QueueRow,
  RebuildStatus,
  RegionSummary,
  SaveResult,
  SpeciesOption,
  SplitRef,
  Status,
  RuleResolved,
} from "./types";

async function getJSON<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText} for ${url}`);
  }
  return (await res.json()) as T;
}

export interface ValidationError extends Error {
  errors: string[];
  status: number;
}

// PUT/confirm write path. Returns {ok, errors} on 200; throws a ValidationError
// carrying the returned errors on 422 (FastAPI puts them under `detail`).
async function writeEntry(
  url: string,
  body: Record<string, unknown>,
): Promise<SaveResult> {
  const res = await fetch(url, {
    method: url.endsWith("/confirm") ? "POST" : "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 422) {
    const payload = await res.json().catch(() => ({}));
    const detail = (payload && (payload.detail ?? payload.errors)) as unknown;
    const errors = Array.isArray(detail)
      ? detail.map((d) => (typeof d === "string" ? d : JSON.stringify(d)))
      : [String(detail ?? "validation failed")];
    const err = new Error("validation failed") as ValidationError;
    err.errors = errors;
    err.status = 422;
    throw err;
  }
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText} for ${url}`);
  }
  return (await res.json()) as SaveResult;
}

export const api = {
  regions: () => getJSON<RegionSummary[]>("/api/regions"),

  entries: (region?: string, status?: Status | "") => {
    const p = new URLSearchParams();
    if (region) p.set("region", region);
    if (status) p.set("status", status);
    const qs = p.toString();
    return getJSON<QueueRow[]>(`/api/entries${qs ? `?${qs}` : ""}`);
  },

  entry: (entryId: string) =>
    getJSON<EntryDetail>(`/api/entries/${encodeURIComponent(entryId)}`),

  searchItems: (q: string) =>
    getJSON<ItemSearchResult[]>(`/api/items/search?q=${encodeURIComponent(q)}`),

  itemBoundaries: (itemId: string) =>
    getJSON<Boundary[]>(`/api/items/${encodeURIComponent(itemId)}/boundaries`),

  itemTributaries: (itemId: string) =>
    getJSON<{ id: string; name: string }[]>(`/api/items/${encodeURIComponent(itemId)}/tributaries`),

  species: () => getJSON<SpeciesOption[]>("/api/species"),

  /** per-rule, per-extent resolved reach — what to highlight on the map */
  reaches: (entryId: string) =>
    getJSON<EntryReaches>(`/api/entries/${encodeURIComponent(entryId)}/reaches`),

  geojson: (itemId: string) =>
    getJSON<GeoJSON>(`/api/items/${encodeURIComponent(itemId)}/geojson`),
  /** what a rule COVERS and what it EXCEPTS, from the reach builder — including the parts the
   *  item layer cannot draw (tributaries, and `within(area)`). Button-driven: the resolve is
   *  cheap, the geometry is not. */
  ruleResolved: (entryId: string, ruleId: string) =>
    getJSON<RuleResolved>(
      `/api/entries/${encodeURIComponent(entryId)}/rules/${encodeURIComponent(ruleId)}/resolved`),

  /** one level of tributaries, scoped to the whole ITEM — the fallback when no rule is selected */
  tributaryGeojson: (itemId: string) =>
    getJSON<GeoJSON>(`/api/items/${encodeURIComponent(itemId)}/tributaries/geojson`),

  save: (entryId: string, region: string, entry: Entry) =>
    writeEntry(`/api/entries/${encodeURIComponent(entryId)}`, { region, entry }),

  confirm: (entryId: string, region: string, entry: Entry, reviewed_by: string) =>
    writeEntry(`/api/entries/${encodeURIComponent(entryId)}/confirm`, {
      region,
      entry,
      reviewed_by,
    }),

  // --- splits.json editing (needs a graph rebuild to take effect) ---
  getSplit: (splitId: string) =>
    getJSON<{ split: Record<string, unknown>; waterbody: string; applies_to: unknown }>(
      `/api/splits/${encodeURIComponent(splitId)}`,
    ),

  saveSplit: async (splitId: string, patch: Record<string, unknown>): Promise<SaveResult> => {
    const res = await fetch(`/api/splits/${encodeURIComponent(splitId)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ patch }),
    });
    if (res.status === 422) {
      const p = await res.json().catch(() => ({}));
      const d = (p && (p.detail ?? p.errors)) as unknown;
      const err = new Error("validation failed") as ValidationError;
      err.errors = Array.isArray(d) ? d.map(String) : [String(d ?? "validation failed")];
      err.status = 422;
      throw err;
    }
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return (await res.json()) as SaveResult;
  },

  deleteSplit: async (splitId: string): Promise<SaveResult> => {
    const res = await fetch(`/api/splits/${encodeURIComponent(splitId)}`, { method: "DELETE" });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return (await res.json()) as SaveResult;
  },

  splitRefs: (splitId: string) =>
    getJSON<SplitRef[]>(`/api/splits/${encodeURIComponent(splitId)}/refs`),

  renameSplit: async (
    splitId: string,
    newId: string,
  ): Promise<{ ok: boolean; new_id: string; updated_rules: SplitRef[]; failed_rules: SplitRef[] }> => {
    const res = await fetch(`/api/splits/${encodeURIComponent(splitId)}/rename`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ new_id: newId }),
    });
    if (res.status === 422) {
      const p = await res.json().catch(() => ({}));
      const d = (p && (p.detail ?? p.errors)) as unknown;
      const err = new Error("rename failed") as ValidationError;
      err.errors = Array.isArray(d) ? d.map(String) : [String(d ?? "rename failed")];
      err.status = 422;
      throw err;
    }
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return await res.json();
  },

  // --- graph rebuild (pipeline.atlas.build --full; CPU-only, no credits) ---
  startRebuild: async (): Promise<RebuildStatus> => {
    const res = await fetch("/api/rebuild", { method: "POST" });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return (await res.json()) as RebuildStatus;
  },

  rebuildStatus: () => getJSON<RebuildStatus>("/api/rebuild/status"),
};
