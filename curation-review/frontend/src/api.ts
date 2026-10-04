// Tiny typed API client matching curation-review/API.md.
// All calls are relative to /api (Vite proxies to http://127.0.0.1:8787).

import type {
  CheckResult,
  EntryKind,
  FieldError,
  Vocab,
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
  Answer,
  AnswerWater,
  EntryInBundle,
  QueueOrder,
  VerifyStatus,
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

/** A refused entry save: the model's errors, each addressed to a field. */
export interface EntryRefused extends Error {
  fieldErrors: FieldError[];
}

function asFieldErrors(detail: unknown): FieldError[] {
  const list = Array.isArray(detail) ? detail : [detail ?? "validation failed"];
  return list.map((d) =>
    d && typeof d === "object" && "msg" in (d as object)
      ? { path: String((d as FieldError).path ?? ""), msg: String((d as FieldError).msg) }
      : { path: "", msg: typeof d === "string" ? d : JSON.stringify(d) });
}

// PUT write path. Returns {ok, errors} on 200; throws an EntryRefused carrying the addressed
// errors on 422 (FastAPI puts them under `detail`).
async function writeEntry(url: string, body: Record<string, unknown>): Promise<SaveResult> {
  const res = await fetch(url, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 422) {
    const payload = await res.json().catch(() => ({}));
    const err = new Error("validation failed") as EntryRefused;
    err.fieldErrors = asFieldErrors(payload?.detail ?? payload?.errors);
    throw err;
  }
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText} for ${url}`);
  }
  return (await res.json()) as SaveResult;
}

let pageMap: Promise<Record<string, number>> | null = null;

export const api = {
  /** printed page -> PDF page of the repo's synopsis (`source_pages` are PRINTED numbers) */
  synopsisPages: () => (pageMap ??= getJSON<Record<string, number>>("/api/synopsis/pages")
    .catch((e) => { pageMap = null; throw e; })),

  regions: () => getJSON<RegionSummary[]>("/api/regions"),

  entries: (region?: string, status?: Status | "", kind?: EntryKind | "",
            verify?: VerifyStatus | "todo" | "", order?: QueueOrder) => {
    const p = new URLSearchParams();
    if (region) p.set("region", region);
    if (status) p.set("status", status);
    if (kind) p.set("kind", kind);
    if (verify) p.set("verify", verify);
    if (order) p.set("order", order);
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

  /** every option list the editors offer, read off the catalogue model */
  vocab: () => getJSON<Vocab>("/api/vocab"),

  /** validate a draft without writing: errors (addressed), warnings, and the generated labels */
  check: async (entry: Entry, region: string, signal?: AbortSignal): Promise<CheckResult> => {
    const res = await fetch("/api/check", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ region, entry }),
      signal,
    });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText} for /api/check`);
    return (await res.json()) as CheckResult;
  },

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

  /** Make the finished `<build>_next` the served build (rename; the old one is kept as `.prev`).
   *  The build itself never touches the served atlas: its handle table is what ships. */
  promoteRebuild: async (): Promise<{ ok: boolean; error?: string }> => {
    const res = await fetch("/api/rebuild/promote", { method: "POST" });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return (await res.json()) as { ok: boolean; error?: string };
  },

  // --- review marks (a sidecar keyed by entry_id + content hash; never inside the entry) ---
  /** mark verified, flag with a note, or clear ("unverified"). Throws EntryRefused on 422. */
  verify: async (entryId: string, state: "verified" | "flagged" | "unverified", note = "") => {
    const res = await fetch(`/api/entries/${encodeURIComponent(entryId)}/verify`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ state, note }),
    });
    if (res.status === 422) {
      const payload = await res.json().catch(() => ({}));
      const err = new Error("refused") as EntryRefused;
      err.fieldErrors = asFieldErrors(payload?.detail);
      throw err;
    }
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return (await res.json()) as { entry_id: string; status: VerifyStatus };
  },

  /** progress over a region, or the whole corpus */
  verification: (region?: string) =>
    getJSON<{ total: number; verified: number; stale: number; flagged: number; unverified: number }>(
      `/api/verification${region ? `?region=${encodeURIComponent(region)}` : ""}`),

  // --- what an angler is told: the live bundle ---
  entryBundle: (entryId: string) =>
    getJSON<EntryInBundle>(`/api/entries/${encodeURIComponent(entryId)}/bundle`),
  answerWaters: (entryId: string, q = "") =>
    getJSON<AnswerWater[]>(
      `/api/entries/${encodeURIComponent(entryId)}/answer/waters?q=${encodeURIComponent(q)}`),
  answer: (sid: number, date: string, fish: string) =>
    getJSON<Answer>(`/api/answer?sid=${sid}&date=${encodeURIComponent(date)}&fish=${encodeURIComponent(fish)}`),
};
