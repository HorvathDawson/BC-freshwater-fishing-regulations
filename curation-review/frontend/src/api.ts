// Tiny typed API client matching curation-review/API.md.
// All calls are relative to /api (Vite proxies to http://127.0.0.1:8787).

import type {
  EntryKind,
  FieldError,
  EntryDetail,
  EntryReaches,
  EntrySplits,
  GeoJSON,
  QueueRow,
  RegionSummary,
  Status,
  RuleResolved,
  PendingBuild,
  QueueOrder,
  ReviewFilter,
  MarkView,
  MarkPost,
  Mode,
  ReviewView,
  Progress,
  SeverityFilter,
  Tier,
  ExtentFix,
  ExtentOpInfo,
  ExtentPreview,
  AreaHit,
  ItemHit,
  SplitHit,
  AnchorCheck,
  DfoDetail,
  DfoProgress,
  DfoRow,
  NewSplit,
  TidalDetail,
  TidalProgress,
  TidalRow,
} from "./types";

async function getJSON<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText} for ${url}`);
  }
  return (await res.json()) as T;
}

/** A refused mark: the server's reasons, each addressed to a field (or none). */
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

let pageMap: Promise<Record<string, number>> | null = null;

export const api = {
  /** printed page -> PDF page of the repo's synopsis (`source_pages` are PRINTED numbers) */
  synopsisPages: () => (pageMap ??= getJSON<Record<string, number>>("/api/synopsis/pages")
    .catch((e) => { pageMap = null; throw e; })),

  regions: () => getJSON<RegionSummary[]>("/api/regions"),


  entries: (region?: string, status?: Status | "", kind?: EntryKind | "",
            verify?: ReviewFilter, order?: QueueOrder, tier?: Tier | "",
            severity?: SeverityFilter) => {
    const p = new URLSearchParams();
    if (tier) p.set("tier", tier);
    if (severity) p.set("severity", severity);
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

  /** per-rule, per-extent resolved reach — what to highlight on the map */
  reaches: (entryId: string) =>
    getJSON<EntryReaches>(`/api/entries/${encodeURIComponent(entryId)}/reaches`),

  /** every split the entry's rules name, as labelled map points (T184: the proxy check) */
  splits: (entryId: string) =>
    getJSON<EntrySplits>(`/api/entries/${encodeURIComponent(entryId)}/splits`),

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

  // --- review marks (T97: the append-only store; T134: the review screen makes them) ---
  /** THE ONE WRITE OF THE APP: a mark. Throws an EntryRefused with the server's reasons. */
  postMark: async (mark: MarkPost): Promise<MarkView> => {
    const res = await fetch("/api/marks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(mark),
    });
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      const err = new Error(`mark refused (${res.status})`) as EntryRefused;
      err.fieldErrors = asFieldErrors(payload?.detail ?? payload?.errors);
      throw err;
    }
    return (await res.json()) as MarkView;
  },

  /** the app's address, the checks and the problem categories */
  mode: () => getJSON<Mode>("/api/mode"),
  /** what the checklist is answered from: evidence, coverage, tier, impact, the app link */
  review: (entryId: string, date?: string) =>
    getJSON<ReviewView>(`/api/entries/${encodeURIComponent(entryId)}/review` +
      (date ? `?date=${encodeURIComponent(date)}` : "")),
  progress: () => getJSON<Progress>("/api/review/progress"),
  /** T133: every entry and split awaiting its build, and the command(s) to run */
  pendingBuild: () => getJSON<PendingBuild>("/api/build"),

  // --- T191: the extent-fix editor (reads only; the fix rides the mark) ---
  extentOps: () => getJSON<{ ops: ExtentOpInfo[]; point_kinds: string[] }>("/api/extent-fix/ops"),
  searchSplits: (q: string) =>
    getJSON<SplitHit[]>(`/api/splits/search?q=${encodeURIComponent(q)}`),
  searchAreas: (q: string) =>
    getJSON<AreaHit[]>(`/api/areas/search?q=${encodeURIComponent(q)}`),
  searchItems: (q: string, kind?: string) =>
    getJSON<ItemHit[]>(`/api/items/search?q=${encodeURIComponent(q)}${kind ? `&kind=${kind}` : ""}`),
  extentPreview: (entryId: string, fix: ExtentFix) =>
    getJSON<ExtentPreview>(`/api/entries/${encodeURIComponent(entryId)}/extent-preview?fix=` +
      encodeURIComponent(JSON.stringify(fix))),

  // --- T164: the DFO review mode (reads; a DFO mark goes through postMark, scope "dfo") ---
  dfoQueue: (f: { region?: string; status?: string; need?: string; verify?: string; q?: string }) => {
    const p = new URLSearchParams();
    for (const [k, v] of Object.entries(f)) if (v) p.set(k, v);
    const qs = p.toString();
    return getJSON<DfoRow[]>(`/api/dfo/queue${qs ? `?${qs}` : ""}`);
  },
  dfoProgress: () => getJSON<DfoProgress>("/api/dfo/progress"),
  dfoLocation: (id: string) => getJSON<DfoDetail>(`/api/dfo/locations/${encodeURIComponent(id)}`),
  dfoReach: (id: string) => getJSON<ExtentPreview>(`/api/dfo/locations/${encodeURIComponent(id)}/reach`),
  dfoAnchor: (id: string, split: NewSplit) =>
    getJSON<AnchorCheck>(`/api/dfo/locations/${encodeURIComponent(id)}/anchor?split=` +
      encodeURIComponent(JSON.stringify(split))),
  dfoExtentPreview: (id: string, fix: ExtentFix) =>
    getJSON<ExtentPreview>(`/api/dfo/locations/${encodeURIComponent(id)}/extent-preview?fix=` +
      encodeURIComponent(JSON.stringify(fix))),

  // --- T212: the Tidal review mode (reads; accepting is a mark through postMark, scope "tidal") ---
  tidalQueue: (f: { confidence?: string; verify?: string; enters?: string; words?: boolean; q?: string; triage?: string }) => {
    const p = new URLSearchParams();
    for (const [k, v] of Object.entries(f)) if (v) p.set(k, String(v));
    const qs = p.toString();
    return getJSON<TidalRow[]>(`/api/tidal/queue${qs ? `?${qs}` : ""}`);
  },
  tidalProgress: () => getJSON<TidalProgress>("/api/tidal/progress"),
  tidalCandidate: (id: string) => getJSON<TidalDetail>(`/api/tidal/candidates/${encodeURIComponent(id)}`),
  tidalEdge: (id: string) => getJSON<GeoJSON>(`/api/tidal/candidates/${encodeURIComponent(id)}/edge`),
};
