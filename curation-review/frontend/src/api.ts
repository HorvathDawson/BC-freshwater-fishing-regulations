// Tiny typed API client matching curation-review/API.md.
// All calls are relative to /api (Vite proxies to http://127.0.0.1:8787).

import type { InputsSync, CheckPlace, CheckProgress, PlaceCheck, WaterReach, WaterView,
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
  Ready,
  QueueOrder,
  ReviewFilter,
  MarkView,
  MarkPost,
  Mode,
  ReviewView,
  Progress,
  IdList,
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
  SplitAnchors,
  AreaName,
  SplitPick,
  SplitPreview,
  Tributary,
  TidalDetail,
  TidalProgress,
  TidalRow,
  TidalSnap,
  MarksHealth,
  InSeasonExtent,
  InSeasonItemSplits,
  InSeasonMatchPost,
  InSeasonPreview,
  InSeasonQueue,
  InSeasonRow,
} from "./types";

/** (T434) a failed answer's words: the server's JSON `detail` (a 503 {detail} names the missing input), else its status */
async function failure(res: Response, url: string): Promise<Error> {
  let detail = "";
  try {
    const text = await res.text();
    try {
      const d = (JSON.parse(text) as { detail?: unknown }).detail;
      detail = d === undefined ? text : typeof d === "string" ? d : asFieldErrors(d).map((e) => e.msg).join("; ") || JSON.stringify(d);
    } catch { detail = text; }   // not JSON: the body as sent
  } catch { /* no body: the status says it */ }
  return new Error(`${res.status} ${res.statusText} for ${url}${detail ? ` — ${detail.slice(0, 500)}` : ""}`);
}

async function getJSON<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw await failure(res, url);
  return (await res.json()) as T;
}

/** T355: a read whose 422 is an answer — `{ok: false, why}` with the server's own words (the model's
 *  refusal: an offset on an area boundary), not an error */
async function getOrRefused<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (res.status === 422) {
    const body = await res.json().catch(() => ({}));
    return { ok: false, why: asFieldErrors((body as { detail?: unknown }).detail).map((e) => e.msg).join("; ") } as T;
  }
  if (!res.ok) throw await failure(res, url);
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

import { addRefusal, resolve as resolveRefusal } from "./refusals";
import { noteReply } from "./curatedSave";

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
    let res: Response;
    try {
      res = await fetch("/api/marks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(mark),
      });
    } catch (e) {
      // owner 2026-10-28: a save that never reached the server is a refused save too
      addRefusal(mark, [`the server could not be reached: ${String(e)}`]);
      throw e;
    }
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      const err = new Error(`mark refused (${res.status})`) as EntryRefused;
      err.fieldErrors = asFieldErrors(payload?.detail ?? payload?.errors);
      addRefusal(mark, err.fieldErrors.length
        ? err.fieldErrors.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [`HTTP ${res.status}`]);
      throw err;
    }
    resolveRefusal(mark);
    const got = await res.json();
    noteReply(got?.write_through);       // T325: the mark's domain apply ran (or is running) — say what it did
    return got as MarkView;
  },

  /** the app's address, the checks and the problem categories */
  mode: () => getJSON<Mode>("/api/mode"),
  /** what the checklist is answered from: evidence, coverage, tier, impact, the app link */
  review: (entryId: string, date?: string) =>
    getJSON<ReviewView>(`/api/entries/${encodeURIComponent(entryId)}/review` +
      (date ? `?date=${encodeURIComponent(date)}` : "")),
  progress: () => getJSON<Progress>("/api/review/progress"),
  /** T414: the ids file `path` names, its progress, and the ids no entry has; a refusal says why */
  idList: async (path: string): Promise<IdList> => {
    const res = await fetch(`/api/review/ids?path=${encodeURIComponent(path)}`);
    const body = await res.json().catch(() => null);
    if (!res.ok)
      throw new Error(asFieldErrors((body as { detail?: unknown } | null)?.detail ?? res.statusText)
        .map((e) => e.msg).join("; "));
    return body as IdList;
  },
  /** T133: every entry and split awaiting its build, and the command(s) to run */
  pendingBuild: () => getJSON<PendingBuild>("/api/build"),
  /** T541: warm-up, and which backend this is (start commit, HEAD moved, marks store, write-through) */
  ready: () => getJSON<Ready>("/api/ready"),
  /** owner 2026-10-28: every mark in force accounted for — orphaned / stale ones listed */
  /** the marks in force on one target (GET /api/marks?target=&scope=) */
  marksOn: (target: string, scope: string) =>
    getJSON<MarkView[]>(`/api/marks?target=${encodeURIComponent(target)}&scope=${encodeURIComponent(scope)}`),
  marksHealth: () => getJSON<MarksHealth>("/api/marks/health"),

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

  // --- T230: the In-season tab (reads, and its one write: the in-season matches file, not a mark) ---
  inseasonQueue: () => getJSON<InSeasonQueue>("/api/inseason/queue"),
  inseasonItemSplits: (itemId: string) =>
    getJSON<InSeasonItemSplits>(`/api/inseason/items/${encodeURIComponent(itemId)}/splits`),
  inseasonPreview: async (extents: InSeasonExtent[]): Promise<InSeasonPreview> => {
    const res = await fetch(`/api/inseason/extent-preview?extent=${encodeURIComponent(JSON.stringify(extents))}`);
    const body = await res.json().catch(() => ({}));
    if (!res.ok) return { resolvable: false, why: String(body?.detail ?? res.statusText), n_sections: null };
    return body as InSeasonPreview;
  },
  /** (T520) the Mac publish path's last run after a saved input (deploy/staging/sync-inputs.sh) */
  inputsSync: () => getJSON<InputsSync>("/api/inputs/sync"),
  /** (T520) re-resolve every in-season match on the shipped release (after a promote), then publish */
  inseasonResolve: async (): Promise<{ release: string; unresolved: string[] }> => {
    const res = await fetch("/api/inseason/resolve", { method: "POST" });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(typeof body?.detail === "string" ? body.detail : res.statusText);
    return body;
  },
  inseasonSave: async (post: InSeasonMatchPost): Promise<InSeasonRow> => {
    const res = await fetch("/api/inseason/matches", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(post),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      const d = body?.detail;
      throw new Error(typeof d === "string" ? d : JSON.stringify(d ?? res.statusText));
    }
    return body as InSeasonRow;
  },

  // --- T164: the DFO review mode (reads; a DFO mark goes through postMark, scope "dfo") ---
  dfoQueue: (f: { region?: string; status?: string; need?: string; verify?: string; q?: string; lake?: string }) => {
    const p = new URLSearchParams();
    for (const [k, v] of Object.entries(f)) if (v) p.set(k, v);
    const qs = p.toString();
    return getJSON<DfoRow[]>(`/api/dfo/queue${qs ? `?${qs}` : ""}`);
  },
  dfoProgress: () => getJSON<DfoProgress>("/api/dfo/progress"),
  // --- T532: the checklist (reads; a check goes through postMark with `checklist`) ---
  checksQueue: (source: "dfo" | "regs" | "all", filter = "", q = "") =>
    getJSON<CheckPlace[]>(`/api/checks/queue?${new URLSearchParams({ source, ...(filter ? { filter } : {}),
                                                                     ...(q ? { q } : {}) })}`),
  checksProgress: (source: "dfo" | "regs" | "all") => getJSON<CheckProgress>(`/api/checks/progress?source=${source}`),
  checksWater: (place: string, source: "dfo" | "regs" | "all") =>
    getJSON<WaterView>(`/api/checks/water?${new URLSearchParams({ place, source })}`),
  checksWaterReach: (key: string) => getJSON<WaterReach>(`/api/checks/water/reach?${new URLSearchParams({ key })}`),
  checkDfo: (id: string) => getJSON<PlaceCheck>(`/api/checks/dfo/${encodeURIComponent(id)}`),
  checkRule: (entryId: string, ruleId: string) =>
    getJSON<PlaceCheck>(`/api/checks/rule/${encodeURIComponent(entryId)}/${encodeURIComponent(ruleId)}`),
  dfoLocation: (id: string) => getJSON<DfoDetail>(`/api/dfo/locations/${encodeURIComponent(id)}`),
  /** the tributary walk is skipped unless `tributaries` (the Extents map's "Show tributaries") */
  dfoReach: (id: string, tributaries = false) =>
    getJSON<ExtentPreview & { walks_tributaries?: boolean }>(
      `/api/dfo/locations/${encodeURIComponent(id)}/reach${tributaries ? "?tributaries=true" : ""}`),
  dfoAnchor: (id: string, split: NewSplit) =>
    getJSON<AnchorCheck>(`/api/dfo/locations/${encodeURIComponent(id)}/anchor?split=` +
      encodeURIComponent(JSON.stringify(split))),
  /** T355: where the BUILD's resolver would cut a proposed split (read only) */
  dfoSplitPreview: (id: string, split: NewSplit, others: NewSplit[] = []) =>
    getOrRefused<SplitPreview>(`/api/dfo/locations/${encodeURIComponent(id)}/split-preview?split=` +
      encodeURIComponent(JSON.stringify(split)) + (others.length ? `&others=${encodeURIComponent(JSON.stringify(others))}` : "")),
  dfoTributaries: (id: string, onItem?: string | null) =>
    getJSON<Tributary[]>(`/api/dfo/locations/${encodeURIComponent(id)}/tributaries`
      + (onItem ? `?on_item=${encodeURIComponent(onItem)}` : "")),
  dfoSplitPick: (id: string, kind: "tributary" | "lake" | "area", ll: [number, number], onItem?: string | null) =>
    getJSON<SplitPick>(`/api/dfo/locations/${encodeURIComponent(id)}/split-pick?kind=${kind}`
      + `&lon=${ll[0]}&lat=${ll[1]}` + (onItem ? `&on_item=${encodeURIComponent(onItem)}` : "")),
  /** T355: area names as you type (the layer's own spelling; prefix first, then nearest the water) */
  dfoAreaNames: (id: string, layer: string, q: string, onItem?: string | null) =>
    getJSON<AreaName[]>(`/api/dfo/locations/${encodeURIComponent(id)}/area-names?layer=${encodeURIComponent(layer)}`
      + `&q=${encodeURIComponent(q)}` + (onItem ? `&on_item=${encodeURIComponent(onItem)}` : "")),
  splitAnchors: () => getJSON<SplitAnchors>("/api/split-anchors"),
  /** T524: `fix` may be the place's PIECES — resolved as their union */
  dfoExtentPreview: (id: string, fix: ExtentFix | ExtentFix[], tributaries = false) =>
    getJSON<ExtentPreview>(`/api/dfo/locations/${encodeURIComponent(id)}/extent-preview?fix=` +
      encodeURIComponent(JSON.stringify(fix)) + (tributaries ? "&tributaries=true" : "")),

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
  tidalSnap: (id: string, ll: [number, number]) =>
    getJSON<TidalSnap>(`/api/tidal/candidates/${encodeURIComponent(id)}/snap?lon=${ll[0]}&lat=${ll[1]}`),
};
