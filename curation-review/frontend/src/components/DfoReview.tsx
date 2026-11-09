import { CloseX } from "./NoticeStack";
import { DfoVintageBanner } from "./DfoVintageBanner";
import { InputsSyncLine } from "./InputsSyncLine";
import { DfoPlaces } from "./DfoPlaces";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { applyPv, PV_DEFAULT, PvBadge, PvControls, type PvFilter } from "./Preverify";
import { api, type EntryRefused } from "../api";
import type {
  AnchorCheck, AnchorKind, Check, CheckAnswer, DfoDetail, DfoHunt, DfoPiece, DfoProgress, DfoRow, DfoSuggestion, EntrySplits,
  ExtentFix, ExtentPreview, Mode, NamedRef, NewSplit, ProposedSplit, SplitAnchors, SplitConfidence, SplitPick,
  SplitPreview, Tributary,
} from "../types";
import { getReviewer, isTyping, setReviewer } from "../review";
import { MapPanel, type EditPoint } from "./MapPanel";
import { TopNav, FoldToggle, useFold } from "./TopNav";
import { ExtentFixEditor, pointWords } from "./ExtentFixEditor";
import { failed, reportFailure } from "../failures";

/** two clicks this close (m) are one cut — the server folds them the same way (`marks.SAME_CUT_M`) */
const SAME_CUT_M = 10;
const groundM = (a: [number, number], b: [number, number]) =>
  Math.hypot((a[0] - b[0]) * 111320 * Math.cos(((a[1] + b[1]) / 2) * Math.PI / 180), (a[1] - b[1]) * 111320);
/** the proposal that is the same cut as this resolved anchor, if any (same blk within 10 m, or the
 *  same point within 10 m) */
function sameCut(list: ProposedSplit[], got: AnchorCheck): ProposedSplit | undefined {
  return list.find((p) =>
    (p.blk && got.blk && p.blk === got.blk && p.measure != null && got.measure != null
      && Math.abs(p.measure - got.measure) <= SAME_CUT_M)
    || (p.coord && got.coord && groundM(p.coord, got.coord) <= SAME_CUT_M));
}
/** a proposal carried into this mark as a `NewSplit` — EVERY field it has (T355: a hand-picked list
 *  dropped an area_boundary's area and the save was refused, Little Qualicum) */
const asNewSplit = (p: ProposedSplit, quote: string): NewSplit => {
  const { id, marks: _marks, n_marks: _n, ...rest } = p;
  return { ...rest, ref: id, quote: p.quote || quote };
};
/** owner 2026-10-27: a new split's confidence when none is set — a point placed by hand on the map
 *  is medium, a registry anchor (a tributary's mouth, a lake's edge) high (`marks.default_confidence`) */
export const defaultConfidence = (a: AnchorKind): SplitConfidence =>
  (["bridge", "dam", "sign", "coord"] as AnchorKind[]).includes(a) ? "medium" : "high";
const CONFIDENCES: SplitConfidence[] = ["high", "medium", "low"];
function ConfChip({ c }: { c: SplitConfidence }) {
  return <span className={`chip-tag conf-${c}`} title="how sure the proposer is of where this cut stands"
    data-testid="split-confidence">{c} confidence</span>;
}
const CHECKS: Check[] = ["T", "R", "P"];

/** ACCEPT A SUGGESTION: the extent-fix editor's value it pre-fills, and the NEW splits its points
 *  name (renamed off any ref already taken in this mark or by a proposal on the river). Pure: it
 *  writes nothing — the owner's "Save extent" records the mark, the only write. A point naming a
 *  split another mark proposed stays as it is (the save carries that proposal in). */
export function suggestionFix(s: DfoSuggestion | null | undefined, taken: string[] = []):
    { fix: ExtentFix; fixes: ExtentFix[]; newSplits: NewSplit[] } | null {
  const sg = s?.suggestion;
  if (!sg) return null;
  const used = new Set(taken);
  const rename: Record<string, string> = {};
  const newSplits = (sg.new_splits ?? []).map((n) => {
    let ref = n.ref, k = 1;
    while (used.has(ref)) { k += 1; ref = `${n.ref}-${k}`; }
    used.add(ref);
    rename[n.ref] = ref;
    return { ...n, ref };
  });
  // T524: a proposal of SEVERAL PIECES (`pieces`, the place's union) pre-fills one editor per piece
  const asFix = (x: DfoPiece): ExtentFix => ({
    scope: "entry", op: x.op,
    points: x.points.map((p) => (p.kind === "new" && p.split_id && rename[p.split_id]
      ? { ...p, split_id: rename[p.split_id] } : { ...p })),
    items: [...x.items], tributaries: x.tributaries ?? null,
    ...(x.area ? { area: x.area } : {}),
  });
  const fixes = (sg.pieces?.length ? sg.pieces : [sg]).map(asFix);
  return { fix: fixes[0], fixes, newSplits };
}
const CONF_WORDS: Record<string, string> = { high: "high", medium: "medium", low: "low", none: "—" };

/** THE SPLIT HUNT's evidence for a suggestion (owner 2026-10-27): every hand-researched cut with
 *  its confidence, what it is, where (lat/lon, the snap to the FWA line), the sources and how the
 *  notice was read. Display only — "accept suggestion" carries each cut (its note holds this
 *  evidence) into the editor; the owner's Save extent is the write. */
function HuntProposals({ hunt }: { hunt: DfoHunt }) {
  const link = (u: string) => /^https?:\/\//.test(u)
    ? <a key={u} href={u} target="_blank" rel="noreferrer">{u.replace(/^https?:\/\/(www\.)?/, "").slice(0, 60)}</a>
    : <code key={u}>{u}</code>;
  return (
    <div className="ext-row" data-testid="dfo-hunt" style={{ display: "block", fontSize: 12 }}>
      <div><span className="k" style={{ color: "#7c3aed" }}>split hunt</span> {hunt.notes}</div>
      {(hunt.proposals ?? []).map((p) => (
        <div key={p.ref} data-testid="dfo-hunt-proposal" style={{ margin: "4px 0 4px 10px" }}>
          <span className={`badge suggest-${p.confidence}`}>{p.confidence}</span>{" "}
          <b>{p.name}</b> <span className="dim">({p.anchor}{p.offset_m ? `, ${p.offset_m > 0 ? "+" : ""}${p.offset_m} m` : ""})</span>
          {" — "}{p.description}
          <div className="dim">
            {p.lat.toFixed(5)}, {p.lon.toFixed(5)}
            {p.snap_m != null ? ` · ${Math.round(p.snap_m)} m to the FWA line` : " · not snapped"}
            {p.warning && <span style={{ color: "#b45309" }}> · {p.warning}</span>}
          </div>
          {p.sources.length > 0 && (
            <div>sources: {p.sources.map((u, i) => <span key={u}>{i ? " · " : ""}{link(u)}</span>)}</div>
          )}
          {p.notes && <div className="dim">notes: {p.notes}</div>}
        </div>
      ))}
      {(hunt.sources ?? []).length > 0 && (
        <div className="dim">notice: {(hunt.sources ?? []).map((u) => link(u))}</div>
      )}
    </div>
  );
}

/** COVERAGE (owner 2026-10-27): why this location had no extent, and the other wordings of the same
 *  place on this water — a cluster member inherits a curated member's extent or adopts the best. */
function SuggestionCoverage({ sugg }: { sugg: DfoSuggestion }) {
  const c = sugg.cluster;
  if (!sugg.why && !c) return null;
  const others = c ? c.members.filter((m) => m !== sugg.location_id) : [];
  return (
    <div className="ext-row dim" data-testid="dfo-coverage" style={{ display: "block", fontSize: 12 }}>
      {sugg.why && <div><span className="k">unplaced because</span> {sugg.why.replace(/_/g, " ")}</div>}
      {c && (
        <div data-testid="dfo-cluster">
          <span className="k">rewordings</span> {c.role}{c.role === "inherits" ? " the curated extent of" : ""}{" "}
          {others.map((m, i) => (
            <span key={m}>{i ? " · " : ""}<a href={`#${DFO_HASH}${encodeURIComponent(m)}`}>{m.split(":").slice(2).join(":")}</a>
              {c.placed.includes(m) ? " (placed)" : ""}{m === c.canonical ? " (canonical)" : ""}</span>
          ))}
        </div>
      )}
      {sugg.before?.suggestion?.words && <div>matcher's own reading: {sugg.before.suggestion.words} ({sugg.before.confidence})</div>}
    </div>
  );
}

/** The extent fix the mark in force carries for this location: what the editor opens on and what
 *  "your extent" shows (the LATEST mark wins — a later mark without a fix is the reviewer taking it off). */
export function savedFixOf(mark: DfoDetail["mark"] | null | undefined): ExtentFix | null {
  return savedFixesOf(mark)[0] ?? null;
}
/** T524: EVERY PIECE of the saved extent — a place may be several pieces on different waters (the Chilliwack below
 *  the Slesse line + the Sumas below Barrow Town Pump Station); it binds their union. One piece is the old mark. */
export function savedFixesOf(mark: DfoDetail["mark"] | null | undefined): ExtentFix[] {
  const fixes = mark?.extent_fix ?? [];
  const own = fixes.filter((f) => f.scope === "entry");
  return own.length ? own : fixes.slice(0, 1);
}
/** (owner 2026-11-09) THE TRIBUTARY CALL A NEW EXTENT FIX / PIECE STARTS ON: the place's own binding flag, else its
 *  water's (the DFO title, "Morice River (including tributaries)") — the builder's own inheritance
 *  (`entries.to_reach_input`). Shown with where it came from; the owner can still change it. */
export function tributaryDefault(d: { location?: { binding?: { tributaries?: boolean | null } } | null;
                                      water?: { tributaries?: boolean | null } | null }):
    { call: boolean | null; why: string } {
  const own = d.location?.binding?.tributaries;
  if (own != null) return { call: own, why: `this place: ${own ? "including" : "without"} tributaries` };
  const w = d.water?.tributaries;
  if (w != null) return { call: w, why: `DFO title: ${w ? "including" : "without"} tributaries` };
  return { call: null, why: "" };
}
/** (owner 2026-11-09) what a reach line says of tributaries: a binding that walks them says so even when the slow
 *  walk is not drawn — never reads as if they were excluded */
export function tribWords(walks: boolean | undefined, drawn: boolean): string {
  if (walks === true) return drawn ? " (with tributaries)" : " (with tributaries — not drawn; tick Show tributaries to draw)";
  if (walks === false) return " (no tributaries)";
  return "";
}
/** (owner 2026-11-09) a curated extent in words: the op, its cuts by name, the waters it holds on */
export function extentWords(e: { op: string; splits?: string[]; item_id?: string; item_ids?: string[] },
                            splitName: (id: string) => string = (x) => x, itemName: (id: string) => string = (x) => x): string {
  const items = e.item_ids ?? (e.item_id ? [e.item_id] : []);
  const cuts = (e.splits ?? []).map(splitName).join(" and ");
  return (OP_WORDS[e.op] ?? e.op) + (cuts ? ` ${cuts}` : "") + (items.length ? ` · on ${items.map(itemName).join(", ")}` : "");
}
/** (owner 2026-11-09: "more obvious when fix extent overrides existing extent") the banner's headline per state */
export const FIX_VS_WORDS: Record<string, string> = {
  replaces: "Your fix REPLACES the current extent",
  same: "Your fix is the same as the current extent",
  applied: "Fix applied — this is now the curated extent",
};
/** T524: the pieces in words, one after another ("… + …") */
export function piecesWords(fs: ExtentFix[], splitName: (id: string) => string = (x) => x,
                            itemName: (id: string) => string = (x) => x): string {
  return fs.map((f, i) => (fs.length > 1 ? `piece ${i + 1}: ` : "") + fixWords(f, splitName, itemName)).join(" + ");
}
/** An extent fix in words: the op, its points by name, the items it holds on, the tributary call. */
export function fixWords(f: ExtentFix, splitName: (id: string) => string = (x) => x,
                         itemName: (id: string) => string = (x) => x): string {
  const pts = f.points.map((p) => pointWords(p, splitName)).join(" and ");
  return [(OP_WORDS[f.op] ?? f.op) + (pts ? ` ${pts}` : "") + (f.area ? ` ${f.area}` : ""),
          f.items.length ? `on ${f.items.map(itemName).join(", ")}` : "on the entry's waters",
          f.tributaries == null ? "tributaries: no call" : f.tributaries ? "with tributaries" : "without tributaries",
  ].join(" · ");
}
const OP_WORDS: Record<string, string> = {
  whole: "the whole water", upstream_of: "upstream of", downstream_of: "downstream of",
  between: "between", within: "within an area", rest: "the rest", steelhead_waters: "steelhead waters",
};
const QUESTIONS: Record<string, string> = {
  T: "The notice's words for the place read right (water + specific area, against the DFO page)?",
  R: "The scraped rows are this location's rules (species, dates, limits)?",
  P: "The extent lands right on the map — every cut it needs exists, in the right place?",
};
const STATE_WORDS: Record<string, string> = {
  needs_cut: "needs a cut", bound: "bound", unbound: "no extent yet", no_water: "no water matched",
  tidal: "tidal water",
};
const VERIFY: [string, string][] = [
  ["", "all"], ["todo", "to do"], ["unmarked", "never marked"], ["stale", "changed since marked"],
  ["wrong", "wrong"], ["unsure", "unsure"], ["deferred", "deferred"], ["verified", "verified"],
];

export const DFO_HASH = "dfo/";

/** what counts as DONE for the fold (owner 2026-11-02): a mark that decides — verified or wrong. "?"
 *  (unsure), deferred, stale and unmarked are not done. */
export const DONE = new Set<string>(["verified", "wrong"]);
export const waterKey = (r: DfoRow) => `${r.region}|${r.water_id ?? ""}|${r.water}`;
const waterCount = (rows: DfoRow[]) => new Set(rows.map(waterKey)).size;

/** THE SEARCH BOX (owner 2026-11-02): the waters whose name, region, or any place wording — a
 *  rewording or variant riding a place included (`all`: every row, members too) — holds the words,
 *  case-insensitive; a matching water keeps all its places. Empty: the list as it is. Pure. */
export function searchWaters(rows: DfoRow[], all: DfoRow[], q: string): DfoRow[] {
  const want = q.trim().toLowerCase();
  if (!want) return rows;
  const hit = new Set<string>();
  for (const r of all) {
    const hay = `${r.water} ${r.text} r${r.region} region ${r.region} ${r.id}`.toLowerCase();
    if (hay.includes(want)) hit.add(waterKey(r));
  }
  return rows.filter((r) => hit.has(waterKey(r)));
}

/** THE QUEUE BY WATER (owner 2026-11-02): one row per PLACE — a group member that is not its place's
 *  canonical location is listed inside the place, not as its own row — and each water's places
 *  together, waters in the order their first place comes in review order. Pure. */
export function byWater(rows: DfoRow[]): DfoRow[] {
  const places = rows.filter((r) => !r.group || r.group.canonical === r.id
    || !rows.some((x) => x.id === r.group!.canonical));
  const order: string[] = [];
  const by = new Map<string, DfoRow[]>();
  for (const r of places) {
    const k = `${r.region}|${r.water_id ?? ""}|${r.water}`;
    if (!by.has(k)) { by.set(k, []); order.push(k); }
    by.get(k)!.push(r);
  }
  return order.flatMap((k) => by.get(k)!);
}

interface Props {
  mode: Mode;
}

/* THE DFO REVIEW MODE (T164): every scraped DFO salmon extent — the notice's own words, its rows,
   its curated extent on the map with the cuts on its water labelled — and a mark: T R P, an extent
   fix, and NEW SPLIT proposals by anchor (confluence, lake inlet / outlet, bridge, dam, signs, a map
   point, an existing split). Everything goes in the mark (POST /api/marks, scope "dfo"); a new split
   never goes into splits.json from here — T185 turns the marks into reviewed changes. */
export function DfoReview({ mode }: Props) {
  const [allRows, setRows] = useState<DfoRow[]>([]);
  // owner 2026-10-28: "loading…" until the first queue arrives, never "0 locations"
  const [loaded, setLoaded] = useState(false);
  const [pvf, setPvf] = useState<PvFilter>(PV_DEFAULT);
  // owner 2026-11-02: GROUPED BY WATER — one item per place (a historic rewording rides its place,
  // listed inside it), each water's places together, waters in review order (first row's place)
  // owner 2026-11-02: a search box over the list (water name, any wording or variant, region), client side
  const [find, setFind] = useState("");
  const rows = useMemo(() => searchWaters(byWater(applyPv(allRows, pvf)), allRows, find), [allRows, pvf, find]);
  // a water whose places are all done collapses to its heading; a click opens it again
  const [opened, setOpened] = useState<Record<string, boolean>>({});
  const [progress, setProgress] = useState<DfoProgress | null>(null);
  const fold = useFold("dfo");
  const [filters, setFilters] = useState({ region: "", status: "active", need: "", verify: "", q: "", lake: "" });
  const [selected, setSelected] = useState<string | null>(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return h.startsWith(DFO_HASH) ? h.slice(DFO_HASH.length) || null : null;
  });
  const [detail, setDetail] = useState<DfoDetail | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const loadRows = useCallback(() => {
    api.dfoQueue(filters).then((r) => { setRows(r); setLoaded(true); }).catch((e) => setError(String(e)));
    // shown, never swallowed: the progress carries the vintage check (T366), so a failed load would hide that too
    api.dfoProgress().then(setProgress).catch((e) => setError(`DFO progress did not load (so whether the rows are current is unknown): ${e}`));
  }, [filters]);
  useEffect(() => { loadRows(); }, [loadRows]);

  const open = useCallback((id: string) => {
    setSelected(id);
    window.history.replaceState(null, "", `#${DFO_HASH}${encodeURIComponent(id)}`);
  }, []);
  // a link to another place (#dfo/<id>, the places panel) opens it here
  useEffect(() => {
    const onHash = () => {
      const h = decodeURIComponent(window.location.hash.slice(1));
      if (h.startsWith(DFO_HASH) && h.length > DFO_HASH.length) setSelected(h.slice(DFO_HASH.length));
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  useEffect(() => {
    if (!selected) { setDetail(null); return; }
    setDetail(null);
    api.dfoLocation(selected).then(setDetail).catch((e) => setError(String(e)));
  }, [selected]);

  const idx = rows.findIndex((r) => r.id === selected);
  const next = idx >= 0 && idx < rows.length - 1 ? rows[idx + 1].id : !selected && rows.length ? rows[0].id : null;
  const prev = idx > 0 ? rows[idx - 1].id : null;
  const nav = useRef({ next, prev });
  nav.current = { next, prev };
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === "j" && nav.current.next) { open(nav.current.next); ev.preventDefault(); }
      else if (ev.key === "k" && nav.current.prev) { open(nav.current.prev); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);
  useEffect(() => {
    if (!selected) return;
    document.querySelector(`[data-dfo="${CSS.escape(selected)}"]`)?.scrollIntoView({ block: "nearest" });
  }, [selected, rows]);

  const set = (patch: Partial<typeof filters>) => setFilters((f) => ({ ...f, ...patch }));
  const all = progress?.all, live = progress?.active;
  const pct = all && all.total ? (100 * all.done) / all.total : 0;
  return (
    <div className="app dfo-mode" data-testid="dfo-mode">
      <div className="filterbar-wrap">
        <TopNav active="dfo" sub="extents" right={<>
          <span className="progress dfo-progress" data-testid="dfo-progress">
            {all ? <>
              <strong>{all.done}/{all.total}</strong> marked
              {live && <span className="dim"> · live {live.done}/{live.total}</span>}
              <span className="dim"> · {all.by_state.needs_cut ?? 0} need a cut · {all.by_state.bound ?? 0} bound
                {(all.by_state.tidal ?? 0) > 0 && <> · {all.by_state.tidal} tidal water</>}
                · {all.new_splits} new split{all.new_splits === 1 ? "" : "s"} proposed</span>
              {(all.by_verify.wrong ?? 0) + (all.by_verify.unsure ?? 0) > 0 &&
                <span className="dim"> · {all.by_verify.wrong ?? 0} wrong, {all.by_verify.unsure ?? 0} unsure</span>}
            </> : "…"}
          </span>
          <FoldToggle fold={fold} set={(filters.region ? 1 : 0) + (filters.status !== "active" ? 1 : 0) + (filters.need ? 1 : 0) + (filters.verify ? 1 : 0) + (filters.lake ? 1 : 0) + (filters.q ? 1 : 0) + (pvf.pv || pvf.first ? 1 : 0)} />
        </>} />
        {fold.open && (
        <div className="filterbar filterbar-more" data-testid="filters-more">
          <div>
            <label>region</label>
            <select aria-label="DFO region" value={filters.region} onChange={(e) => set({ region: e.target.value })}>
              <option value="">all</option>
              {Object.entries(progress?.regions ?? {}).map(([r, c]) => (
                <option key={r} value={r}>{r} ({c.done}/{c.total})</option>
              ))}
            </select>
          </div>
          <div>
            <label>page</label>
            <select aria-label="live or dormant" value={filters.status} onChange={(e) => set({ status: e.target.value })}>
              <option value="active">live page</option>
              <option value="dormant">dormant (archived)</option>
              <option value="">both</option>
            </select>
          </div>
          <div>
            <label>extent</label>
            <select aria-label="extent state" value={filters.need} onChange={(e) => set({ need: e.target.value })}>
              <option value="">any</option>
              {Object.entries(STATE_WORDS).map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          <div>
            <label>review</label>
            <select aria-label="DFO review" value={filters.verify} onChange={(e) => set({ verify: e.target.value })}>
              {VERIFY.map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          <div>
            <label>lake areas</label>
            <select aria-label="lake split later" data-testid="dfo-lake-filter" value={filters.lake}
              onChange={(e) => set({ lake: e.target.value })}>
              <option value="">hide lake split later</option>
              <option value="all">show all</option>
              <option value="later">only lake split later</option>
            </select>
          </div>
          <PvControls value={pvf} onChange={setPvf} rows={allRows} />
          <input className="dfo-search" placeholder="water or words…" value={filters.q}
            onChange={(e) => set({ q: e.target.value })} aria-label="search DFO" />
                  </div>
        )}
        <div className="filterbar-line"><div style={{ width: `${pct}%`, height: "100%", background: "var(--accent)" }} /></div>
      </div>
      {progress && <DfoVintageBanner notes={progress.vintage} />}
      {/* (T520) a DFO write-through that wrote an entries / wording file went to staging: what became of it */}
      <InputsSyncLine reason="write-through" />
      {notice && <div className="notice inline-notice" data-testid="dfo-notice" style={{ margin: "2px 8px", padding: "2px 8px" }}>
        <span style={{ flex: 1 }}>{notice}</span><CloseX onClick={() => setNotice("")} /></div>}
      {error && <div className="errors inline-notice" style={{ margin: "2px 8px", padding: "2px 8px" }}>
        <span style={{ flex: 1 }}>{error}</span><CloseX onClick={() => setError("")} /></div>}
      <div className="main">
        <div className="queue" data-testid="dfo-queue">
          <div className="dfo-find inline-notice" style={{ padding: "4px 6px" }}>
            <input type="search" placeholder="find a water, a wording, a region…" value={find} aria-label="find a water"
              data-testid="dfo-find" onChange={(e) => setFind(e.target.value)} style={{ flex: 1 }} />
            {find && <span className="dim" data-testid="dfo-find-count">{waterCount(rows)} water{waterCount(rows) === 1 ? "" : "s"} · {rows.length} place{rows.length === 1 ? "" : "s"}</span>}
          </div>
          <div className="queue-group">{loaded ? <>{rows.length} place{rows.length === 1 ? "" : "s"} on {waterCount(rows)} water{waterCount(rows) === 1 ? "" : "s"}</> : "loading…"}</div>
          {rows.map((r, i) => {
            const k = waterKey(r);
            const first = i === 0 || waterKey(rows[i - 1]) !== k;
            const mine = rows.filter((x) => waterKey(x) === k);
            const done = mine.every((x) => DONE.has(x.verify));
            const shut = done && !opened[k] && !mine.some((x) => x.id === selected);
            return (<div key={r.id}>
            {first && (
              <div className={`queue-group dfo-water-head${done ? " done" : ""}`} data-testid="dfo-water-head"
                style={done ? { cursor: "pointer" } : undefined} title={done ? (shut ? "every place is marked — click to open" : "click to fold") : undefined}
                onClick={done ? () => setOpened((o) => ({ ...o, [k]: shut })) : undefined}>
                {done && <span>{shut ? "▸ " : "▾ "}</span>}
                {r.water || r.kind} <span className="dim">· r{r.region} · {mine.length} place(s)</span>
                {done && <span className="chip-tag" data-testid="dfo-water-done"> ✓ done ({mine.length})</span>}
              </div>
            )}
            {!shut && <div data-dfo={r.id} className={`queue-row${r.id === selected ? " selected" : ""}`}
              onClick={() => open(r.id)}>
              <div className="name">
                {r.water || r.kind}
                {r.lake_area && <span className="badge sev-chip sev-later" data-testid="dfo-lake-later" title="a sub-area of a lake: needs a lake split, deferred (owner 2026-10-28)">lake split later</span>}
                <span className={`badge dfo-${r.state}`}>{STATE_WORDS[r.state]}
                  {r.state === "needs_cut" && r.need.missing ? ` (${r.need.missing})` : ""}</span>
                {r.verify !== "unmarked" && <span className={`badge verify-${r.verify}`}>{r.verify}</span>}
                {r.fix_vs_current && <span className={`chip-tag fix-vs-${r.fix_vs_current}`} data-testid="dfo-row-fix-vs"
                  title={FIX_VS_WORDS[r.fix_vs_current]}>{r.fix_vs_current === "replaces" ? "replaces extent"
                    : r.fix_vs_current === "same" ? "same as current" : "fix applied"}</span>}
              </div>
              <div className="meta">
                <span>r{r.region}{r.section ? ` · ${r.section}` : ""}</span>
                {r.status !== "active" && (r.historic_only
                  ? <span className="chip-tag" data-testid="dfo-historic-only" title="no longer on the live page and no live wording rewords it: its own place on this water">historic only</span>
                  : <span className="chip-tag">dormant</span>)}
                {r.new_splits > 0 && <span className="chip-tag new">+{r.new_splits} split</span>}
                {r.group && <span className="chip-tag" data-testid="dfo-group-size"
                  title="historic rewordings of this place — one extent decision labels the group">group of {r.group.size}</span>}
                {r.suggested && r.verify === "unmarked" && <span className="chip-tag suggested"
                  title="a suggested extent is waiting on this location">suggested · {r.suggested}</span>}
                {r.fix_proposed && <span className="chip-tag suggested" data-testid="dfo-fix-chip"
                  title="Claude proposed a fix from your note — open the item">fix proposed</span>}
                {r.tidal?.status === "proposed" && <span className="chip-tag suggested" data-testid="dfo-tidal-chip"
                  title={`proposed, not decided — ${r.tidal.why}`}>tidal water? proposed</span>}
                {r.verify === "unmarked" && <PvBadge pv={r.pv} />}
                <span>{r.n_rows} row{r.n_rows === 1 ? "" : "s"}</span>
              </div>
              <div className="meta dfo-text">{r.text || "(the whole water)"}</div>
            </div>}
          </div>); })}
        </div>
        {detail ? (
          <DfoLocation key={detail.row.id} detail={detail} mode={mode}
            nav={{ pos: idx + 1, total: rows.length, onNext: next ? () => open(next) : undefined,
                   onPrev: prev ? () => open(prev) : undefined }}
            onMarked={(reused, saved) => {
              setNotice(saved + (reused.length
                ? ` · already proposed — reused: ${reused.map((r) => `${r.name} (${r.id})`).join(", ")}` : ""));
              loadRows();
              if (next) open(next);
              else api.dfoLocation(detail.row.id).then(setDetail).catch(failed("the DFO location"));
            }} />
        ) : (
          <div className="detail"><div className="empty-state">{selected ? "loading…" : "Select a DFO location from the queue."}</div></div>
        )}
      </div>
    </div>
  );
}

let nextRef = 1;
const newRef = (taken: string[]) => {
  let r = `n${nextRef++}`;
  while (taken.includes(r)) r = `n${nextRef++}`;
  return r;
};

function DfoLocation({ detail, mode, nav, onMarked }: {
  detail: DfoDetail; mode: Mode;
  nav: { pos: number; total: number; onNext?: () => void; onPrev?: () => void };
  onMarked: (reused: { ref: string; id: string; name: string }[], saved: string) => void;
}) {
  const id = detail.row.id;
  const was = detail.mark;
  const [answers, setAnswers] = useState<Partial<Record<Check, CheckAnswer>>>({});
  const [problem, setProblem] = useState("");
  const [note, setNote] = useState("");
  const [reviewer, setName] = useState(getReviewer);
  const [splits, setSplits] = useState<NewSplit[]>([]);
  const [checks, setChecks] = useState<Record<string, AnchorCheck>>({});
  // owner 2026-10-28: the historic group's members to label with this decision ("same extent", on)
  const [keep, setKeep] = useState<Record<string, boolean>>(
    () => Object.fromEntries((detail.group?.members ?? []).map((m) => [m.id, true])));
  const savedAll = useMemo(() => savedFixesOf(was), [was]);
  // a new fix / piece starts on the place's tributary call (its binding, else the DFO title) — owner 2026-11-09
  const tribDefault = tributaryDefault(detail);
  const saved = savedAll[0] ?? null;
  // the editor opens PRE-FILLED with the saved fix: marking again carries it on, never drops it.
  // T524: the extent is a list of PIECES (one editor each); the place binds their union
  const [fixes, setFixes] = useState<ExtentFix[]>(savedAll);
  const fix = fixes[0] ?? null;
  /** set the FIRST piece (the two cuts off one reference); null clears every piece */
  const setFix = (f: ExtentFix | null) => setFixes((cur) => (f ? [f, ...cur.slice(1)] : []));
  /** the pieces with piece `i` as `f` — what a piece's preview resolves (the union) */
  const piecesWith = (i: number, f: ExtentFix): ExtentFix[] => fixes.map((x, j) => (j === i ? f : x))
    .concat(i >= fixes.length ? [f] : []);
  const [savedFc, setSavedFc] = useState<GeoJSON.FeatureCollection | null>(null);
  const [reach, setReach] = useState<(ExtentPreview & { walks_tributaries?: boolean }) | null>(null);
  // "Show tributaries": off by default (the walk is the slow, huge part of a reach); remembered per viewer
  const [showTribs, setShowTribs] = useState<boolean>(() => {
    try { return window.localStorage.getItem("dfo-show-tributaries") === "1"; } catch { return false; }
  });
  const [loadingTribs, setLoadingTribs] = useState(false);
  const toggleTribs = (on: boolean) => {
    setShowTribs(on);
    setLoadingTribs(on);
    try { window.localStorage.setItem("dfo-show-tributaries", on ? "1" : "0"); } catch { /* per-viewer only */ }
  };
  const [fixPreview, setFixPreview] = useState<GeoJSON.FeatureCollection | null>(null);
  // T355: a new split's live preview (the build's resolver), orange on the map
  const [cutPv, setCutPv] = useState<GeoJSON.FeatureCollection | null>(null);
  const [pick, setPick] = useState<{ cb: ((ll: [number, number]) => void) | null }>({ cb: null });
  const [errors, setErrors] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [okMsg, setOkMsg] = useState("");
  const since = useRef(Date.now());

  // what the extent binds now — magenta until a fix is previewed
  useEffect(() => {
    let live = true;
    api.dfoReach(id, showTribs).then((r) => live && setReach(r)).catch(failed("the DFO reach"))
      .finally(() => live && setLoadingTribs(false));
    return () => { live = false; };
  }, [id, showTribs]);

  const splitAtId = (sid: string) => {
    const f = detail.splits.features.find((x) => x.properties.split_id === sid);
    return f ? f.geometry.coordinates : null;
  };
  // the saved extent on the map, teal: the builder's own resolve of it, plus its points
  useEffect(() => {
    let live = true;
    setSavedFc(null);
    if (!saved) return;
    const want = savedAll.flatMap((f) => f.points);
    const pts: GeoJSON.Feature[] = want.flatMap((p, i) => {
      const at = p.kind === "coord" ? p.coord : p.kind === "split" && p.split_id ? splitAtId(p.split_id) : null;
      return at ? [{ type: "Feature", properties: { label: `saved ${i + 1}: ${pointWords(p, (x) => x)}` },
                     geometry: { type: "Point", coordinates: at } } as GeoJSON.Feature] : [];
    });
    api.dfoExtentPreview(id, savedAll.length > 1 ? savedAll : saved, showTribs)
      .then((r) => live && setSavedFc({ type: "FeatureCollection",
        features: [...((r.geojson?.features ?? []) as GeoJSON.Feature[]), ...pts] }))
      // (T434) the reach did not load: reported; the saved points (local) are still drawn
        .catch((e) => { reportFailure("the DFO saved reach", e); if (live) setSavedFc({ type: "FeatureCollection", features: pts }); });
    return () => { live = false; };
  }, [id, savedAll, showTribs]);  // eslint-disable-line react-hooks/exhaustive-deps

  const savedNew = (was?.new_splits ?? []) as NewSplit[];
  const items: NamedRef[] = detail.items.map((i) => ({ id: i.id, name: i.name }));
  // T210: the proposals other marks made on this river (deduped), not yet carried by this mark
  const shared = detail.proposed.filter((p) => !splits.some((n) => n.ref === p.id));
  const mapSplits: NamedRef[] = detail.splits.features.map((f) => ({ id: f.properties.split_id, name: f.properties.name }));
  const splitAt = (sid: string) => {
    const f = detail.splits.features.find((x) => x.properties.split_id === sid);
    return f ? f.geometry.coordinates : null;
  };

  // the map's labelled points: the cuts on the water, plus every NEW split proposed (amber)
  const named: EntrySplits = useMemo(() => ({
    ...detail.splits,
    features: [
      ...detail.splits.features,
      ...shared.flatMap((p) => p.coord ? [{
        type: "Feature" as const, geometry: { type: "Point" as const, coordinates: p.coord },
        properties: { split_id: `proposed:${p.id}`, label: `PROPOSED ${p.name || p.anchor}`
                        + (p.n_marks > 1 ? ` (${p.n_marks} marks)` : ""),
                      water: "", name: `PROPOSED ${p.name || p.anchor}`, rules: ["extent"], parts: [],
                      to_verify: true, grade: null, item_id: p.on_item },
      }] : []),
    ],
  }), [detail.splits, detail.proposed, splits, checks]);  // eslint-disable-line react-hooks/exhaustive-deps

  // owner 2026-11-02: THE EDITABLE POINTS — each new cut where it lands (an offset cut at its offset),
  // and each REFERENCE an offset cut is measured from (hollow, not a cut); all draggable on the map
  const editPoints = useMemo(() => editPointsOf(splits, checks), [splits, checks]);
  const [moveErr, setMoveErr] = useState("");
  async function movePoint(pid: string, ll: [number, number]) {
    setMoveErr("");
    try {
      const got = await movedSplits(id, pid, ll, splits, checks);
      setSplits(got.splits);
      setChecks((m) => ({ ...m, ...got.checks }));
    } catch (e) { setMoveErr(String(e instanceof Error ? e.message : e)); }
  }
  async function setOffset(ref: string, off: number | null) {
    const n = splits.find((x) => x.ref === ref);
    if (!n) return;
    const ns = { ...n, offset_m: off || null };
    const got = await api.dfoAnchor(id, ns);
    if (!got.ok) { setMoveErr(`${ref}: ${got.why}`); return; }
    setMoveErr("");
    setSplits(splits.map((x) => (x.ref === ref ? ns : x)));
    setChecks((m) => ({ ...m, [ref]: got }));
  }

  const reachFc = (reach?.geojson ?? null) as GeoJSON.FeatureCollection | null;
  const preview = fixPreview ?? reachFc;

  // THE SUGGESTED EXTENT (never applied): its points violet on the map, and its reach when every
  // point already exists; "accept suggestion" pre-fills the editor — "Save extent" is the write
  const sugg = detail.suggestion ?? null;
  const [suggestedFc, setSuggestedFc] = useState<GeoJSON.FeatureCollection | null>(null);
  const [accepted, setAccepted] = useState("");
  useEffect(() => {
    let live = true;
    setSuggestedFc(null);
    const sg = sugg?.suggestion;
    if (!sg) return;
    const pts: GeoJSON.Feature[] = sg.points.flatMap((p, i) => {
      const meta = sugg?.points?.[i];
      const prop = p.kind === "new" ? detail.proposed.find((x) => x.id === p.split_id) : undefined;
      const at = meta?.coord ?? prop?.coord ?? (p.kind === "split" && p.split_id ? splitAtId(p.split_id) : null);
      return at ? [{ type: "Feature", properties: { label: `SUGGESTED ${i + 1}: ${meta?.label ?? p.split_id ?? ""}` },
                     geometry: { type: "Point", coordinates: at } } as GeoJSON.Feature] : [];
    });
    setSuggestedFc({ type: "FeatureCollection", features: pts });
    const built = sg.op === "whole" || sg.points.every((p) => p.kind === "split");
    const want = { whole: 0, upstream_of: 1, downstream_of: 1, between: 2 }[sg.op as string];
    if (built && want === sg.points.length) {
      api.dfoExtentPreview(id, { scope: "entry", op: sg.op, points: sg.points, items: sg.items,
                                 tributaries: sg.tributaries }, showTribs)
        .then((r) => live && r.resolvable && setSuggestedFc({ type: "FeatureCollection",
          features: [...((r.geojson?.features ?? []) as GeoJSON.Feature[]), ...pts] }))
        .catch(failed("the DFO reach preview"));
    }
    return () => { live = false; };
  }, [id, sugg, showTribs]);  // eslint-disable-line react-hooks/exhaustive-deps

  function acceptSuggestion() {
    const got = suggestionFix(sugg, [...splits.map((n) => n.ref), ...detail.proposed.map((p) => p.id),
                                     ...savedNew.map((n) => n.ref)]);
    if (!got) return;
    setFixes(got.fixes);
    setFixPreview(null);
    if (got.newSplits.length) setSplits((cur) => [...cur.filter((n) => !got.newSplits.some((x) => x.ref === n.ref)),
                                                  ...got.newSplits]);
    setAccepted(`suggestion pre-filled in the editor below${got.newSplits.length
      ? ` with ${got.newSplits.length} new split${got.newSplits.length === 1 ? "" : "s"}` : ""} — check it, then Save extent (nothing is saved yet)`);
  }

  function setAnswer(c: Check, a: CheckAnswer) { setAnswers((x) => ({ ...x, [c]: a })); }

  // A PROPOSED FIX SAYS THE PLACEMENT IS WRONG (owner 2026-11-02, Little Qualicum): the mark model refuses
  // an extent fix or a new split on a mark whose P is y. So the moment the reviewer ADDS one (a fix that
  // differs from the saved one, or a new split), P is set to n — visibly, labelled — unless they already
  // answered P; set back to y, the save is held here with a warning, never refused by the server.
  // (the model's T210 exception: a fix naming only EXISTING cuts may say P y — the cuts are right)
  const proposing = splits.length > 0 || fixes.some((f) => f.points.some((p) => p.kind !== "split"));
  const addedProposal = splits.length > 0 || JSON.stringify(fixes) !== JSON.stringify(savedAll);
  const [autoN, setAutoN] = useState(false);
  useEffect(() => {
    if (addedProposal && proposing && answers.P === undefined) { setAnswers((x) => ({ ...x, P: "n" })); setAutoN(true); }
  }, [addedProposal, proposing]);  // eslint-disable-line react-hooks/exhaustive-deps
  /** why Save / Record is held, or "" — the server's own rules, said before the save */
  const held = proposing && answers.P === "y"
    ? "a new split (or a new point in the extent) says the placement is wrong: answer P n (or ?), or remove it"
    : CHECKS.some((c) => answers[c] === "n") && !(problem && note.trim())
      ? "an n answer needs a problem (pick one below) and a note saying what is wrong"
      : "";

  async function submit(defer = false, extentOnly = false) {
    setErrors([]); setOkMsg("");
    if (held && !defer) { setErrors([`Not saved yet — ${held}.`]); return; }
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      // a point of the fix that names a split another mark proposed rides in this mark too
      // (the mark is self-contained); the server folds it back onto the proposal's id
      const used = fixes.flatMap((f) => f.points.filter((p) => p.kind === "new").map((p) => p.split_id));
      const carried = [...splits, ...shared.filter((p) => used.includes(p.id))
        .map((p) => asNewSplit(p, detail.verbatim.specific_area)),
        // a point of the saved fix that names a split THIS location's saved mark proposed
        ...savedNew.filter((n) => used.includes(n.ref) && !splits.some((x) => x.ref === n.ref)
          && !shared.some((p) => p.id === n.ref))];
      // "Save extent" on its own: nothing answered yet means Place is not settled (?), so the
      // mark can be recorded; any answer the reviewer gave is kept as it is
      const sendAnswers = extentOnly && !Object.keys(answers).length ? { P: "?" as CheckAnswer } : answers;
      const got = await api.postMark({
        scope: "dfo", target: id, answers: sendAnswers, problem: problem || null, note, reviewer: reviewer.trim(),
        seconds: Math.round((Date.now() - since.current) / 100) / 10,
        ...(fixes.length ? { extent_fix: fixes } : {}),
        ...(carried.length ? { new_splits: carried } : {}),
        ...(defer ? { verdict: "deferred" as const } : {}),
        ...(groupTargets.length > 1 ? { targets: groupTargets } : {}),
      });
      const said = `saved ✓ ${detail.row.water || id}`
        + (groupTargets.length > 1 ? ` (+${groupTargets.length - 1} grouped)` : "")
        + (fixes.length ? ` — your extent: ${piecesWords(fixes, splitNameOf, itemNameOf)}` : " — mark recorded (no extent fix)");
      setOkMsg(said);
      onMarked(got.reused ?? [], said);
    } catch (e) {
      const fe = (e as EntryRefused).fieldErrors;
      setErrors([`NOT SAVED — ${fe ? "refused by the server" : "the save failed"}:`,
        ...(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [String(e)])]);
      window.setTimeout(() => document.querySelector('[data-testid="dfo-errors"]')
        ?.scrollIntoView({ block: "nearest" }), 0);
    } finally {
      setBusy(false);
    }
  }
  const groupTargets = (detail.group?.members ?? []).filter((m) => m.this || keep[m.id] !== false)
    .map((m) => m.id);
  // T449: the tidal-water proposal (`dfo.tidal_of`) — the owner's call is a mark like any other: accept is every
  // check y with call "tidal water" (verified); reject is deferred with call "not tidal" (still to curate)
  const tidal = detail.row.tidal ?? null;
  async function decideTidal(call: "tidal water" | "not tidal") {
    setErrors([]); setOkMsg("");
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      const accept = call === "tidal water";
      await api.postMark({
        scope: "dfo", target: id, reviewer: reviewer.trim(), call,
        seconds: Math.round((Date.now() - since.current) / 100) / 10,
        ...(accept ? { answers: { T: "y", R: "y", P: "y" }, note: note.trim() || `wholly in tidal water: ${tidal?.why ?? ""}` }
          : { answers: {}, verdict: "deferred" as const,
              note: note.trim() || "not wholly in tidal water — its freshwater extent is still to curate" }),
      });
      const said = `saved ✓ ${detail.row.water || id} — ${accept ? "tidal water (binds nothing)" : "not tidal"}`;
      setOkMsg(said);
      onMarked([], said);
    } catch (e) {
      const fe = (e as EntryRefused).fieldErrors;
      setErrors([`NOT SAVED — ${fe ? "refused by the server" : "the save failed"}:`,
        ...(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [String(e)])]);
    } finally {
      setBusy(false);
    }
  }
  // Enter records the mark; y n ? answer the next unanswered check
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === "Enter") { void submit(); ev.preventDefault(); return; }
      const a = ev.key === "y" ? "y" : ev.key === "n" ? "n" : ev.key === "?" ? "?" : null;
      if (!a) return;
      const c = CHECKS.find((k) => !answers[k]);
      if (c) { setAnswer(c, a as CheckAnswer); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });  // eslint-disable-line react-hooks/exhaustive-deps

  const splitNameOf = (sid: string) => mapSplits.find((x) => x.id === sid)?.name
    ?? shared.find((p) => p.id === sid)?.name ?? sid;
  const itemNameOf = (iid: string) => items.find((x) => x.id === iid)?.name ?? iid;
  const wrongChecks = CHECKS.filter((c) => answers[c] === "n");
  const problems = wrongChecks.flatMap((c) => mode.problems[c] ?? []);
  const loc = detail.location;
  const ex = loc.binding.extents;

  return (
    <div className="detail review-mode" data-testid="dfo-detail">
      <div className="detail-cols">
        <div className="detail-content">
          <div className="verify-bar">
            <div className="verify-row">
              <strong>{detail.row.water || detail.row.kind}</strong>
              <span className="dim">r{detail.row.region}{loc.section ? ` · section ${loc.section}` : ""} · {loc.status}</span>
              {was && <span className={`badge verify-${was.status}`} title={was.reasons.join(", ")}>
                {was.status}{was.reasons.length ? ` (${was.reasons.join(", ")})` : ""} · {was.reviewer} {was.date}</span>}
              <span className="spacer" style={{ flex: 1 }} />
              <span className="verify-nav">
                <button className="btn small" disabled={!nav.onPrev} onClick={nav.onPrev}>‹ k</button>
                <span className="dim">{nav.pos || "–"}/{nav.total}</span>
                <button className="btn small" disabled={!nav.onNext} onClick={nav.onNext}>j ›</button>
              </span>
            </div>
          </div>

          {errors.length > 0 && (
            <div className="errors dfo-errors" role="alert" data-testid="dfo-errors"
              style={{ position: "static", margin: "6px 0" }}>
              {errors.map((e, i) => <div key={i} style={i === 0 && e.startsWith("NOT SAVED") ? { fontWeight: 700 } : undefined}>{e}</div>)}
            </div>
          )}
          {okMsg && <div className="notice dfo-saved inline-notice" role="status" data-testid="dfo-saved">
            <span style={{ flex: 1 }}>{okMsg}</span><CloseX onClick={() => setOkMsg("")} /></div>}

          <div className="source-pin" data-testid="dfo-source">
            <div className="evidence-head">
              <h3 style={{ margin: 0 }}>{detail.verbatim.waters}</h3>
              {detail.page_url && <a href={detail.page_url} target="_blank" rel="noreferrer">the DFO page ↗</a>}
              <span className="dim">{loc.location_id}</span>
            </div>
            <div className="verbatim" data-testid="dfo-verbatim">{detail.verbatim.specific_area || "(no specific area — the whole water)"}</div>
            <div className="dim" style={{ fontSize: 12 }}>
              op <code>{loc.source_text.op ?? "—"}</code>
              {(loc.source_text.anchor_types ?? []).length > 0 && <> · anchors {(loc.source_text.anchor_types ?? []).join(", ")}</>}
              {loc.duplicate_of && <> · reworded copy of <code>{loc.duplicate_of}</code></>}
            </div>
            <div className="coverage" style={{ marginTop: 4 }}>
              {detail.rows.length ? detail.rows.join("\n") : "(no scraped rows ride on this location)"}
            </div>
          </div>

          <div className="section entry-scope" data-testid="dfo-extent">
            <div className="scope-head">
              <h3>The extent now</h3>
              <span className={`badge dfo-${detail.row.state}`}>{STATE_WORDS[detail.row.state]}</span>
              {detail.need.op && <span className="dim">the notice reads as <strong>{detail.need.op}</strong>
                {detail.need.points ? ` (${detail.need.points} cut${detail.need.points === 1 ? "" : "s"})` : ""}</span>}
            </div>
            <div className="ext-row">
              <span className="k">water</span>
              {detail.items.length ? detail.items.map((i) => <span key={i.id} className="chip-tag" title={i.id}>{i.name} · MU {i.mus.join(", ")}</span>)
                : <span className="warn-text">not matched to the registry — nothing binds</span>}
              {detail.water?.tributaries != null && <span className="dim">tributaries {detail.water.tributaries ? "yes" : "no"}</span>}
            </div>
            <div className="ext-row">
              <span className="k">extent</span>
              {ex.length ? ex.map((e, i) => (
                <span key={i} className="ext-where">{e.op}{(e.splits ?? []).length ? `(${(e.splits ?? []).join(", ")})` : ""}</span>
              )) : <span className="warn-text">none curated{detail.need.missing ? ` — needs ${detail.need.missing} cut${detail.need.missing === 1 ? "" : "s"}` : ""}</span>}
              {reach && <span className="dim">{reach.resolvable ? `binds ${reach.n_sections} section(s)${tribWords(reach.walks_tributaries, showTribs)} — magenta on the map` : reach.why}</span>}
              <label className="dim" data-testid="dfo-show-tribs"
                title="Off: the map draws only the place's own water(s). On: also walks and draws its tributaries (slow, large).">
                <input type="checkbox" checked={showTribs} onChange={(e) => toggleTribs(e.target.checked)} /> Show tributaries
                {loadingTribs && <span className="dim" data-testid="dfo-tribs-loading"> <style>{"@keyframes spin{to{transform:rotate(360deg)}}"}</style><span className="spinner" aria-hidden="true"
                  style={{ display: "inline-block", width: 10, height: 10, border: "2px solid currentColor", borderTopColor: "transparent", borderRadius: "50%", animation: "spin 0.8s linear infinite", verticalAlign: "-1px" }} /> loading tributaries…</span>}
                {reach?.resolvable && reach.walks_tributaries === false && showTribs ? " (this binding has none)" : ""}
              </label>
            </div>
            {loc.binding.notes.length > 0 && <div className="ext-row"><span className="k">notes</span>{loc.binding.notes.join(" · ")}</div>}
            {detail.splits.unplaced.length > 0 && (
              <div className="ext-row dim">not on the map: {detail.splits.unplaced.map((u) => u.name).join(", ")}</div>
            )}
            {detail.group && (
              <div className="ext-row" data-testid="dfo-group">
                <span className="k">group of {detail.group.members.length}</span>
                <span className="dim"> one extent decision labels every member ticked (one mark each)</span>
                <ul className="named-splits">
                  {detail.group.members.map((m) => (
                    <li key={m.id}>
                      <label className="check" title={m.why ?? "the current place"}>
                        <input type="checkbox" checked={m.this || keep[m.id] !== false} disabled={m.this}
                          data-testid="dfo-group-keep"
                          onChange={(e) => setKeep((k) => ({ ...k, [m.id]: e.target.checked }))} />
                        {" "}same extent
                      </label>{" "}
                      {m.this ? <strong>{m.text || "(whole water)"}</strong>
                        : <a href={`#${DFO_HASH}${encodeURIComponent(m.id)}`}>{m.text || "(whole water)"}</a>}
                      <span className="dim"> {m.status} · {m.verify}{m.why ? ` — ${m.why}` : " — current"}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {(detail.places?.length ?? 0) > 0 ? (
              <DfoPlaces places={detail.places!} href={(x) => `#${DFO_HASH}${encodeURIComponent(x)}`}
                onAttached={(msg) => setOkMsg(msg)} />
            ) : detail.siblings.length > 0 && (
              <details className="ext-row">
                <summary className="dim">{detail.siblings.length} other reach{detail.siblings.length === 1 ? "" : "es"} on this water</summary>
                <ul className="named-splits">
                  {detail.siblings.map((s) => (
                    <li key={s.id}><a href={`#${DFO_HASH}${encodeURIComponent(s.id)}`}>{s.text || "(whole water)"}</a>
                      <span className="dim"> {s.status}{s.section ? ` · ${s.section}` : ""}</span></li>
                  ))}
                </ul>
              </details>
            )}
          </div>

          {detail.fix_vs_current && (() => {
            const fv = detail.fix_vs_current;
            const many = (xs: unknown[]) => xs.length > 1;
            const words = (xs: typeof fv.current) => xs.length
              ? xs.map((e, i) => (many(xs) ? `piece ${i + 1}: ` : "") + extentWords(e, splitNameOf, itemNameOf)).join(" + ")
              : "none curated";
            const tw = (t: boolean | null) => (t == null ? "" : t ? " · with tributaries" : " · without tributaries");
            return (
              <div className={`section fix-vs fix-vs-${fv.state}`} data-testid="dfo-fix-vs-current" role="status"
                style={fv.state === "replaces" ? { border: "2px solid #b45309", background: "#fff7ed", padding: 8 } : undefined}>
                <div className="ext-row"><strong data-testid="dfo-fix-vs-state">{FIX_VS_WORDS[fv.state]}</strong></div>
                {fv.state === "replaces" ? (<>
                  <div className="ext-row"><span className="k" style={{ color: "#c026d3" }}>current (magenta)</span>
                    <span data-testid="dfo-fix-vs-old" style={{ textDecoration: "line-through" }}>{words(fv.current)}{tw(fv.tributaries.current)}</span></div>
                  <div className="ext-row"><span className="k" style={{ color: "#0d9488" }}>your fix (teal)</span>
                    <span data-testid="dfo-fix-vs-new">{words(fv.fix)}{tw(fv.tributaries.fix ?? fv.tributaries.current)}</span></div>
                </>) : (
                  <div className="ext-row"><span className="k">extent</span><span>{words(fv.current)}{tw(fv.tributaries.current)}</span></div>
                )}
              </div>
            );
          })()}
          <div className="section saved-extent" data-testid="dfo-saved-extent">
            {saved ? (
              <div className="ext-row">
                <span className="k" style={{ color: "#0d9488" }}>your extent (saved {was?.date ?? "?"})</span>
                <span data-testid="dfo-saved-words">{piecesWords(savedAll, splitNameOf, itemNameOf)}</span>
                <span className="dim">teal on the map{was?.reviewer ? ` · ${was.reviewer}` : ""}</span>
              </div>
            ) : (
              <div className="ext-row dim">no extent saved on this location yet — curate it below</div>
            )}
          </div>

          {tidal && (
            <div className="section dfo-tidal-proposal" data-testid="dfo-tidal-proposal">
              <div className="ext-row">
                <span className="k" style={{ color: "#b45309" }}>{tidal.status === "proposed" ? "proposed: tidal water"
                  : tidal.status === "accepted" ? "tidal water" : "not tidal (rejected)"}</span>
                <span data-testid="dfo-tidal-why">{tidal.why}</span>
              </div>
              {tidal.status === "proposed" && <>
                <div className="ext-row dim" style={{ fontSize: 12 }}>
                  Accept: the place lies wholly in tidal water — it binds nothing on the freshwater map and is not
                  counted as missing. Reject: it keeps needing its freshwater extent (the note below is kept).
                </div>
                <div className="ext-row">
                  <button type="button" className="btn small" data-testid="dfo-tidal-accept" disabled={busy}
                    onClick={() => void decideTidal("tidal water")}>accept: tidal water</button>
                  <button type="button" className="btn small" data-testid="dfo-tidal-reject" disabled={busy}
                    onClick={() => void decideTidal("not tidal")}>reject: not tidal</button>
                </div>
              </>}
            </div>
          )}
          {detail.fix_proposal && (
            <div className="section dfo-fix-proposal" data-testid="dfo-fix-proposal">
              <div className="ext-row">
                <span className="k" style={{ color: "#b45309" }}>{detail.fix_proposal.owner ? "needs you" : "fix proposed"}</span>
                <span data-testid="dfo-fix-why">{detail.fix_proposal.why}</span>
                {detail.fix_proposal.suggestion && (
                  <button type="button" className="btn small" data-testid="dfo-use-fix"
                    title="pre-fill the editor with this fix — nothing is saved until you Save extent"
                    onClick={() => {
                      const got = suggestionFix({ suggestion: detail.fix_proposal!.suggestion } as DfoSuggestion,
                        [...splits.map((n) => n.ref), ...detail.proposed.map((p) => p.id), ...savedNew.map((n) => n.ref)]);
                      if (!got) return;
                      setFixes(got.fixes);
                      setFixPreview(null);
                      if (got.newSplits.length) setSplits((cur) => [...cur.filter((n) => !got.newSplits.some((x) => x.ref === n.ref)), ...got.newSplits]);
                      setAccepted("fix proposal pre-filled in the editor below — check it, then Save extent (nothing is saved yet)");
                    }}>use this fix</button>
                )}
              </div>
              {accepted && !sugg?.suggestion && <div className="ext-row notice inline-notice" role="status">
                <span style={{ flex: 1 }}>{accepted}</span><CloseX onClick={() => setAccepted("")} /></div>}
              {(detail.fix_proposal.split_edits ?? []).map((e) => (
                <div key={e.split_id + e.field} className="ext-row dim" style={{ fontSize: 12 }}>
                  proposed split edit <code>{e.split_id}</code> {e.field}: {String(e.before)} → <strong>{String(e.after)}</strong> (not applied)
                </div>
              ))}
            </div>
          )}
          {sugg?.suggestion && (
            <div className="section dfo-suggestion" data-testid="dfo-suggestion">
              <div className="ext-row">
                <span className="k" style={{ color: "#7c3aed" }}>suggested</span>
                <span data-testid="dfo-suggestion-words">
                  {sugg.suggestion.words ?? fixWords({ scope: "entry", op: sugg.suggestion.op,
                    points: sugg.suggestion.points, items: sugg.suggestion.items,
                    tributaries: sugg.suggestion.tributaries }, splitNameOf, itemNameOf)}</span>
                <span className={`badge suggest-${sugg.confidence}`} data-testid="dfo-suggestion-confidence">
                  {CONF_WORDS[sugg.confidence] ?? sugg.confidence}{sugg.matched === "partial" ? " · one end open" : ""}</span>
                <button type="button" className="btn small" data-testid="dfo-accept-suggestion"
                  onClick={acceptSuggestion}
                  title="pre-fills the extent editor below — nothing is saved until you press Save extent">
                  accept suggestion</button>
              </div>
              <div className="ext-row dim" style={{ fontSize: 12 }}>{sugg.reason}</div>
              {sugg.caveat && (
                <div className="ext-row" data-testid="dfo-suggestion-caveat" style={{ fontSize: 12, color: "#b45309" }}>
                  read before accepting: {sugg.caveat}</div>
              )}
              <SuggestionCoverage sugg={sugg} />
              {sugg.suggestion.new_splits.length > 0 && (
                <div className="ext-row dim" style={{ fontSize: 12 }}>
                  new split{sugg.suggestion.new_splits.length === 1 ? "" : "s"}:{" "}
                  {sugg.suggestion.new_splits.map((n) => `${anchorWords(n)} (${n.confidence ?? defaultConfidence(n.anchor)} confidence)`).join(" · ")} — violet on the map</div>
              )}
              {sugg.hunt?.status === "found" && <HuntProposals hunt={sugg.hunt} />}
              {accepted && <div className="ext-row notice inline-notice" role="status" data-testid="dfo-suggestion-accepted">
                <span style={{ flex: 1 }}>{accepted}</span><CloseX onClick={() => setAccepted("")} /></div>}
            </div>
          )}
          {!sugg?.suggestion && detail.suggestion?.hunt?.status === "not_found" && (
            <div className="section dfo-suggestion" data-testid="dfo-hunt-not-found">
              <div className="ext-row">
                <span className="k" style={{ color: "#7c3aed" }}>split hunt</span>
                <span>not found — {detail.suggestion.hunt.why}</span>
              </div>
              <SuggestionCoverage sugg={detail.suggestion} />
            </div>
          )}

          <NewSplits id={id} items={detail.items} hints={detail.hints} quote={detail.verbatim.specific_area}
            op={detail.need.op} shared={shared} value={splits} onChange={setSplits} checks={checks}
            onChecked={(ref, c) => setChecks((m) => ({ ...m, [ref]: c }))}
            onPick={(cb) => setPick({ cb })} picking={!!pick.cb} onOffset={setOffset} onPreview={setCutPv}
            onCuts={(refs) => {
              // two cuts off one reference ("100 m up and down of the weir"): the extent between them,
              // pre-filled in the editor (Save extent is still the write)
              if (refs.length !== 2) return;
              setFix({ scope: "entry", op: "between", points: refs.map((r) => ({ kind: "new" as const, split_id: r })),
                       items: detail.items.slice(0, 1).map((i) => i.id), tributaries: fix?.tributaries ?? null });
              setFixPreview(null);
            }} />
          {moveErr && <div className="fix-row warn-text inline-notice" data-testid="dfo-move-refused">
            <span style={{ flex: 1 }}>not moved: {moveErr}</span><CloseX onClick={() => setMoveErr("")} /></div>}

          <div className="section">
{(fixes.length ? fixes : [undefined]).map((pc, i) => (
              <ExtentFixEditor key={i} defaultOpen scope="entry" entryId={id} piece={i}
                currentOp={i === 0 ? detail.need.op ?? ex[0]?.op : "downstream_of"}
                mapSplits={mapSplits} waters={items} value={pc}
                onChange={(f) => setFixes((cur) => (f ? piecesWith(i, f) : cur.filter((_x, j) => j !== i)))}
                defaultTributaries={tribDefault.call} defaultTributariesWhy={tribDefault.why} tributariesDrawn={showTribs}
                onPreview={(fc) => setFixPreview(fc)}
                onPick={(cb) => setPick({ cb })} splitAt={splitAt}
                newSplits={[
                  ...splits.map((n) => ({ id: n.ref, name: `${n.ref}: ${anchorWords(n)}` })),
                  ...shared.map((p) => ({ id: p.id, name: `${p.id}: ${proposedWords(p)} (proposed)` })),
                  ...savedNew.filter((n) => !splits.some((x) => x.ref === n.ref) && !shared.some((p) => p.id === n.ref))
                    .map((n) => ({ id: n.ref, name: `${n.ref}: ${anchorWords(n)} (in your saved mark)` })),
                ]}
                sources={["map", "search", "new"]}
                previewFn={(f) => api.dfoExtentPreview(id, fixes.length > 1 || i > 0 ? piecesWith(i, f) : f, showTribs)} />
            ))}
            {fixes.length > 0 && (
              <div className="fix-row">
                <button type="button" className="btn small" data-testid="dfo-add-piece"
                  title="a place on more than one water (the Chilliwack below the Slesse line AND the Sumas below the pump station): one more piece — the place binds the union"
                  onClick={() => setFixes((cur) => [...cur, { scope: "entry", op: "downstream_of", points: [],
                                                              items: [], tributaries: tribDefault.call }])}>
                  + add another piece</button>
                {fixes.length > 1 && <span className="dim">{fixes.length} pieces — the place binds their union;
                  "preview on map" on any piece shows the union</span>}
              </div>
            )}
            <div className="fix-row" style={{ marginTop: 6 }}>
              <button type="button" className="btn primary small" disabled={busy || !!held} data-testid="dfo-save-extent"
                onClick={() => void submit(false, true)}
                title="records a mark carrying this extent (Place ? unless you answered)">
                {busy ? "saving…" : "Save extent"}</button>
              <span className="dim">{fixes.length ? "writes a new mark with this extent; the latest mark wins"
                : "nothing to save yet — set the op and its points"}</span>
            </div>
          </div>

          <div className="checklist" data-testid="dfo-checklist">
            <div className="checklist-head"><strong>Review</strong> <span className="dim">y n ? answer the next check · Enter records · j k move</span></div>
            {CHECKS.map((c) => (
              <div key={c} className="check-row">
                <strong>{c}</strong><span className="check-q">{QUESTIONS[c]}
                  {c === "P" && proposing && answers.P === "n" && <span className="chip-tag" data-testid="dfo-p-auto"
                    title={autoN ? "set to n when you proposed a fix — change it if you mean ?" : undefined}
                    style={{ marginLeft: 6 }}>placement wrong — fix proposed</span>}</span>
                <span className="check-a">
                  {(["y", "n", "?"] as CheckAnswer[]).map((a) => (
                    <button key={a} type="button" className={`ans ans-${a === "?" ? "q" : a}${answers[c] === a ? " on" : ""}`}
                      aria-pressed={answers[c] === a} data-testid={`dfo-${c}-${a}`} onClick={() => setAnswer(c, a)}>{a}</button>
                  ))}
                </span>
              </div>
            ))}
            {wrongChecks.length > 0 && (
              <div className="check-wrong">
                <select aria-label="problem" value={problem} onChange={(e) => setProblem(e.target.value)}>
                  <option value="">— the problem —</option>
                  {problems.map((p) => <option key={p} value={p}>{p}</option>)}
                </select>
              </div>
            )}
            <textarea className="check-note" placeholder="note (what is wrong, where the cut is)" value={note}
              onChange={(e) => setNote(e.target.value)} />
            {held && <div className="warn-text" role="alert" data-testid="dfo-held">Save held: {held}.</div>}
            <div className="verdict-bar" style={{ position: "static", margin: 0 }}>
              <input placeholder="your name" value={reviewer} onChange={(e) => setName(e.target.value)} aria-label="reviewer" />
              <button className="btn primary" disabled={busy || !!held} data-testid="dfo-record" onClick={() => void submit()}>
                {busy ? "recording…" : "Record mark"}</button>
              <button className="btn small" disabled={busy} onClick={() => void submit(true)}>defer</button>
              {(fixes.length > 0 || splits.length > 0) && <span className="dim">carries {splits.length ? `${splits.length} new split${splits.length === 1 ? "" : "s"}` : ""}{fixes.length && splits.length ? " + " : ""}{fixes.length ? (fixes.length > 1 ? `an extent of ${fixes.length} pieces` : "an extent fix") : ""} — proposals only (T185)</span>}
            </div>
            {answers.P === "y" && proposing && (
              <div className="warn-text" data-testid="dfo-p-hint">
                A cut that is not built yet (a new or proposed split) means Place is ? or n. With only the existing
                cuts, Place y records as it is.</div>
            )}
          </div>
          {was?.new_splits && was.new_splits.length > 0 && (
            <div className="section dim" style={{ fontSize: 12 }}>
              In the mark in force: {was.new_splits.map((n) => `${n.ref} ${anchorWords(n)}`).join(" · ")}
            </div>
          )}
        </div>
        <div className="detail-map">
          <div className="dim" style={{ fontSize: 12, marginBottom: 4 }}>
            magenta: {fixPreview ? "the proposed extent" : "what the extent binds now"} ·{" "}
            <span style={{ color: "#0d9488" }}>teal: your saved extent</span> ·{" "}
            <span style={{ color: "#7c3aed" }}>violet dashed: the suggestion</span> · amber labels: NEW splits proposed in this mark
          </div>
          <MapPanel itemId={detail.items[0]?.id ?? null} alsoItemIds={detail.items.slice(1).map((i) => i.id)}
            namedSplits={named} preview={preview} saved={savedFc} suggested={suggestedFc} onPick={pick.cb}
            editPoints={editPoints} onMovePoint={(pid, ll) => void movePoint(pid, ll)} cutPreview={cutPv} />
        </div>
      </div>
    </div>
  );
}

export function proposedWords(p: ProposedSplit): string {
  return anchorWords(asNewSplit(p, ""))
    + (p.n_marks > 1 ? ` · proposed on ${p.n_marks} marks` : "")
    + ` · ${p.confidence} confidence`;
}

export function anchorWords(n: NewSplit): string {
  const off = n.offset_m ? `${Math.abs(n.offset_m)} m ${n.offset_m > 0 ? "above" : "below"} ` : "";
  const end = n.area_end === "upstream" ? "where it enters " : n.area_end === "downstream" ? "where it leaves " : "";
  const what = n.anchor === "confluence" ? `confluence of ${n.name || n.item_id || "?"}`
    : n.anchor === "lake_inlet" ? `inlet of ${n.name || n.item_id || "?"}`
    : n.anchor === "lake_outlet" ? `outlet of ${n.name || n.item_id || "?"}`
    : n.anchor === "lake" ? `both ends of ${n.name || n.item_id || "?"}`
    : n.anchor === "split" ? `split ${n.split_id}`
    : n.anchor === "area_boundary" ? `${end}${n.area || "?"} (${n.area_layer || "parks_bc"} boundary)`
      + (n.wsc_descendants ? " + its WSC subtree" : "") + ((n.exclude_gnis ?? []).length ? ` · not gnis ${(n.exclude_gnis ?? []).join(", ")}` : "")
    : n.anchor === "border" ? `B.C. border${n.name ? ` (${n.name})` : ""}`
    : `${n.anchor}${n.name ? ` ${n.name}` : ""}`;
  return off + what + (n.side ? ` · extent ${n.side}` : "");
}

/* NEW SPLIT PROPOSALS (T164, T210; one panel with a type picker, T355). A split is proposed by its
   TYPE — point, confluence, lake (inlet / outlet / both), area boundary, border — each filling exactly
   the `marks.NewSplit` fields its splits.json anchor needs (`proposals.split_record`), with an offset
   on every type the build can offset. The cut is PREVIEWED LIVE by the build's own resolver
   (GET /split-preview: `resolve_split_defs` on the build's chains) — nothing here computes geometry.
   A cut already proposed (any mark on this river) is the SAME split: reused, never entered twice, and
   EDITED IN PLACE through this mark (`NewSplit.amends`: the edit replaces that cut, keeping its id).
   Recorded in the mark, never in splits.json. */
type SplitType = "point" | "confluence" | "lake" | "area_boundary" | "border" | "split";
const TYPES: { v: SplitType; words: string; hint: string }[] = [
  { v: "point", words: "point", hint: "a bridge, dam, sign or map point: click the river" },
  { v: "confluence", words: "confluence", hint: "a tributary's mouth: click it, or pick it from the list" },
  { v: "lake", words: "lake", hint: "a lake's outlet, inlet or both ends: click the lake" },
  { v: "area_boundary", words: "area boundary", hint: "where the water crosses a park / admin polygon: click it" },
  { v: "border", words: "border", hint: "where the water crosses the B.C. outline (a click picks the crossing)" },
];
const POINT_KINDS: { v: AnchorKind; words: string }[] = [
  { v: "bridge", words: "bridge" }, { v: "dam", words: "dam" }, { v: "sign", words: "sign" }, { v: "coord", words: "map point" },
];
const LAKE_EDGES: { v: AnchorKind; words: string }[] = [
  { v: "lake_outlet", words: "outlet" }, { v: "lake_inlet", words: "inlet" }, { v: "lake", words: "both ends" },
];
/** the anchors the build can offset (`marks.OFFSET_ANCHORS`) until the server's list arrives */
// (T434) the anchor policy comes from the server only (GET /api/split-anchors); a failed read is shown, never replaced
// by a copy kept here (a second copy of the policy drifts)
let anchorsOnce: Promise<SplitAnchors> | null = null;
const loadAnchors = () => (anchorsOnce ??= api.splitAnchors().catch((e) => { anchorsOnce = null; throw e; }));

export const typeOf = (a: AnchorKind): SplitType =>
  a === "confluence" || a === "area_boundary" || a === "border" || a === "split" ? a
    : a.startsWith("lake") ? "lake" : "point";

/** the draft as another type: the fields the new type does not have are dropped (pure) */
export function retype(d: NewSplit, t: SplitType, offsettable: AnchorKind[]): NewSplit {
  const anchor: AnchorKind = t === "point" ? (typeOf(d.anchor) === "point" ? d.anchor : "coord")
    : t === "lake" ? (typeOf(d.anchor) === "lake" ? d.anchor : "lake_outlet") : t;
  const keep: NewSplit = { ref: d.ref, anchor, on_item: d.on_item, quote: d.quote, side: d.side,
    confidence: d.confidence ?? null, amends: d.amends ?? null,
    offset_m: offsettable.includes(anchor) ? d.offset_m ?? null : null };
  if (t === typeOf(d.anchor)) return { ...d, ...keep };
  return { ...keep, name: null, coord: t === "point" || t === "border" ? d.coord ?? null : null };
}

/** is the draft enough to resolve? (the server says the rest) */
export function draftReady(d: NewSplit | null): boolean {
  if (!d) return false;
  switch (typeOf(d.anchor)) {
    case "point": return !!d.coord;
    case "confluence": case "lake": return !!((d.item_id ?? "").trim() || (d.name ?? "").trim());
    case "area_boundary": return !!(d.area ?? "").trim();
    case "split": return !!d.split_id;
    default: return true;
  }
}

/** the preview as map features: the cuts, an offset cut's reference, an area's edge (pure) */
export function previewFc(pv: SplitPreview | null, mouths: Tributary[] = []): GeoJSON.FeatureCollection | null {
  const feats: GeoJSON.Feature[] = mouths.map((t) => ({ type: "Feature", properties: { kind: "mouth", label: t.name },
    geometry: { type: "Point", coordinates: t.mouth } }) as GeoJSON.Feature);
  if (pv?.outline) feats.push(pv.outline as GeoJSON.Feature);
  for (const c of pv?.reference ?? [])
    feats.push({ type: "Feature", properties: { kind: "reference", label: "reference" },
                 geometry: { type: "Point", coordinates: c.coord } } as GeoJSON.Feature);
  for (const c of pv?.cuts ?? [])
    feats.push({ type: "Feature", properties: { kind: "cut", label: `cut · ${c.measure.toFixed(0)} m` },
                 geometry: { type: "Point", coordinates: c.coord } } as GeoJSON.Feature);
  return feats.length ? { type: "FeatureCollection", features: feats } : null;
}

/** one autocomplete choice: `key` is what is picked, `label` shown, `sub` dim beside it */
export interface Choice { key: string; label: string; sub?: string }

/** T355: AN AUTOCOMPLETE BOX — `fetch(q)` answers as you type (150 ms debounce); ↑ ↓ move, Enter or a
 *  click picks, Esc closes. Picking hands back the choice exactly as the source spells it. */
export function Autocomplete({ value, placeholder, fetch, onPick, onText, testid, label, minChars = 1 }: {
  value: string; placeholder: string; fetch: (q: string) => Promise<Choice[]>; onPick: (c: Choice) => void;
  onText?: (t: string) => void; testid?: string; label: string; minChars?: number;
}) {
  const [q, setQ] = useState(value);
  const [hits, setHits] = useState<Choice[]>([]);
  const [open, setOpen] = useState(false);
  const [at, setAt] = useState(0);
  useEffect(() => { setQ(value); }, [value]);
  useEffect(() => {
    if (!open || q.trim().length < minChars) { setHits([]); return; }
    let live = true;
    const t = window.setTimeout(() => fetch(q.trim()).then((h) => { if (live) { setHits(h); setAt(0); } })
      .catch(failed("the search")), 150);
    return () => { live = false; window.clearTimeout(t); };
  }, [q, open]);  // eslint-disable-line react-hooks/exhaustive-deps
  const pick = (c: Choice) => { setQ(c.label); setOpen(false); setHits([]); onPick(c); };
  return (
    <span className="fix-search">
      <input className="fix-text" placeholder={placeholder} value={q} aria-label={label} data-testid={testid}
        aria-autocomplete="list" aria-expanded={open && hits.length > 0}
        onFocus={() => setOpen(true)} onBlur={() => window.setTimeout(() => setOpen(false), 150)}
        onChange={(e) => { setQ(e.target.value); setOpen(true); onText?.(e.target.value); }}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown") { setAt((i) => Math.min(i + 1, hits.length - 1)); e.preventDefault(); }
          else if (e.key === "ArrowUp") { setAt((i) => Math.max(i - 1, 0)); e.preventDefault(); }
          else if (e.key === "Enter" && open && hits[at]) { pick(hits[at]); e.preventDefault(); e.stopPropagation(); }
          else if (e.key === "Escape") setOpen(false);
        }} />
      {open && hits.length > 0 && (
        <ul className="fix-hits" role="listbox" data-testid={testid ? `${testid}-hits` : undefined}>
          {hits.map((h, i) => (
            <li key={h.key} role="option" aria-selected={i === at}
              style={i === at ? { background: "#fde68a" } : undefined}
              onMouseDown={(e) => e.preventDefault()} onMouseEnter={() => setAt(i)} onClick={() => pick(h)}>
              {h.label} {h.sub && <span className="dim">{h.sub}</span>}
            </li>
          ))}
        </ul>
      )}
    </span>
  );
}

/** the loaded tributaries matching `q` (contained, case-insensitive), prefix first — a list filter (pure) */
export function tribChoices(tribs: Tributary[], q: string): Choice[] {
  const k = q.trim().toLowerCase();
  return tribs.filter((t) => (t.name ?? "").toLowerCase().includes(k))
    .sort((a, b) => Number(!a.name.toLowerCase().startsWith(k)) - Number(!b.name.toLowerCase().startsWith(k))
      || a.name.localeCompare(b.name))
    .slice(0, 15).map((t) => ({ key: t.id, label: t.name, sub: t.id }));
}

function NewSplits({ id, items, hints, quote, op, shared, value, onChange, checks, onChecked, onPick,
                     picking, onOffset, onCuts, onPreview }: {
  id: string; items: DfoDetail["items"]; hints: DfoDetail["hints"]; quote: string; op: string | null;
  shared: ProposedSplit[];
  value: NewSplit[]; onChange: (v: NewSplit[]) => void;
  checks: Record<string, AnchorCheck>; onChecked: (ref: string, c: AnchorCheck) => void;
  onPick: (cb: ((ll: [number, number]) => void) | null) => void; picking: boolean;
  onOffset: (ref: string, off: number | null) => Promise<void>;
  onCuts: (refs: string[]) => void;
  /** T355: the live preview on the map (null clears it) */
  onPreview: (fc: GeoJSON.FeatureCollection | null) => void;
}) {
  const [anchors, setAnchors] = useState<SplitAnchors | null>(null);
  useEffect(() => { loadAnchors().then(setAnchors, failed("the split anchor policy")); }, []);
  const offsettable: AnchorKind[] = anchors?.offset_anchors ?? [];
  const sideOf = (): NewSplit["side"] =>
    op === "downstream_of" ? "downstream" : op === "upstream_of" ? "upstream" : null;
  const hintFor = (t: SplitType) => hints.find((h) =>
    t === "confluence" ? h.anchor === "confluence" : t === "lake" ? h.anchor.startsWith("lake") : false);

  const [draft, setDraft] = useState<NewSplit | null>(null);
  /** the ref being edited: one in this mark, or a proposal's id (an edit in place, `amends`) */
  const [editing, setEditing] = useState<string | null>(null);
  const [pv, setPv] = useState<SplitPreview | null>(null);
  const [pvBusy, setPvBusy] = useState(false);
  const [said, setSaid] = useState<AnchorCheck | null>(null);
  const [msg, setMsg] = useState("");
  const [reusedWords, setReusedWords] = useState("");
  const [busy, setBusy] = useState(false);
  const [tribs, setTribs] = useState<Tributary[] | null>(null);
  const [areaHits, setAreaHits] = useState<SplitPick["hits"]>([]);
  // owner 2026-11-02: the anchor as a REFERENCE (not a cut): the cuts are N m up / down of it
  const [asRef, setAsRef] = useState(false);
  const [up, setUp] = useState("");
  const [down, setDown] = useState("");
  const type = draft ? typeOf(draft.anchor) : null;
  const canOffset = !!draft && offsettable.includes(draft.anchor);

  const set = (patch: Partial<NewSplit>) => { setSaid(null); setDraft((d) => (d ? { ...d, ...patch } : d)); };
  const start = (t: SplitType) => {
    setReusedWords(""); setSaid(null); setMsg(""); setEditing(null); setAreaHits([]);
    const h = hintFor(t);
    const base: NewSplit = { ref: "", anchor: "coord", on_item: items[0]?.id ?? null, quote, side: sideOf() };
    const d = retype(base, t, offsettable);
    setDraft({ ...d, ...(h ? { name: h.items[0]?.name ?? h.name ?? null, item_id: h.items[0]?.id ?? null } : {}),
               confidence: defaultConfidence(d.anchor) });
  };
  /** edit a cut in place: one of this mark's (by its ref), or a proposal (`amends`: that cut, by its anchor) */
  const edit = (n: NewSplit, amends: NewSplit | null) => {
    setReusedWords(""); setSaid(null); setMsg(""); setAsRef(false); setAreaHits([]);
    setEditing(n.ref);
    setDraft({ ...n, ...(amends ? { amends: { ...amends, amends: null } } : {}) });
  };
  const reset = () => {
    setDraft(null); setEditing(null); setSaid(null); setPv(null); setAreaHits([]); setMsg("");
    onPick(null); onPreview(null);
  };

  // the tributaries of the water, for a confluence (loaded once per water)
  useEffect(() => {
    if (type !== "confluence" || tribs !== null) return;
    api.dfoTributaries(id, draft?.on_item).then(setTribs).catch(failed("the tributaries"));
  }, [type, id, draft?.on_item, tribs]);
  useEffect(() => { setTribs(null); }, [draft?.on_item]);

  // THE LIVE PREVIEW: the build's resolver on the draft, a moment after it changes
  const draftKey = draft ? JSON.stringify({ ...draft, ref: "", name: "", quote: "", confidence: null, side: null }) : "";
  useEffect(() => {
    if (!draft || !draftReady(draft)) { setPv(null); onPreview(type === "confluence" ? previewFc(null, tribs ?? []) : null); return; }
    let live = true;
    const t = window.setTimeout(() => {
      setPvBusy(true);
      api.dfoSplitPreview(id, { ...draft, ref: draft.ref || "draft", name: (draft.name ?? "").trim() || null },
                          value.filter((v) => v.ref !== editing))
        .then((r) => { if (!live) return; setPv(r); onPreview(previewFc(r, type === "confluence" ? tribs ?? [] : [])); })
        .catch((e) => live && setPv({ ok: false, why: String(e) }))
        .finally(() => live && setPvBusy(false));
    }, 350);
    return () => { live = false; window.clearTimeout(t); };
  }, [draftKey, tribs]);  // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => () => { onPick(null); onPreview(null); }, []);  // eslint-disable-line react-hooks/exhaustive-deps

  /** a map click for the draft's type: a point, a tributary's mouth, a lake, an area polygon */
  function pickOnMap() {
    if (picking) { onPick(null); return; }
    setMsg("");
    onPick(async (ll) => {
      onPick(null);
      if (!draft) return;
      if (type === "point" || type === "border") { set({ coord: ll }); return; }
      const kind = type === "confluence" ? "tributary" : type === "lake" ? "lake" : "area";
      try {
        const r = await api.dfoSplitPick(id, kind, ll, draft.on_item);
        if (!r.ok) { setMsg(r.why ?? "nothing there"); return; }
        if (kind === "area") {
          if (r.hits.length === 1) pickArea(r.hits[0]); else setAreaHits(r.hits);
          return;
        }
        const h = r.hits[0];
        set({ item_id: h.id, name: h.name });
      } catch (e) { setMsg(String(e)); }
    });
  }
  function pickArea(h: SplitPick["hits"][number]) {
    setAreaHits([]);
    set({ area: h.name, area_layer: h.layer ?? null, area_field: null,
          name: draft?.name || `${h.name} boundary` });
  }

  async function addOffsets(base: NewSplit) {
    const offs = [Number(up) > 0 ? Number(up) : 0, Number(down) > 0 ? -Number(down) : 0].filter((x) => x);
    if (!offs.length) { setSaid({ ok: false, why: "give the metres upstream and / or downstream of the reference" }); return; }
    const taken = [...value.map((v) => v.ref), ...shared.map((p) => p.id)];
    const add: NewSplit[] = [];
    for (const off of offs) {
      const nm = (base.name || "").trim() || anchorWords(base);
      const cut: NewSplit = { ...base, ref: "draft", amends: null, offset_m: off,
        name: `${nm} (${Math.abs(off)} m ${off > 0 ? "upstream" : "downstream"})` };
      // T355: the REAL id it lands under, from the server (placed beside this mark's other cuts)
      const real = (await api.dfoSplitPreview(id, cut, [...value, ...add])).split_id;
      const ref = real || newRef([...taken, ...add.map((a) => a.ref)]);
      const ns: NewSplit = { ...cut, ref };
      const got = await api.dfoAnchor(id, ns);
      if (!got.ok) { setSaid(got); return; }
      onChecked(ref, got);
      add.push(ns);
    }
    onChange([...value, ...add]);
    onCuts(add.map((a) => a.ref));
    setAsRef(false); setUp(""); setDown("");
    reset();
  }

  async function commit() {
    if (!draft) return;
    setBusy(true);
    try {
      const base: NewSplit = { ...draft, name: (draft.name || "").trim() || null };
      if (asRef && canOffset && !editing) { await addOffsets(base); return; }
      const inMark = editing ? value.find((n) => n.ref === editing) : undefined;
      // T355: the REAL id (the preview's, from the server) — a local key only if the server gave none
      const real = editing ? null : (pv?.split_id ?? (await api.dfoSplitPreview(id, { ...base, ref: "draft" }, value)).split_id);
      const ref = editing ?? real ?? newRef([...value.map((v) => v.ref), ...shared.map((p) => p.id)]);
      const ns: NewSplit = { ...base, ref };
      // the check the save runs (`dfo.check_anchor`): the mark's stamp
      const got = await api.dfoAnchor(id, ns);
      setSaid(got);
      if (!got.ok) return;
      if (editing) {
        onChecked(ref, got);
        onChange(inMark ? value.map((n) => (n.ref === ref ? ns : n)) : [...value, ns]);
        setReusedWords(inMark ? `${ref} changed` : `${ref} edited — the edit rides in this mark and replaces the proposal (Save extent / Record mark)`);
        reset();
        return;
      }
      // the same cut as one already in this mark (same blk within 10 m)
      const mine = value.find((n) => checks[n.ref]?.blk === got.blk && checks[n.ref]?.measure != null
        && got.measure != null && typeOf(n.anchor) === typeOf(ns.anchor)
        && Math.abs((checks[n.ref].measure as number) - got.measure) <= SAME_CUT_M);
      if (mine) { setReusedWords(`already proposed — reused (${mine.ref})`); reset(); return; }
      // ... or one another mark proposed
      const dup = typeOf(ns.anchor) === "point" ? sameCut(shared, got) : undefined;
      if (dup) {
        onChange([...value, asNewSplit(dup, quote)]);
        setReusedWords(`already proposed — reused (${dup.id}: ${proposedWords(dup)})`);
        reset();
        return;
      }
      onChecked(ref, got);
      onChange([...value, ns]);
      setReusedWords("");
      reset();
    } catch (e) {
      setSaid({ ok: false, why: String(e) });
    } finally {
      setBusy(false);
    }
  }

  const layers = Object.keys(anchors?.area_layers ?? {});
  const typeHint = TYPES.find((t) => t.v === type)?.hint;
  return (
    <div className="section extent-fix" data-testid="dfo-new-splits">
      <div className="fix-row"><strong>New splits</strong>
        <span className="dim">a cut the notice names that does not exist — proposed in the mark, never written to splits.json (T185)</span></div>
      {value.map((n) => (
        <div key={n.ref} className="fix-row" data-testid={`dfo-split-${n.ref}`}>
          <span className="chip-tag new">{n.ref}</span>
          <span>{anchorWords(n)}</span>
          <ConfChip c={n.confidence ?? defaultConfidence(n.anchor)} />
          {n.amends && <span className="chip-tag" title="replaces the proposed cut at that anchor">edit of {anchorWords(n.amends)}</span>}
          {checks[n.ref] && <span className="dim">→ {checks[n.ref].words}{checks[n.ref].blk ? ` · blk ${checks[n.ref].blk} @ ${checks[n.ref].measure?.toFixed(0)} m` : ""}</span>}
          {offsettable.includes(n.anchor)
            ? <OffsetInput value={n.offset_m ?? null} onSet={(v) => void onOffset(n.ref, v)} />
            : <span className="dim" title="the build cuts it where it is">no offset</span>}
          <button type="button" className="btn small" data-testid={`dfo-edit-${n.ref}`} onClick={() => edit(n, null)}>edit</button>
          <button type="button" className="btn small" onClick={() => onChange(value.filter((x) => x.ref !== n.ref))}>remove</button>
        </div>
      ))}
      {shared.length > 0 && (
        <div data-testid="dfo-proposed">
          <div className="fix-row"><span className="k">already proposed</span>
            <span className="dim">on this river, by any mark — amber on the map; pick one as a point in the extent below, or edit it in place</span></div>
          {shared.map((p) => (
            <div key={p.id} className="fix-row" data-testid={`dfo-proposed-${p.id}`}>
              <span className="chip-tag new">{p.id}</span>
              <span>{proposedWords(p)}</span>
              <span className="dim" title={p.marks.map((m) => `${m.target} (${m.reviewer})`).join("\n")}>
                {p.marks[0]?.target.split(":").slice(1).join(":")}{p.n_marks > 1 ? ` +${p.n_marks - 1}` : ""}</span>
              <button type="button" className="btn small" onClick={() => onChange([...value, asNewSplit(p, quote)])}>
                this location needs it too</button>
              <button type="button" className="btn small" data-testid={`dfo-proposed-edit-${p.id}`}
                title="change its type, anchor or offset — recorded in this mark, it replaces the proposal (same id)"
                onClick={() => edit(asNewSplit(p, quote), asNewSplit(p, quote))}>edit</button>
            </div>
          ))}
        </div>
      )}
      {reusedWords && <div className="fix-row notice" data-testid="dfo-reused">{reusedWords}</div>}

      <div className="fix-row" data-testid="dfo-split-types">
        <span className="k">{editing ? `edit ${editing}` : "+ add a split"}</span>
        {TYPES.map((t) => (
          <button key={t.v} type="button" className={`ans trib-call${type === t.v ? " on" : ""}`} title={t.hint}
            aria-pressed={type === t.v} data-testid={`dfo-type-${t.v}`} disabled={!items.length}
            onClick={() => (draft ? setDraft(retype(draft, t.v, offsettable)) : start(t.v))}>{t.words}</button>
        ))}
        {draft && <button type="button" className="btn small" onClick={reset}>cancel</button>}
      </div>

      {draft && (
        <div className="fix-split-draft" data-testid="dfo-split-draft">
          <div className="fix-row dim">{typeHint}{editing && !value.some((n) => n.ref === editing)
            ? ` · editing the proposal ${editing}: the edit rides in this mark and replaces it (same id)` : ""}</div>
          {items.length > 1 && (
            <div className="fix-row"><span className="k">on</span>
              <select aria-label="the water it cuts" value={draft.on_item ?? ""} onChange={(e) => set({ on_item: e.target.value })}>
                {items.map((i) => <option key={i.id} value={i.id}>{i.name}</option>)}
              </select></div>
          )}
          {type === "point" && (
            <div className="fix-row"><span className="k">it is a</span>
              {POINT_KINDS.map((k) => (
                <button key={k.v} type="button" className={`ans trib-call${draft.anchor === k.v ? " on" : ""}`}
                  aria-pressed={draft.anchor === k.v} data-testid={`dfo-anchor-${k.v}`}
                  onClick={() => set({ anchor: k.v })}>{k.words}</button>
              ))}
            </div>
          )}
          {type === "lake" && (
            <div className="fix-row"><span className="k">the lake's</span>
              {LAKE_EDGES.map((k) => (
                <button key={k.v} type="button" className={`ans trib-call${draft.anchor === k.v ? " on" : ""}`}
                  aria-pressed={draft.anchor === k.v} data-testid={`dfo-anchor-${k.v}`}
                  onClick={() => set({ anchor: k.v })}>{k.words}</button>
              ))}
              {!!draft.offset_m && <span className="dim">with an offset the build measures from the end it runs away from
                (upstream: the inlet, downstream: the outlet)</span>}
            </div>
          )}
          {type === "split" && (
            <div className="fix-row"><span className="k">existing split</span><code>{draft.split_id}</code></div>
          )}
          {type !== "split" && (
            <div className="fix-row">
              <button type="button" className={`btn small${picking ? " primary" : ""}`} data-testid="dfo-pick"
                onClick={pickOnMap}>
                {picking ? "click the map… (cancel)" : type === "point" ? "click the river"
                  : type === "border" ? "click the crossing (optional)" : type === "confluence" ? "click the mouth"
                  : type === "lake" ? "click the lake" : "click the area"}</button>
              {(type === "point" || type === "border") && draft.coord &&
                <span className="dim">{draft.coord[1].toFixed(5)}, {draft.coord[0].toFixed(5)}</span>}
              {type === "confluence" && (
                <Autocomplete label="tributary" testid="dfo-tributary" minChars={0}
                  value={draft.name ?? ""} placeholder={tribs === null ? "loading tributaries…" : `type a tributary (${tribs.length})`}
                  fetch={async (q) => tribChoices(tribs ?? [], q)}
                  onPick={(c) => set({ item_id: c.key, name: c.label })} />
              )}
              {type === "lake" && (
                <Autocomplete label="lake" testid="dfo-lake" value={draft.name ?? ""} placeholder="or type the lake…"
                  minChars={2}
                  fetch={async (q) => (await api.searchItems(q, "lake")).slice(0, 15)
                    .map((h) => ({ key: h.id, label: h.name, sub: `MU ${h.mus.join(", ")} · ${h.id}` }))}
                  onPick={(c) => set({ item_id: c.key, name: c.label })} />
              )}
              {(type === "confluence" || type === "lake") && (draft.item_id || draft.name) &&
                <span className="fix-chosen">→ {draft.name ?? draft.item_id} <span className="dim">{draft.item_id}</span></span>}
            </div>
          )}
          {type === "area_boundary" && (
            <>
              {areaHits.length > 0 && (
                <div className="fix-row"><span className="k">the click is in</span>
                  {areaHits.map((h) => (
                    <button key={`${h.layer}:${h.name}`} type="button" className="btn small" onClick={() => pickArea(h)}>
                      {h.name} <span className="dim">{h.layer}</span></button>
                  ))}</div>
              )}
              <div className="fix-row">
                <span className="k">layer</span>
                <select aria-label="area layer" data-testid="dfo-area-layer" value={draft.area_layer ?? "parks_bc"}
                  onChange={(e) => set({ area_layer: e.target.value, area_field: null })}>
                  {layers.map((l) => <option key={l} value={l}>{l}</option>)}
                </select>
                <span className="k">{draft.area_field || anchors?.area_layers[draft.area_layer ?? "parks_bc"]}</span>
                <Autocomplete label="area name" testid="dfo-area-name" value={draft.area ?? ""}
                  placeholder="type the polygon's name…"
                  fetch={async (q) => (await api.dfoAreaNames(id, draft.area_layer ?? "parks_bc", q, draft.on_item))
                    .map((a) => ({ key: a.name, label: a.name, sub: a.distance_km != null ? `${a.distance_km} km` : a.layer }))}
                  onText={(t) => set({ area: t || null })}
                  onPick={(c) => set({ area: c.key, area_field: null, name: draft.name || `${c.key} boundary` })} />
              </div>
              <div className="fix-row">
                <span className="k">end</span>
                {([[null, "both"], ["upstream", "where it enters"], ["downstream", "where it leaves"]] as [NewSplit["area_end"], string][]).map(([v, w]) => (
                  <button key={w} type="button" className={`ans trib-call${(draft.area_end ?? null) === v ? " on" : ""}`}
                    aria-pressed={(draft.area_end ?? null) === v} onClick={() => set({ area_end: v })}>{w}</button>
                ))}
                <label className="check" title="also cut every stream of the water's watershed-code subtree where it crosses (needs the water named by WSC)">
                  <input type="checkbox" checked={!!draft.wsc_descendants}
                    onChange={(e) => set({ wsc_descendants: e.target.checked || null })} /> its WSC subtree too</label>
                <span className="k">leave uncut (gnis)</span>
                <input className="fix-text" style={{ width: 140 }} aria-label="exclude gnis" placeholder="17209, …"
                  value={(draft.exclude_gnis ?? []).join(", ")}
                  onChange={(e) => { const g = e.target.value.split(/[ ,]+/).filter((x) => /^\d+$/.test(x));
                                     set({ exclude_gnis: g.length ? g : null }); }} />
              </div>
            </>
          )}
          <div className="fix-row">
            <span className="k">name</span>
            <input className="fix-text" aria-label="split name" value={draft.name ?? ""}
              onChange={(e) => set({ name: e.target.value || null,
                ...(type === "confluence" || type === "lake" ? { item_id: null } : {}) })} />
            <span className="k">offset</span>
            {canOffset ? (
              <>
                <input className="fix-num" type="number" step={10} style={{ width: 80 }} aria-label="offset" data-testid="dfo-draft-offset"
                  value={draft.offset_m ?? ""} disabled={asRef}
                  onChange={(e) => set({ offset_m: e.target.value === "" ? null : Number(e.target.value) || null })} />
                <span className="dim">m (+ upstream, − downstream)</span>
                {!editing && (
                  <label className="check" title="the anchor is the named feature itself (a weir, a bridge); the cuts are measured along the river from it">
                    <input type="checkbox" checked={asRef} data-testid="dfo-as-reference"
                      onChange={(e) => setAsRef(e.target.checked)} /> two cuts off it</label>
                )}
                {asRef && <>
                  <input className="fix-num" type="number" min={0} step={10} placeholder="m up" aria-label="metres upstream"
                    data-testid="dfo-offset-up" value={up} onChange={(e) => setUp(e.target.value)} style={{ width: 70 }} />
                  <span className="dim">up</span>
                  <input className="fix-num" type="number" min={0} step={10} placeholder="m down" aria-label="metres downstream"
                    data-testid="dfo-offset-down" value={down} onChange={(e) => setDown(e.target.value)} style={{ width: 70 }} />
                  <span className="dim">down</span>
                </>}
              </>
            ) : (
              <span className="dim" data-testid="dfo-no-offset">none — the build cannot offset {type === "border" ? "the border" : "an area boundary"} (it cuts where the water crosses it)</span>
            )}
          </div>
          <div className="fix-row">
            <span className="k">extent is</span>
            {([[null, "—"], ["upstream", "upstream"], ["downstream", "downstream"]] as [NewSplit["side"], string][]).map(([v, w]) => (
              <button key={w} type="button" className={`ans trib-call${(draft.side ?? null) === v ? " on" : ""}`}
                aria-pressed={(draft.side ?? null) === v} onClick={() => set({ side: v })}>{w}</button>
            ))}
            <span className="k">confidence</span>
            {CONFIDENCES.map((c) => (
              <button key={c} type="button" className={`ans trib-call${(draft.confidence ?? defaultConfidence(draft.anchor)) === c ? " on" : ""}`}
                aria-pressed={(draft.confidence ?? defaultConfidence(draft.anchor)) === c} data-testid={`dfo-confidence-${c}`}
                title="how sure you are of where this cut stands — medium by default for a point you placed"
                onClick={() => set({ confidence: c })}>{c}</button>
            ))}
          </div>
          <div className="fix-row" data-testid="dfo-split-id">
            <span className="k">split id</span>
            <code>{editing ?? pv?.split_id ?? "—"}</code>
            <span className="dim">{editing ? "kept" : "the splits.json id it lands under"}</span>
          </div>
          <div className="fix-row" data-testid="dfo-split-preview">
            <span className="k">preview</span>
            {!draftReady(draft) ? <span className="dim">{type === "point" ? "click the river" : type === "area_boundary"
              ? "name the area (or click it)" : "pick the " + (type === "confluence" ? "tributary" : "lake")}</span>
              : pvBusy ? <span className="dim">the build is resolving it…</span>
              : pv?.ok ? <span data-testid="dfo-split-preview-ok"><strong>{pv.words}</strong>
                  {(pv.cuts ?? []).some((c) => c.concern) && <span className="warn-text"> · {(pv.cuts ?? []).map((c) => c.concern).filter(Boolean).join("; ")}</span>}
                  <span className="dim"> · orange on the map{(pv.notes ?? []).length ? ` · ${(pv.notes ?? []).join("; ")}` : ""}</span></span>
              : pv ? <span className="warn-text" data-testid="dfo-split-preview-refused">the build cuts nothing: {pv.why}</span> : null}
          </div>
          <div className="fix-row">
            <button type="button" className="btn small primary" disabled={busy || !draftReady(draft)}
              data-testid="dfo-add-split" onClick={() => void commit()}>
              {busy ? "checking…" : editing ? "save the edit" : asRef ? "add the offset cuts" : "add"}</button>
            {msg && <span className="warn-text">{msg}</span>}
          </div>
        </div>
      )}
      {said && !said.ok && <div className="fix-row warn-text" data-testid="dfo-anchor-refused">not added: {said.why}</div>}
    </div>
  );
}

/** a new cut's offset along the river, edited in place: + metres upstream, - downstream of its reference */
function OffsetInput({ value, onSet }: { value: number | null; onSet: (v: number | null) => void }) {
  const [v, setV] = useState(value == null ? "" : String(value));
  useEffect(() => { setV(value == null ? "" : String(value)); }, [value]);
  const commit = () => {
    const n = v.trim() === "" ? null : Number(v);
    if (n !== null && !Number.isFinite(n)) return;
    if ((n || null) !== (value || null)) onSet(n || null);
  };
  return (
    <label className="dim" title="offset along the river from the reference: + upstream, - downstream (m)">
      offset <input type="number" step={10} className="fix-num" style={{ width: 70 }} value={v}
        data-testid="dfo-offset" onChange={(e) => setV(e.target.value)} onBlur={commit}
        onKeyDown={(e) => { if (e.key === "Enter") commit(); }} /> m
    </label>
  );
}

const refKey = (c: [number, number]) => `${c[0].toFixed(6)},${c[1].toFixed(6)}`;

/** THE EDITABLE POINTS (owner 2026-11-02): each new cut where it lands (`cut:<ref>`), and the reference
 *  each offset cut is measured from (`ref:<lon,lat>`, shared by the cuts off one reference). Pure. */
export function editPointsOf(splits: NewSplit[], checks: Record<string, AnchorCheck>): EditPoint[] {
  const out: EditPoint[] = [];
  const refs = new Map<string, EditPoint>();
  for (const n of splits) {
    const c = checks[n.ref];
    const at = (c?.coord ?? n.coord) as [number, number] | null | undefined;
    if (at) out.push({ id: `cut:${n.ref}`, coord: at, kind: "cut",
      label: `NEW ${n.ref}${n.offset_m ? ` (${n.offset_m > 0 ? "+" : ""}${n.offset_m} m)` : ""}` });
    const r = n.offset_m ? (c?.ref?.coord ?? n.coord) as [number, number] | null | undefined : null;
    if (r && n.coord && !refs.has(refKey(n.coord as [number, number]))) {
      refs.set(refKey(n.coord as [number, number]), { id: `ref:${refKey(n.coord as [number, number])}`, coord: r,
        kind: "reference", label: `reference${n.name ? `: ${n.name.replace(/ \(.*\)$/, "")}` : ""}` });
    }
  }
  return [...refs.values(), ...out];
}

/** A DRAGGED POINT re-resolved by the server (`api.dfoAnchor`): a reference moves every cut made off it
 *  (their offsets kept); an offset cut keeps its reference and takes the new along-river distance as its
 *  offset (the drop snapped to the same blue line); a plain cut moves. -> the new splits and checks. */
export async function movedSplits(id: string, pid: string, ll: [number, number], splits: NewSplit[],
                                  checks: Record<string, AnchorCheck>):
    Promise<{ splits: NewSplit[]; checks: Record<string, AnchorCheck> }> {
  const out: Record<string, AnchorCheck> = {};
  if (pid.startsWith("ref:")) {
    const key = pid.slice(4);
    const next: NewSplit[] = [];
    for (const n of splits) {
      if (n.offset_m && n.coord && refKey(n.coord as [number, number]) === key) {
        const ns = { ...n, coord: ll };
        const got = await api.dfoAnchor(id, ns);
        if (!got.ok) throw new Error(`${n.ref}: ${got.why}`);
        out[n.ref] = got;
        next.push(ns);
      } else next.push(n);
    }
    return { splits: next, checks: out };
  }
  const ref = pid.slice(4);
  const n = splits.find((x) => x.ref === ref);
  if (!n) return { splits, checks: out };
  if (n.offset_m && checks[ref]?.ref) {
    const r = checks[ref].ref!;
    const at = await api.dfoAnchor(id, { ...n, coord: ll, offset_m: null });
    if (!at.ok) throw new Error(`${ref}: ${at.why}`);
    if (at.blk !== r.blk) throw new Error(`${ref}: dropped on another blue line than its reference — drag it along the river`);
    const off = Math.round((at.measure ?? 0) - r.measure);
    if (!off) throw new Error(`${ref}: dropped on its reference — an offset cut is off it`);
    const ns = { ...n, offset_m: off,
      name: (n.name ?? "").replace(/ \(\d+(\.\d+)? m (upstream|downstream)\)$/, "") + ` (${Math.abs(off)} m ${off > 0 ? "upstream" : "downstream"})` };
    const got = await api.dfoAnchor(id, ns);
    if (!got.ok) throw new Error(`${ref}: ${got.why}`);
    out[ref] = got;
    return { splits: splits.map((x) => (x.ref === ref ? ns : x)), checks: out };
  }
  const ns = { ...n, coord: ll };
  const got = await api.dfoAnchor(id, ns);
  if (!got.ok) throw new Error(`${ref}: ${got.why}`);
  out[ref] = got;
  return { splits: splits.map((x) => (x.ref === ref ? ns : x)), checks: out };
}
