import { SynopsisLink, printedPageOf } from "./SynopsisLink";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, type EntryRefused } from "../api";
import type {
  CheckAnswer, EntrySplits, Mode, NewSplit, TidalAlternate, TidalCandidate, TidalDetail, TidalProgress, TidalRow,
} from "../types";
import { getReviewer, isTyping, setReviewer } from "../review";
import { MapPanel } from "./MapPanel";
import { TopNav, FoldToggle, useFold } from "./TopNav";
import { applyPv, PV_DEFAULT, PvBadge, PvControls, type PvFilter } from "./Preverify";

export const TIDAL_HASH = "tidal/";

const VERIFY: [string, string][] = [
  ["", "all"], ["todo", "to do"], ["unmarked", "never marked"], ["stale", "changed since marked"],
  ["verified", "accepted"], ["wrong", "wrong"], ["unsure", "unsure"], ["deferred", "deferred"],
];
const CROSSING_WORDS: Record<string, string> = {
  clean: "polygon crosses once", at_mouth: "polygon edge at the mouth", blobs: "polygon crosses in pieces",
  misses: "polygon misses the stream",
};
const SOURCE_WORDS: Record<string, string> = {
  synopsis_box: "synopsis p.21 box", dfo_tidal_area: "DFO tidal-area page", dfo_notice: "DFO salmon notice",
  synopsis_row: "synopsis row", curated_split: "curated split", curated_split_offset: "curated split (offset)",
  tidal_gauge: "tidal gauge",
};
const PROBLEMS = ["split misplaced", "wrong water", "split missing"];
/** the triage classes and what they mean (T212 follow-up, `tidal-triage.md`); "review" = missed + sanity + multi-crossing */
export const TRIAGE_WORDS: Record<string, string> = {
  missed: "missed — words disagree, polygon unedited", sanity: "sanity — edited, but words disagree",
  "multi-crossing": "multi-crossing — the line goes in, out, in again (gaps filled)",
  "owner-edited": "owner-edited (accepted)", "book agrees": "words agree (accepted)",
  "no claim": "no claim (accepted at the polygon)", "owner-decided": "owner-decided (the owner made the call)",
};
/** the queue's default: only what the owner still has to look at */
export const DEFAULT_TRIAGE = "review";

/** ACCEPT A PROPOSAL: the new split it pre-fills (the proposed cut on the stream, named
 *  "<water>: mouth (tidal boundary)", its aliases in the note) — or, when a curated split sits within
 *  25 m on the same line (`proposed.reuse`), that split named (anchor "split") to take the alias. Pure — the owner's "Record mark" is
 *  the only write. */
export function acceptSplit(c: TidalCandidate): NewSplit {
  const words = c.sources.filter((s) => s.kind !== "tidal_gauge").map((s) => s.words);
  const ru = c.proposed.reuse;
  return {
    ref: `tidal-${c.blk}`, anchor: ru ? "split" : "coord", split_id: ru ? ru.split_id : null,
    name: c.proposed.split_name, on_item: c.items[0] ?? null,
    coord: c.proposed.coord, quote: words.join(" | ").slice(0, 2000),
    note: (ru ? `reuses curated split ${ru.split_id} (${ru.distance_m} m from the tidal point), no new cut; ` : "")
      + (c.proposed.aliases.length ? `aliases: ${c.proposed.aliases.join("; ")}` : ""),
  };
}

/** what each switchable point is, in words */
export const ALT_KIND: Record<TidalAlternate["kind"], string> = {
  words: "the words' landmark", polygon: "polygon crossing", mouth: "FWA mouth", dem: "DEM tidal limit",
  proposed: "the proposal",
};

/** SWITCH THE POINT (owner 2026-10-27): the new split one alternate pre-fills — the proposal's own
 *  split (reuse and all) when it is the proposed one, else a cut at that point named for what it is.
 *  Pure — "Record mark" stays the only write. */
export function altSplit(c: TidalCandidate, a: TidalAlternate): NewSplit {
  if (a.proposed) {
    const base = acceptSplit(c);
    return { ...base, note: `chosen point: ${a.kind} (the proposal) — ${a.label} (${Math.round(a.above_mouth_m)} m above the FWA mouth)`
      + (base.note ? `; ${base.note}` : "") };
  }
  const landmark = a.kind === "words" && a.words ? a.words.split(": ").pop()!.slice(0, 60) : null;
  const name = landmark ? `${c.water}: ${landmark} (tidal boundary)`
    : a.kind === "mouth" ? `${c.water}: mouth (tidal boundary)` : `${c.water}: tidal boundary`;
  const base = acceptSplit(c);
  return {
    ...base, anchor: "coord", split_id: null, name, coord: a.coord,
    note: `chosen point: ${a.kind} — ${ALT_KIND[a.kind]}: ${a.label} (${Math.round(a.above_mouth_m)} m above the FWA mouth`
      + `${a.located_by ? `; ${a.located_by}` : ""}) from the proposed ${c.proposed.split_name}`
      + (c.proposed.aliases.length ? `; aliases: ${c.proposed.aliases.join("; ")}` : ""),
  };
}

/** DFO CONFIRMS THE MOUTH (owner 2026-10-28): the new split a bulk accept records — the cut at the FWA mouth
 *  (DFO's default rule), or the proposal's own split when it already sits there or reuses a curated split. */
export function mouthSplit(c: TidalCandidate): NewSplit {
  const base = acceptSplit(c);
  if (c.proposed.reuse || c.proposed.above_mouth_m < 1) {
    return { ...base, note: `DFO confirms the mouth (T212 pass 2, bulk accept)${base.note ? `; ${base.note}` : ""}` };
  }
  return {
    ...base, anchor: "coord", split_id: null, name: `${c.water}: mouth (tidal boundary)`, coord: c.mouth.coord,
    note: `DFO confirms the mouth (T212 pass 2, bulk accept): the cut at the FWA mouth, not the proposal's `
      + `${Math.round(c.proposed.above_mouth_m)} m${c.proposed.aliases.length ? `; aliases: ${c.proposed.aliases.join("; ")}` : ""}`,
  };
}
/** the rows a "DFO confirms mouth" bulk accept takes: upgrade / new, with no mark in force */
export const mouthAcceptable = (rows: TidalRow[]) =>
  rows.filter((r) => (r.dfo_mouth === "upgrade" || r.dfo_mouth === "new") && (r.verify === "unmarked" || r.verify === "stale"));

/** DFO'S DEFAULT (owner 2026-10-28): "Unless otherwise specified, the tidal boundary is a straight line drawn
 *  between the two most seaward points of land … on either side of the mouth" — the definitions page */
export const DFO_DEFAULT_URL = "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/definitions-eng.html";
/** the new split a "DFO default: mouth" bulk accept records — the cut at the FWA mouth (or the proposal's
 *  own split when it already sits there) */
export function defaultSplit(c: TidalCandidate): NewSplit {
  const base = acceptSplit(c);
  const why = `DFO default (unless otherwise specified): the mouth — ${DFO_DEFAULT_URL} (bulk accept)`;
  if (c.proposed.above_mouth_m < 1) return { ...base, note: `${why}${base.note ? `; ${base.note}` : ""}` };
  return {
    ...base, anchor: "coord", split_id: null, name: `${c.water}: mouth (tidal boundary)`, coord: c.mouth.coord,
    note: `${why}: the cut at the FWA mouth, not the proposal's ${Math.round(c.proposed.above_mouth_m)} m`
      + (c.proposed.aliases.length ? `; aliases: ${c.proposed.aliases.join("; ")}` : ""),
  };
}
/** the rows the "DFO default: mouth" bulk accept takes: default high (never a conflict), never marked
 *  — or un-accepted by the owner */
export const defaultAcceptable = (rows: TidalRow[]) =>
  rows.filter((r) => r.dfo_default === "high" && !(r.boundary_words?.length) && (r.verify === "unmarked" || r.unaccepted));

const GROUP_WORDS: [string, string][] = [
  ["words", "words name it"], ["words_locate", "words name it — locate"], ["dfo_mouth", "DFO confirms mouth"], ["dfo_default", "DFO default: mouth"], ["conflict", "conflicts"],
];

interface Props {
  mode: Mode;
}

/* THE TIDAL REVIEW MODE (T212): every proposed tidal boundary — the owner's DFO tidal polygon held
   against the words (synopsis p.21, the DFO tidal pages and salmon notices, curated splits, tidal
   gauges) — listed, on the map with the polygon's edge, and accepted one by one. Accepting
   PRE-FILLS a mark (Place y + the cut as a new split); "Record mark" is the one write (POST
   /api/marks, scope "tidal"). Nothing here writes splits.json (T185). */
export function TidalReview({ mode }: Props) {
  const [allRows, setRows] = useState<TidalRow[]>([]);
  const [pvf, setPvf] = useState<PvFilter>(PV_DEFAULT);
  const [mouthOnly, setMouthOnly] = useState(false);
  const [defaultOnly, setDefaultOnly] = useState(false);
  const rows = useMemo(() => applyPv(allRows.filter((r) => (!mouthOnly || r.dfo_mouth) && (!defaultOnly || r.dfo_default)), pvf),
    [allRows, pvf, mouthOnly, defaultOnly]);
  const [bulkBusy, setBulkBusy] = useState(false);
  const queueSeq = useRef(0);
  const [progress, setProgress] = useState<TidalProgress | null>(null);
  const fold = useFold("tidal");
  const [filters, setFilters] = useState({ confidence: "", verify: "", enters: "", words: false, q: "", triage: DEFAULT_TRIAGE });
  const [selected, setSelected] = useState<string | null>(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return h.startsWith(TIDAL_HASH) ? h.slice(TIDAL_HASH.length) || null : null;
  });
  const [detail, setDetail] = useState<TidalDetail | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const loadRows = useCallback(() => {
    const seq = ++queueSeq.current;      // only the latest request lands (a slow earlier one must not overwrite it)
    api.tidalQueue(mouthOnly || defaultOnly ? { ...filters, triage: "" } : filters)
      .then((got) => { if (seq === queueSeq.current) setRows(got); }).catch((e) => setError(String(e)));
    api.tidalProgress().then(setProgress).catch(() => {});
  }, [filters, mouthOnly, defaultOnly]);

  // ACCEPT ALL "DFO DEFAULT: MOUTH" (owner 2026-10-28): EVERY candidate nothing specifies — the whole queue,
  // whatever the filters — one owner mark each at the FWA mouth; conflicts and marked rows skipped
  async function acceptDefaults() {
    const who = getReviewer().trim();
    if (!who) { setError("Type your name in a candidate's review box first: every mark names its reviewer."); return; }
    let take: TidalRow[];
    try { take = defaultAcceptable(await api.tidalQueue({})); } catch (e) { setError(String(e)); return; }
    if (!take.length) { setNotice("nothing to accept: every DFO-default row is marked or a conflict"); return; }
    if (!window.confirm(`Accept all ${take.length} "DFO default: mouth" candidate(s): the tidal cut at the FWA mouth?`
      + `\n\nNo source specifies their boundary, so DFO's default applies ("unless otherwise specified … the mouth").`
      + `\nOne mark each (P y), by ${who}. Conflicts and already-marked rows are skipped; a mark can be changed later.`)) return;
    setBulkBusy(true);
    let ok = 0;
    const bad: string[] = [];
    for (const r of take) {
      try {
        const d = await api.tidalCandidate(r.id);
        await api.postMark({ scope: "tidal", target: r.id, answers: { P: "y" }, problem: null,
          note: `DFO default (unless otherwise specified): the mouth — ${r.dfo_default_why ?? ""} — bulk accept`, reviewer: who, seconds: 0,
          new_splits: [defaultSplit(d.candidate)] });
        ok += 1;
        if (ok % 50 === 0) { setNotice(`saving… ${ok}/${take.length}`); api.tidalProgress().then(setProgress).catch(() => {}); }
      } catch (e) {
        bad.push(`${r.water}: ${String(e)}`);
      }
    }
    setBulkBusy(false);
    setNotice(`saved ✓ ${ok} DFO-default mark(s)${bad.length ? ` · ${bad.length} NOT saved` : ""}`);
    if (bad.length) setError(bad.join(" · "));
    loadRows();
  }

  // ACCEPT THE "DFO CONFIRMS MOUTH" BUCKET: one mark per unmarked upgrade / new row (marks are the only write)
  async function acceptMouths() {
    const take = mouthAcceptable(rows);
    const who = getReviewer().trim();
    if (!who) { setError("Type your name in a candidate's review box first: every mark names its reviewer."); return; }
    if (!take.length) { setNotice("nothing to accept: every DFO-confirms-mouth row here is marked or a conflict"); return; }
    if (!window.confirm(`Accept ${take.length} "DFO confirms the mouth" candidate(s): the tidal cut at the FWA mouth?`
      + `\n\nOne mark each (P y), by ${who}. Conflicts and already-marked rows are skipped; a mark can be changed later.`)) return;
    setBulkBusy(true);
    let ok = 0;
    const bad: string[] = [];
    for (const r of take) {
      try {
        const d = await api.tidalCandidate(r.id);
        await api.postMark({ scope: "tidal", target: r.id, answers: { P: "y" }, problem: null,
          note: `DFO confirms the mouth (T212 pass 2): ${r.dfo_mouth_why ?? ""} — bulk accept`, reviewer: who, seconds: 0,
          new_splits: [mouthSplit(d.candidate)] });
        ok += 1;
      } catch (e) {
        bad.push(`${r.water}: ${String(e)}`);
      }
    }
    setBulkBusy(false);
    setNotice(`saved ✓ ${ok} DFO-confirms-mouth mark(s)${bad.length ? ` · ${bad.length} NOT saved` : ""}`);
    if (bad.length) setError(bad.join(" · "));
    loadRows();
  }
  useEffect(() => { loadRows(); }, [loadRows]);

  const open = useCallback((id: string) => {
    setSelected(id);
    window.history.replaceState(null, "", `#${TIDAL_HASH}${encodeURIComponent(id)}`);
  }, []);
  useEffect(() => {
    if (!selected) { setDetail(null); return; }
    setDetail(null);
    api.tidalCandidate(selected).then(setDetail).catch((e) => setError(String(e)));
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
    document.querySelector(`[data-tidal="${CSS.escape(selected)}"]`)?.scrollIntoView({ block: "nearest" });
  }, [selected, rows]);

  const set = (patch: Partial<typeof filters>) => setFilters((f) => ({ ...f, ...patch }));
  const cov = (progress?.coverage ?? {}) as Record<string, number>;
  const pct = progress && progress.total ? (100 * progress.done) / progress.total : 0;
  return (
    <div className="app dfo-mode tidal-mode" data-testid="tidal-mode">
      <div className="filterbar-wrap">
        <TopNav active="tidal" right={<>
          <span className="progress dfo-progress" data-testid="tidal-progress">
            {progress ? <>
              <strong>{progress.done}/{progress.total}</strong> marked
              <span data-testid="tidal-triage-counts"> · <strong>{progress.by_triage?.missed ?? 0}</strong> missed
                · <strong>{progress.by_triage?.sanity ?? 0}</strong> sanity
                · <strong>{progress.by_triage?.["multi-crossing"] ?? 0}</strong> multi-crossing</span>
              <span className="dim"> · {progress.with_words} named by the words
                · named ocean streams met by the polygon {cov.named_ocean_entering_found_by_polygon ?? "?"}/{cov.named_ocean_entering_total ?? "?"}</span>
            </> : "…"}
          </span>
          <FoldToggle fold={fold} set={(filters.triage !== DEFAULT_TRIAGE ? 1 : 0) + (filters.confidence ? 1 : 0) + (filters.enters ? 1 : 0) + (filters.verify ? 1 : 0) + (filters.words ? 1 : 0) + (filters.q ? 1 : 0) + (mouthOnly ? 1 : 0) + (defaultOnly ? 1 : 0) + (pvf.pv || pvf.first ? 1 : 0)} />
        </>} />
        {fold.open && (
        <div className="filterbar filterbar-more" data-testid="filters-more">
          <div data-testid="tidal-triage-filter">
            <label>triage</label>
            <select aria-label="triage" value={filters.triage} onChange={(e) => set({ triage: e.target.value })}>
              <option value="review">missed + sanity + multi-crossing ({(progress?.by_triage?.missed ?? 0) + (progress?.by_triage?.sanity ?? 0)
                + (progress?.by_triage?.["multi-crossing"] ?? 0)})</option>
              {Object.keys(TRIAGE_WORDS).map((k) => (
                <option key={k} value={k}>{TRIAGE_WORDS[k]} ({progress?.by_triage?.[k as keyof TidalProgress["by_triage"]] ?? 0})</option>
              ))}
              <option value="">all</option>
            </select>
          </div>
          <label className="dim" style={{ display: "flex", gap: 4, alignItems: "center" }}>
            <input type="checkbox" data-testid="tidal-show-all" checked={filters.triage === ""}
              onChange={(e) => set({ triage: e.target.checked ? "" : DEFAULT_TRIAGE })} /> show all
          </label>
          <div>
            <label>confidence</label>
            <select aria-label="confidence" value={filters.confidence} onChange={(e) => set({ confidence: e.target.value })}>
              <option value="">all</option>
              {["high", "medium", "low"].map((c) => (
                <option key={c} value={c}>{c} ({progress?.by_confidence[c] ?? 0})</option>
              ))}
            </select>
          </div>
          <div>
            <label>enters</label>
            <select aria-label="enters" value={filters.enters} onChange={(e) => set({ enters: e.target.value })}>
              <option value="">any</option>
              <option value="ocean">the sea</option>
              <option value="tidal_water">tidal water (a tidal river / estuary)</option>
            </select>
          </div>
          <div>
            <label>review</label>
            <select aria-label="tidal review" value={filters.verify} onChange={(e) => set({ verify: e.target.value })}>
              {VERIFY.map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          <label className="dim" style={{ display: "flex", gap: 4, alignItems: "center" }}>
            <input type="checkbox" checked={filters.words} onChange={(e) => set({ words: e.target.checked })}
              data-testid="tidal-words-only" /> words name it
          </label>
          <PvControls value={pvf} onChange={setPvf} rows={allRows} />
          <label className="dim" style={{ display: "flex", gap: 4, alignItems: "center" }} data-testid="tidal-mouth-bucket"
            title="T212 pass 2: a DFO tidal-water area at the stream's mouth confirms tidal water reaches the FWA mouth (DFO's default rule)">
            <input type="checkbox" checked={mouthOnly} onChange={(e) => setMouthOnly(e.target.checked)} /> DFO confirms mouth
            ({progress?.by_dfo_mouth?.upgrade ?? 0} high · {progress?.by_dfo_mouth?.new ?? 0} new · {progress?.by_dfo_mouth?.conflict ?? 0} conflict)
          </label>
          {mouthOnly && (
            <button className="btn small primary" disabled={bulkBusy || !mouthAcceptable(rows).length} data-testid="tidal-mouth-accept"
              onClick={() => void acceptMouths()}>{bulkBusy ? "saving…" : `accept ${mouthAcceptable(rows).length} at the mouth`}</button>
          )}
          <label className="dim" style={{ display: "flex", gap: 4, alignItems: "center" }} data-testid="tidal-default-bucket"
            title="DFO definitions page: unless otherwise specified, the tidal boundary is a line across the mouth. These streams have no source naming the boundary itself.">
            <input type="checkbox" checked={defaultOnly} onChange={(e) => setDefaultOnly(e.target.checked)} /> DFO default: mouth
            ({progress?.by_dfo_default?.high ?? 0} high · {progress?.by_dfo_default?.conflict ?? 0} conflict)
          </label>
          {defaultOnly && (
            <button className="btn small primary" disabled={bulkBusy || !progress?.groups?.default_acceptable} data-testid="tidal-default-accept"
              onClick={() => void acceptDefaults()}>{bulkBusy ? "saving…" : `Accept all ${progress?.groups?.default_acceptable ?? 0} at the mouth (DFO default)`}</button>
          )}
          <input className="dfo-search" placeholder="stream…" value={filters.q}
            onChange={(e) => set({ q: e.target.value })} aria-label="search tidal" />
                  </div>
        )}
        <div className="filterbar-line"><div style={{ width: `${pct}%`, height: "100%", background: "var(--accent)" }} /></div>
        {progress?.groups && (
          <div className="dim" data-testid="tidal-groups" style={{ display: "flex", flexWrap: "wrap", gap: 14, padding: "4px 8px", fontSize: 12 }}>
            {GROUP_WORDS.map(([k, w]) => {
              const g = progress.groups![k as "words"];
              return (
                <span key={k} className="progress slim" title={`${w}: ${g.marked} of ${g.total} marked`}>
                  {w} <strong>{g.marked}/{g.total}</strong>
                  <span className="bar"><i style={{ width: `${g.total ? (100 * g.marked) / g.total : 0}%` }} /></span>
                </span>);
            })}
            <span className="progress slim">unmarked <strong>{progress.groups.unmarked.unmarked}</strong></span>
          </div>
        )}
      </div>
      {notice && <div className="notice" data-testid="tidal-notice" style={{ margin: 8 }}>{notice}{" "}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setNotice("")}>dismiss</button></div>}
      {error && <div className="errors" style={{ margin: 8 }}>{error}{" "}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setError("")}>dismiss</button></div>}
      <div className="main">
        <div className="queue" data-testid="tidal-queue">
          <div className="queue-group">{rows.length} proposed tidal boundar{rows.length === 1 ? "y" : "ies"} — read only; accepting records a mark</div>
          {rows.map((r) => (
            <div key={r.id} data-tidal={r.id} className={`queue-row${r.id === selected ? " selected" : ""}`}
              onClick={() => open(r.id)}>
              <div className="name">
                {r.water}
                <span className={`badge suggest-${r.confidence}`}>{r.confidence}</span>
                {r.verify !== "unmarked" && <span className={`badge verify-${r.verify}`}>{r.verify === "verified" ? "accepted" : r.verify}</span>}
              </div>
              <div className="meta">
                <span>{r.zones.slice(0, 2).join(", ") || `blk ${r.blk}`}</span>
                <span>{r.enters === "ocean" ? "sea" : "tidal water"}</span>
                <span>{r.above_mouth_m > 75 ? `${Math.round(r.above_mouth_m)} m above mouth` : "at the mouth"}</span>
                {r.n_words > 0 && <span className="chip-tag suggested">words · {r.agreement}</span>}
                {r.words_disagree && <span className="chip-tag new">words disagree</span>}
                {r.triage && <span className={`chip-tag${r.triage === "missed" || r.triage === "sanity" ? " new" : ""}`}
                  title={r.triage_why ?? ""}>{r.triage}</span>}
                {r.n_web > 0 && <span className="chip-tag suggested">web · {r.n_web}</span>}
                {r.dfo_mouth && <span className={`chip-tag${r.dfo_mouth === "conflict" ? " new" : " suggested"}`} title={r.dfo_mouth_why ?? ""}>
                  {r.dfo_mouth === "conflict" ? "DFO mouth: conflict" : r.dfo_mouth === "new" ? "DFO mouth: new" : "DFO confirms mouth"}</span>}
                {r.dfo_default && <span className={`chip-tag${r.dfo_default === "conflict" ? " new" : " suggested"}`} title={r.dfo_default_why ?? ""}>
                  {r.dfo_default === "conflict" ? "DFO default: conflict" : "DFO default: mouth"}</span>}
                {r.words_status === "locate" && <span className="chip-tag new" title={(r.boundary_words ?? []).join(" | ")}>words name it — locate</span>}
                {r.words_status === "located" && r.n_words === 0 && <span className="chip-tag suggested" title={(r.boundary_words ?? []).join(" | ")}>
                  words located{r.words_confidence ? ` · ${r.words_confidence}` : ""}</span>}
                {r.unaccepted && <span className="chip-tag new" title="the owner un-accepted this: no specified boundary">un-accepted</span>}
                {r.verify === "unmarked" && <PvBadge pv={r.pv} />}
                {r.multi_crossing && r.triage !== "multi-crossing" && <span className="chip-tag" title="the line crosses the polygon in, out and in again">{r.n_cuts} crossings</span>}
              </div>
              <div className="meta dfo-text">{CROSSING_WORDS[r.crossing] ?? r.crossing}</div>
            </div>
          ))}
        </div>
        {detail ? (
          <TidalCandidateView key={detail.row.id} detail={detail} mode={mode}
            nav={{ pos: idx + 1, total: rows.length, onNext: next ? () => open(next) : undefined,
                   onPrev: prev ? () => open(prev) : undefined }}
            onMarked={(said) => {
              setNotice(said);
              loadRows();
              if (next) open(next);
              else api.tidalCandidate(detail.row.id).then(setDetail).catch(() => {});
            }} />
        ) : (
          <div className="detail"><div className="empty-state">{selected ? "loading…" : "Select a proposed tidal boundary from the queue."}</div></div>
        )}
      </div>
    </div>
  );
}

function TidalCandidateView({ detail, mode, nav, onMarked }: {
  detail: TidalDetail; mode: Mode;
  nav: { pos: number; total: number; onNext?: () => void; onPrev?: () => void };
  onMarked: (said: string) => void;
}) {
  const c = detail.candidate;
  const was = detail.mark;
  const [answer, setAnswerP] = useState<CheckAnswer | null>(null);
  const [problem, setProblem] = useState("");
  const [note, setNote] = useState("");
  const [reviewer, setName] = useState(getReviewer);
  const [split, setSplit] = useState<NewSplit | null>(null);
  const [accepted, setAccepted] = useState("");
  const [picking, setPicking] = useState(false);
  const alts = useMemo(() => detail.alternates ?? [], [detail.alternates]);
  // the point the owner switched to (an alternate's id, or "custom"); null until he picks one —
  // the map opens on the proposal's point meanwhile
  const [chosen, setChosen] = useState<string | null>(null);
  // the snapped custom point's distance above the mouth (for the "Recording:" line)
  const [customAbove, setCustomAbove] = useState<number | null>(null);
  const focus = chosen ?? alts.find((a) => a.proposed)?.id ?? null;
  const [edge, setEdge] = useState<GeoJSON.FeatureCollection | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const since = useRef(Date.now());

  useEffect(() => {
    let live = true;
    api.tidalEdge(c.id).then((g) => live && setEdge(g as unknown as GeoJSON.FeatureCollection)).catch(() => {});
    return () => { live = false; };
  }, [c.id]);

  function accept() {
    const pa = alts.find((a) => a.proposed);
    if (pa) { choose(pa); return; }
    setSplit(acceptSplit(c));
    setAnswerP("y");
    setAccepted("proposal pre-filled: Place y with the cut as a new split — check it, then Record mark (nothing is saved yet)");
  }
  /** SWITCH TO ONE ALTERNATE (owner 2026-10-28): picking a point = "THIS is the tidal boundary" —
   *  the mark's cut becomes it (its source in the note); the owner then answers y and Records. "n"
   *  stays for rejecting the candidate outright. Nothing is saved until Record mark. */
  function choose(a: TidalAlternate) {
    setChosen(a.id);
    setPicking(false);
    setSplit(altSplit(c, a));
    setProblem("");
    setAccepted(`the cut is ${ALT_KIND[a.kind]} (${Math.round(a.above_mouth_m)} m above the mouth)`
      + `${a.proposed ? " — the proposal" : ""}: press y, then Record mark (nothing is saved yet)`);
  }
  function pickPoint() {
    setPicking(true);
  }
  const onPick = picking ? (ll: [number, number]) => {
    setPicking(false);
    api.tidalSnap(c.id, ll).then((sn) => {
      setChosen("custom");
      setCustomAbove(sn.above_mouth_m);
      setSplit({ ...acceptSplit(c), anchor: "coord", split_id: null, coord: sn.coord, name: `${c.water}: tidal boundary`,
                 note: `chosen point: custom — placed by the reviewer (snapped ${Math.round(sn.distance_m)} m to the stream`
                   + `${sn.above_mouth_m != null ? `, ${Math.round(sn.above_mouth_m)} m above the FWA mouth` : ""}) from the proposed ${c.proposed.split_name}` });
      setProblem("");
      setAccepted(`your point is the cut (snapped ${Math.round(sn.distance_m)} m to the stream`
        + `${sn.above_mouth_m != null ? `, ${Math.round(sn.above_mouth_m)} m above the mouth` : ""}): press y, then Record mark (nothing is saved yet)`);
    }).catch(() => setErrors([`that point is not on ${c.water} (more than 500 m from it): click on the stream`]));
  } : null;

  // the map's points: the candidate's (proposed / polygon / mouth / located words), the pending cut, the mark in force's
  const named: EntrySplits = useMemo(() => ({
    ...detail.points,
    features: [
      // the chosen point is drawn amber (the proposal's colour) so the map matches "Recording:"
      ...detail.points.features.map((f) => (chosen && chosen !== "custom"
        ? { ...f, properties: { ...f.properties, to_verify: f.properties.split_id === `alt:${chosen}` } } : f)),
      ...(split?.coord && chosen === "custom" ? [{
        type: "Feature" as const, geometry: { type: "Point" as const, coordinates: split.coord },
        properties: { split_id: "tidal:new", label: `YOUR POINT ${split.name ?? "tidal boundary"}`, water: "",
                      name: `NEW ${split.name ?? ""}`, rules: ["extent"], parts: [], to_verify: true, grade: null,
                      item_id: split.on_item ?? null },
      }] : []),
    ],
  }), [detail.points, split, chosen]);
  const savedFc: GeoJSON.FeatureCollection | null = useMemo(() => {
    const pts = (was?.new_splits ?? []).filter((n) => n.coord);
    return pts.length ? { type: "FeatureCollection", features: pts.map((n) => ({
      type: "Feature", properties: { label: `your mark: ${n.name ?? n.ref}` },
      geometry: { type: "Point", coordinates: n.coord as [number, number] } })) } : null;
  }, [was]);

  async function submit(defer = false) {
    setErrors([]);
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      await api.postMark({
        scope: "tidal", target: c.id, answers: answer ? { P: answer } : {}, problem: problem || null, note,
        reviewer: reviewer.trim(), seconds: Math.round((Date.now() - since.current) / 100) / 10,
        ...(split ? { new_splits: [split] } : {}),
        ...(defer ? { verdict: "deferred" as const } : {}),
      });
      onMarked(`saved ✓ ${c.water}` + (split && answer === "y" ? " — accepted: the tidal cut is proposed in the mark (T185 builds it)" : ""));
    } catch (e) {
      const fe = (e as EntryRefused).fieldErrors;
      setErrors([`NOT SAVED — ${fe ? "refused by the server" : "the save failed"}:`,
        ...(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [String(e)])]);
    } finally {
      setBusy(false);
    }
  }
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === "Enter") { void submit(); ev.preventDefault(); return; }
      if (ev.key === "a") { accept(); ev.preventDefault(); return; }
      const a = ev.key === "y" ? "y" : ev.key === "n" ? "n" : ev.key === "?" ? "?" : null;
      if (a) { setAnswerP(a as CheckAnswer); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });  // eslint-disable-line react-hooks/exhaustive-deps

  const problems = [...new Set([...PROBLEMS, ...(mode.problems.P ?? [])])];
  return (
    <div className="detail review-mode" data-testid="tidal-detail">
      <div className="detail-cols">
        <div className="detail-content">
          <div className="verify-bar">
            <div className="verify-row">
              <strong>{c.water}</strong>
              <span className="dim">blk {c.blk} · wsc {c.wsc}{c.zones.length ? ` · MU ${c.zones.slice(0, 3).join(", ")}` : ""}</span>
              <span className={`badge suggest-${c.confidence}`} data-testid="tidal-confidence">{c.confidence}</span>
              {was && <span className={`badge verify-${was.status}`} title={was.reasons.join(", ")}>
                {was.status === "verified" ? "accepted" : was.status}{was.reasons.length ? ` (${was.reasons.join(", ")})` : ""} · {was.reviewer} {was.date}</span>}
              <span className="spacer" style={{ flex: 1 }} />
              <span className="verify-nav">
                <button className="btn small" disabled={!nav.onPrev} onClick={nav.onPrev}>‹ k</button>
                <span className="dim">{nav.pos || "–"}/{nav.total}</span>
                <button className="btn small" disabled={!nav.onNext} onClick={nav.onNext}>j ›</button>
              </span>
            </div>
          </div>
          {errors.length > 0 && (
            <div className="errors" role="alert" data-testid="tidal-errors" style={{ position: "static", margin: "6px 0" }}>
              {errors.map((e, i) => <div key={i}>{e}</div>)}
            </div>
          )}

          <div className="section dfo-suggestion" data-testid="tidal-proposal">
            <div className="ext-row">
              <span className="k" style={{ color: "#d97706" }}>proposed</span>
              <span data-testid="tidal-proposal-words"><strong>{c.proposed.split_name}</strong>
                {" "}— {c.words_point || c.proposed.above_mouth_m > 75 ? `${Math.round(c.proposed.above_mouth_m)} m above the FWA mouth` : "at the FWA mouth"}
                {" "}(measure {c.proposed.measure.toFixed(0)} m; {c.proposed.coord[1].toFixed(5)}, {c.proposed.coord[0].toFixed(5)})</span>
              <button type="button" className="btn small primary" data-testid="tidal-accept" onClick={accept}
                title="pre-fills the mark below (a) — nothing is saved until you press Record mark">accept proposal</button>
            </div>
            <div className="ext-row dim" style={{ fontSize: 12 }}>from {c.proposed.basis} · {c.why}</div>
            {c.proposed.aliases.length > 0 && <div className="ext-row dim" style={{ fontSize: 12 }}>aliases: {c.proposed.aliases.join(" · ")}</div>}
            {c.proposed_before_words && Math.abs(c.proposed_before_words.measure - c.proposed.measure) >= 1 && (
              <div className="ext-row dim" style={{ fontSize: 12 }} data-testid="tidal-moved">moved by the words from
                {" "}{Math.round(c.proposed_before_words.above_mouth_m)} m ({c.proposed_before_words.basis}) — owner 2026-10-27: the words win</div>)}
            {accepted && <div className="ext-row notice" role="status" data-testid="tidal-accepted">{accepted}</div>}
          </div>

          {alts.length > 0 && (
            <div className="section" data-testid="tidal-alternates">
              <div className="scope-head"><h3>Switch the point</h3>
                <span className="dim">click one (here or on the map) to pre-fill the mark with it · or place your own</span></div>
              {alts.map((a) => (
                <label key={a.id} className={`ext-row alt-row${chosen === a.id ? " on" : ""}`} data-testid={`tidal-alt-${a.id}`}
                  style={{ cursor: a.coord ? "pointer" : "default", alignItems: "baseline" }}>
                  <input type="radio" name={`alt-${c.id}`} checked={chosen === a.id} disabled={!a.coord}
                    onChange={() => choose(a)} />
                  <span className="k" style={{ minWidth: 120 }}>{ALT_KIND[a.kind]}{a.uppermost ? " ↑" : ""}</span>
                  <span style={{ flex: 1 }}>
                    <strong>{Math.round(a.above_mouth_m)} m</strong> above the mouth
                    {a.proposed && <span className="chip-tag suggested" style={{ marginLeft: 6 }}>proposed</span>}
                    {a.supported && <span className="chip-tag new" style={{ marginLeft: 6 }} data-testid="tidal-supported">the words say so</span>}
                    {a.kind === "dem" && a.status && <span className="chip-tag" style={{ marginLeft: 6 }}>{a.status}</span>}
                    <span className="dim" style={{ fontSize: 12 }}> · {a.label}{a.located_by ? ` · ${a.located_by}` : ""}</span>
                  </span>
                </label>
              ))}
              <div className="ext-row">
                <label className="ext-row" style={{ gap: 6 }}>
                  <input type="radio" name={`alt-${c.id}`} checked={chosen === "custom"} disabled={!c.items.length}
                    onChange={() => pickPoint()} data-testid="tidal-alt-custom" />
                  <span className="k" style={{ minWidth: 120 }}>your point</span>
                  <span className="dim">{picking ? "click the stream on the map… (snapped to it)" : chosen === "custom" && split?.coord
                    ? `${split.coord[1].toFixed(5)}, ${split.coord[0].toFixed(5)} (snapped to the stream)` : "click to place a point on the stream"}</span>
                </label>
              </div>
            </div>
          )}

          <div className="section entry-scope" data-testid="tidal-polygon">
            <div className="scope-head"><h3>The polygon</h3>
              <span className="dim">{CROSSING_WORDS[c.polygon.crossing] ?? c.polygon.crossing}</span></div>
            <div className="ext-row">
              {c.polygon.measure != null
                ? <span>leaves the stream at measure {c.polygon.measure.toFixed(0)} m · {c.polygon.n_cuts} crossing{c.polygon.n_cuts === 1 ? "" : "s"}
                    {c.polygon.inside_pieces > 1 ? ` · ${c.polygon.inside_pieces} inside pieces, ${c.polygon.outside_below_m.toFixed(0)} m outside below the top` : ""}</span>
                : <span className="warn-text">does not reach this stream (its mouth is {c.polygon.mouth_gap_m.toFixed(0)} m away)</span>}
            </div>
            {c.polygon.multi_crossing && (
              <div className="ext-row" data-testid="tidal-multi">
                <span className="k">multi-crossing</span>
                <span style={{ fontSize: 12 }}>crossings at {(c.polygon.cuts ?? []).map((m) => m.toFixed(0)).join(", ")}{c.polygon.n_cuts > (c.polygon.cuts ?? []).length ? " …" : ""} m
                  {" "}· boundary = the top of the run joined to the mouth ({c.polygon.measure?.toFixed(0)} m)
                  {(c.polygon.gaps_filled ?? []).length ? ` · gaps filled ${(c.polygon.gaps_filled ?? []).map(([a, b]) => `${a.toFixed(0)}–${b.toFixed(0)}`).join(", ")} m` : ""}
                  {(c.polygon.ignored_pieces ?? []).length ? ` · separate patch ignored ${(c.polygon.ignored_pieces ?? []).map(([a, b]) => `${a.toFixed(0)}–${b.toFixed(0)}`).join(", ")} m` : ""}</span>
              </div>
            )}
            {c.joins_big_river_tidal_reach_at_m != null && (
              <div className="ext-row dim" style={{ fontSize: 12 }}>joins its big river's tidal reach {(c.joins_big_river_tidal_reach_at_m / 1000).toFixed(1)} km above that river's mouth</div>
            )}
            <div className="ext-row dim" style={{ fontSize: 12 }}>enters {c.enters === "ocean" ? "the sea" : "tidal water (a tidal river or estuary)"} · agreement: {c.agreement}{c.words_disagree ? " · the words disagree with each other" : ""}</div>
          </div>

          {c.triage && (
            <div className="section" data-testid="tidal-triage">
              <div className="ext-row">
                <span className="k">triage</span>
                <strong>{TRIAGE_WORDS[c.triage.class] ?? c.triage.class}</strong>
                <span className="dim" style={{ fontSize: 12 }}>{c.triage.why}
                  {c.triage.edit_sites.length ? ` · edit sites ${c.triage.edit_sites.join(", ")}` : ""}</span>
              </div>
            </div>
          )}

          <div className="source-pin" data-testid="tidal-sources">
            <div className="evidence-head"><h3 style={{ margin: 0 }}>What the words say</h3></div>
            {c.sources.length === 0 && <div className="dim">No text names this stream's tidal boundary — the polygon alone (unnamed-stream default: the mouth).</div>}
            {c.sources.map((s, i) => {
              const pp = s.kind === "synopsis_row" || s.kind === "synopsis_box" ? printedPageOf(s.source) : null;
              return (
                <div key={i} className="ev-row" data-testid={`tidal-source-${i}`}>
                  <span className="k">{SOURCE_WORDS[s.kind] ?? s.kind}</span>
                  <div className="verbatim">{s.words}</div>
                  <div className="ev-meta dim">
                    {s.measure != null ? `at ${s.measure.toFixed(0)} m${s.located_by ? ` (${s.located_by})` : ""}` : "not located"}
                    {s.agrees === true ? " · agrees" : s.agrees === false ? ` · DISAGREES (${s.polygon_minus_source_m?.toFixed(0)} m)` : ""}
                    {" · "}{s.source}
                    {pp != null && <>{" · "}<SynopsisLink printed={pp} water={s.kind === "synopsis_row" ? c.water : undefined}
                      testid={`tidal-source-${i}-pdf`} /></>}
                  </div>
                </div>
              );
            })}
            {(c.triage?.web ?? []).map((w, i) => (
              <div key={`web${i}`} className="ev-row" data-testid={`tidal-web-${i}`}>
                <span className="k">web · {w.confidence}</span>
                <div className="verbatim">{w.marker}{w.quote ? <> — <q>{w.quote}</q></> : null}
                  {w.line && <span className="dim"> (line {w.endpoints?.join(" – ") ?? ""}: {w.line.map(([x, y]) => `${y.toFixed(5)}, ${x.toFixed(5)}`).join(" → ")})</span>}</div>
                <div className="ev-meta dim">
                  {w.located ? `at ${w.located.measure.toFixed(0)} m (${w.located.located_by})` : "not located"}
                  {w.agrees === true ? " · agrees" : w.agrees === false ? " · DISAGREES" : ""}
                  {" · "}<a href={w.url} target="_blank" rel="noreferrer">{w.site}</a></div>
              </div>
            ))}
          </div>

          <div className="checklist" data-testid="tidal-checklist">
            <div className="checklist-head"><strong>Review</strong> <span className="dim">a accept · y n ? answer · Enter records · j k move</span></div>
            <div className="check-row">
              <strong>P</strong>
              <span className="check-q">The tidal boundary is here — the cut goes at the proposed (or your) point?</span>
              <span className="check-a">
                {(["y", "n", "?"] as CheckAnswer[]).map((a) => (
                  <button key={a} type="button" className={`ans ans-${a === "?" ? "q" : a}${answer === a ? " on" : ""}`}
                    aria-pressed={answer === a} data-testid={`tidal-P-${a}`} onClick={() => setAnswerP(a)}>{a}</button>
                ))}
              </span>
            </div>
            <div className="fix-row">
              {split ? <>
                <span className="chip-tag new">{split.ref}</span>
                <span>{split.name} at {split.coord?.[1].toFixed(5)}, {split.coord?.[0].toFixed(5)}</span>
                <button type="button" className="btn small" onClick={() => { setSplit(null); setChosen(null); }}>remove</button>
              </> : <span className="dim">no cut in the mark yet — accept the proposal, or click your own point</span>}
              <button type="button" className={`btn small${picking ? " primary" : ""}`} data-testid="tidal-pick"
                disabled={!c.items.length} onClick={() => (picking ? setPicking(false) : pickPoint())}>
                {picking ? "click the stream on the map… (cancel)" : "the boundary is elsewhere: click it"}</button>
            </div>
            {answer === "n" && (
              <div className="check-wrong">
                <select aria-label="problem" value={problem} onChange={(e) => setProblem(e.target.value)}>
                  <option value="">— the problem —</option>
                  {problems.map((p) => <option key={p} value={p}>{p}</option>)}
                </select>
              </div>
            )}
            <textarea className="check-note" placeholder="note (where the boundary is, what says so)" value={note}
              onChange={(e) => setNote(e.target.value)} />
            <div className="ext-row" data-testid="tidal-recording" style={{ fontSize: 13 }}>
              <span className="k">recording</span>
              <span>{(() => {
                if (!split?.coord) return <span className="dim">no point yet — pick one above (or accept the proposal), then y</span>;
                const a = chosen && chosen !== "custom" ? alts.find((x) => x.id === chosen) : undefined;
                const above = a ? a.above_mouth_m : chosen === "custom" ? customAbove : c.proposed.above_mouth_m;
                const what = chosen === "custom" ? "your point" : a ? ALT_KIND[a.kind] : "the proposal";
                return <>
                  <strong>{split.name}</strong> — {what}{a?.supported ? " (the words say so)" : ""}
                  {" · "}{split.coord[1].toFixed(5)}, {split.coord[0].toFixed(5)}
                  {above != null ? ` · ${Math.round(above)} m above the mouth` : ""}
                  {" · "}<span className={answer === "y" ? "" : "warn-text"}>Place {answer ?? "—"}</span>
                </>;
              })()}</span>
            </div>
            <div className="verdict-bar" style={{ position: "static", margin: 0 }}>
              <input placeholder="your name" value={reviewer} onChange={(e) => setName(e.target.value)} aria-label="reviewer" />
              <button className="btn primary" disabled={busy} data-testid="tidal-record" onClick={() => void submit()}>
                {busy ? "recording…" : "Record mark"}</button>
              <button className="btn small" disabled={busy} onClick={() => void submit(true)}>defer</button>
              <span className="dim">a proposal only — never written to splits.json (T185)</span>
            </div>
          </div>
        </div>
        <div className="detail-map">
          <div className="dim" style={{ fontSize: 12, marginBottom: 4 }}>
            amber: the PROPOSED cut · ringed: the point you switched to (click any point to switch) ·{" "}
            <span style={{ color: "#7c3aed" }}>violet dashed: the tidal polygon's edge</span> ·{" "}
            <span style={{ color: "#0d9488" }}>teal: your mark's cut</span> · labels: polygon edge, FWA mouth, located words
          </div>
          <MapPanel itemId={c.items[0] ?? null} alsoItemIds={c.items.slice(1)} namedSplits={named}
            suggested={edge} saved={savedFc} onPick={onPick}
            selectedSplitId={focus === "custom" ? "tidal:new" : focus ? `alt:${focus}` : null}
            onSelectSplit={(sid) => {
              const a = sid?.startsWith("alt:") ? alts.find((x) => `alt:${x.id}` === sid) : undefined;
              if (a && a.coord) choose(a);
            }}
            selectZoom={15} fitNamed={alts.length === 0} />
        </div>
      </div>
    </div>
  );
}
