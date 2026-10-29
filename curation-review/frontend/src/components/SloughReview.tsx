import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, type EntryRefused } from "../api";
import type { MarkPost } from "../types";
import { getReviewer, isTyping, setReviewer } from "../review";
import { MapPanel } from "./MapPanel";
import { TopNav, FoldToggle, useFold } from "./TopNav";
import { POND_HASH, PondReview } from "./PondReview";

export const SLOUGH_HASH = "sloughs/";

/* THE SLOUGH REVIEW MODE (T55, owner 2026-10-28): every slough / canal candidate still drawn as a lake.
   A PRE-MARK is a proposed answer — "stream", because the piece is narrow (≤ 44 m) or a wide marsh /
   swamp channel — shown with why; it is NOT a mark. The owner confirms it (one, or every visible one
   after checking), or changes it. Intermittent pieces carry no pre-mark: they stay on the owner's list.
   The one write is the mark (POST /api/marks, scope "slough", `call`). */

export interface SloughPremark { call: "stream"; basis: "narrow" | "wide marsh"; why: string }
export interface SloughRow {
  id: string; name: string | null; stream: string | null; region: string | null; fwa: string | null;
  width_m: number | null; area_ha: number | null; rule_class: string; group: string; intermittent: boolean;
  wide_wetland: boolean; complex: string | null; img_water: number | null; img_veg: number | null;
  lat: number; lon: number; page_verdict: string | null; page_note?: string | null; premark: SloughPremark | null; why: string | null;
  spot_check: boolean; verify: string; reasons: string[]; call: "stream" | "not stream" | null;
  reviewer: string | null;
}
interface SloughDetail {
  row: SloughRow;
  finder: { poly_layer?: string; fwa_feature_code?: string; mu?: string; elongation?: number;
            stream_name?: string; stream_item?: string | null; class?: string; class_reason?: string[];
            evidence?: string[]; entries_missing_on_polygon?: string[] };
  outline: GeoJSON.FeatureCollection;
  mark: { status: string; reviewer: string; date: string; call: string | null; note: string } | null;
}
interface SloughProgress {
  total: number; done: number; by_group: Record<string, number>; by_basis: Record<string, number>;
  intermittent: number; by_verify: Record<string, number>; by_call: Record<string, number>;
}

async function get<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} for ${url}`);
  return (await r.json()) as T;
}

const GROUPS: [string, string][] = [
  ["premark", "pre-marked stream — confirm"], ["owner list", "your list (no pre-mark)"],
  ["owner", "marked on the review page"], ["default", "not for review (lake class, ponds)"], ["", "all"],
];
const BASES: [string, string][] = [["", "any"], ["narrow", "narrow (≤ 44 m)"], ["wide marsh", "wide marsh / swamp"],
  ["intermittent", "intermittent"]];
const VERIFY: [string, string][] = [["todo", "to do"], ["", "all"], ["verified", "decided"], ["unsure", "unsure"],
  ["deferred", "deferred"], ["stale", "changed since"]];

const gmaps = (r: { lat: number; lon: number }) => `https://www.google.com/maps/@${r.lat},${r.lon},16z/data=!3m1!1e3`;
const m = (v: number | null | undefined) => (v == null ? "?" : `${Math.round(v)} m`);
const pct = (v: number | null | undefined) => (v == null ? "?" : `${Math.round(v * 100)}%`);

function errWords(e: unknown): string[] {
  const fe = (e as EntryRefused).fieldErrors;
  return [`NOT SAVED — ${fe ? "refused by the server" : "the save failed"}:`,
    ...(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [String(e)])];
}

/** the one write: a slough mark — one target, or many (`targets`) in one commit */
async function postSlough(body: { target?: string; targets?: string[]; call: "stream" | "not stream" | null;
                                  answer: "y" | "?"; note: string; reviewer: string; seconds?: number;
                                  defer?: boolean }) {
  const mark = {
    scope: "slough", target: body.target ?? "", ...(body.targets ? { targets: body.targets } : {}),
    answers: body.defer ? {} : { P: body.answer }, call: body.answer === "y" && !body.defer ? body.call : null,
    note: body.note, reviewer: body.reviewer, ...(body.seconds != null ? { seconds: body.seconds } : {}),
    ...(body.defer ? { verdict: "deferred" } : {}),
  };
  return api.postMark(mark as unknown as MarkPost) as unknown as Promise<{ n: number }>;
}

/** the Sloughs tab: the slough queue, or its "Small ponds (Q154)" view (`#sloughs/q154/<id>`) */
export function SloughReview() {
  const [ponds, setPonds] = useState(() => decodeURIComponent(window.location.hash.slice(1)).startsWith(POND_HASH));
  // the top bar's "Small ponds" sub-tab changes the hash
  useEffect(() => {
    const onHash = () => setPonds(decodeURIComponent(window.location.hash.slice(1)).startsWith(POND_HASH));
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  return ponds ? <PondReview /> : <SloughList />;
}

function SloughList() {
  const [rows, setRows] = useState<SloughRow[]>([]);
  const [progress, setProgress] = useState<SloughProgress | null>(null);
  const fold = useFold("sloughs");
  const [filters, setFilters] = useState({ group: "premark", basis: "", verify: "todo", region: "", q: "" });
  const [selected, setSelected] = useState<string | null>(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return h.startsWith(SLOUGH_HASH) && !h.startsWith(POND_HASH) ? h.slice(SLOUGH_HASH.length) || null : null;
  });
  const [detail, setDetail] = useState<SloughDetail | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const [notice, setNotice] = useState("");
  const [reviewer, setName] = useState(getReviewer);
  const [busy, setBusy] = useState(false);

  const asked = useRef(0);   // a slower, older answer never overwrites the newer filter's
  const loadRows = useCallback(() => {
    const qs = new URLSearchParams(Object.entries(filters).filter(([, v]) => v) as [string, string][]).toString();
    const n = ++asked.current;
    get<SloughRow[]>(`/api/sloughs/queue${qs ? `?${qs}` : ""}`).then((got) => { if (n === asked.current) setRows(got); })
      .catch((e) => setErrors([String(e)]));
    get<SloughProgress>("/api/sloughs/progress").then(setProgress).catch(() => {});
  }, [filters]);
  useEffect(() => { loadRows(); }, [loadRows]);

  // a pasted link / the back button: open what the hash names
  useEffect(() => {
    const onHash = () => {
      const h = decodeURIComponent(window.location.hash.slice(1));
      if (h.startsWith(SLOUGH_HASH) && !h.startsWith(POND_HASH) && h.length > SLOUGH_HASH.length) setSelected(h.slice(SLOUGH_HASH.length));
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  const open = useCallback((id: string) => {
    setSelected(id);
    window.history.replaceState(null, "", `#${SLOUGH_HASH}${encodeURIComponent(id)}`);
  }, []);
  useEffect(() => {
    if (!selected) { setDetail(null); return; }
    setDetail(null);
    get<SloughDetail>(`/api/sloughs/candidates/${encodeURIComponent(selected)}`).then(setDetail)
      .catch((e) => setErrors([String(e)]));
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
    if (selected) document.querySelector(`[data-slough="${CSS.escape(selected)}"]`)?.scrollIntoView({ block: "nearest" });
  }, [selected, rows]);

  // ACCEPT ALL VISIBLE: the pre-marked rows the queue shows now that carry no mark in force
  const acceptable = useMemo(() => rows.filter((r) => r.premark && (r.verify === "unmarked" || r.verify === "stale")), [rows]);
  async function acceptAll() {
    setErrors([]);
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    const by = acceptable.reduce<Record<string, number>>((a, r) => ({ ...a, [r.premark!.basis]: (a[r.premark!.basis] ?? 0) + 1 }), {});
    const words = Object.entries(by).map(([k, n]) => `${n} ${k}`).join(", ");
    if (!window.confirm(`Mark ${acceptable.length} visible pre-marked piece(s) as STREAM (${words})?\n\nOne mark each, by ${reviewer.trim()}. Only accept what you have checked; a mark can be changed later.`)) return;
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      const got = await postSlough({ targets: acceptable.map((r) => r.id), call: "stream", answer: "y",
        note: "accepted the pre-mark (accept all visible)", reviewer: reviewer.trim() });
      setNotice(`saved ✓ ${got.n} marked stream`);
      loadRows();
    } catch (e) {
      setErrors(errWords(e));
    } finally {
      setBusy(false);
    }
  }

  const set = (patch: Partial<typeof filters>) => setFilters((f) => ({ ...f, ...patch }));
  const bar = progress && progress.total ? (100 * progress.done) / progress.total : 0;
  return (
    <div className="app dfo-mode slough-mode" data-testid="slough-mode">
      <div className="filterbar-wrap">
        <TopNav active="sloughs" sub="sloughs" right={<>
          <span className="progress dfo-progress" data-testid="slough-progress">
            {progress ? <>
              <strong>{progress.done}/{progress.total}</strong> decided
              <span className="dim"> · pre-marked {(progress.by_basis.narrow ?? 0) + (progress.by_basis["wide marsh"] ?? 0)}
                {" "}({progress.by_basis.narrow ?? 0} narrow, {progress.by_basis["wide marsh"] ?? 0} wide marsh)
                {" "}· your list {progress.by_group["owner list"] ?? 0} ({progress.intermittent} intermittent)
                {" "}· {progress.by_group.owner ?? 0} marked on the review page</span>
            </> : "…"}
          </span>
          <FoldToggle fold={fold} set={(filters.group !== "premark" ? 1 : 0) + (filters.basis ? 1 : 0) + (filters.verify !== "todo" ? 1 : 0) + (filters.region ? 1 : 0) + (filters.q ? 1 : 0)} />
        </>} />
        {fold.open && (
        <div className="filterbar filterbar-more" data-testid="filters-more">
          <div>
            <label>group</label>
            <select aria-label="group" data-testid="slough-group" value={filters.group} onChange={(e) => set({ group: e.target.value })}>
              {GROUPS.map(([k, w]) => <option key={k} value={k}>{w}{k ? ` (${progress?.by_group[k] ?? 0})` : ""}</option>)}
            </select>
          </div>
          <div>
            <label>why</label>
            <select aria-label="basis" data-testid="slough-basis" value={filters.basis} onChange={(e) => set({ basis: e.target.value })}>
              {BASES.map(([k, w]) => <option key={k} value={k}>{w}{k === "intermittent" ? ` (${progress?.intermittent ?? 0})`
                : k ? ` (${progress?.by_basis[k] ?? 0})` : ""}</option>)}
            </select>
          </div>
          <div>
            <label>review</label>
            <select aria-label="slough review" value={filters.verify} onChange={(e) => set({ verify: e.target.value })}>
              {VERIFY.map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          <div>
            <label>region</label>
            <input style={{ width: 48 }} value={filters.region} onChange={(e) => set({ region: e.target.value.trim() })} aria-label="region" placeholder="all" />
          </div>
          <input className="dfo-search" placeholder="stream / id…" value={filters.q}
            onChange={(e) => set({ q: e.target.value })} aria-label="search sloughs" />
                  </div>
        )}
        <div className="filterbar-line"><div style={{ width: `${bar}%`, height: "100%", background: "var(--accent)" }} /></div>
      </div>
      {notice && <div className="notice" data-testid="slough-notice" style={{ margin: 8 }}>{notice}{" "}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setNotice("")}>dismiss</button></div>}
      {errors.length > 0 && <div className="errors" role="alert" style={{ margin: 8 }}>{errors.map((e, i) => <div key={i}>{e}</div>)}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setErrors([])}>dismiss</button></div>}
      <div className="main">
        <div className="queue" data-testid="slough-queue">
          <div className="queue-group" style={{ display: "flex", flexWrap: "wrap", gap: 6, alignItems: "center" }}>
            <span>{rows.length} candidate{rows.length === 1 ? "" : "s"} — a pre-mark is a proposal, not a mark</span>
            <input placeholder="your name" value={reviewer} onChange={(e) => setName(e.target.value)} aria-label="reviewer"
              style={{ width: 110 }} />
            <button className="btn small primary" data-testid="slough-accept-all" disabled={busy || acceptable.length === 0}
              title="one mark each (stream) for every pre-marked row the queue shows now, unmarked — check them first"
              onClick={() => void acceptAll()}>
              accept all visible ({acceptable.length})</button>
          </div>
          {rows.map((r) => (
            <div key={r.id} data-slough={r.id} className={`queue-row${r.id === selected ? " selected" : ""}`} onClick={() => open(r.id)}>
              <div className="name">
                {r.name || r.stream || "unnamed"}
                {r.verify !== "unmarked"
                  ? <span className={`badge verify-${r.verify}`}>{r.verify === "verified" ? r.call : r.verify}</span>
                  : r.page_verdict ? <span className="badge verify-verified" title="your mark on the review page">page: {r.page_verdict}</span>
                  : r.premark ? <PremarkBadge pm={r.premark} />
                  : r.intermittent ? <span className="chip-tag new" title="intermittent: no pre-mark (owner 2026-10-28)">intermittent</span> : null}
              </div>
              <div className="meta">
                <span>{m(r.width_m)} wide</span>
                <span>{r.area_ha ?? "?"} ha</span>
                <span>{r.fwa}</span>
                <span>R{r.region}</span>
                {r.spot_check && <span className="chip-tag" title="one of the 25 random spot-check pieces">spot check</span>}
              </div>
            </div>
          ))}
        </div>
        {detail ? (
          <SloughView key={detail.row.id} detail={detail} reviewer={reviewer} setName={setName}
            nav={{ pos: idx + 1, total: rows.length, onNext: next ? () => open(next) : undefined, onPrev: prev ? () => open(prev) : undefined }}
            onMarked={(said) => { setNotice(said); loadRows(); if (next) open(next); }} />
        ) : (
          <div className="detail"><div className="empty-state">{selected ? "loading…" : "Select a candidate from the queue."}</div></div>
        )}
      </div>
    </div>
  );
}

function PremarkBadge({ pm }: { pm: SloughPremark }) {
  return (
    <span className="chip-tag pv pv-pre-verified" data-testid="slough-premark" data-basis={pm.basis}
      title={`${pm.why}\nA proposal only — your mark is the decision`}>
      pre-mark: stream · {pm.basis}
    </span>
  );
}

function SloughView({ detail, reviewer, setName, nav, onMarked }: {
  detail: SloughDetail; reviewer: string; setName: (s: string) => void;
  nav: { pos: number; total: number; onNext?: () => void; onPrev?: () => void };
  onMarked: (said: string) => void;
}) {
  const r = detail.row;
  const f = detail.finder;
  const was = detail.mark;
  const [call, setCall] = useState<"stream" | "not stream" | "?" | null>(null);
  const [note, setNote] = useState("");
  const [errors, setErrors] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const since = useRef(Date.now());
  const fly = useMemo(() => ({ coord: [r.lon, r.lat] as [number, number],
                               zoom: r.area_ha != null && r.area_ha > 20 ? 13 : 16, n: 1 }), [r.lon, r.lat, r.area_ha]);

  async function submit(choice = call, defer = false) {
    setErrors([]);
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    if (!choice && !defer) { setErrors(["Pick stream, not stream or unsure first."]); return; }
    if (defer && !note.trim()) { setErrors(["To defer, write a note saying why."]); return; }
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      await postSlough({ target: r.id, call: choice === "?" ? null : choice, answer: choice === "?" ? "?" : "y",
        note: note || (choice === "stream" && r.premark ? "accepted the pre-mark" : ""), reviewer: reviewer.trim(),
        seconds: Math.round((Date.now() - since.current) / 100) / 10, defer });
      onMarked(`saved ✓ ${r.stream ?? r.id}: ${defer ? "deferred" : choice === "?" ? "unsure" : choice}`);
    } catch (e) {
      setErrors(errWords(e));
    } finally {
      setBusy(false);
    }
  }
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === "a" && r.premark) { void submit("stream"); ev.preventDefault(); }
      else if (ev.key === "s") { setCall("stream"); ev.preventDefault(); }
      else if (ev.key === "l") { setCall("not stream"); ev.preventDefault(); }
      else if (ev.key === "?") { setCall("?"); ev.preventDefault(); }
      else if (ev.key === "Enter") { void submit(); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });  // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="detail review-mode" data-testid="slough-detail">
      <div className="detail-cols">
        <div className="detail-content">
          <div className="verify-bar">
            <div className="verify-row">
              <strong>{r.name || r.stream || "unnamed"}</strong>
              <span className="dim">poly {r.id} · R{r.region}{f.mu ? ` · MU ${f.mu}` : ""}</span>
              {was && <span className={`badge verify-${was.status}`}>{was.status === "verified" ? was.call : was.status} · {was.reviewer} {was.date}</span>}
              <span className="spacer" style={{ flex: 1 }} />
              <span className="verify-nav">
                <button className="btn small" disabled={!nav.onPrev} onClick={nav.onPrev}>‹ k</button>
                <span className="dim">{nav.pos || "–"}/{nav.total}</span>
                <button className="btn small" disabled={!nav.onNext} onClick={nav.onNext}>j ›</button>
              </span>
            </div>
          </div>
          {errors.length > 0 && <div className="errors" role="alert" style={{ position: "static", margin: "6px 0" }}>
            {errors.map((e, i) => <div key={i}>{e}</div>)}</div>}

          <div className="section dfo-suggestion" data-testid="slough-premark-box">
            <div className="ext-row">
              <span className="k" style={{ color: "#d97706" }}>pre-mark</span>
              {r.premark ? <>
                <span><strong>STREAM</strong> — {r.premark.basis === "narrow" ? "narrow" : "wide marsh / swamp"}: {r.premark.why}</span>
                <button type="button" className="btn small primary" data-testid="slough-accept" disabled={busy}
                  onClick={() => void submit("stream")} title="records your mark: stream (a)">accept: stream (a)</button>
              </> : r.page_verdict ? <span>none — you marked it <strong>{r.page_verdict}</strong> on the review page</span>
                : r.intermittent ? <span>none — <strong>intermittent</strong> (FWA {r.fwa}): left for you, never bulk-folded</span>
                : r.width_m == null ? <span>none — <strong>no width measured</strong>: your call</span>
                : <span>none — {r.why ?? "wider than 44 m and not marsh: your call"}</span>}
            </div>
            <div className="ext-row dim" style={{ fontSize: 12 }}>a proposal only: nothing is saved until you accept or record a mark</div>
          </div>

          <div className="section entry-scope" data-testid="slough-facts">
            <div className="scope-head"><h3>The piece</h3>
              <a href={gmaps(r)} target="_blank" rel="noreferrer" data-testid="slough-gmaps">Google Maps satellite ↗</a></div>
            <div className="ext-row"><span className="k">on</span><span>{f.stream_name ?? r.stream ?? "?"}{f.stream_item ? ` (${f.stream_item})` : ""}</span></div>
            <div className="ext-row"><span className="k">width</span><span><strong>{m(r.width_m)}</strong> mean · {r.area_ha ?? "?"} ha
              {f.elongation != null ? ` · ${f.elongation.toFixed(1)}:1 long` : ""}</span></div>
            <div className="ext-row"><span className="k">FWA</span><span>{r.fwa}{f.fwa_feature_code ? ` · ${f.fwa_feature_code}` : ""}{f.poly_layer ? ` · ${f.poly_layer}` : ""}</span></div>
            <div className="ext-row"><span className="k">satellite</span><span>{pct(r.img_water)} open water · {pct(r.img_veg)} green
              <span className="dim"> (ESRI pixels; your not-stream pieces: median 30% water, 41% green)</span></span></div>
            {r.page_note && <div className="ext-row" data-testid="slough-page-note"><span className="k">your note</span><span>“{r.page_note}” <span className="dim">(review page)</span></span></div>}
            <div className="ext-row"><span className="k">finder</span><span>{r.rule_class}{r.complex ? ` · complex ${r.complex}` : ""}</span></div>
            {(f.entries_missing_on_polygon ?? []).length > 0 && (
              <div className="ext-row"><span className="k">rules</span><span className="dim">bind the river but not this piece: {f.entries_missing_on_polygon!.slice(0, 5).join(", ")}</span></div>)}
            {(f.evidence ?? []).length > 0 && (
              <details><summary className="dim">evidence ({f.evidence!.length})</summary>
                {f.evidence!.map((e, i) => <div key={i} className="ev-row dim" style={{ fontSize: 12 }}>{e}</div>)}</details>)}
          </div>

          <div className="checklist" data-testid="slough-checklist">
            <div className="checklist-head"><strong>Your call</strong> <span className="dim">a accept pre-mark · s stream · l not stream · ? unsure · Enter records · j k move</span></div>
            <div className="check-row">
              <span className="check-q">Is this piece part of the stream (a slough / canal / channel)?</span>
              <span className="check-a">
                {([["stream", "stream", "y"], ["not stream", "not stream (lake)", "n"], ["?", "unsure", "q"]] as const).map(([k, w, cls]) => (
                  <button key={k} type="button" className={`ans ans-${cls}${call === k ? " on" : ""}`} aria-pressed={call === k}
                    data-testid={`slough-call-${cls}`} onClick={() => setCall(k)}>{w}</button>
                ))}
              </span>
            </div>
            <textarea className="check-note" placeholder="note (optional)" value={note} onChange={(e) => setNote(e.target.value)} />
            <div className="verdict-bar" style={{ position: "static", margin: 0 }}>
              <input placeholder="your name" value={reviewer} onChange={(e) => setName(e.target.value)} aria-label="reviewer" />
              <button className="btn primary" disabled={busy} data-testid="slough-record" onClick={() => void submit()}>
                {busy ? "recording…" : "Record mark"}</button>
              <button className="btn small" disabled={busy} onClick={() => void submit(null, true)}>defer</button>
            </div>
          </div>
        </div>
        <div className="detail-map">
          <div className="dim" style={{ fontSize: 12, marginBottom: 4 }}>
            <span style={{ color: "#7c3aed" }}>violet: the candidate polygon's outline</span> · use the map's satellite button to see the ground
          </div>
          {/* no itemId: the stream's own fit would pull the view off a small piece; the basemap draws the water */}
          <MapPanel itemId={null} suggested={detail.outline} fitNamed={false} flyTo={fly} selectZoom={15} />
        </div>
      </div>
    </div>
  );
}
