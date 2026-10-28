import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, type EntryRefused } from "../api";
import type { MarkPost } from "../types";
import { getReviewer, isTyping, setReviewer } from "../review";
import { MapPanel } from "./MapPanel";

export const POND_HASH = "sloughs/q154/";

/* SMALL PONDS (Q154, owner 2026-10-28) — a view of the Sloughs tab. Q154: should a small unnamed lake polygon a
   river runs through take the river's rules? The proposed cutoff: under 1 ha AND mean width ≤ 2 × the modelled
   river width, man-made excluded (10,650 of 136,080 polygons). The owner sees a sample on the satellite map and
   calls each one "pool" (river rules) / "real lake" / unsure: a mark of scope "q154" (POST /api/marks), the one
   write. The tally sets those calls against the cutoff's prediction. */

export interface PondRow {
  id: string; set: "example" | "sample"; n: number | null; stratum: string; why: string; region: string | null;
  river: string | null; order: number; ha: number; avg_width_m: number; length_m: number; elongation: number;
  river_width_m: number | null; ratio: number | null; man_made: boolean; caught: boolean; md_read: string | null;
  lat: number; lon: number; verify: string; reasons: string[]; call: "pool" | "real lake" | null;
  agrees: boolean | null; reviewer: string | null;
}
interface PondDetail {
  row: PondRow; map: GeoJSON.FeatureCollection; bounds: [number, number, number, number] | null;
  mark: { status: string; reviewer: string; date: string; call: string | null; note: string } | null;
}
interface Tally {
  total: number; done: number; called: number; agree: number; caught_but_lake: number; missed_pool: number;
  by_prediction: Record<string, Record<string, number>>; by_stratum: Record<string, Record<string, number>>;
  cutoff: { max_ha?: number; max_ratio?: number; caught_total?: number; set_total?: number };
}

async function get<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} for ${url}`);
  return (await r.json()) as T;
}

const SETS: [string, string][] = [["", "all 80"], ["example", "the 20 md examples"], ["sample", "the 60-pond sample"]];
const VERIFY: [string, string][] = [["", "all"], ["todo", "to do"], ["decided", "decided"], ["unsure", "unsure"]];
const CAUGHT: [string, string][] = [["", "any"], ["yes", "caught (→ river rules)"], ["no", "not caught (stays a lake)"]];

const fmt = (v: number | null | undefined, d = 0) => (v == null ? "?" : v.toFixed(d));
const gmaps = (r: { lat: number; lon: number }) => `https://www.google.com/maps/@${r.lat},${r.lon},17z/data=!3m1!1e3`;

/** a zoom that holds the pond with room around it */
function zoomFor(b: [number, number, number, number] | null, lat: number): number {
  if (!b) return 16;
  const span = Math.max((b[2] - b[0]) * Math.cos((lat * Math.PI) / 180) * 111320, (b[3] - b[1]) * 110540, 20);
  // maplibre's zoom counts 512 px tiles: 78,271 m per pixel at zoom 0 on the equator; the map is ~400 px wide
  const z = Math.log2((78271 * Math.cos((lat * Math.PI) / 180) * 400) / (span * 3));
  return Math.max(9, Math.min(18, z));
}

function errWords(e: unknown): string[] {
  const fe = (e as EntryRefused).fieldErrors;
  return [`NOT SAVED — ${fe ? "refused by the server" : "the save failed"}:`,
    ...(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [String(e)])];
}

type Choice = "pool" | "real lake" | "?";

/** the one write: a q154 mark */
async function postPond(body: { target: string; choice: Choice | null; note: string; reviewer: string;
                                 seconds?: number; defer?: boolean }) {
  const mark = {
    scope: "q154", target: body.target,
    answers: body.defer ? {} : { P: body.choice === "?" ? "?" : "y" },
    call: !body.defer && body.choice && body.choice !== "?" ? body.choice : null,
    note: body.note, reviewer: body.reviewer, ...(body.seconds != null ? { seconds: body.seconds } : {}),
    ...(body.defer ? { verdict: "deferred" } : {}),
  };
  return api.postMark(mark as unknown as MarkPost);
}

export function PondReview({ onLeave, onSloughs }: { onLeave: () => void; onSloughs: () => void }) {
  const [rows, setRows] = useState<PondRow[]>([]);
  const [tally, setTally] = useState<Tally | null>(null);
  const [filters, setFilters] = useState({ set: "", verify: "", caught: "" });
  const [selected, setSelected] = useState<string | null>(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return h.startsWith(POND_HASH) ? h.slice(POND_HASH.length) || null : null;
  });
  const [detail, setDetail] = useState<PondDetail | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const [notice, setNotice] = useState("");
  const [reviewer, setName] = useState(getReviewer);

  const asked = useRef(0);
  const loadRows = useCallback(() => {
    const qs = new URLSearchParams(Object.entries(filters).filter(([, v]) => v) as [string, string][]).toString();
    const n = ++asked.current;
    get<PondRow[]>(`/api/q154/queue${qs ? `?${qs}` : ""}`).then((got) => { if (n === asked.current) setRows(got); })
      .catch((e) => setErrors([String(e)]));
    get<Tally>("/api/q154/tally").then(setTally).catch(() => {});
  }, [filters]);
  useEffect(() => { loadRows(); }, [loadRows]);

  const open = useCallback((id: string) => {
    setSelected(id);
    window.history.replaceState(null, "", `#${POND_HASH}${encodeURIComponent(id)}`);
  }, []);
  useEffect(() => {
    if (!selected && rows.length) { open(rows[0].id); return; }
  }, [rows, selected, open]);
  useEffect(() => {
    if (!selected) { setDetail(null); return; }
    setDetail(null);
    get<PondDetail>(`/api/q154/ponds/${encodeURIComponent(selected)}`).then(setDetail).catch((e) => setErrors([String(e)]));
  }, [selected]);

  const idx = rows.findIndex((r) => r.id === selected);
  const next = idx >= 0 && idx < rows.length - 1 ? rows[idx + 1].id : null;
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
    if (selected) document.querySelector(`[data-pond="${CSS.escape(selected)}"]`)?.scrollIntoView({ block: "nearest" });
  }, [selected, rows]);

  const set = (patch: Partial<typeof filters>) => setFilters((f) => ({ ...f, ...patch }));
  const bar = tally && tally.total ? (100 * tally.done) / tally.total : 0;
  return (
    <div className="app dfo-mode slough-mode" data-testid="pond-mode">
      <div className="filterbar-wrap">
        <div className="filterbar">
          <div className="mode-tabs" role="tablist">
            <button className="btn small" role="tab" aria-selected={false} onClick={onLeave}>Synopsis entries</button>
            <button className="btn small" role="tab" aria-selected={false} onClick={onSloughs}>Sloughs</button>
            <button className="btn small primary" role="tab" aria-selected>Small ponds (Q154)</button>
          </div>
          <div>
            <label>show</label>
            <select aria-label="set" data-testid="pond-set" value={filters.set} onChange={(e) => set({ set: e.target.value })}>
              {SETS.map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          <div>
            <label>cutoff says</label>
            <select aria-label="caught" value={filters.caught} onChange={(e) => set({ caught: e.target.value })}>
              {CAUGHT.map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          <div>
            <label>review</label>
            <select aria-label="pond review" value={filters.verify} onChange={(e) => set({ verify: e.target.value })}>
              {VERIFY.map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          {tally && <TallyLine t={tally} />}
        </div>
        <div className="filterbar-line"><div style={{ width: `${bar}%`, height: "100%", background: "var(--accent)" }} /></div>
      </div>
      {notice && <div className="notice" data-testid="pond-notice" style={{ margin: 8 }}>{notice}{" "}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setNotice("")}>dismiss</button></div>}
      {errors.length > 0 && <div className="errors" role="alert" style={{ margin: 8 }}>{errors.map((e, i) => <div key={i}>{e}</div>)}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setErrors([])}>dismiss</button></div>}
      <div className="main">
        <div className="queue" data-testid="pond-queue">
          <div className="queue-group">{rows.length} pond{rows.length === 1 ? "" : "s"} — the cutoff's prediction is a proposal, your mark is the call
            {tally && <TallyTable t={tally} />}</div>
          {rows.map((r) => (
            <div key={r.id} data-pond={r.id} className={`queue-row${r.id === selected ? " selected" : ""}`} onClick={() => open(r.id)}>
              <div className="name">
                {r.set === "example" ? `#${r.n} ` : ""}{r.river || "unnamed creek"}
                <CaughtBadge caught={r.caught} />
                {r.verify !== "unmarked" && <span className={`badge verify-${r.verify}`}>{r.verify === "verified" ? r.call : r.verify}</span>}
                {r.agrees === false && <span className="chip-tag pv-disagree" title="your call differs from the cutoff's">≠ cutoff</span>}
              </div>
              <div className="meta">
                <span>{fmt(r.ha, 2)} ha</span>
                <span>{fmt(r.avg_width_m)} / {fmt(r.river_width_m)} m</span>
                <span>× {fmt(r.ratio, 1)}</span>
                <span>R{r.region ?? "?"}</span>
                <span className="dim">{r.set === "example" ? "md example" : r.stratum}</span>
              </div>
            </div>
          ))}
        </div>
        {detail ? (
          <PondView key={detail.row.id} detail={detail} reviewer={reviewer} setName={setName}
            nav={{ pos: idx + 1, total: rows.length, onNext: next ? () => open(next) : undefined, onPrev: prev ? () => open(prev) : undefined }}
            onMarked={(said) => { setNotice(said); loadRows(); if (next) open(next); }} />
        ) : (
          <div className="detail"><div className="empty-state">{selected ? "loading…" : "Select a pond from the list."}</div></div>
        )}
      </div>
    </div>
  );
}

function CaughtBadge({ caught }: { caught: boolean }) {
  return caught
    ? <span className="chip-tag pv-pre-verified" data-testid="pond-caught" title="under 1 ha and ≤ 2 × the river's width: would take the river's rules">caught → river rules</span>
    : <span className="chip-tag pv-unchecked" data-testid="pond-not-caught" title="the cutoff leaves it a lake">not caught</span>;
}

function TallyTable({ t }: { t: Tally }) {
  const cell = (p: string, k: string) => t.by_prediction[p]?.[k] ?? 0;
  return (
    <table className="dim" data-testid="pond-tally-table" style={{ fontSize: 12, borderCollapse: "collapse", margin: "4px 0" }}>
      <thead><tr><th style={{ textAlign: "left" }}>cutoff says</th><th>pool</th><th>real lake</th><th>unsure</th><th>to do</th></tr></thead>
      <tbody>{(["caught", "not caught"] as const).map((p) => (
        <tr key={p}><td>{p === "caught" ? "caught (river rules)" : "not caught (lake)"}</td>
          {["pool", "real lake", "unsure", "todo"].map((k) => <td key={k} style={{ textAlign: "center", padding: "0 8px" }}>
            <strong>{cell(p, k)}</strong></td>)}</tr>))}</tbody>
    </table>
  );
}

function TallyLine({ t }: { t: Tally }) {
  return (
    <span className="progress dfo-progress" data-testid="pond-tally" style={{ lineHeight: 1.35 }}>
      <strong>{t.done}/{t.total}</strong> marked · <strong>{t.agree}/{t.called}</strong> agree with the cutoff
      {t.called > 0 && <> (<span title="caught, but you called it a real lake">{t.caught_but_lake} caught-but-lake</span>,{" "}
        <span title="not caught, but you called it a pool">{t.missed_pool} missed pools</span>)</>}
    </span>
  );
}

function PondView({ detail, reviewer, setName, nav, onMarked }: {
  detail: PondDetail; reviewer: string; setName: (s: string) => void;
  nav: { pos: number; total: number; onNext?: () => void; onPrev?: () => void };
  onMarked: (said: string) => void;
}) {
  const r = detail.row;
  const was = detail.mark;
  const [choice, setChoice] = useState<Choice | null>(null);
  const [note, setNote] = useState("");
  const [errors, setErrors] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const since = useRef(Date.now());
  const fly = useMemo(() => {
    const b = detail.bounds;
    const c: [number, number] = b ? [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2] : [r.lon, r.lat];
    return { coord: c, zoom: zoomFor(b, r.lat), n: 1 };
  }, [detail.bounds, r.lon, r.lat]);

  async function submit(c: Choice | null = choice, defer = false) {
    setErrors([]);
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    if (!c && !defer) { setErrors(["Pick pool, real lake or unsure first."]); return; }
    if (defer && !note.trim()) { setErrors(["To defer, write a note saying why."]); return; }
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      await postPond({ target: r.id, choice: c, note, reviewer: reviewer.trim(), defer,
        seconds: Math.round((Date.now() - since.current) / 100) / 10 });
      onMarked(`saved ✓ ${r.id}: ${defer ? "deferred" : c === "?" ? "unsure" : c}`);
    } catch (e) {
      setErrors(errWords(e));
    } finally {
      setBusy(false);
    }
  }
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === "p") { void submit("pool"); ev.preventDefault(); }
      else if (ev.key === "l") { void submit("real lake"); ev.preventDefault(); }
      else if (ev.key === "?") { void submit("?"); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });  // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="detail review-mode" data-testid="pond-detail">
      <div className="detail-cols">
        <div className="detail-content">
          <div className="verify-bar">
            <div className="verify-row">
              <strong>{r.set === "example" ? `Example #${r.n}: ` : ""}{r.river || "unnamed creek"}</strong>
              <span className="dim">{r.id} · R{r.region ?? "?"}</span>
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

          <div className="section dfo-suggestion" data-testid="pond-prediction">
            <div className="ext-row">
              <span className="k" style={{ color: "#d97706" }}>cutoff</span>
              <span>{r.caught
                ? <><strong>CAUGHT</strong> — would take the river's rules (under 1 ha and ≤ 2 × the river's width)</>
                : <><strong>NOT CAUGHT</strong> — stays a lake ({r.man_made ? "man-made" : [r.ha >= 1 ? "1 ha or more" : null,
                    r.ratio == null || r.ratio > 2 ? "wider than 2 × the river" : null].filter(Boolean).join(", ")})</>}</span>
            </div>
            <div className="ext-row dim" style={{ fontSize: 12 }}>
              {r.set === "example" ? `example #${r.n} of q154-examples.md` : `random sample · ${r.stratum} (${r.why})`}
            </div>
          </div>

          <div className="section entry-scope" data-testid="pond-facts">
            <div className="scope-head"><h3>The pond</h3>
              <a href={gmaps(r)} target="_blank" rel="noreferrer">Google Maps ↗</a></div>
            <div className="ext-row"><span className="k">area</span><span><strong>{fmt(r.ha, 2)} ha</strong> · {fmt(r.length_m)} m long · {fmt(r.elongation, 1)}:1</span></div>
            <div className="ext-row"><span className="k">mean width</span><span><strong>{fmt(r.avg_width_m)} m</strong> <span className="dim">(2 × area ÷ perimeter; a round pond reads at half its diameter)</span></span></div>
            <div className="ext-row"><span className="k">river width</span><span><strong>{fmt(r.river_width_m)} m</strong> modelled <span className="dim">(order {r.order}; rough, within 2× either way)</span></span></div>
            <div className="ext-row"><span className="k">ratio</span><span><strong>× {fmt(r.ratio, 2)}</strong> <span className="dim">(mean width ÷ river width; the cutoff is ≤ 2)</span></span></div>
            {r.man_made && <div className="ext-row"><span className="k">FWA</span><span>man-made (reservoir / dugout)</span></div>}
            {r.md_read && <details><summary className="dim">Claude's read of this one (from the md)</summary>
              <div className="dim" style={{ fontSize: 12 }}>{r.md_read}</div></details>}
          </div>

          <div className="checklist" data-testid="pond-checklist">
            <div className="checklist-head"><strong>Your call</strong> <span className="dim">p pool · l real lake · ? unsure (each records) · j k move</span></div>
            <div className="check-row">
              <span className="check-q">Is this a pool / widening of the river (river rules) or a real lake?</span>
              <span className="check-a">
                {([["pool", "pool (river rules)", "y"], ["real lake", "real lake", "n"], ["?", "unsure", "q"]] as const).map(([k, w, cls]) => (
                  <button key={k} type="button" className={`ans ans-${cls}${choice === k ? " on" : ""}`} aria-pressed={choice === k}
                    data-testid={`pond-call-${cls}`} onClick={() => setChoice(k)}>{w}</button>
                ))}
              </span>
            </div>
            <textarea className="check-note" placeholder="note (optional)" value={note} onChange={(e) => setNote(e.target.value)} />
            <div className="verdict-bar" style={{ position: "static", margin: 0 }}>
              <input placeholder="your name" value={reviewer} onChange={(e) => setName(e.target.value)} aria-label="reviewer" />
              <button className="btn primary" disabled={busy} data-testid="pond-record" onClick={() => void submit()}>
                {busy ? "recording…" : "Record mark"}</button>
              <button className="btn small" disabled={busy} onClick={() => void submit(null, true)}>defer</button>
            </div>
          </div>
        </div>
        <div className="detail-map">
          <div className="dim" style={{ fontSize: 12, marginBottom: 4 }}>
            <span style={{ color: "#e11d48" }}>red: the pond's FWA outline</span> · <span style={{ color: "#0891b2" }}>bright blue: the river through it</span> · pale blue: other streams
          </div>
          <MapPanel itemId={null} suggested={detail.map} fitNamed={false} flyTo={fly} selectZoom={15} satellite />
        </div>
      </div>
    </div>
  );
}
