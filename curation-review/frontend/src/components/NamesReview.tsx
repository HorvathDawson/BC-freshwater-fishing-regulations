import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, type EntryRefused } from "../api";
import type { MarkPost } from "../types";
import { getReviewer, isTyping, setReviewer } from "../review";
import { MapPanel } from "./MapPanel";

export const NAMES_HASH = "names/";

/* NAMES (T224, owner 2026-10-28) — names for waters from local sources (a city's stream layer, OpenStreetMap, an ECCC
   station, DFO, the synopsis), each with its REAL source and evidence. An accepted name is searchable AND, on a water
   with no official name, its shown name, labelled with its source. The 181 high-confidence rows were auto-accepted:
   spot-check the sample. Every other row waits for your call: accept / reject (a mark of scope "name", the one write).
   "Accept / reject all visible" is one mark per row in one commit. */

export interface NameRow {
  id: string; proposed: string; group: "owner" | "auto" | "review"; spot: boolean;
  confidence: "high" | "medium" | "low"; relation: string; target: Record<string, string>; target_label: string;
  current_names: string[]; sources: string[]; rival_names: string[]; corroborated_by: string[]; display: boolean;
  owner_decision: string | null; lon: number | null; lat: number | null; verify: string; reasons: string[];
  call: "accept" | "reject" | null; fate: "accepted" | "rejected" | "waiting"; reviewer: string | null;
}
interface NameSourceView {
  source: string; label: string; ref: string | null; official: boolean; confidence: string; flags: string[];
  evidence: Record<string, unknown>;
}
interface NameDetail {
  row: NameRow; sources: NameSourceView[]; map: GeoJSON.FeatureCollection;
  mark: { status: string; reviewer: string; date: string; call: string | null; note: string } | null;
}
interface Progress {
  total: number; by_group: Record<string, number>; by_fate: Record<string, number>;
  spot: { total: number; done: number }; review: { total: number; done: number; by_confidence: Record<string, number> };
}

async function get<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} for ${url}`);
  return (await r.json()) as T;
}

const VIEWS: [string, string, Record<string, string>][] = [
  ["spot", "spot-check (auto-accepted sample)", { group: "auto", spot: "true" }],
  ["review-high", "to review: high", { group: "review", confidence: "high" }],
  ["review-medium", "to review: medium", { group: "review", confidence: "medium" }],
  ["review-low", "to review: low", { group: "review", confidence: "low" }],
  ["auto", "all auto-accepted", { group: "auto" }],
  ["owner", "your decisions (08MH079)", { group: "owner" }],
  ["all", "everything", {}],
];
const VERIFY: [string, string][] = [["", "all"], ["todo", "to do"], ["verified", "decided"], ["unsure", "unsure"]];

const pct = (v: unknown) => (typeof v === "number" ? `${Math.round(v * 100)} %` : "?");
const gmaps = (r: { lat: number; lon: number }) => `https://www.google.com/maps/@${r.lat},${r.lon},16z/data=!3m1!1e3`;

function errWords(e: unknown): string[] {
  const fe = (e as EntryRefused).fieldErrors;
  return [`NOT SAVED — ${fe ? "refused by the server" : "the save failed"}:`,
    ...(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [String(e)])];
}

type Choice = "accept" | "reject" | "?";

/** the one write: name mark(s) — `targets` for "all visible" */
async function postName(body: { targets: string[]; choice: Choice; note: string; reviewer: string; seconds?: number }) {
  const mark = {
    scope: "name", ...(body.targets.length === 1 ? { target: body.targets[0] } : { targets: body.targets }),
    answers: { P: body.choice === "?" ? "?" : "y" },
    call: body.choice === "?" ? null : body.choice,
    note: body.note, reviewer: body.reviewer, ...(body.seconds != null ? { seconds: body.seconds } : {}),
  };
  return api.postMark(mark as unknown as MarkPost);
}

export function NamesReview({ onLeave }: { onLeave: () => void }) {
  const [rows, setRows] = useState<NameRow[]>([]);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [view, setView] = useState("spot");
  const [verify, setVerify] = useState("");
  const [q, setQ] = useState("");
  const [selected, setSelected] = useState<string | null>(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return h.startsWith(NAMES_HASH) ? h.slice(NAMES_HASH.length) || null : null;
  });
  const [detail, setDetail] = useState<NameDetail | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const [notice, setNotice] = useState("");
  const [reviewer, setName] = useState(getReviewer);
  const [busy, setBusy] = useState(false);

  const asked = useRef(0);
  const loadRows = useCallback(() => {
    const params = { ...(VIEWS.find(([k]) => k === view)?.[2] ?? {}), ...(verify ? { verify } : {}), ...(q.trim() ? { q } : {}) };
    const qs = new URLSearchParams(params).toString();
    const n = ++asked.current;
    get<NameRow[]>(`/api/names/queue${qs ? `?${qs}` : ""}`).then((got) => { if (n === asked.current) setRows(got); })
      .catch((e) => setErrors([String(e)]));
    get<Progress>("/api/names/progress").then(setProgress).catch(() => {});
  }, [view, verify, q]);
  useEffect(() => { loadRows(); }, [loadRows]);

  const open = useCallback((id: string) => {
    setSelected(id);
    window.history.replaceState(null, "", `#${NAMES_HASH}${encodeURIComponent(id)}`);
  }, []);
  useEffect(() => { if (!selected && rows.length) open(rows[0].id); }, [rows, selected, open]);
  useEffect(() => {
    if (!selected) { setDetail(null); return; }
    setDetail(null);
    get<NameDetail>(`/api/names/candidates/${encodeURIComponent(selected)}`).then(setDetail).catch((e) => setErrors([String(e)]));
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
    if (selected) document.querySelector(`[data-name="${CSS.escape(selected)}"]`)?.scrollIntoView({ block: "nearest" });
  }, [selected, rows]);

  async function allVisible(choice: "accept" | "reject") {
    setErrors([]);
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    const targets = rows.filter((r) => r.verify === "unmarked" || r.verify === "stale").map((r) => r.id);
    if (!targets.length) { setNotice("nothing left to mark in this view"); return; }
    if (!window.confirm(`${choice} all ${targets.length} unmarked rows in this view?`)) return;
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      await postName({ targets, choice, note: `${choice} all visible (${view})`, reviewer: reviewer.trim() });
      setNotice(`saved ✓ ${targets.length} marks: ${choice}`);
      loadRows();
    } catch (e) { setErrors(errWords(e)); } finally { setBusy(false); }
  }

  const sp = progress?.spot, rv = progress?.review;
  return (
    <div className="app dfo-mode slough-mode" data-testid="names-mode">
      <div className="filterbar-wrap">
        <div className="filterbar">
          <div className="mode-tabs" role="tablist">
            <button className="btn small" role="tab" aria-selected={false} onClick={onLeave}>Synopsis entries</button>
            <button className="btn small primary" role="tab" aria-selected>Names</button>
          </div>
          <div>
            <label>show</label>
            <select aria-label="view" data-testid="names-view" value={view} onChange={(e) => { setView(e.target.value); setSelected(null); }}>
              {VIEWS.map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          <div>
            <label>review</label>
            <select aria-label="names review" value={verify} onChange={(e) => setVerify(e.target.value)}>
              {VERIFY.map(([k, w]) => <option key={k} value={k}>{w}</option>)}
            </select>
          </div>
          <div><label>find</label><input value={q} onChange={(e) => setQ(e.target.value)} placeholder="name / blk" /></div>
          {progress && <span className="progress dfo-progress" data-testid="names-progress">
            spot-check <strong>{sp?.done}/{sp?.total}</strong> · review <strong>{rv?.done}/{rv?.total}</strong>
            {" "}· accepted {progress.by_fate.accepted ?? 0}</span>}
          <button className="btn small" disabled={busy} data-testid="names-accept-all" onClick={() => void allVisible("accept")}>accept all visible</button>
          <button className="btn small" disabled={busy} data-testid="names-reject-all" onClick={() => void allVisible("reject")}>reject all visible</button>
        </div>
      </div>
      {notice && <div className="notice" data-testid="names-notice" style={{ margin: 8 }}>{notice}{" "}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setNotice("")}>dismiss</button></div>}
      {errors.length > 0 && <div className="errors" role="alert" style={{ margin: 8 }}>{errors.map((e, i) => <div key={i}>{e}</div>)}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setErrors([])}>dismiss</button></div>}
      <div className="main">
        <div className="queue" data-testid="names-queue">
          <div className="queue-group">{rows.length} name{rows.length === 1 ? "" : "s"} — accepted names are searchable; on a water with no official name, its shown name (labelled with its source)</div>
          {rows.map((r) => (
            <div key={r.id} data-name={r.id} className={`queue-row${r.id === selected ? " selected" : ""}`} onClick={() => open(r.id)}>
              <div className="name">
                {r.proposed}
                <span className={`chip-tag ${r.fate === "accepted" ? "pv-pre-verified" : r.fate === "rejected" ? "pv-disagree" : "pv-unchecked"}`}>{r.fate}</span>
                {r.verify !== "unmarked" && <span className={`badge verify-${r.verify}`}>{r.verify === "verified" ? r.call : r.verify}</span>}
              </div>
              <div className="meta">
                <span>{r.confidence}</span>
                <span>{r.current_names.length ? `now ${r.current_names[0]}` : "unnamed"}</span>
                <span className="dim">{r.sources.join(", ")}</span>
              </div>
            </div>
          ))}
        </div>
        {detail ? (
          <NameView key={detail.row.id} detail={detail} reviewer={reviewer} setName={setName}
            nav={{ pos: idx + 1, total: rows.length, onNext: next ? () => open(next) : undefined, onPrev: prev ? () => open(prev) : undefined }}
            onMarked={(said) => { setNotice(said); loadRows(); if (next) open(next); }} />
        ) : (
          <div className="detail"><div className="empty-state">{selected ? "loading…" : "Select a name from the list."}</div></div>
        )}
      </div>
    </div>
  );
}

function NameView({ detail, reviewer, setName, nav, onMarked }: {
  detail: NameDetail; reviewer: string; setName: (s: string) => void;
  nav: { pos: number; total: number; onNext?: () => void; onPrev?: () => void };
  onMarked: (said: string) => void;
}) {
  const r = detail.row;
  const was = detail.mark;
  const [note, setNote] = useState("");
  const [errors, setErrors] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const since = useRef(Date.now());
  const fly = useMemo(() => (r.lon != null && r.lat != null ? { coord: [r.lon, r.lat] as [number, number], zoom: 15, n: 1 } : undefined),
    [r.lon, r.lat]);

  async function submit(c: Choice) {
    setErrors([]);
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      await postName({ targets: [r.id], choice: c, note, reviewer: reviewer.trim(),
        seconds: Math.round((Date.now() - since.current) / 100) / 10 });
      onMarked(`saved ✓ ${r.proposed}: ${c === "?" ? "unsure" : c}`);
    } catch (e) { setErrors(errWords(e)); } finally { setBusy(false); }
  }
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === "a") { void submit("accept"); ev.preventDefault(); }
      else if (ev.key === "r") { void submit("reject"); ev.preventDefault(); }
      else if (ev.key === "?") { void submit("?"); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });  // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="detail review-mode" data-testid="names-detail">
      <div className="detail-cols">
        <div className="detail-content">
          <div className="verify-bar">
            <div className="verify-row">
              <strong>{r.proposed}</strong>
              <span className="dim">{r.target_label} · {r.confidence} · {r.relation}</span>
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

          <div className="section dfo-suggestion" data-testid="names-fate">
            <div className="ext-row">
              <span className="k" style={{ color: "#d97706" }}>now</span>
              <span>{r.fate === "accepted"
                ? <><strong>ACCEPTED</strong> — {r.owner_decision ?? (r.group === "auto" ? "auto-accepted (high confidence, one candidate, no rival)" : "your mark")}
                    {r.display ? "; shown as the water's name (it has no official one)" : "; searchable"}</>
                : r.fate === "rejected" ? <><strong>REJECTED</strong> by your mark</> : <><strong>WAITING</strong> for your call</>}</span>
            </div>
            <div className="ext-row"><span className="k">water</span>
              <span>{r.current_names.length ? <>named <strong>{r.current_names.join(" · ")}</strong></> : <strong>no name today</strong>}
                {r.rival_names.length > 0 && <span className="dim"> · other names proposed: {r.rival_names.join(", ")}</span>}</span></div>
            {r.lat != null && r.lon != null && <div className="ext-row"><span className="k">map</span>
              <a href={gmaps({ lat: r.lat, lon: r.lon })} target="_blank" rel="noreferrer">Google Maps satellite ↗</a></div>}
          </div>

          <div className="section entry-scope" data-testid="names-sources">
            <div className="scope-head"><h3>Sources</h3></div>
            {detail.sources.map((s, i) => (
              <div key={i} className="ext-row" style={{ alignItems: "flex-start" }}>
                <span className="k">{s.official ? "official" : s.confidence}</span>
                <span><strong>{s.label}</strong>{s.ref ? <span className="dim"> · {s.ref}</span> : null}
                  <div className="dim" style={{ fontSize: 12 }}>
                    {"blk_overlap" in s.evidence && <>{pct(s.evidence.blk_overlap)} of the FWA line, {pct(s.evidence.source_overlap)} of the source line · </>}
                    {"station_name" in s.evidence && <>station {String(s.evidence.station)} “{String(s.evidence.station_name)}” ({String(s.evidence.trust)}) · </>}
                    {"distance_m" in s.evidence && <>{String(s.evidence.distance_m)} m away · </>}
                    {s.flags.join(", ")}
                  </div></span>
              </div>
            ))}
          </div>

          <div className="checklist" data-testid="names-checklist">
            <div className="checklist-head"><strong>Your call</strong> <span className="dim">a accept · r reject · ? unsure (each records) · j k move</span></div>
            <div className="check-row">
              <span className="check-q">Is “{r.proposed}” a right name for this water?</span>
              <span className="check-a">
                <button type="button" className="ans ans-y" data-testid="names-accept" disabled={busy} onClick={() => void submit("accept")}>accept</button>
                <button type="button" className="ans ans-n" data-testid="names-reject" disabled={busy} onClick={() => void submit("reject")}>reject</button>
                <button type="button" className="ans ans-q" disabled={busy} onClick={() => void submit("?")}>unsure</button>
              </span>
            </div>
            <textarea className="check-note" placeholder="note (optional)" value={note} onChange={(e) => setNote(e.target.value)} />
            <div className="verdict-bar" style={{ position: "static", margin: 0 }}>
              <input placeholder="your name" value={reviewer} onChange={(e) => setName(e.target.value)} aria-label="reviewer" />
            </div>
          </div>
        </div>
        <div className="detail-map">
          <div className="dim" style={{ fontSize: 12, marginBottom: 4 }}>the point: where the source names it · the atlas lines show the FWA water</div>
          <MapPanel itemId={null} suggested={detail.map} fitNamed={false} flyTo={fly} selectZoom={15} satellite />
        </div>
      </div>
    </div>
  );
}
