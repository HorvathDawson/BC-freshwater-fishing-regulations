import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, type EntryRefused } from "../api";
import type {
  AnchorCheck, AnchorKind, Check, CheckAnswer, DfoDetail, DfoProgress, DfoRow, EntrySplits, ExtentFix,
  ExtentPreview, Mode, NamedRef, NewSplit,
} from "../types";
import { getReviewer, isTyping, setReviewer } from "../review";
import { MapPanel } from "./MapPanel";
import { ExtentFixEditor, ItemSearch, SplitSearch } from "./ExtentFixEditor";

/** The anchor kinds a NEW split can take (`marks.AnchorKind`), in the reviewer's words. */
const ANCHORS: { v: AnchorKind; words: string }[] = [
  { v: "confluence", words: "confluence of a named tributary" },
  { v: "lake_outlet", words: "a lake's outlet" },
  { v: "lake_inlet", words: "a lake's inlet" },
  { v: "bridge", words: "a bridge (click it on the map)" },
  { v: "dam", words: "a dam / hatchery fence (click it)" },
  { v: "sign", words: "boundary signs (click them)" },
  { v: "coord", words: "a map point (click it)" },
  { v: "split", words: "an existing curated split" },
];
const CLICKED: AnchorKind[] = ["bridge", "dam", "sign", "coord"];
const CHECKS: Check[] = ["T", "R", "P"];
const QUESTIONS: Record<string, string> = {
  T: "The notice's words for the place read right (water + specific area, against the DFO page)?",
  R: "The scraped rows are this location's rules (species, dates, limits)?",
  P: "The extent lands right on the map — every cut it needs exists, in the right place?",
};
const STATE_WORDS: Record<string, string> = {
  needs_cut: "needs a cut", bound: "bound", unbound: "no extent yet", no_water: "no water matched",
};
const VERIFY: [string, string][] = [
  ["", "all"], ["todo", "to do"], ["unmarked", "never marked"], ["stale", "changed since marked"],
  ["wrong", "wrong"], ["unsure", "unsure"], ["deferred", "deferred"], ["verified", "verified"],
];

export const DFO_HASH = "dfo/";

interface Props {
  mode: Mode;
  /** back to the synopsis entries */
  onLeave: () => void;
}

/* THE DFO REVIEW MODE (T164): every scraped DFO salmon extent — the notice's own words, its rows,
   its curated extent on the map with the cuts on its water labelled — and a mark: T R P, an extent
   fix, and NEW SPLIT proposals by anchor (confluence, lake inlet / outlet, bridge, dam, signs, a map
   point, an existing split). Everything goes in the mark (POST /api/marks, scope "dfo"); a new split
   never goes into splits.json from here — T185 turns the marks into reviewed changes. */
export function DfoReview({ mode, onLeave }: Props) {
  const [rows, setRows] = useState<DfoRow[]>([]);
  const [progress, setProgress] = useState<DfoProgress | null>(null);
  const [filters, setFilters] = useState({ region: "", status: "active", need: "", verify: "", q: "" });
  const [selected, setSelected] = useState<string | null>(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return h.startsWith(DFO_HASH) ? h.slice(DFO_HASH.length) || null : null;
  });
  const [detail, setDetail] = useState<DfoDetail | null>(null);
  const [error, setError] = useState("");

  const loadRows = useCallback(() => {
    api.dfoQueue(filters).then(setRows).catch((e) => setError(String(e)));
    api.dfoProgress().then(setProgress).catch(() => {});
  }, [filters]);
  useEffect(() => { loadRows(); }, [loadRows]);

  const open = useCallback((id: string) => {
    setSelected(id);
    window.history.replaceState(null, "", `#${DFO_HASH}${encodeURIComponent(id)}`);
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
        <div className="filterbar">
          <div className="mode-tabs" role="tablist">
            <button className="btn small" role="tab" aria-selected={false} onClick={onLeave}>Synopsis entries</button>
            <button className="btn small primary" role="tab" aria-selected>DFO salmon extents</button>
          </div>
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
          <input className="dfo-search" placeholder="water or words…" value={filters.q}
            onChange={(e) => set({ q: e.target.value })} aria-label="search DFO" />
          <span className="progress dfo-progress" data-testid="dfo-progress">
            {all ? <>
              <strong>{all.done}/{all.total}</strong> marked
              {live && <span className="dim"> · live {live.done}/{live.total}</span>}
              <span className="dim"> · {all.by_state.needs_cut ?? 0} need a cut · {all.by_state.bound ?? 0} bound
                · {all.new_splits} new split{all.new_splits === 1 ? "" : "s"} proposed</span>
              {(all.by_verify.wrong ?? 0) + (all.by_verify.unsure ?? 0) > 0 &&
                <span className="dim"> · {all.by_verify.wrong ?? 0} wrong, {all.by_verify.unsure ?? 0} unsure</span>}
            </> : "…"}
          </span>
        </div>
        <div className="filterbar-line"><div style={{ width: `${pct}%`, height: "100%", background: "var(--accent)" }} /></div>
      </div>
      {error && <div className="errors" style={{ margin: 8 }}>{error}{" "}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setError("")}>dismiss</button></div>}
      <div className="main">
        <div className="queue" data-testid="dfo-queue">
          <div className="queue-group">{rows.length} location{rows.length === 1 ? "" : "s"} — the DFO page's reaches, one each</div>
          {rows.map((r) => (
            <div key={r.id} data-dfo={r.id} className={`queue-row${r.id === selected ? " selected" : ""}`}
              onClick={() => open(r.id)}>
              <div className="name">
                {r.water || r.kind}
                <span className={`badge dfo-${r.state}`}>{STATE_WORDS[r.state]}
                  {r.state === "needs_cut" && r.need.missing ? ` (${r.need.missing})` : ""}</span>
                {r.verify !== "unmarked" && <span className={`badge verify-${r.verify}`}>{r.verify}</span>}
              </div>
              <div className="meta">
                <span>r{r.region}{r.section ? ` · ${r.section}` : ""}</span>
                {r.status !== "active" && <span className="chip-tag">dormant</span>}
                {r.new_splits > 0 && <span className="chip-tag new">+{r.new_splits} split</span>}
                <span>{r.n_rows} row{r.n_rows === 1 ? "" : "s"}</span>
              </div>
              <div className="meta dfo-text">{r.text || "(the whole water)"}</div>
            </div>
          ))}
        </div>
        {detail ? (
          <DfoLocation key={detail.row.id} detail={detail} mode={mode}
            nav={{ pos: idx + 1, total: rows.length, onNext: next ? () => open(next) : undefined,
                   onPrev: prev ? () => open(prev) : undefined }}
            onMarked={() => {
              loadRows();
              if (next) open(next);
              else api.dfoLocation(detail.row.id).then(setDetail).catch(() => {});
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
  onMarked: () => void;
}) {
  const id = detail.row.id;
  const was = detail.mark;
  const [answers, setAnswers] = useState<Partial<Record<Check, CheckAnswer>>>({});
  const [problem, setProblem] = useState("");
  const [note, setNote] = useState("");
  const [reviewer, setName] = useState(getReviewer);
  const [splits, setSplits] = useState<NewSplit[]>([]);
  const [checks, setChecks] = useState<Record<string, AnchorCheck>>({});
  const [fix, setFix] = useState<ExtentFix | null>(null);
  const [reach, setReach] = useState<ExtentPreview | null>(null);
  const [fixPreview, setFixPreview] = useState<GeoJSON.FeatureCollection | null>(null);
  const [pick, setPick] = useState<{ cb: ((ll: [number, number]) => void) | null }>({ cb: null });
  const [errors, setErrors] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const since = useRef(Date.now());

  // what the extent binds now — magenta until a fix is previewed
  useEffect(() => {
    let live = true;
    api.dfoReach(id).then((r) => live && setReach(r)).catch(() => {});
    return () => { live = false; };
  }, [id]);

  const items: NamedRef[] = detail.items.map((i) => ({ id: i.id, name: i.name }));
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
      ...splits.flatMap((n) => {
        const at = checks[n.ref]?.coord ?? n.coord;
        return at ? [{
          type: "Feature" as const, geometry: { type: "Point" as const, coordinates: at as [number, number] },
          properties: { split_id: `new:${n.ref}`, label: `NEW ${n.ref}: ${anchorWords(n)}`, water: "",
                        name: `NEW ${n.ref}`, rules: ["extent"], parts: [], to_verify: true, grade: null,
                        item_id: n.on_item ?? null },
        }] : [];
      }),
    ],
  }), [detail.splits, splits, checks]);

  const reachFc = (reach?.geojson ?? null) as GeoJSON.FeatureCollection | null;
  const preview = fixPreview ?? reachFc;

  function setAnswer(c: Check, a: CheckAnswer) { setAnswers((x) => ({ ...x, [c]: a })); }

  async function submit(defer = false) {
    setErrors([]);
    if (!reviewer.trim()) { setErrors(["Type your name first: every mark names its reviewer."]); return; }
    setReviewer(reviewer.trim());
    setBusy(true);
    try {
      await api.postMark({
        scope: "dfo", target: id, answers, problem: problem || null, note, reviewer: reviewer.trim(),
        seconds: Math.round((Date.now() - since.current) / 100) / 10,
        ...(fix ? { extent_fix: [fix] } : {}),
        ...(splits.length ? { new_splits: splits } : {}),
        ...(defer ? { verdict: "deferred" as const } : {}),
      });
      onMarked();
    } catch (e) {
      const fe = (e as EntryRefused).fieldErrors;
      setErrors(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg) : [String(e)]);
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
              {reach && <span className="dim">{reach.resolvable ? `binds ${reach.n_sections} section(s) — magenta on the map` : reach.why}</span>}
            </div>
            {loc.binding.notes.length > 0 && <div className="ext-row"><span className="k">notes</span>{loc.binding.notes.join(" · ")}</div>}
            {detail.splits.unplaced.length > 0 && (
              <div className="ext-row dim">not on the map: {detail.splits.unplaced.map((u) => u.name).join(", ")}</div>
            )}
            {detail.siblings.length > 0 && (
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

          <NewSplits id={id} items={detail.items} hints={detail.hints} quote={detail.verbatim.specific_area}
            value={splits} onChange={setSplits} checks={checks}
            onChecked={(ref, c) => setChecks((m) => ({ ...m, [ref]: c }))}
            onPick={(cb) => setPick({ cb })} picking={!!pick.cb} />

          <div className="section">
            <ExtentFixEditor scope="entry" entryId={id} currentOp={detail.need.op ?? ex[0]?.op}
              mapSplits={mapSplits} waters={items} value={fix ?? undefined}
              onChange={(f) => setFix(f)} onPreview={(fc) => setFixPreview(fc)}
              onPick={(cb) => setPick({ cb })} splitAt={splitAt}
              newSplits={splits.map((n) => ({ id: n.ref, name: `${n.ref}: ${anchorWords(n)}` }))}
              sources={["map", "search", "new"]}
              previewFn={(f) => api.dfoExtentPreview(id, f)} />
          </div>

          <div className="checklist" data-testid="dfo-checklist">
            <div className="checklist-head"><strong>Review</strong> <span className="dim">y n ? answer the next check · Enter records · j k move</span></div>
            {CHECKS.map((c) => (
              <div key={c} className="check-row">
                <strong>{c}</strong><span className="check-q">{QUESTIONS[c]}</span>
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
            <div className="verdict-bar" style={{ position: "static", margin: 0 }}>
              <input placeholder="your name" value={reviewer} onChange={(e) => setName(e.target.value)} aria-label="reviewer" />
              <button className="btn primary" disabled={busy} data-testid="dfo-record" onClick={() => void submit()}>
                {busy ? "recording…" : "Record mark"}</button>
              <button className="btn small" disabled={busy} onClick={() => void submit(true)}>defer</button>
              {(fix || splits.length > 0) && <span className="dim">carries {splits.length ? `${splits.length} new split${splits.length === 1 ? "" : "s"}` : ""}{fix && splits.length ? " + " : ""}{fix ? "an extent fix" : ""} — proposals only (T185)</span>}
            </div>
            {errors.length > 0 && <div className="errors">{errors.map((e, i) => <div key={i}>{e}</div>)}</div>}
          </div>
          {was?.new_splits && was.new_splits.length > 0 && (
            <div className="section dim" style={{ fontSize: 12 }}>
              In the mark in force: {was.new_splits.map((n) => `${n.ref} ${anchorWords(n)}`).join(" · ")}
            </div>
          )}
        </div>
        <div className="detail-map">
          <div className="dim" style={{ fontSize: 12, marginBottom: 4 }}>
            magenta: {fixPreview ? "the proposed extent" : "what the extent binds now"} · amber labels: NEW splits proposed in this mark
          </div>
          <MapPanel itemId={detail.items[0]?.id ?? null} alsoItemIds={detail.items.slice(1).map((i) => i.id)}
            namedSplits={named} preview={preview} onPick={pick.cb} />
        </div>
      </div>
    </div>
  );
}

export function anchorWords(n: NewSplit): string {
  const off = n.offset_m ? `${Math.abs(n.offset_m)} m ${n.offset_m > 0 ? "above" : "below"} ` : "";
  const what = n.anchor === "confluence" ? `confluence of ${n.name || n.item_id || "?"}`
    : n.anchor === "lake_inlet" ? `inlet of ${n.name || n.item_id || "?"}`
    : n.anchor === "lake_outlet" ? `outlet of ${n.name || n.item_id || "?"}`
    : n.anchor === "split" ? `split ${n.split_id}`
    : `${n.anchor}${n.name ? ` ${n.name}` : ""}`;
  return off + what + (n.side ? ` · extent ${n.side}` : "");
}

/* NEW SPLIT PROPOSALS (T164): a cut the extent needs that does not exist yet. Each is checked on the
   server (GET /anchor: snapped to the water, a tributary's mouth, a lake's edge) and drawn on the map;
   it is recorded in the mark, never in splits.json. */
function NewSplits({ id, items, hints, quote, value, onChange, checks, onChecked, onPick, picking }: {
  id: string; items: DfoDetail["items"]; hints: DfoDetail["hints"]; quote: string;
  value: NewSplit[]; onChange: (v: NewSplit[]) => void;
  checks: Record<string, AnchorCheck>; onChecked: (ref: string, c: AnchorCheck) => void;
  onPick: (cb: ((ll: [number, number]) => void) | null) => void; picking: boolean;
}) {
  const blank = (): NewSplit => ({ ref: "", anchor: "confluence", on_item: items[0]?.id ?? null, quote, side: null });
  const [draft, setDraft] = useState<NewSplit>(blank);
  const [said, setSaid] = useState<AnchorCheck | null>(null);
  const [busy, setBusy] = useState(false);
  const upd = (p: Partial<NewSplit>) => { setSaid(null); setDraft((d) => ({ ...d, ...p })); };

  async function add() {
    setBusy(true);
    const ref = newRef(value.map((v) => v.ref));
    const ns = { ...draft, ref };
    try {
      const got = await api.dfoAnchor(id, ns);
      setSaid(got);
      if (got.ok) {
        onChecked(ref, got);
        onChange([...value, ns]);
        setDraft(blank());
        setSaid(null);
      }
    } catch (e) {
      setSaid({ ok: false, why: String(e) });
    } finally {
      setBusy(false);
    }
  }
  const clicked = CLICKED.includes(draft.anchor);
  return (
    <div className="section extent-fix" data-testid="dfo-new-splits">
      <div className="fix-row"><strong>New splits</strong>
        <span className="dim">a cut the notice names that does not exist — proposed in the mark, never written to splits.json (T185)</span></div>
      {value.map((n) => (
        <div key={n.ref} className="fix-row" data-testid={`dfo-split-${n.ref}`}>
          <span className="chip-tag new">{n.ref}</span>
          <span>{anchorWords(n)}</span>
          {checks[n.ref] && <span className="dim">→ {checks[n.ref].words} · blk {checks[n.ref].blk} @ {checks[n.ref].measure?.toFixed(0)} m</span>}
          <button type="button" className="btn small" onClick={() => onChange(value.filter((x) => x.ref !== n.ref))}>remove</button>
        </div>
      ))}
      {hints.length > 0 && (
        <div className="fix-row">
          <span className="k">the notice names</span>
          {hints.map((h) => (
            <button key={h.name} type="button" className="split-chip" title={h.items.map((i) => i.id).join(", ")}
              onClick={() => upd(CLICKED.includes(h.anchor) ? { anchor: h.anchor, name: null, item_id: null, coord: null }
                : { anchor: h.anchor, name: h.items[0]?.name ?? h.name, item_id: h.items[0]?.id ?? null, coord: null })}>
              {h.anchor === "confluence" ? "⤚ " : h.anchor.startsWith("lake") ? "◯ " : "✚ "}{h.name}
              {h.items.length || CLICKED.includes(h.anchor) ? "" : " (not found)"}
            </button>
          ))}
        </div>
      )}
      <div className="fix-row">
        <span className="k">anchor</span>
        <select aria-label="new split anchor" value={draft.anchor}
          onChange={(e) => upd({ anchor: e.target.value as AnchorKind, coord: null, item_id: null, name: null, split_id: null })}>
          {ANCHORS.map((a) => <option key={a.v} value={a.v}>{a.words}</option>)}
        </select>
        {(draft.anchor === "confluence" || draft.anchor === "lake_inlet" || draft.anchor === "lake_outlet") && (
          <>
            <ItemSearch kind={draft.anchor === "confluence" ? "stream" : "lake"}
              placeholder={draft.anchor === "confluence" ? "tributary name…" : "lake name…"}
              onPick={(h) => upd({ name: h.name, item_id: h.id })} onText={(t) => upd({ name: t, item_id: null })} />
            {draft.name && <span className="fix-chosen">→ {draft.name}{draft.item_id ? ` (${draft.item_id})` : ""}</span>}
          </>
        )}
        {draft.anchor === "split" && (
          <>
            <SplitSearch onPick={(h) => upd({ split_id: h.id, name: h.label })} />
            {draft.split_id && <span className="fix-chosen">→ {draft.split_id}</span>}
          </>
        )}
        {clicked && (
          <>
            <input className="fix-text" placeholder="its name (e.g. Highway 19 bridge)" value={draft.name ?? ""}
              onChange={(e) => upd({ name: e.target.value || null })} />
            <button type="button" className={`btn small${picking ? " primary" : ""}`} data-testid="dfo-pick"
              onClick={() => {
                if (picking) { onPick(null); return; }
                onPick((ll) => { upd({ coord: ll }); onPick(null); });
              }}>{picking ? "click the map… (cancel)" : draft.coord ? "move it" : "click the map"}</button>
            {draft.coord && <span className="fix-chosen">→ {draft.coord[1].toFixed(5)}, {draft.coord[0].toFixed(5)}</span>}
          </>
        )}
      </div>
      <div className="fix-row">
        <span className="k">cuts</span>
        <select aria-label="the water it cuts" value={draft.on_item ?? ""} onChange={(e) => upd({ on_item: e.target.value || null })}>
          {items.map((i) => <option key={i.id} value={i.id}>{i.name}</option>)}
        </select>
        <span className="k">extent is</span>
        {([[null, "—"], ["upstream", "upstream"], ["downstream", "downstream"]] as [NewSplit["side"], string][]).map(([v, w]) => (
          <button key={w} type="button" className={`ans trib-call${(draft.side ?? null) === v ? " on" : ""}`}
            aria-pressed={(draft.side ?? null) === v} onClick={() => upd({ side: v })}>{w}</button>
        ))}
        <span className="dim">of it</span>
        <span className="k">offset m</span>
        <input className="fix-text" style={{ width: 70 }} type="number" placeholder="+up / −down" value={draft.offset_m ?? ""}
          onChange={(e) => upd({ offset_m: e.target.value === "" ? null : Number(e.target.value) })} />
      </div>
      <div className="fix-row">
        <span className="k">from the words</span>
        <input className="fix-text" style={{ flex: 1 }} value={draft.quote ?? ""} onChange={(e) => upd({ quote: e.target.value })} />
        <button type="button" className="btn small primary" disabled={busy || !items.length} data-testid="dfo-add-split"
          onClick={() => void add()}>{busy ? "checking…" : "check + add"}</button>
      </div>
      {said && !said.ok && <div className="fix-row warn-text" data-testid="dfo-anchor-refused">not added: {said.why}</div>}
    </div>
  );
}
