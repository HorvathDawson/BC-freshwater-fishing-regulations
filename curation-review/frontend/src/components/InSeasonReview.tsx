import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api";
import type {
  EntrySplits, InSeasonEvidence, InSeasonExtent, InSeasonItemSplits, InSeasonOp, InSeasonPreview, InSeasonQueue, InSeasonRow,
} from "../types";
import { getReviewer, isTyping, setReviewer } from "../review";
import { MapPanel } from "./MapPanel";
import { TopNav, setBadge } from "./TopNav";
import { ItemSearch } from "./ExtentFixEditor";

export const INSEASON_HASH = "inseason/";

const OPS: { op: InSeasonOp; words: string; n: number }[] = [
  { op: "whole", words: "the whole water", n: 0 },
  { op: "upstream_of", words: "upstream of", n: 1 },
  { op: "downstream_of", words: "downstream of", n: 1 },
  { op: "between", words: "between", n: 2 },
];
const nSplits = (op: InSeasonOp) => OPS.find((o) => o.op === op)?.n ?? 0;
type Filter = "unmatched" | "matched" | "all";

const STATE_CHIP: Record<InSeasonRow["state"], string> = {
  matched: "chip-tag conf-high", unmatched: "chip-tag conf-low", stale: "chip-tag conf-medium",
};
const dates = (r: InSeasonRow) =>
  r.dates.from || r.dates.to ? `${r.dates.from ?? "…"} → ${r.dates.to ?? "open"}` : r.effective || "no dates";

/** an AUTO match says so (owner add-on 2026-10-28), with its confidence; the evidence is the title */
function AutoChip({ r }: { r: InSeasonRow }) {
  if (r.match?.source !== "auto") return null;
  const c = r.match.confidence ?? "medium";
  return <span className={`chip-tag conf-${c}`} data-testid="inseason-auto"
    title={`matched automatically (${c}) — ${evidenceWords(r.match.evidence)}`}>auto · {c}</span>;
}
function evidenceWords(ev: InSeasonEvidence | null | undefined): string {
  if (!ev) return "";
  const parts = [ev.items ? `water: ${Object.entries(ev.items).map(([k, v]) => `${v} (${k})`).join(", ")} via ${ev.via ?? "?"}` : "",
                 ev.disambiguated_by ?? "",
                 ev.splits && Object.keys(ev.splits).length
                   ? `boundaries: ${Object.entries(ev.splits).map(([w, s]) => `“${w}” → ${s}`).join("; ")}` : "",
                 ...(ev.notes ?? [])];
  return parts.filter(Boolean).join(" · ");
}

function EffectChip({ r }: { r: InSeasonRow }) {
  if (r.in_effect == null) return <span className="chip-tag" title="no dates the app can read">dates ?</span>;
  return r.in_effect
    ? <span className="chip-tag synced" title={`in effect today (${dates(r)})`}>in effect</span>
    : <span className="chip-tag" title={dates(r)}>not in effect</span>;
}

/* THE IN-SEASON TAB (T230): the provincial in-season feed's rows (T225) and WHERE each applies. A row is
   matched to extents on registry waters (whole / upstream of / downstream of / between named splits, several
   = a union), previewed on the map with the builder's own extent resolver, and saved to
   data/curated/inseason/matches.json — the review app's one write that is not a mark (owner 2026-10-28:
   Claude/owner tooling, a history note per edit). Informational only: a match is a banner, never an answer. */
export function InSeasonReview() {
  const [queue, setQueue] = useState<InSeasonQueue | null>(null);
  const [filter, setFilter] = useState<Filter>("unmatched");
  const [error, setError] = useState("");
  const [selected, setSelected] = useState<string | null>(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return h.startsWith(INSEASON_HASH) ? h.slice(INSEASON_HASH.length) || null : null;
  });

  const load = useCallback(() => {
    api.inseasonQueue().then((q) => {
      setQueue(q);
      setBadge("inseason", `${q.progress.matched}/${q.progress.total}`);
    }).catch((e) => setError(String(e)));
  }, []);
  useEffect(() => { load(); }, [load]);

  const rows = useMemo(() => (queue?.rows ?? []).filter((r) =>
    filter === "all" || (filter === "matched" ? r.state !== "unmatched" : r.state === "unmatched")), [queue, filter]);
  const row = queue?.rows.find((r) => r.id === selected) ?? null;

  const open = useCallback((id: string) => {
    setSelected(id);
    window.history.replaceState(null, "", `#${INSEASON_HASH}${encodeURIComponent(id)}`);
  }, []);
  const idx = rows.findIndex((r) => r.id === selected);
  const nav = useRef({ next: null as string | null, prev: null as string | null });
  nav.current = { next: idx >= 0 && idx < rows.length - 1 ? rows[idx + 1].id : !selected && rows.length ? rows[0].id : null,
                  prev: idx > 0 ? rows[idx - 1].id : null };
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === "j" && nav.current.next) { open(nav.current.next); ev.preventDefault(); }
      else if (ev.key === "k" && nav.current.prev) { open(nav.current.prev); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  const p = queue?.progress;
  const pct = p && p.total ? (100 * p.matched) / p.total : 0;
  return (
    <div className="app inseason-mode" data-testid="inseason-mode">
      <div className="filterbar-wrap">
        <TopNav active="inseason" right={<>
          <span className="progress" data-testid="inseason-progress">
            {p ? <>
              <strong>{p.matched}/{p.total}</strong> matched{p.auto ? ` (${p.auto} auto)` : ""}
              <span className="dim"> · {p.unmatched} unmatched{p.removed ? ` · ${p.removed} removed` : ""}
                {p.stale ? ` · ${p.stale} stale` : ""}</span>
            </> : "…"}
          </span>
          <select aria-label="in-season filter" data-testid="inseason-filter" value={filter}
            onChange={(e) => setFilter(e.target.value as Filter)}>
            <option value="unmatched">unmatched</option>
            <option value="matched">matched (+ stale)</option>
            <option value="all">all</option>
          </select>
        </>} />
        <div className="filterbar-line"><div style={{ width: `${pct}%`, height: "100%", background: "var(--accent)" }} /></div>
      </div>
      {error && <div className="errors" style={{ margin: 8 }}>{error}{" "}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setError("")}>dismiss</button></div>}
      {queue && !queue.available && <div className="notice" style={{ margin: 8 }} data-testid="inseason-nofeed">{queue.message}</div>}
      <div className="main">
        <div className="queue" data-testid="inseason-queue">
          <div className="queue-group">{rows.length} row{rows.length === 1 ? "" : "s"} — the provincial in-season changes</div>
          {rows.map((r) => (
            <div key={r.id} data-inseason={r.id} className={`queue-row${r.id === selected ? " selected" : ""}`}
              onClick={() => open(r.id)}>
              <div className="name">
                {r.water || "(no water)"}
                <span className={STATE_CHIP[r.state]}>{r.state}</span>
                <AutoChip r={r} />
                <EffectChip r={r} />
              </div>
              <div className="meta">
                <span>r{r.region}</span>
                <span className={`chip-tag${r.status === "new" || r.status === "changed" ? " new" : ""}`}>{r.status}</span>
                <span>{dates(r)}</span>
              </div>
              <div className="meta">{r.change}</div>
            </div>
          ))}
        </div>
        {row ? (
          <InSeasonDetail key={row.id} row={row} source={queue?.source ?? null}
            nav={{ pos: idx + 1, total: rows.length,
                   onNext: nav.current.next ? () => open(nav.current.next!) : undefined,
                   onPrev: nav.current.prev ? () => open(nav.current.prev!) : undefined }}
            onSaved={load} />
        ) : (
          <div className="detail"><div className="empty-state">{selected && queue ? "no such row in the feed" : "Select an in-season row."}</div></div>
        )}
      </div>
    </div>
  );
}

interface Nav { pos: number; total: number; onNext?: () => void; onPrev?: () => void }

function InSeasonDetail({ row, source, nav, onSaved }: {
  row: InSeasonRow; source: string | null; nav: Nav; onSaved: () => void;
}) {
  // an unmatched row starts from auto's best guess (never saved until the owner saves it)
  const start = row.match?.extents ?? row.guess?.extents ?? [];
  const [extents, setExtents] = useState<InSeasonExtent[]>(() =>
    start.map(({ op, item_id, splits }) => ({ op, item_id, splits: [...splits] })));
  const [names, setNames] = useState<Record<string, string>>(() =>
    Object.fromEntries(start.map((e) => [e.item_id, e.item_name ?? e.item_id])));
  const [splitsOf, setSplitsOf] = useState<Record<string, InSeasonItemSplits>>({});
  const [from, setFrom] = useState(row.match?.effective?.from ?? "");
  const [to, setTo] = useState(row.match?.effective?.to ?? "");
  const [note, setNote] = useState("");
  const [by, setBy] = useState(getReviewer);
  const [preview, setPreview] = useState<InSeasonPreview | null>(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  // each water's named splits (the picker and the map's labels), fetched once per water
  const items = useMemo(() => [...new Set(extents.map((e) => e.item_id))], [extents]);
  useEffect(() => {
    for (const id of items) {
      if (splitsOf[id]) continue;
      api.inseasonItemSplits(id).then((s) => {
        setSplitsOf((m) => ({ ...m, [id]: s }));
        setNames((m) => ({ ...m, [id]: s.item.name }));
      }).catch(() => {});
    }
  }, [items, splitsOf]);

  const complete = extents.length > 0 && extents.every((e) => e.splits.length === nSplits(e.op) && e.splits.every(Boolean));
  const key = JSON.stringify(extents);
  useEffect(() => {
    setPreview(null);
    if (!complete) return;
    let live = true;
    const t = window.setTimeout(() => api.inseasonPreview(extents).then((p) => live && setPreview(p)).catch(() => {}), 250);
    return () => { live = false; window.clearTimeout(t); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, complete]);

  const named: EntrySplits = useMemo(() => {
    const chosen = new Set(extents.flatMap((e) => e.splits));
    const feats = items.flatMap((id) => (splitsOf[id]?.splits.features ?? []).map((f) => ({
      ...f, properties: { ...f.properties, rules: chosen.has(f.properties.split_id) ? ["extent"] : [] } })));
    return { type: "FeatureCollection", features: feats,
             unplaced: items.flatMap((id) => splitsOf[id]?.splits.unplaced ?? []) };
  }, [items, splitsOf, extents]);

  const patch = (i: number, p: Partial<InSeasonExtent>) => setExtents((xs) => xs.map((x, j) => {
    if (j !== i) return x;
    const next = { ...x, ...p };
    const n = nSplits(next.op);
    next.splits = [...next.splits.slice(0, n), ...Array(Math.max(0, n - next.splits.length)).fill("")];
    return next;
  }));

  const save = (clear: boolean) => {
    if (!note.trim()) { setMsg({ ok: false, text: "a note is required: it is this edit's history row" }); return; }
    setBusy(true);
    setMsg(null);
    setReviewer(by);
    api.inseasonSave({ id: row.id, extents: clear ? null : extents,
                       effective: clear ? null : { from: from || null, to: to || null }, note: note.trim(), by })
      .then((r) => { setMsg({ ok: true, text: clear ? "match cleared" : `saved: ${r.match?.extents.length ?? 0} extent(s)` });
                     setNote(""); onSaved(); })
      .catch((e) => setMsg({ ok: false, text: `NOT SAVED: ${e instanceof Error ? e.message : e}` }))
      .finally(() => setBusy(false));
  };

  return (
    <div className="detail review-mode" data-testid="inseason-detail">
      <div className="detail-cols">
        <div className="detail-content">
          <div className="verify-bar">
            <div className="verify-row">
              <strong>{row.water}</strong>
              <span className="dim">r{row.region} · {row.status}</span>
              <span className={STATE_CHIP[row.state]}>{row.state}</span>
              <AutoChip r={row} />
              <EffectChip r={row} />
              <span className="spacer" style={{ flex: 1 }} />
              <span className="verify-nav">
                <button className="btn small" disabled={!nav.onPrev} onClick={nav.onPrev}>‹ k</button>
                <span className="dim">{nav.pos || "–"}/{nav.total}</span>
                <button className="btn small" disabled={!nav.onNext} onClick={nav.onNext}>j ›</button>
              </span>
            </div>
          </div>

          <div className="source-pin" data-testid="inseason-source">
            <div className="evidence-head">
              <h3 style={{ margin: 0 }}>{row.water}</h3>
              {source && <a href={source} target="_blank" rel="noreferrer">the in-season page ↗</a>}
              <span className="dim">{row.id}</span>
            </div>
            <div className="dim" style={{ fontSize: 12 }}>{row.section}</div>
            <div className="verbatim" data-testid="inseason-change">{row.change}</div>
            <div className="dim" style={{ fontSize: 12 }}>
              effective <strong>{row.effective || "—"}</strong>
              {(row.from || row.to) && <> · read as {row.from ?? "…"} → {row.to ?? "open"}</>}
              {row.dates.source === "curated" && <> · <strong>curated override</strong> {dates(row)}</>}
              {row.parsed?.kinds?.length ? <> · {row.parsed.kinds.join(", ")}</> : null}
              {row.firstSeen && <> · first seen {row.firstSeen.slice(0, 10)}</>}
              {row.removedAt && <> · removed {row.removedAt.slice(0, 10)}</>}
            </div>
          </div>

          {row.state === "stale" && <div className="notice" style={{ margin: "6px 0" }}>
            The feed no longer holds this row: the match stays in the file until it is cleared by hand.</div>}

          {row.match?.source === "auto" && <div className="notice" style={{ margin: "6px 0" }} data-testid="inseason-auto-evidence">
            <strong>Matched automatically ({row.match.confidence ?? "?"}).</strong> {evidenceWords(row.match.evidence)}.
            {" "}Saving here makes it yours: auto never writes over an owner match.</div>}
          {!row.match && row.guess && <div className="notice" style={{ margin: "6px 0" }} data-testid="inseason-guess">
            <strong>Not matched automatically:</strong> {row.guess.why}.
            {row.guess.extents.length ? " The editor starts from auto's best guess — check it before saving." : ""}
            {evidenceWords(row.guess.evidence) && <div className="dim" style={{ fontSize: 12 }}>{evidenceWords(row.guess.evidence)}</div>}
          </div>}

          <div className="section" data-testid="inseason-editor">
            <h3>Where it applies</h3>
            {extents.length === 0 && <div className="dim">No extent yet — add a water.</div>}
            {extents.map((e, i) => {
              const opts = splitsOf[e.item_id]?.splits;
              const all = [...(opts?.features.map((f) => f.properties) ?? []), ...(opts?.unplaced ?? [])];
              return (
                <div key={i} className="verify-row" style={{ flexWrap: "wrap", gap: 6, margin: "4px 0" }} data-testid="inseason-extent">
                  <strong>{names[e.item_id] ?? e.item_id}</strong>
                  <span className="dim" style={{ fontSize: 11 }}>{e.item_id}</span>
                  <select aria-label="op" value={e.op} onChange={(ev) => patch(i, { op: ev.target.value as InSeasonOp })}>
                    {OPS.map((o) => <option key={o.op} value={o.op}>{o.words}</option>)}
                  </select>
                  {e.splits.map((s, k) => (
                    <select key={k} aria-label={`split ${k + 1}`} value={s}
                      onChange={(ev) => patch(i, { splits: e.splits.map((x, j) => (j === k ? ev.target.value : x)) })}>
                      <option value="">{opts ? (all.length ? "pick a split…" : "no named splits on this water") : "loading…"}</option>
                      {all.map((sp) => <option key={sp.split_id} value={sp.split_id}>{sp.label}</option>)}
                    </select>
                  ))}
                  {preview?.per_extent?.[i] && (
                    <span className={preview.per_extent[i].why ? "chip-tag conf-low" : "chip-tag conf-high"}
                      title={preview.per_extent[i].why ?? ""}>{preview.per_extent[i].n_sections} sections</span>
                  )}
                  <button className="btn small" onClick={() => setExtents((xs) => xs.filter((_, j) => j !== i))}>remove</button>
                </div>
              );
            })}
            <div style={{ margin: "6px 0" }}>
              <ItemSearch placeholder="add a water (search the registry)…" onPick={(h) => {
                setNames((m) => ({ ...m, [h.id]: h.name }));
                setExtents((xs) => [...xs, { op: "whole", item_id: h.id, splits: [] }]);
              }} />
            </div>
            <div className="verify-row" style={{ gap: 6, flexWrap: "wrap" }}>
              <label className="dim">effective override (optional)</label>
              <input type="date" aria-label="effective from" value={from} onChange={(e) => setFrom(e.target.value)} />
              <span className="dim">to</span>
              <input type="date" aria-label="effective to" value={to} onChange={(e) => setTo(e.target.value)} />
            </div>
            <div style={{ margin: "6px 0" }}>
              <textarea aria-label="note" data-testid="inseason-note" placeholder="note (required): why this extent — kept in the history"
                rows={2} style={{ width: "100%" }} value={note} onChange={(e) => setNote(e.target.value)} />
            </div>
            <div className="verify-row" style={{ gap: 6 }}>
              <input aria-label="by" placeholder="by" value={by} onChange={(e) => setBy(e.target.value)} style={{ width: 120 }} />
              <button className="btn primary" data-testid="inseason-save" disabled={busy || !complete || !note.trim()}
                onClick={() => save(false)}>Save match</button>
              <button className="btn" data-testid="inseason-clear" disabled={busy || row.state === "unmatched" || !note.trim()}
                onClick={() => save(true)}>Clear match</button>
              <span className="dim" style={{ fontSize: 12 }}>
                {!complete ? (extents.length ? "pick every split" : "")
                  : preview ? (preview.resolvable ? `binds ${preview.n_sections} section${preview.n_sections === 1 ? "" : "s"}` : `binds nothing: ${preview.why}`)
                  : "previewing…"}
              </span>
            </div>
            {msg && <div className={msg.ok ? "notice" : "errors"} role="status" data-testid="inseason-msg"
              style={{ position: "static", margin: "6px 0" }}>{msg.text}</div>}
          </div>

          {row.match && (
            <div className="section dim" style={{ fontSize: 12 }}>
              In force: {row.match.extents.map((e) => `${e.item_name ?? e.item_id} ${e.op.replace("_", " ")}`
                + (e.split_labels.length ? ` ${e.split_labels.join(" and ")}` : "")).join(" · ")} — {row.match.by} {row.match.at}
            </div>
          )}
          {row.history.length > 0 && (
            <div className="section">
              <h3>History</h3>
              <ul style={{ fontSize: 12, margin: 0, paddingLeft: 18 }}>
                {row.history.map((h, i) => (
                  <li key={i}><code>{h.at}</code> {h.action} {h.by ? `· ${h.by}` : ""} — {h.note}
                    {h.extents && <span className="dim"> ({h.extents.map((e) => `${e.op} ${e.item_id}${e.splits.length ? ` ${e.splits.join(", ")}` : ""}`).join("; ")})</span>}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
        <div className="detail-map">
          <div className="dim" style={{ fontSize: 12, marginBottom: 4 }}>
            magenta: what the extent{extents.length > 1 ? "s bind (union)" : " binds"} · labels: the named splits on {extents.length > 1 ? "these waters" : "the water"}
          </div>
          <MapPanel itemId={items[0] ?? null} alsoItemIds={items.slice(1)} namedSplits={named}
            preview={(preview?.geojson ?? null) as GeoJSON.FeatureCollection | null} />
        </div>
      </div>
    </div>
  );
}
