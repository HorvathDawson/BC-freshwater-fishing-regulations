import { useEffect, useState } from "react";
import { api } from "../api";
import type {
  ExtentFix, ExtentOp, ExtentOpInfo, ExtentPreview, FixPoint, FixPointKind, ItemHit, NamedRef, SplitHit,
} from "../types";

/** The owner's words for the ops, until the server's list arrives (`marks.OP_WORDS` is the source). */
const FALLBACK_OPS: ExtentOpInfo[] = [
  { op: "whole", points: 0, words: "whole" },
  { op: "upstream_of", points: 1, words: "upstream" },
  { op: "downstream_of", points: 1, words: "downstream" },
  { op: "between", points: 2, words: "between" },
];
let opsOnce: Promise<ExtentOpInfo[]> | null = null;
const loadOps = () => (opsOnce ??= api.extentOps().then((x) => x.ops).catch(() => FALLBACK_OPS));

/** Where a point comes from: a split on this map, any curated split, or a NEW point by anchor
 *  type (T164's anchor types: a tributary's confluence, a lake's inlet / outlet, a map click). */
const SOURCES: { v: string; kind: FixPointKind; words: string }[] = [
  { v: "map", kind: "split", words: "a split on this map" },
  { v: "search", kind: "split", words: "any curated split…" },
  { v: "confluence", kind: "confluence", words: "NEW: confluence of a tributary" },
  { v: "lake_inlet", kind: "lake_inlet", words: "NEW: a lake's inlet" },
  { v: "lake_outlet", kind: "lake_outlet", words: "NEW: a lake's outlet" },
  { v: "coord", kind: "coord", words: "NEW: click the map" },
];

export function pointWords(p: FixPoint, splitName: (id: string) => string): string {
  if (p.kind === "split") return p.split_id ? splitName(p.split_id) : "—";
  if (p.kind === "coord") return p.coord ? `${p.coord[1].toFixed(5)}, ${p.coord[0].toFixed(5)}` : "click the map";
  const what = p.kind === "confluence" ? "confluence of" : p.kind === "lake_inlet" ? "inlet of" : "outlet of";
  return `${what} ${p.name || p.item_id || "—"}`;
}

interface Props {
  scope: string;                       // "entry" or a rule id
  entryId: string;
  /** the op the extent has now, so the editor opens on it */
  currentOp?: string;
  /** the splits already on this map (the item's boundaries and the ones the rules name) */
  mapSplits: NamedRef[];
  /** the entry's own waters */
  waters: NamedRef[];
  value: ExtentFix | undefined;
  onChange: (fix: ExtentFix | null) => void;
  /** the preview's geometry for the map (null clears it) */
  onPreview: (fc: GeoJSON.FeatureCollection | null) => void;
  /** start / stop the map-click pick for a NEW point */
  onPick: (cb: ((ll: [number, number]) => void) | null) => void;
  /** the coordinates of a split drawn on this map, to mark it in the preview */
  splitAt: (id: string) => [number, number] | null;
}

/* THE EXTENT-FIX EDITOR (T191). The reviewer proposes the extent — op, 1 or 2 split points, the
   items it holds on, tributaries — and it is recorded IN THE MARK (`Mark.extent_fix`), never in a
   file; T185 turns it into a proposed edit. The preview is the builder's own resolve on an
   in-memory copy (GET /extent-preview); a NEW point cannot be previewed until it is built. */
export function ExtentFixEditor({ scope, entryId, currentOp, mapSplits, waters, value, onChange,
                                  onPreview, onPick, splitAt }: Props) {
  const [open, setOpen] = useState(!!value);
  const [ops, setOps] = useState<ExtentOpInfo[]>(FALLBACK_OPS);
  useEffect(() => { loadOps().then(setOps); }, []);
  const [preview, setPreview] = useState<ExtentPreview | null>(null);
  const [busy, setBusy] = useState(false);
  const [pickAt, setPickAt] = useState<number | null>(null);
  const splitName = (id: string) => mapSplits.find((s) => s.id === id)?.name ?? id;

  const fix: ExtentFix = value ?? {
    scope, op: (ops.some((o) => o.op === currentOp) ? currentOp : "whole") as ExtentOp,
    points: [], items: [], tributaries: null,
  };
  const want = ops.find((o) => o.op === fix.op)?.points ?? 0;
  const set = (patch: Partial<ExtentFix>) => { setPreview(null); onPreview(null); onChange({ ...fix, ...patch }); };
  const setPoint = (i: number, p: FixPoint) => {
    const pts = [...fix.points];
    pts[i] = p;
    set({ points: pts.slice(0, want) });
  };
  useEffect(() => () => onPick(null), []);  // eslint-disable-line react-hooks/exhaustive-deps

  function pick(i: number) {
    if (pickAt === i) { setPickAt(null); onPick(null); return; }
    setPickAt(i);
    onPick((ll) => {
      setPoint(i, { kind: "coord", coord: ll });
      setPickAt(null);
      onPick(null);
    });
  }

  async function runPreview() {
    setBusy(true);
    try {
      const got = await api.extentPreview(entryId, { ...fix, points: fix.points.slice(0, want) });
      setPreview(got);
      const pts: GeoJSON.Feature[] = fix.points.slice(0, want).flatMap((p, i) => {
        const at = p.kind === "coord" ? p.coord : p.kind === "split" && p.split_id ? splitAt(p.split_id) : null;
        return at ? [{ type: "Feature", properties: { label: `${i + 1}: ${pointWords(p, splitName)}` },
                       geometry: { type: "Point", coordinates: at } } as GeoJSON.Feature] : [];
      });
      onPreview({ type: "FeatureCollection",
                  features: [...((got.geojson?.features ?? []) as GeoJSON.Feature[]), ...pts] });
    } catch (e) {
      setPreview({ resolvable: false, why: String(e), n_sections: null });
    } finally {
      setBusy(false);
    }
  }

  if (!open) {
    return (
      <button type="button" className="btn small fix-open" data-testid={`fix-open-${scope}`}
        onClick={() => setOpen(true)} title="propose the right extent — it is recorded in the mark">
        fix extent…
      </button>
    );
  }
  return (
    <div className="extent-fix" data-testid={`fix-${scope}`}>
      <div className="fix-row">
        <strong>Fix extent</strong>
        <span className="dim">recorded in the mark, never in a file (T185 turns it into a proposed edit)</span>
        <span className="spacer" />
        <button type="button" className="btn small" onClick={() => {
          onChange(null); onPreview(null); onPick(null); setPreview(null); setOpen(false);
        }}>remove</button>
      </div>
      <div className="fix-row">
        <span className="k">op</span>
        <select aria-label={`op ${scope}`} value={fix.op}
          onChange={(e) => set({ op: e.target.value as ExtentOp })}>
          {ops.map((o) => (
            <option key={o.op} value={o.op}>
              {o.words}{o.points ? ` (${o.points} point${o.points === 1 ? "" : "s"})` : ""}
            </option>
          ))}
        </select>
        {fix.op === "within" && (
          <input className="fix-text" placeholder="area id (area:…) or name" value={fix.area ?? ""}
            onChange={(e) => set({ area: e.target.value || null })} />
        )}
      </div>
      {Array.from({ length: want }, (_, i) => (
        <PointPicker key={`${fix.op}-${i}`} n={i + 1} of={want} point={fix.points[i]} mapSplits={mapSplits}
          onPoint={(p) => setPoint(i, p)} picking={pickAt === i} onPickMap={() => pick(i)}
          splitName={splitName} />
      ))}
      <div className="fix-row">
        <span className="k">on</span>
        {waters.length === 0 && <span className="dim">no water matched</span>}
        {waters.map((w) => (
          <label key={w.id} className="fix-check" title={w.id}>
            <input type="checkbox" checked={fix.items.includes(w.id)}
              onChange={(e) => set({ items: e.target.checked ? [...fix.items, w.id]
                                                                : fix.items.filter((x) => x !== w.id) })} />
            {w.name}
          </label>
        ))}
        {fix.items.filter((x) => !waters.some((w) => w.id === x)).map((x) => (
          <span key={x} className="chip-tag new">{x}
            <button type="button" className="x" onClick={() => set({ items: fix.items.filter((y) => y !== x) })}>×</button>
          </span>
        ))}
        <ItemSearch placeholder="+ another water…" onPick={(h) => !fix.items.includes(h.id) && set({ items: [...fix.items, h.id] })} />
        <span className="dim">{fix.items.length ? "" : "(none ticked = the entry's waters)"}</span>
      </div>
      <div className="fix-row">
        <span className="k">tributaries</span>
        {([[null, "no call"], [true, "include"], [false, "exclude"]] as [boolean | null, string][]).map(([v, w]) => (
          <button key={w} type="button" className={`ans trib-call${(fix.tributaries ?? null) === v ? " on" : ""}`}
            aria-pressed={(fix.tributaries ?? null) === v} onClick={() => set({ tributaries: v })}>{w}</button>
        ))}
      </div>
      <div className="fix-row">
        <button type="button" className="btn small" disabled={busy} data-testid={`fix-preview-${scope}`}
          onClick={runPreview}>{busy ? "resolving…" : "preview on map"}</button>
        {preview && (preview.resolvable
          ? <span data-testid={`fix-preview-says-${scope}`}><strong>binds {preview.n_sections?.toLocaleString()} section(s)</strong>
              {preview.truncated && " (drawing capped)"} · magenta on the map
              {preview.outcomes && Object.values(preview.outcomes).some((o) => o !== "bound") &&
                <span className="warn-text"> · {Object.entries(preview.outcomes).map(([k, v]) => `${k} ${v}`).join(", ")}</span>}
            </span>
          : <span className="warn-text" data-testid={`fix-preview-says-${scope}`}>no preview: {preview.why}</span>)}
        {!preview && fix.points.some((p) => p && p.kind !== "split") && (
          <span className="dim">a NEW point has no preview until T185 builds it</span>
        )}
      </div>
    </div>
  );
}

function PointPicker({ n, of, point, mapSplits, onPoint, picking, onPickMap, splitName }: {
  n: number; of: number; point?: FixPoint; mapSplits: NamedRef[]; onPoint: (p: FixPoint) => void;
  picking: boolean; onPickMap: () => void; splitName: (id: string) => string;
}) {
  const [src, setSrc] = useState<string>(
    !point ? "map" : point.kind === "split" ? (mapSplits.some((s) => s.id === point.split_id) ? "map" : "search")
      : point.kind);
  const kind = SOURCES.find((s) => s.v === src)!.kind;
  return (
    <div className="fix-row fix-point">
      <span className="k">point{of > 1 ? ` ${n}` : ""}</span>
      <select aria-label={`point ${n} source`} value={src} onChange={(e) => setSrc(e.target.value)}>
        {SOURCES.map((s) => <option key={s.v} value={s.v}>{s.words}</option>)}
      </select>
      {src === "map" && (
        <select aria-label={`point ${n} split`} value={point?.kind === "split" ? point.split_id ?? "" : ""}
          onChange={(e) => e.target.value && onPoint({ kind: "split", split_id: e.target.value })}>
          <option value="">— pick a split —</option>
          {mapSplits.map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}
        </select>
      )}
      {src === "search" && <SplitSearch onPick={(h) => onPoint({ kind: "split", split_id: h.id })} />}
      {(src === "confluence" || src === "lake_inlet" || src === "lake_outlet") && (
        <ItemSearch kind={src === "confluence" ? "stream" : "lake"}
          placeholder={src === "confluence" ? "tributary name…" : "lake name…"}
          onPick={(h) => onPoint({ kind, name: h.name, item_id: h.id })}
          onText={(t) => onPoint({ kind, name: t })} />
      )}
      {src === "coord" && (
        <button type="button" className={`btn small${picking ? " primary" : ""}`} onClick={onPickMap}>
          {picking ? "click the map… (cancel)" : "click the map"}
        </button>
      )}
      {point && (src === "map" ? false : true) && (
        <span className="fix-chosen" title={JSON.stringify(point)}>→ {pointWords(point, splitName)}</span>
      )}
    </div>
  );
}

function useSearch<T>(fn: (q: string) => Promise<T[]>) {
  const [q, setQ] = useState("");
  const [hits, setHits] = useState<T[]>([]);
  useEffect(() => {
    if (q.trim().length < 2) { setHits([]); return; }
    let live = true;
    const t = window.setTimeout(() => fn(q).then((h) => live && setHits(h)).catch(() => {}), 200);
    return () => { live = false; window.clearTimeout(t); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);
  return { q, setQ, hits, clear: () => { setQ(""); setHits([]); } };
}

function SplitSearch({ onPick }: { onPick: (h: SplitHit) => void }) {
  const s = useSearch(api.searchSplits);
  return (
    <span className="fix-search">
      <input placeholder="split name…" value={s.q} onChange={(e) => s.setQ(e.target.value)} />
      {s.hits.length > 0 && (
        <ul className="fix-hits">
          {s.hits.map((h) => (
            <li key={h.id} onClick={() => { onPick(h); s.clear(); }}>
              {h.water}: {h.label} <span className="dim">{h.kind ?? ""} · {h.id}</span>
            </li>
          ))}
        </ul>
      )}
    </span>
  );
}

function ItemSearch({ kind, placeholder, onPick, onText }: {
  kind?: string; placeholder: string; onPick: (h: ItemHit) => void; onText?: (t: string) => void;
}) {
  const s = useSearch((q) => api.searchItems(q, kind));
  return (
    <span className="fix-search">
      <input placeholder={placeholder} value={s.q}
        onChange={(e) => { s.setQ(e.target.value); onText?.(e.target.value); }} />
      {s.hits.length > 0 && (
        <ul className="fix-hits">
          {s.hits.map((h) => (
            <li key={h.id} onClick={() => { onPick(h); s.clear(); }}>
              {h.name} <span className="dim">{h.kind} · MU {h.mus.join(", ")} · {h.id}</span>
            </li>
          ))}
        </ul>
      )}
    </span>
  );
}
