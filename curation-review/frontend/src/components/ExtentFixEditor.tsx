import { useEffect, useState } from "react";
import { api } from "../api";
import type {
  AreaHit, ExtentFix, ExtentOp, ExtentOpInfo, ExtentPreview, FixPoint, FixPointKind, ItemHit, NamedRef, SplitHit,
} from "../types";
import { failed } from "../failures";
import { cutNamer, otherWaters, pointWords } from "../extentWords";

/** The ops and the owner's words for them, from the server only (`marks.OP_WORDS`, GET /api/extent-fix/ops). (T434) A
 *  failed read is shown; there is no copy here (a second copy of the policy drifts). */
let opsOnce: Promise<ExtentOpInfo[]> | null = null;
export const loadOps = () => (opsOnce ??= api.extentOps().then((x) => x.ops).catch((e) => { opsOnce = null; throw e; }));

/** Where a point comes from: a split on this map, any curated split, or a NEW point by anchor
 *  type (T164's anchor types: a tributary's confluence, a lake's inlet / outlet, a map click). */
const SOURCES: { v: string; kind: FixPointKind; words: string }[] = [
  { v: "map", kind: "split", words: "a split on this map" },
  { v: "search", kind: "split", words: "any curated split…" },
  { v: "confluence", kind: "confluence", words: "NEW: confluence of a tributary" },
  { v: "lake_inlet", kind: "lake_inlet", words: "NEW: a lake's inlet" },
  { v: "lake_outlet", kind: "lake_outlet", words: "NEW: a lake's outlet" },
  { v: "coord", kind: "coord", words: "NEW: click the map" },
  // T164: a NEW split proposed in the same mark (the DFO mode's "add a split"), by its ref
  { v: "new", kind: "new", words: "a NEW split proposed below" },
];

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
  /** T164: the NEW splits proposed in this mark (a point may name one by its ref) */
  newSplits?: NamedRef[];
  /** T164: which point sources to offer (`SOURCES`' values); default every one but "new" */
  sources?: string[];
  /** T164: the preview call (default: the catalogue entry's extent-preview) */
  previewFn?: (fix: ExtentFix) => Promise<ExtentPreview>;
  /** DFO: open the editor from the start, even with no fix yet (curating the extent is always on offer) */
  defaultOpen?: boolean;
  /** T524: which PIECE of a DFO place this editor is (0-based; absent = the one extent). Piece 1 keeps the plain
   *  test ids; a later piece's carry `-p<n>` */
  piece?: number;
  /** (owner 2026-11-09) what the place binds with now, in words ("with tributaries — the DFO title"): shown beside
   *  the call, which starts as the place's own (none = it keeps inheriting; T524 review) */
  tributariesNow?: string;
  /** every cut label the screen knows (T525: words, never a bare id) — a cut with none is shown as missing */
  cutLabels: Record<string, string>;
  /** every water name the screen knows, for one ticked beyond its own (T525: "Bulkley River", never "gnis:2195") */
  itemNames: Record<string, string>;
  /** whether the preview draws the tributary walk (the slow toggle); off, a walking binding still SAYS it walks */
  tributariesDrawn?: boolean;
}

/* THE EXTENT-FIX EDITOR (T191). The reviewer proposes the extent — op, 1 or 2 split points, the
   items it holds on, tributaries — and it is recorded IN THE MARK (`Mark.extent_fix`); saving the mark
   writes it to the curated file (T325 write-through). The preview is the builder's own resolve on an
   in-memory copy (GET /extent-preview); a NEW point cannot be previewed until it is built. */
export function ExtentFixEditor({ scope, entryId, currentOp, mapSplits, waters, value, onChange,
                                  onPreview, onPick, splitAt, newSplits = [], sources,
                                  previewFn, defaultOpen = false, piece, tributariesNow = "", cutLabels, itemNames,
                                  tributariesDrawn = true }: Props) {
  const tid = piece ? `${scope}-p${piece + 1}` : scope;
  const offered = SOURCES.filter((x) => (sources ? sources.includes(x.v) : x.v !== "new"));
  const [open, setOpen] = useState(!!value || defaultOpen);
  const [ops, setOps] = useState<ExtentOpInfo[]>([]);
  useEffect(() => { loadOps().then(setOps, failed("the extent ops")); }, []);
  const [preview, setPreview] = useState<ExtentPreview | null>(null);
  const [busy, setBusy] = useState(false);
  const [pickAt, setPickAt] = useState<number | null>(null);
  // the names of the waters and cuts picked here (a search hit carries it), beside the ones the caller knows
  const [picked, setPicked] = useState<Record<string, string>>({});
  const [pickedCuts, setPickedCuts] = useState<Record<string, string>>({});
  const splitName = cutNamer(cutLabels, pickedCuts, Object.fromEntries(mapSplits.map((s) => [s.id, s.name])));

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
      const trimmed = { ...fix, points: fix.points.slice(0, want) };
      const got = await (previewFn ? previewFn(trimmed) : api.extentPreview(entryId, trimmed));
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
      <button type="button" className="btn small fix-open" data-testid={`fix-open-${tid}`}
        onClick={() => setOpen(true)} title="propose the right extent — it is recorded in the mark">
        fix extent…
      </button>
    );
  }
  return (
    <div className="extent-fix" data-testid={`fix-${tid}`}>
      <div className="fix-row">
        <strong>{piece !== undefined && piece > 0 ? `Piece ${piece + 1}` : "The new extent"}</strong>
        <span className="spacer" />
        <button type="button" className="btn small" data-testid={`fix-drop-${tid}`} onClick={() => {
          // a DFO piece stays open: the editor at this place now shows the next piece (T524)
          onChange(null); onPreview(null); onPick(null); setPreview(null); if (piece === undefined) setOpen(false);
        }}>{piece === undefined ? "discard this fix" : "drop this piece"}</button>
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
          <AreaSearch value={fix.area ?? null} onPick={(a) => set({ area: a })} scope={scope} />
        )}
      </div>
      {Array.from({ length: want }, (_, i) => (
        <PointPicker key={`${fix.op}-${i}`} n={i + 1} of={want} point={fix.points[i]} mapSplits={mapSplits}
          onPoint={(p) => setPoint(i, p)} picking={pickAt === i} onPickMap={() => pick(i)}
          splitName={splitName} offered={offered} newSplits={newSplits} first={waters.map((w) => w.name)}
          onLabel={(id, label) => setPickedCuts((m) => ({ ...m, [id]: label }))} />
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
        {otherWaters(fix.items, waters.map((w) => w.id), { ...itemNames, ...picked }).map(({ id: x, name }) => (
          <span key={x} className="chip-tag new" title={x} data-testid="fix-other-water">
            {name ?? <span className="field-error">water {x} has no name</span>}
            <button type="button" className="x" onClick={() => set({ items: fix.items.filter((y) => y !== x) })}>×</button>
          </span>
        ))}
        <ItemSearch placeholder="+ another water…" onPick={(h) => {
          if (fix.items.includes(h.id)) return;
          setPicked((m) => ({ ...m, [h.id]: h.name }));
          set({ items: [...fix.items, h.id] });
        }} />
        <span className="dim">{fix.items.length ? "" : "(none ticked = the entry's waters)"}</span>
      </div>
      <div className="fix-row">
        <span className="k">tributaries</span>
        {([[null, "no call"], [true, "include"], [false, "exclude"]] as [boolean | null, string][]).map(([v, w]) => (
          <button key={w} type="button" className={`ans trib-call${(fix.tributaries ?? null) === v ? " on" : ""}`}
            aria-pressed={(fix.tributaries ?? null) === v} onClick={() => set({ tributaries: v })}>{w}</button>
        ))}
        {tributariesNow && <span className="dim" data-testid={`fix-trib-now-${tid}`}>now: {tributariesNow}</span>}
      </div>
      <div className="fix-row">
        <button type="button" className="btn small" disabled={busy} data-testid={`fix-preview-${tid}`}
          onClick={runPreview}>{busy ? "resolving…" : "preview on map"}</button>
        {preview && (preview.resolvable
          ? <span data-testid={`fix-preview-says-${tid}`}><strong>binds {preview.n_sections?.toLocaleString()} section(s)</strong>
              {preview.truncated && " (drawing capped)"} · on the map
              {preview.outcomes && Object.values(preview.outcomes).some((o) => o !== "bound") &&
                <span className="warn-text"> · {Object.entries(preview.outcomes).map(([k, v]) => `${k} ${v}`).join(", ")}</span>}
              {preview.walks_tributaries === true && <span data-testid={`fix-preview-tribs-${tid}`}>{tributariesDrawn
                ? " · with tributaries" : " · with tributaries — not drawn; tick Show tributaries to draw"}</span>}
              {preview.walks_tributaries === false && <span> · no tributaries</span>}
              {preview.others && <span style={{ display: "block" }} data-testid="fix-others">
                It has no stretch of its own. It applies to {preview.others.length} placed place{preview.others.length === 1 ? "" : "s"} of this water right now
                {preview.others.length ? ": " + preview.others.map((o) => `“${o.text.slice(0, 70)}” (${o.extent})`).join("; ") : ""}.
                {preview.others_unplaced?.length ? ` Not included until placed: ${preview.others_unplaced.map((o) => `“${o.text.slice(0, 60)}”`).join("; ")}.` : ""}
                {" "}A place placed later joins by itself; it is never taken out of “the rest of the water”.
                {preview.one_sibling && <span className="warn-text" style={{ display: "block" }} data-testid="fix-one-sibling">{preview.one_sibling}</span>}</span>}
              {preview.dead_pieces?.length ? <span className="warn-text" data-testid={`fix-dead-pieces-${tid}`}>
                {" "}· piece {preview.dead_pieces.join(", ")} binds NOTHING alone — the build refuses it.
                {(preview.dead_why ?? []).map((w, i) => <span key={i} style={{ display: "block" }} data-testid="fix-dead-why">{w}</span>)}</span> : null}
            </span>
          : <span className="warn-text" data-testid={`fix-preview-says-${tid}`}>no preview: {preview.why}</span>)}
        {!preview && fix.points.some((p) => p && p.kind !== "split") && (
          <span className="dim">a NEW point has no preview until its cut is built (once the save lands)</span>
        )}
      </div>
    </div>
  );
}

function PointPicker({ n, of, point, mapSplits, onPoint, picking, onPickMap, splitName, offered,
                      newSplits, onLabel, first }: {
  first: string[];
  n: number; of: number; point?: FixPoint; mapSplits: NamedRef[]; onPoint: (p: FixPoint) => void;
  picking: boolean; onPickMap: () => void; splitName: (id: string) => string;
  offered: typeof SOURCES; newSplits: NamedRef[];
  /** a cut picked by search, with its label (so the editor names it) */
  onLabel: (id: string, label: string) => void;
}) {
  const [src, setSrc] = useState<string>(
    !point ? "map" : point.kind === "split" ? (mapSplits.some((s) => s.id === point.split_id) ? "map" : "search")
      : point.kind);
  const kind = SOURCES.find((s) => s.v === src)!.kind;
  return (
    <div className="fix-row fix-point">
      <span className="k">point{of > 1 ? ` ${n}` : ""}</span>
      <select aria-label={`point ${n} source`} value={src} onChange={(e) => setSrc(e.target.value)}>
        {offered.map((s) => <option key={s.v} value={s.v}>{s.words}</option>)}
      </select>
      {src === "map" && (
        <select aria-label={`point ${n} split`} value={point?.kind === "split" ? point.split_id ?? "" : ""}
          onChange={(e) => e.target.value && onPoint({ kind: "split", split_id: e.target.value })}>
          <option value="">— pick a split —</option>
          {mapSplits.map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}
        </select>
      )}
      {src === "new" && (
        <select aria-label={`point ${n} new split`} value={point?.kind === "new" ? point.split_id ?? "" : ""}
          onChange={(e) => e.target.value && onPoint({ kind: "new", split_id: e.target.value })}>
          <option value="">{newSplits.length ? "— pick a new split —" : "— add a split below first —"}</option>
          {newSplits.map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}
        </select>
      )}
      {src === "search" && <SplitSearch first={first} onPick={(h) => { onLabel(h.id, h.label); onPoint({ kind: "split", split_id: h.id }); }} />}
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
      {point && src !== "map" && src !== "new" && (
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
    const t = window.setTimeout(() => fn(q).then((h) => live && setHits(h)).catch(failed("the search")), 200);
    return () => { live = false; window.clearTimeout(t); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);
  return { q, setQ, hits, clear: () => { setQ(""); setHits([]); } };
}

export function SplitSearch({ onPick, first = [] }: { onPick: (h: SplitHit) => void;
  /** T590: the names of the waters this picker is on — their cuts are offered first */
  first?: string[] }) {
  const key = first.join("|");
  const s = useSearch((q: string) => api.searchSplits(q, key ? key.split("|") : []));
  return (
    <span className="fix-search">
      <input placeholder="split name…" value={s.q} onChange={(e) => s.setQ(e.target.value)} />
      {s.hits.length > 0 && (
        <ul className="fix-hits">
          {s.hits.map((h) => (
            <li key={h.id} onClick={() => { onPick(h); s.clear(); }}>
              {h.own ? <b>{h.water}</b> : h.water}: {h.label} <span className="dim">{h.kind ?? ""} · {h.id}{h.own === false && first.length ? " · another water" : ""}</span>
            </li>
          ))}
        </ul>
      )}
    </span>
  );
}

/** THE "WITHIN AN AREA" PICK LIST (owner 2026-10-26: the op showed no areas). The atlas's areas
 *  (`/api/areas/search`) are listed as soon as the box has focus, narrowed by what is typed; a pick
 *  records the area id, and the chosen area is shown beside the box. */
export function AreaSearch({ value, onPick, scope }: {
  value: string | null; onPick: (id: string | null) => void; scope: string;
}) {
  const [q, setQ] = useState("");
  const [open, setOpen] = useState(false);
  const [hits, setHits] = useState<AreaHit[]>([]);
  const [names, setNames] = useState<Record<string, string>>({});
  useEffect(() => {
    if (!open) return;
    let live = true;
    const t = window.setTimeout(() => api.searchAreas(q).then((h) => {
      if (!live) return;
      setHits(h);
      setNames((m) => ({ ...m, ...Object.fromEntries(h.map((x) => [x.id, x.name])) }));
    }).catch(failed("the item search")), 150);
    return () => { live = false; window.clearTimeout(t); };
  }, [q, open]);
  return (
    <span className="fix-search" data-testid={`fix-area-${scope}`}>
      <input placeholder="search areas (park, reserve, region…)" value={q} aria-label={`area ${scope}`}
        onFocus={() => setOpen(true)} onBlur={() => window.setTimeout(() => setOpen(false), 200)}
        onChange={(e) => setQ(e.target.value)} />
      {open && hits.length > 0 && (
        <ul className="fix-hits">
          {hits.map((h) => (
            <li key={h.id} onMouseDown={(e) => e.preventDefault()}
              onClick={() => { onPick(h.id); setQ(""); setOpen(false); }}>
              {h.name} <span className="dim">{h.id}</span>
            </li>
          ))}
        </ul>
      )}
      {value && (
        <span className="chip-tag new" title={value}>{names[value] ?? value}
          <button type="button" className="x" onClick={() => onPick(null)}>×</button>
        </span>
      )}
    </span>
  );
}

export function ItemSearch({ kind, placeholder, onPick, onText }: {
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
