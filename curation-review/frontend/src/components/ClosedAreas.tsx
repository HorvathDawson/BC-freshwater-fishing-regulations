import { useEffect, useMemo, useRef, useState } from "react";
import { api, type EntryRefused } from "../api";
import { areaGroups, areaStatus, closedLine } from "../checklist";
import { failed } from "../failures";
import type { ClosedPiece, ClosedPreview, DfoClosure, DfoClosureArea } from "../types";
import type { EditPoint } from "./MapPanel";
import { SplitSearch } from "./ExtentFixEditor";

/** what the checklist map shows for the area being edited (`ck-area-edit`): the draft's shape, its markers (draggable),
 *  and the map click it is waiting for */
export interface AreaEditLayers {
  shapes: GeoJSON.FeatureCollection; pairs: [[number, number], [number, number]][]; points: EditPoint[];
  onPick: ((ll: [number, number]) => void) | null; onMovePoint: (id: string, ll: [number, number]) => void;
}
const toMap = (detail: AreaEditLayers | null) => { window.dispatchEvent(new CustomEvent("ck-area-edit", { detail })); };
const fly = (at: [number, number]) => { window.dispatchEvent(new CustomEvent("ck-fly", { detail: at })); };

type Pick = "centre" | "end0" | "end1" | "line0" | "line1";

/** a fresh placement for an area nothing places yet */
function blank(a: DfoClosureArea, c: DfoClosure): ClosedPiece {
  const head = { key: a.key, water_id: c.water_id, name: a.name, item: a.item, on_item: c.on_item ?? "" };
  if (a.kind === "circle") return { ...head, kind: "circle", radius_m: a.radius_m ?? 400 };
  if (a.kind === "line") return { ...head, kind: "line", line: [] };
  return { ...head, kind: "stretch", points: [], new_splits: [] };
}
/** whether the draft is whole enough to draw and save */
function whole(p: ClosedPiece): boolean {
  if (p.kind === "circle") return !!p.centre && !!p.radius_m && p.radius_m > 0;
  if (p.kind === "line") return p.line?.length === 2;
  return p.points?.length === 2 && p.points.every(Boolean);
}

/* ENTRY B, SAVEABLE (T573, owner 2026-11-10): each closed area is confirmed, moved or reported on its own — one mark
   per area (scope `closed_area`), written through to the closed-areas file. A circle's centre is set by a map click (or
   by dragging its marker) and its radius typed; a stretch's two ends are picked from the existing cuts or clicked on the
   map (a new cut; an existing cut close by is offered instead); an unplaced area is placed the same way. Nothing here
   touches the place's own extent (entry A). */
export function ClosedAreas({ closure, locationId, reviewer, onSaved, onError }: {
  closure: DfoClosure; locationId: string; reviewer: string;
  onSaved: (text: string) => void; onError: (lines: string[]) => void;
}) {
  const [editing, setEditing] = useState<string | null>(null);
  const [draft, setDraft] = useState<ClosedPiece | null>(null);
  const [preview, setPreview] = useState<ClosedPreview | null>(null);
  const [pick, setPick] = useState<Pick | null>(null);
  const [wrong, setWrong] = useState<string | null>(null);
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const groups = useMemo(() => areaGroups(closure.areas), [closure.areas]);
  const area = closure.areas.find((a) => a.key === editing) ?? null;
  const seq = useRef(0);

  // the draft drawn by the server's own rule (a ring at true ground radius, a new cut resolved on the water)
  useEffect(() => {
    if (!draft || !whole(draft)) { setPreview(null); return; }
    const n = ++seq.current;
    api.closedAreaPreview(locationId, draft).then((p) => { if (n === seq.current) setPreview(p); },
      failed("the closed area's preview"));
  }, [draft, locationId]);

  const setEnd = (i: number, ll: [number, number]) => setDraft((d) => {
    if (!d) return d;
    const ref = `hand-${d.key}-${i + 1}`;
    const pts = [...(d.points ?? [])];
    pts[i] = { kind: "new", split_id: ref };
    const rest = (d.new_splits ?? []).filter((n) => n.ref !== ref);
    return { ...d, points: pts, new_splits: [...rest, { ref, anchor: "coord", on_item: d.on_item, coord: ll,
      name: `${d.name} closed area, ${i === 0 ? "upper" : "lower"} end (placed by hand)` }] };
  });
  const useCut = (i: number, id: string) => setDraft((d) => {
    if (!d) return d;
    const pts = [...(d.points ?? [])];
    const was = pts[i];
    pts[i] = { kind: "split", split_id: id };
    const keep = new Set(pts.filter((p) => p?.kind === "new").map((p) => p.split_id));
    return { ...d, points: pts, new_splits: (d.new_splits ?? []).filter((n) => keep.has(n.ref) && n.ref !== (was?.kind === "new" && !keep.has(was.split_id) ? was.split_id : "")) };
  });
  const onMap = (p: Pick, ll: [number, number]) => {
    if (p === "centre") setDraft((d) => d && { ...d, centre: ll, centre_from: "placed by the owner on the map", confidence: undefined });
    else if (p === "end0" || p === "end1") setEnd(p === "end0" ? 0 : 1, ll);
    else setDraft((d) => { if (!d) return d; const line = [...(d.line ?? [])]; line[p === "line0" ? 0 : 1] = ll; return { ...d, line }; });
  };

  // the map: the draft's shape + markers, the candidates of an end DFO describes two ways, the click being waited for
  useEffect(() => {
    const cands: EditPoint[] = closure.areas.flatMap((a) => (a.candidates ?? []).flatMap((c, i) => (c.coord
      ? [{ id: `cand-${a.key}-${i}`, coord: c.coord, label: `${a.name}: ${c.label}`, kind: "reference" as const }] : [])));
    if (!draft) { toMap(cands.length ? { shapes: { type: "FeatureCollection", features: [] }, pairs: [], points: cands, onPick: null, onMovePoint: () => {} } : null); return; }
    const feats: GeoJSON.Feature[] = [];
    const points: EditPoint[] = [...cands];
    if (draft.kind === "circle" && draft.centre) {
      if (preview?.ring) feats.push({ type: "Feature", properties: { name: `${draft.name} (your change)`, draft: true }, geometry: { type: "Polygon", coordinates: [preview.ring] } });
      points.push({ id: "centre", coord: draft.centre, label: `${draft.name}: centre`, kind: "cut" });
    }
    if (draft.kind === "line") {
      (draft.line ?? []).forEach((ll, i) => ll && points.push({ id: `line${i}`, coord: ll, label: `line end ${i + 1}`, kind: "cut" }));
      if (draft.line?.length === 2 && draft.line.every(Boolean))
        feats.push({ type: "Feature", properties: { name: `${draft.name} (your change)`, draft: true }, geometry: { type: "LineString", coordinates: draft.line } });
    }
    const pairs: [[number, number], [number, number]][] = [];
    if (draft.kind === "stretch" && preview) {
      preview.ends.forEach((e, i) => e.coord && points.push({ id: `end${i}`, coord: e.coord, label: `${i === 0 ? "upper" : "lower"} end: ${e.label}`, kind: e.new ? "cut" : "reference" }));
      const [a, b] = preview.ends;
      if (a?.coord && b?.coord) pairs.push([a.coord, b.coord]);
    }
    toMap({ shapes: { type: "FeatureCollection", features: feats }, pairs, points,
            // after the first end, the map waits for the second (two clicks place a line or a stretch)
            onPick: pick ? (ll) => {
              onMap(pick, ll);
              setPick(pick === "line0" && !draft.line?.[1] ? "line1" : pick === "end0" && !draft.points?.[1] ? "end1" : null);
            } : null,
            onMovePoint: (id, ll) => { if (id === "centre" || id === "end0" || id === "end1" || id === "line0" || id === "line1") onMap(id, ll); } });
  }, [draft, preview, pick, closure.areas]);  // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => () => toMap(null), []);

  function open(a: DfoClosureArea, from?: ClosedPiece) {
    setWrong(null); setNote(""); onError([]);
    setEditing(a.key);
    setDraft(from ?? a.piece ?? blank(a, closure));
    setPick(!from && !a.piece ? (a.kind === "circle" ? "centre" : a.kind === "line" ? "line0" : "end0") : null);
    const at = a.centre ?? a.ends[0]?.coord;
    if (at) fly(at);
  }
  const close = () => { setEditing(null); setDraft(null); setPreview(null); setPick(null); setWrong(null); setNote(""); };

  async function save(a: DfoClosureArea, body: { answers: { P: "y" | "n" }; piece?: ClosedPiece | null; problem?: string; note?: string }, said: string) {
    onError([]);
    if (!reviewer.trim()) { onError(["Type your name first: every mark names its reviewer."]); return; }
    setBusy(true);
    try {
      await api.postMark({ scope: "closed_area", target: a.target, reviewer: reviewer.trim(), ...body });
      close();
      onSaved(said);
    } catch (e) {
      const fe = (e as EntryRefused).fieldErrors;
      onError([`Refused: ${fe ? fe.map((x) => x.msg).join("; ") : String(e)}`]);
    } finally {
      setBusy(false);
    }
  }

  const row = (a: DfoClosureArea) => {
    const st = areaStatus(a);
    const at = a.centre ?? a.ends[0]?.coord ?? a.line?.[0];
    const on = editing === a.key;
    return (
      <li key={a.key} data-testid="ck-closed-area" data-key={a.key} className={a.live ? undefined : "ck-kept"}>
        <div style={at ? { cursor: "pointer" } : undefined} title={at ? "show it on the map" : undefined} onClick={() => at && fly(at)}>
          <b className={a.state === "changed" || a.state === "radius" ? "warn-text" : undefined}>{closedLine(a)}</b>
          {a.confidence && !a.confirmed ? <span className="chip-tag"> {a.confidence} confidence</span> : null}
          <span className={`ck-area-state ${st.tone}`} data-testid="ck-area-state"> {st.words}</span>
        </div>
        <div className="dim">DFO: “{a.words}”{a.note ? ` · ${a.note}` : ""}{a.placed_note ? ` · ${a.placed_note}` : ""}
          {a.mark?.verdict === "wrong" && a.mark.note ? ` · your note: ${a.mark.note}` : ""}</div>
        {a.ends.filter((e) => e.new && e.nearest).map((e, i) => (
          <div key={i} className={e.nearest!.near ? "warn-text" : "dim"} data-testid="ck-area-nearest">
            {e.label}: a new cut. Nearest existing cut on this water: “{e.nearest!.label}”, {e.nearest!.metres.toLocaleString()} m away
            {e.nearest!.near ? ` (within ${closure.near_m} m: use the existing one)` : ` (further than ${closure.near_m} m: stays a new cut)`}.</div>))}
        {!on && (a.candidates?.length ?? 0) > 0 && (
          <div className="ck-cands" data-testid="ck-area-candidates">
            <div><b>DFO describes this end two ways. Both are marked on the map: pick one, or click your own.</b></div>
            {a.candidates!.map((c, i) => (
              <div key={i} className="ck-cand">
                <button className="btn small" disabled={busy} data-testid={`ck-area-cand-${i}`} onClick={() => { open(a, c.piece); if (c.coord) fly(c.coord); }}>
                  Use: {c.label}{c.metres != null ? ` (closed area ${(c.metres / 1000).toFixed(1)} km)` : ""}{c.is_now ? " · the proposal now" : ""}</button>
                <span className="dim"> {c.why}</span>
              </div>))}
          </div>)}
        {!on && wrong !== a.key && (
          <div className="ck-area-btns">
            {a.piece && !a.confirmed && <button className="btn small primary" disabled={busy} data-testid="ck-area-confirm"
              onClick={() => void save(a, { answers: { P: "y" }, piece: a.piece }, `${a.name}: confirmed as placed.`)}>Confirm</button>}
            <button className="btn small" disabled={busy} data-testid="ck-area-edit" onClick={() => open(a)}>
              {a.piece ? (a.kind === "circle" ? "Move it / change the radius" : a.kind === "line" ? "Move the line" : "Change its ends") : "Place it yourself"}</button>
            <button className="btn small" disabled={busy} data-testid="ck-area-wrong" onClick={() => { close(); setWrong(a.key); }}>Something's wrong</button>
          </div>)}
        {wrong === a.key && (
          <div className="ck-area-edit" data-testid="ck-area-wrong-form">
            <textarea value={note} onChange={(e) => setNote(e.target.value)} placeholder="What is wrong with this area? (required)" rows={2} />
            <div className="ck-area-btns">
              <button className="btn small primary" disabled={busy || !note.trim()} data-testid="ck-area-wrong-save"
                onClick={() => void save(a, { answers: { P: "n" }, problem: "closed area placed wrong", note: note.trim() }, `${a.name}: your problem report is saved${a.confirmed ? "; it is no longer confirmed" : ""}.`)}>Save “something's wrong”</button>
              <button className="btn small" onClick={close}>Cancel</button>
            </div>
          </div>)}
        {on && draft && area && (
          <div className="ck-area-edit" data-testid="ck-area-editor">
            {draft.kind === "circle" && (<>
              <div>{draft.centre ? <>Centre: {draft.centre[1].toFixed(5)}, {draft.centre[0].toFixed(5)}. Drag its marker, or </> : null}
                <button className={`btn small${pick === "centre" ? " primary" : ""}`} data-testid="ck-area-pick-centre" onClick={() => setPick(pick === "centre" ? null : "centre")}>
                  {pick === "centre" ? "Click the map now…" : draft.centre ? "click the map for a new centre" : "Click the map to set the centre"}</button></div>
              <label>Radius (m) <input type="number" min={1} max={20000} step={10} data-testid="ck-area-radius" value={draft.radius_m ?? ""}
                onChange={(e) => setDraft({ ...draft, radius_m: Number(e.target.value) || undefined })} />
                {area.radius_m ? <span className="dim"> DFO's words: {area.radius_m} m</span> : null}</label>
            </>)}
            {draft.kind === "line" && [0, 1].map((i) => (
              <div key={i}>End {i + 1}: {draft.line?.[i] ? `${draft.line[i][1].toFixed(5)}, ${draft.line[i][0].toFixed(5)} ` : "not set "}
                <button className={`btn small${pick === `line${i}` ? " primary" : ""}`} data-testid={`ck-area-pick-line${i}`}
                  onClick={() => setPick(pick === `line${i}` ? null : (`line${i}` as Pick))}>{pick === `line${i}` ? "Click the map now…" : "click the map"}</button></div>))}
            {draft.kind === "stretch" && [0, 1].map((i) => {
              const e = preview?.ends[i];
              const p = draft.points?.[i];
              return (
                <div key={i} className="ck-area-end" data-testid={`ck-area-end-${i}`}>
                  <b>{i === 0 ? "Upper end" : "Lower end"}:</b> {p ? (e ? `${e.new ? "NEW cut · " : ""}${e.label}` : p.split_id) : "not set"}
                  {e?.why ? <span className="warn-text"> — {e.why}</span> : null}
                  <div className="ck-area-btns">
                    <span>an existing cut: <SplitSearch onPick={(h) => useCut(i, h.id)} /></span>
                    <button className={`btn small${pick === `end${i}` ? " primary" : ""}`} data-testid={`ck-area-pick-end${i}`}
                      onClick={() => setPick(pick === `end${i}` ? null : (`end${i}` as Pick))}>{pick === `end${i}` ? "Click the river now…" : "or click the map (a new cut)"}</button>
                  </div>
                  {e?.new && e.nearest && (
                    <div className={e.nearest.near ? "warn-text" : "dim"} data-testid={`ck-area-near-${i}`}>
                      Nearest existing cut: “{e.nearest.label}”, {e.nearest.metres.toLocaleString()} m away{e.nearest.near ? `: within ${closure.near_m} m, use it instead of a new cut ` : " "}
                      <button className={`btn small${e.nearest.near ? " primary" : ""}`} data-testid={`ck-area-use-near-${i}`} onClick={() => useCut(i, e.nearest!.split_id)}>Use “{e.nearest.label}”</button>
                    </div>)}
                </div>);
            })}
            {preview && !preview.ok && preview.why ? <div className="warn-text" data-testid="ck-area-why">{preview.why}</div> : null}
            {preview?.ok && preview.metres != null ? <div className="dim">The closed area is {preview.metres >= 1000 ? `${(preview.metres / 1000).toFixed(1)} km` : `${preview.metres} m`} long.</div> : null}
            <div className="ck-area-btns">
              <button className="btn small primary" disabled={busy || !whole(draft) || !preview?.ok} data-testid="ck-area-save"
                onClick={() => void save(area, { answers: { P: "y" }, piece: draft }, `${area.name}: your placement is saved and confirmed.`)}>Save this placement (confirms it)</button>
              <button className="btn small" data-testid="ck-area-cancel" onClick={close}>Cancel</button>
            </div>
            <div className="dim">Teal on the map is your change, not saved yet. Saving writes this one area; the place's own extent is not touched.</div>
          </div>)}
      </li>);
  };

  return (
    <div className="ck-entry b" data-testid="ck-entry-b"><span className="k">B · closed areas</span> “{closure.rule.verbatim}” on {groups.live.length} areas, red on the map.
      {closure.rule.reading ? <div className="dim" data-testid="ck-rule-reading">DFO's word is “{closure.rule.dfo_words}”. “{closure.rule.verbatim}” is {closure.rule.reading}.</div> : null}
      <div className="dim">Each area is yours to confirm, move or report; ours are proposals until you do. An area takes effect only while DFO's live page names it.</div>
      <ol className="dfo-pieces">{groups.live.map(row)}</ol>
      {groups.kept.length > 0 && (<>
        <div className="ck-sec" data-testid="ck-kept-head">Not on the live page — kept ready</div>
        <div className="dim">Only an older wording names these. They are placed and checked like the others, drawn grey and dashed here only, and take effect only if DFO goes back to those words.</div>
        <ol className="dfo-pieces">{groups.kept.map(row)}</ol>
      </>)}
      {closure.new_cuts.length > 0 && <div className="dim">{closure.new_cuts.length} of its cuts are new and not built.</div>}
    </div>
  );
}
