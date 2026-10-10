import { useEffect, useState } from "react";
import { api, type ValidationError } from "../api";
import type { SplitRef } from "../types";

interface Props {
  splitId: string;
  onChanged?: () => void; // after a successful save/delete/rename
  onPreviewPoint?: (pt: { lon: number; lat: number } | null) => void; // "show on map" for point splits
}

// Edit a curated split in pipeline/splits.json (label / note / anchor / id). splits.json is the
// hand-curated source of truth; edits take effect after a graph rebuild (pipeline.build --full, no credits).
export function SplitEditor({ splitId, onChanged, onPreviewPoint }: Props) {
  const [raw, setRaw] = useState<Record<string, unknown> | null>(null); // current splits.json record
  const [waterbody, setWaterbody] = useState("");
  const [label, setLabel] = useState("");
  const [note, setNote] = useState("");
  const [isPoint, setIsPoint] = useState(false);
  const [lon, setLon] = useState("");
  const [lat, setLat] = useState("");
  const [anchorJson, setAnchorJson] = useState(""); // non-point anchors
  const [newId, setNewId] = useState("");
  const [refs, setRefs] = useState<SplitRef[] | null>(null); // rules that bind this id (impact)
  const [showOnMap, setShowOnMap] = useState(false);
  const [msg, setMsg] = useState("");
  const [errs, setErrs] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  function load() {
    setMsg("");
    setErrs([]);
    api
      .getSplit(splitId)
      .then((d) => {
        const s = d.split as Record<string, unknown>;
        setRaw(s);
        setWaterbody(d.waterbody ?? "");
        setLabel(String(s.label ?? ""));
        setNote(String(s.note ?? ""));
        const a = (s.anchor ?? {}) as Record<string, unknown>;
        if (a.type === "point" && Array.isArray(a.coord)) {
          setIsPoint(true);
          setLon(String((a.coord as number[])[0] ?? ""));
          setLat(String((a.coord as number[])[1] ?? ""));
          setAnchorJson("");
        } else {
          setIsPoint(false);
          setAnchorJson(JSON.stringify(a, null, 1));
        }
      })
      .catch((e) => setErrs([String((e as Error).message ?? e)]));
  }

  useEffect(() => {
    load();
    setNewId("");
    setShowOnMap(false);
    onPreviewPoint?.(null);
    api.splitRefs(splitId).then(setRefs).catch(() => setRefs(null)); // always show impact
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [splitId]);

  // push the live point to the map when the toggle is on / coords change
  useEffect(() => {
    if (!onPreviewPoint) return;
    if (showOnMap && isPoint && lon && lat) onPreviewPoint({ lon: Number(lon), lat: Number(lat) });
    else onPreviewPoint(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showOnMap, lon, lat, isPoint]);

  function buildAnchor(): Record<string, unknown> | null {
    if (isPoint) {
      const a = { ...((raw?.anchor as Record<string, unknown>) ?? {}) };
      a.type = "point";
      a.coord = [Number(lon), Number(lat)];
      a.is_lonlat = true;
      return a;
    }
    const t = anchorJson.trim();
    if (!t) return null;
    try {
      return JSON.parse(t);
    } catch {
      return null;
    }
  }

  async function save() {
    setBusy(true);
    setMsg("");
    setErrs([]);
    try {
      const patch: Record<string, unknown> = { label, note };
      const a = buildAnchor();
      if (isPoint) {
        if (Number.isNaN(Number(lon)) || Number.isNaN(Number(lat))) { setErrs(["lon/lat must be numbers"]); setBusy(false); return; }
        patch.anchor = a;
      } else if (anchorJson.trim()) {
        if (a === null) { setErrs(["anchor is not valid JSON"]); setBusy(false); return; }
        patch.anchor = a;
      }
      await api.saveSplit(splitId, patch);
      setMsg("saved to splits.json — rebuild (pipeline.build --full) to apply");
      load();
      onChanged?.();
    } catch (e) {
      const ve = e as ValidationError;
      setErrs(ve.errors ?? [String((e as Error).message ?? e)]);
    } finally {
      setBusy(false);
    }
  }

  async function remove() {
    if (!window.confirm(`Delete split "${splitId}" from splits.json? (rebuild to apply)`)) return;
    setBusy(true); setMsg(""); setErrs([]);
    try {
      await api.deleteSplit(splitId);
      setMsg("deleted from splits.json — rebuild to apply");
      onChanged?.();
    } catch (e) {
      const ve = e as ValidationError;
      setErrs(ve.errors ?? [String((e as Error).message ?? e)]);
    } finally { setBusy(false); }
  }

  async function doRename() {
    if (!newId.trim()) return;
    if (!window.confirm(`Rename "${splitId}" → "${newId}" and rewrite ${refs?.length ?? 0} rule(s)?`)) return;
    setBusy(true); setMsg(""); setErrs([]);
    try {
      const r = await api.renameSplit(splitId, newId.trim());
      setMsg(`renamed to ${r.new_id} · ${r.updated_rules.length} rule(s) rewritten` +
        (r.failed_rules.length ? ` · ${r.failed_rules.length} FAILED` : "") + " — rebuild to apply");
      onChanged?.();
    } catch (e) {
      const ve = e as ValidationError;
      setErrs(ve.errors ?? [String((e as Error).message ?? e)]);
    } finally { setBusy(false); }
  }

  return (
    <div className="split-editor">
      <div className="split-editor-head">
        edit split in splits.json {waterbody && <span className="dim">· {waterbody}</span>}
        <button className="btn" style={{ float: "right", padding: "1px 8px" }} onClick={load} disabled={busy}>
          ↻ refresh
        </button>
      </div>
      <div className="dim" style={{ marginBottom: 6 }}>
        id: <code>{splitId}</code> — kind <code>{String(raw?.kind ?? "?")}</code>
      </div>

      <label className="fld"><span>label</span>
        <input value={label} onChange={(e) => setLabel(e.target.value)} /></label>
      <label className="fld"><span>note</span>
        <input value={note} onChange={(e) => setNote(e.target.value)} /></label>

      {isPoint ? (
        <div className="fld">
          <span>coord (lon, lat)</span>
          <div className="coord-row">
            <input value={lon} onChange={(e) => setLon(e.target.value)} placeholder="lon" />
            <input value={lat} onChange={(e) => setLat(e.target.value)} placeholder="lat" />
            <label className="show-map">
              <input type="checkbox" checked={showOnMap} onChange={(e) => setShowOnMap(e.target.checked)} /> show on map
            </label>
          </div>
        </div>
      ) : (
        <label className="fld"><span>anchor (JSON)</span>
          <textarea rows={5} value={anchorJson} onChange={(e) => setAnchorJson(e.target.value)} spellCheck={false} /></label>
      )}

      <div className="split-editor-actions">
        <button className="btn primary" disabled={busy} onClick={save}>Save split</button>
        <button className="btn danger" disabled={busy} onClick={remove}>Delete split</button>
      </div>

      {/* rename id + live impact/orphan preview */}
      <div className="split-rename">
        <div className="dim">
          {refs == null ? "…" : refs.length === 0
            ? "no rules bind this id — safe to rename/delete."
            : <><strong>{refs.length} rule(s)</strong> bind this id — a delete would ORPHAN them; a rename rewrites them:</>}
        </div>
        {refs && refs.length > 0 && (
          <ul className="split-refs">
            {refs.map((r) => (
              <li key={`${r.entry_id}.${r.rule_id}`}>
                <code>{r.rule_id}</code> <span className="dim">({r.entry_name} · r{r.region})</span> — {r.label}
              </li>
            ))}
          </ul>
        )}
        <div className="split-editor-actions">
          <input value={newId} onChange={(e) => setNewId(e.target.value)}
            placeholder={String(raw?.id ?? splitId)} />
          <button className="btn primary" disabled={busy || !newId.trim()} onClick={doRename}>Rename id</button>
        </div>
      </div>

      {errs.length > 0 && <div className="errors">{errs.map((e, i) => <div key={i}>{e}</div>)}</div>}
      {msg && <div className="rebuild-note">⚠ {msg}</div>}
      <div className="dim" style={{ marginTop: 4 }}>
        Edits change splits.json (source of truth); the map/boundaries update after a graph rebuild.
      </div>
    </div>
  );
}
