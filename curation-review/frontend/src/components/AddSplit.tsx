import { useEffect, useState } from "react";
import { api, type ValidationError } from "../api";

interface Props {
  itemId: string;
  /** the entry's raw name — the name a NEW waterbody group takes */
  entryName: string;
  onAdded: () => void;
  onPreviewPoint?: (pt: { lon: number; lat: number } | null) => void;
}

/** The splits.json target an item id names (`gnis:28136` -> {gnis_id: "28136"}, the shape
 *  `splits.applies_keys` reads back). */
function appliesTo(itemId: string): Record<string, string> | null {
  const [kind, id] = itemId.split(":", 2);
  if (!id) return null;
  return kind === "gnis" ? { gnis_id: id } : kind === "wsc" ? { wsc: id } : kind === "blk" ? { blk: id } : null;
}

/* ADD A SPLIT (fix mode; owner 2026-10-23: "add splits and rebuild then continue review"). A point
   cut on this water, into its splits.json group (or a new group on this water). Saving writes
   splits.json only: the split then waits in the pending-build panel with the commands the owner
   runs (atlas → binding diff → promote → reach → deliver); once they land it is queued in S0 for
   its own split mark, and the entries whose binding moved read "binding moved". */
export function AddSplit({ itemId, entryName, onAdded, onPreviewPoint }: Props) {
  const [open, setOpen] = useState(false);
  const [groups, setGroups] = useState<{ name: string; n_splits: number }[]>([]);
  const [group, setGroup] = useState("");
  const [id, setId] = useState("");
  const [label, setLabel] = useState("");
  const [lon, setLon] = useState("");
  const [lat, setLat] = useState("");
  const [note, setNote] = useState("");
  const [errs, setErrs] = useState<string[]>([]);
  const [msg, setMsg] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!open) return;
    api.splitGroups(itemId).then((g) => { setGroups(g); setGroup(g[0]?.name ?? ""); }).catch(() => setGroups([]));
  }, [open, itemId]);
  useEffect(() => {
    if (!onPreviewPoint) return;
    const x = Number(lon), y = Number(lat);
    onPreviewPoint(open && lon && lat && !Number.isNaN(x) && !Number.isNaN(y) ? { lon: x, lat: y } : null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, lon, lat]);

  async function add() {
    setErrs([]); setMsg("");
    const x = Number(lon), y = Number(lat);
    if (!id.trim() || Number.isNaN(x) || Number.isNaN(y) || !lon || !lat) {
      setErrs(["an id and a lon/lat are needed"]);
      return;
    }
    const target = appliesTo(itemId);
    setBusy(true);
    try {
      await api.addSplit({
        split: {
          id: id.trim(), label: label.trim() || id.trim(), kind: "point",
          anchor: { type: "point", coord: [x, y], is_lonlat: true },
          // the server opens the note with "Verify: added in fix mode <date>" — tier S0
          note: "", ...(note.trim() ? { _note: note.trim() } : {}),
        },
        ...(group ? { waterbody: group }
          : { new_waterbody: { name: entryName, applies_to: target ?? {} } }),
      });
      setMsg("added to splits.json — it waits in the pending-build panel above, with the commands");
      setId(""); setLabel(""); setLon(""); setLat(""); setNote("");
      onAdded();
    } catch (e) {
      const ve = e as ValidationError;
      setErrs(ve.errors ?? [String(e)]);
    } finally {
      setBusy(false);
    }
  }

  if (!open) {
    return (
      <button type="button" className="btn small" data-testid="add-split-open" onClick={() => setOpen(true)}>
        + add a split on this water
      </button>
    );
  }
  return (
    <div className="split-editor add-split" data-testid="add-split">
      <div className="split-editor-head">add a split point on {itemId}</div>
      <label className="fld"><span>group</span>
        <select value={group} onChange={(e) => setGroup(e.target.value)}>
          {groups.map((g) => <option key={g.name} value={g.name}>{g.name} ({g.n_splits})</option>)}
          <option value="">a new group “{entryName}” on {itemId}</option>
        </select>
      </label>
      <label className="fld"><span>id</span>
        <input value={id} placeholder="adam_river__new_point" onChange={(e) => setId(e.target.value)} /></label>
      <label className="fld"><span>label</span>
        <input value={label} onChange={(e) => setLabel(e.target.value)} /></label>
      <div className="fld"><span>coord (lon, lat)</span>
        <div className="coord-row">
          <input value={lon} placeholder="lon" onChange={(e) => setLon(e.target.value)} />
          <input value={lat} placeholder="lat" onChange={(e) => setLat(e.target.value)} />
        </div>
      </div>
      <label className="fld"><span>note</span>
        <input value={note} placeholder="why, and how it was placed" onChange={(e) => setNote(e.target.value)} /></label>
      <div className="split-editor-actions">
        <button className="btn primary" disabled={busy} onClick={() => void add()} data-testid="add-split-save">Add split</button>
        <button className="btn" disabled={busy} onClick={() => { setOpen(false); onPreviewPoint?.(null); }}>cancel</button>
      </div>
      {errs.length > 0 && <div className="errors">{errs.map((e, i) => <div key={i}>{e}</div>)}</div>}
      {msg && <div className="rebuild-note">{msg}</div>}
    </div>
  );
}
