import { useEffect, useMemo, useState } from "react";
import type { Boundary, Extent, Op } from "../types";
import { api } from "../api";
import { splitArity } from "../format";

const OPS: Op[] = ["whole", "upstream_of", "downstream_of", "between"];

interface Props {
  itemIds: string[]; // the entry's matched mainstem item(s) — their tributaries populate the dropdown
  excludes: Extent[];
  onChange: (next: Extent[]) => void;
}

// Editor for entry.tributaries.excludes — hand-curated carve-outs subtracted from the tributary set
// (e.g. Atnarko/Bella Coola "…tributaries EXCEPT Burnt Bridge Creek upstream of Sitkatapa Creek"). The
// dropdown unions the tributaries of ALL the entry's mainstems (a shared multi-reg like Atnarko/Bella
// Coola has two). Pick a named tributary, then the reach on it: op=whole excludes the whole tributary;
// upstream_of a point excludes that point and everything above it (including its own upstream tributaries).
export function ExcludesEditor({ itemIds, excludes, onChange }: Props) {
  const [tribs, setTribs] = useState<{ id: string; name: string }[]>([]);
  const [cache, setCache] = useState<Record<string, Boundary[]>>({}); // item id -> its boundaries

  const idsKey = itemIds.join(",");
  useEffect(() => {
    const ids = idsKey ? idsKey.split(",") : [];
    if (ids.length === 0) { setTribs([]); return; }
    let cancelled = false;
    Promise.all(ids.map((id) => api.itemTributaries(id).catch(() => [])))
      .then((lists) => {
        if (cancelled) return;
        const byId = new Map<string, { id: string; name: string }>();
        for (const list of lists) for (const t of list) if (!ids.includes(t.id)) byId.set(t.id, t);
        setTribs([...byId.values()].sort((a, b) => a.name.localeCompare(b.name)));
      });
    return () => { cancelled = true; };
  }, [idsKey]);

  useEffect(() => {
    const want = excludes.map((e) => e.item_id).filter((x): x is string => !!x && !(x in cache));
    want.forEach((id) => api.itemBoundaries(id).then((b) => setCache((c) => ({ ...c, [id]: b }))).catch(() => {}));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [excludes]);

  const tribName = useMemo(() => Object.fromEntries(tribs.map((t) => [t.id, t.name])), [tribs]);

  function reachOptions(ex: Extent): Boundary[] {
    return (ex.item_id ? cache[ex.item_id] ?? [] : [])
      .filter((b) => !(b.curated && !b.in_splits))
      .slice()
      .sort((a, b) => (b.curated ? 1 : 0) - (a.curated ? 1 : 0) || (a.label || a.id).localeCompare(b.label || b.id));
  }

  function update(i: number, patch: Partial<Extent>) {
    onChange(excludes.map((ex, j) => (j === i ? { ...ex, ...patch } : ex)));
  }
  function setOp(i: number, op: Op) {
    const arity = splitArity(op);
    let splits = excludes[i].splits;
    if (arity != null && splits.length > arity) splits = splits.slice(0, arity);
    if (op === "whole") splits = [];
    update(i, { op, splits });
  }

  return (
    <div className="excludes">
      {excludes.length === 0 && (
        <div className="dim">no tributary carve-outs. Add one if the reg says "…tributaries EXCEPT …".</div>
      )}
      {excludes.map((ex, i) => {
        const arity = splitArity(ex.op);
        const need = arity != null && arity > 0;
        const opts = reachOptions(ex);
        return (
          <div className="extent-row" key={i}>
            <select value={ex.item_id ?? ""} onChange={(e) => update(i, { item_id: e.target.value || null, splits: [] })}>
              <option value="">— pick tributary —</option>
              {ex.item_id && !tribName[ex.item_id] && <option value={ex.item_id}>{ex.item_id}</option>}
              {tribs.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
            <select value={ex.op} onChange={(e) => setOp(i, e.target.value as Op)} title="whole = the whole tributary">
              {OPS.map((o) => <option key={o} value={o}>{o}</option>)}
            </select>
            {need && (
              ex.item_id ? (
                <select className="split-multi" multiple value={ex.splits}
                  onChange={(e) => update(i, { splits: Array.from(e.target.selectedOptions).map((o) => o.value).slice(0, arity ?? undefined) })}>
                  {opts.length === 0 && <option disabled>no splits on this tributary</option>}
                  {opts.map((b) => <option key={b.id} value={b.id}>{b.curated ? "★ " : ""}{b.label || b.id}</option>)}
                </select>
              ) : <span className="dim">pick a tributary first</span>
            )}
            {need && <span className="dim">{ex.splits.length}/{arity}{ex.splits.length !== arity ? " ⚠" : ""}</span>}
            <button className="btn" style={{ padding: "2px 8px" }} onClick={() => onChange(excludes.filter((_, j) => j !== i))}>remove</button>
          </div>
        );
      })}
      <button className="btn" style={{ padding: "3px 10px", marginTop: 4 }}
        onClick={() => onChange([...excludes, { op: "whole", splits: [] }])}>
        + add carve-out
      </button>
      {excludes.length > 0 && (
        <div className="dim" style={{ marginTop: 4 }}>
          whole = exclude the entire tributary · upstream_of a point = exclude it + everything upstream (incl. its upstream tributaries)
        </div>
      )}
    </div>
  );
}
