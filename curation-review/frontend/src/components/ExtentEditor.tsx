import { useMemo } from "react";
import type { Boundary, Extent, Op } from "../types";
import { splitArity } from "../format";

const OPS: Op[] = ["whole", "upstream_of", "downstream_of", "between", "within"];

interface Props {
  extents: Extent[];
  boundaries: Boundary[];
  onChange: (next: Extent[]) => void;
  /** registry id -> display name, for showing which waters an extent spans */
  itemNames?: Record<string, string>;
}

// Per-rule extent editor: op dropdown + split-id multiselect populated from the
// item's boundaries. Arity is enforced in the UI (whole=0, up/down=1, between=2).
export function ExtentEditor({ extents, boundaries, onChange, itemNames = {} }: Props) {
  // Bindable options: drop curated splits no longer in splits.json (orphans — they vanish on rebuild),
  // keep auto boundaries, and float curated splits (splits.json) to the top.
  const baseOptions = useMemo(
    () =>
      boundaries
        .filter((b) => !(b.curated && !b.in_splits))
        .slice()
        .sort(
          (a, b) =>
            (b.curated ? 1 : 0) - (a.curated ? 1 : 0) ||
            (a.label || a.id).localeCompare(b.label || b.id),
        ),
    [boundaries],
  );

  // Include any id THIS extent already binds that isn't in the list (an orphan) so it stays removable.
  function optionsFor(splits: string[]): Boundary[] {
    const extra = splits
      .filter((id) => !baseOptions.some((o) => o.id === id))
      .map((id) => ({ id, label: `${id} (orphan — remove)`, kind: "", ref: "", wbk: "", curated: true } as Boundary));
    return [...baseOptions, ...extra];
  }

  function update(i: number, patch: Partial<Extent>) {
    onChange(extents.map((ex, j) => (j === i ? { ...ex, ...patch } : ex)));
  }
  function setOp(i: number, op: Op) {
    const arity = splitArity(op);
    let splits = extents[i].splits;
    if (arity != null && splits.length > arity) splits = splits.slice(0, arity);
    if (op === "whole") splits = [];
    // `item_ids` is derived from the CUT-POINTS' owners, so it is meaningless once there are no
    // cuts; leaving it set would scope a whole-water extent to whatever the previous op happened to
    // bind. `item_id` is the curator's own choice and survives.
    update(i, op === "whole" ? { op, splits, item_ids: [] } : { op, splits });
  }
  // Which registry items the chosen cut-points belong to.
  function owners(splits: string[]): string[] {
    const seen = new Set<string>();
    for (const id of splits) {
      const b = baseOptions.find((o) => o.id === id);
      if (b?.item_id) seen.add(b.item_id);
    }
    return [...seen];
  }

  function setSplits(i: number, selected: string[]) {
    const arity = splitArity(extents[i].op);
    const splits = arity != null ? selected.slice(0, arity) : selected;
    // A reach whose two ends sit on DIFFERENT waters has to be scoped to both. Scoped to one, the
    // other end falls outside the scope and the reach cannot resolve at all — which is what happened
    // to "downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge", bounded by a cut on the
    // Chilliwack and one on the Vedder. So the scope follows the cut-points automatically.
    const own = owners(splits);
    update(i, own.length > 1
      ? { splits, item_ids: own, item_id: null }
      : { splits, item_ids: [] });
  }
  function add() {
    onChange([...extents, { op: "whole", splits: [] }]);
  }
  function remove(i: number) {
    onChange(extents.filter((_, j) => j !== i));
  }

  return (
    <div>
      {extents.length === 0 && (
        <div className="dim" style={{ marginBottom: 6 }}>
          (no extents — add one, or leave empty for a needs_review / no_registry rule)
        </div>
      )}
      {extents.map((ex, i) => {
        const arity = splitArity(ex.op);
        const needSplits = arity != null && arity > 0;
        return (
          <div className="extent-row" key={i}>
            <select value={ex.op} onChange={(e) => setOp(i, e.target.value as Op)}>
              {OPS.map((o) => (
                <option key={o} value={o}>
                  {o}
                </option>
              ))}
            </select>
            {needSplits &&
              (() => {
                const opts = optionsFor(ex.splits);
                return opts.length > 0 ? (
                  <select
                    className="split-multi"
                    multiple
                    value={ex.splits}
                    onChange={(e) =>
                      setSplits(
                        i,
                        Array.from(e.target.selectedOptions).map((o) => o.value),
                      )
                    }
                  >
                    {opts.map((b) => (
                      <option key={b.id} value={b.id}>
                        {b.curated ? "★ " : ""}
                        {b.label && b.label !== b.id ? `${b.label} — ${b.id}` : b.id}
                      </option>
                    ))}
                  </select>
                ) : (
                  <span className="dim">no bindable splits on matched item</span>
                );
              })()}
            {/* Which water this extent selects from. A whole-water extent on a MULTI-WATER entry is
                how a rule includes one member on its own — the Sumas River polygon inside DFO's
                Chilliwack/Vedder row, or a reservoir polygon beside its river. Extents are UNIONed,
                so "the river above the lake, plus the lake" is two extents, and without this control
                the second one could not be expressed in the app at all. Hidden on a single-water
                entry, where there is nothing to choose. */}
            {ex.op === "whole" && Object.keys(itemNames).length > 1 && (
              <select
                value={ex.item_id ?? ""}
                title="which of this entry's waters this extent selects — default is all of them"
                onChange={(e) =>
                  update(i, { item_id: e.target.value || null, item_ids: [] })
                }
              >
                <option value="">every water this entry covers</option>
                {Object.entries(itemNames).map(([id, nm]) => (
                  <option key={id} value={id}>
                    only {nm} — {id}
                  </option>
                ))}
              </select>
            )}
            {ex.op === "within" && (
              <input
                type="text"
                placeholder="area id (e.g. area:park:foo)"
                value={ex.area_id ?? ""}
                onChange={(e) => update(i, { area_id: e.target.value })}
              />
            )}
            {needSplits && (
              <span className="dim">
                {ex.splits.length}/{arity} split{arity === 1 ? "" : "s"}
                {ex.splits.length !== arity ? " ⚠" : ""}
              </span>
            )}
            {ex.item_id && (
              <span className="badge" title="this extent selects from one of the entry's waters only">
                only {itemNames[ex.item_id] ?? ex.item_id}
              </span>
            )}
            {(ex.item_ids?.length ?? 0) > 1 && (
              <span
                className="badge"
                title="the two cut-points are on different waters, so this extent is scoped to both — scoping it to one would put the other end out of scope"
              >
                spans {ex.item_ids!.map((id) => itemNames[id] ?? id).join(" + ")}
              </span>
            )}
            <button className="btn" style={{ padding: "2px 8px" }} onClick={() => remove(i)}>
              remove
            </button>
          </div>
        );
      })}
      <button className="btn" style={{ padding: "3px 10px", marginTop: 4 }} onClick={add}>
        + add extent
      </button>
    </div>
  );
}
