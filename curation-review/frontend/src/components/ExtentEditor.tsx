import { useMemo } from "react";
import type { Boundary, Extent, Op } from "../types";
import { splitArity } from "../format";

const OPS: Op[] = ["whole", "upstream_of", "downstream_of", "between", "within"];

interface Props {
  extents: Extent[];
  boundaries: Boundary[];
  onChange: (next: Extent[]) => void;
}

// Per-rule extent editor: op dropdown + split-id multiselect populated from the
// item's boundaries. Arity is enforced in the UI (whole=0, up/down=1, between=2).
export function ExtentEditor({ extents, boundaries, onChange }: Props) {
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
    update(i, { op, splits });
  }
  function setSplits(i: number, selected: string[]) {
    const arity = splitArity(extents[i].op);
    const splits = arity != null ? selected.slice(0, arity) : selected;
    update(i, { splits });
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
                        {b.label || b.id}
                      </option>
                    ))}
                  </select>
                ) : (
                  <span className="dim">no bindable splits on matched item</span>
                );
              })()}
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
