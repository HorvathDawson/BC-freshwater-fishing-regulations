import type { See } from "../../types";
import { ErrorsAt, F, RowTools, Tags, Text, move, put } from "../../model";

/** `see` — POINTERS ("See Lonzo Creek"), not rules: they bind nothing (catalogue.See). Each quotes
 *  the row (`verbatim`, a contiguous run of regs_verbatim) and either names the entries it points
 *  at (`entry_ids`) OR says why it points at none (`unresolved`) — exactly one; the model refuses
 *  both or neither. `suggestions` are entry ids offered (the related rows); any id may be typed. */
export function SeeEditor({ value, onChange, path, suggestions = [] }: {
  value?: See[] | null; onChange: (v: See[] | undefined) => void; path: string;
  suggestions?: string[];
}) {
  const items = value ?? [];
  const emit = (next: See[]) => onChange(next.length ? next : undefined);
  const set = (i: number, patch: Partial<See>) =>
    emit(items.map((s, j) => (j === i ? put(s, patch) : s)));
  return (
    <div className="see-list">
      {items.map((s, i) => {
        const at = `${path}.${i}`;
        // which half of the either/or this pointer is: `unresolved` once that key is set and no entry is named
        const unresolved = s.unresolved !== undefined && !(s.entry_ids ?? []).length;
        return (
          <div className="clause see-item" key={i} data-path={at}>
            <span className="dim">#{i + 1}</span>
            <RowTools i={i} n={items.length} onMove={(d) => emit(move(items, i, d))}
              onRemove={() => emit(items.filter((_, j) => j !== i))} />
            <F path={`${at}.verbatim`} hint="the printed pointer — a contiguous quote of regs_verbatim">
              <Text value={s.verbatim} label="verbatim" onChange={(x) => set(i, { verbatim: x ?? "" })} />
            </F>
            <label className="check">
              <input type="checkbox" checked={unresolved}
                onChange={(e) => set(i, e.target.checked
                  ? { entry_ids: undefined, unresolved: s.unresolved ?? "" }
                  : { unresolved: undefined, entry_ids: s.entry_ids })} />
              points at no entry (unresolved)
            </label>
            {unresolved ? (
              <F path={`${at}.unresolved`} hint="why it cannot be followed — prose on another page, a sign at a trailhead">
                <Text value={s.unresolved} label="unresolved"
                  onChange={(x) => set(i, { unresolved: x ?? "" })} />
              </F>
            ) : (
              <F path={`${at}.entry_ids`} deep hint="the entries it points at — every one must exist, never this entry">
                <Tags values={s.entry_ids} suggestions={suggestions} label="entry_ids"
                  placeholder="entry id + Enter" onChange={(x) => set(i, { entry_ids: x })} />
              </F>
            )}
            <ErrorsAt path={at} />
          </div>
        );
      })}
      <button type="button" className="btn small" onClick={() => emit([...items, { verbatim: "", entry_ids: [] }])}>
        + add pointer
      </button>
      {items.length === 0 && <span className="hint"> no pointers</span>}
    </div>
  );
}
