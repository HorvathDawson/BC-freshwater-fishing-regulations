import type { Exempts } from "../../types";
import { ErrorsAt, F, Pick, RowTools, Text, move, put, useVocab } from "../../model";

/** `exempts` — what this rule LIFTS: a zone default by slug (`default_id`, from the model's
 *  `EXEMPTABLE_DEFAULTS`) OR one rule by id (`target`, in `entry_id` when it is another entry's).
 *  Exactly one of the two; the model refuses both or neither. */
export function ExemptsEditor({ value, onChange, path, siblings }: {
  value?: Exempts[]; onChange: (v: Exempts[] | undefined) => void; path: string; siblings: string[];
}) {
  const { exemptable_defaults } = useVocab();
  const xs = value ?? [];
  const emit = (next: Exempts[]) => onChange(next.length ? next : undefined);
  const set = (i: number, patch: Partial<Exempts>) => emit(xs.map((x, j) => (j === i ? put(x, patch) : x)));
  return (
    <div className="exempts">
      {xs.map((x, i) => {
        const mode = x.target != null ? "target" : "default_id";
        return (
          <div className="band-row" key={i}>
            <select aria-label="exempts mode" value={mode} onChange={(e) =>
              set(i, e.target.value === "target"
                ? { default_id: undefined, target: "" }
                : { target: undefined, entry_id: undefined, default_id: exemptable_defaults[0] })}>
              <option value="default_id">a zone default</option>
              <option value="target">one rule</option>
            </select>
            {mode === "default_id" ? (
              <F path={`${path}.${i}.default_id`}>
                <Pick value={x.default_id} options={exemptable_defaults} none={null}
                  onChange={(v) => set(i, { default_id: v })} />
              </F>
            ) : (
              <>
                <F path={`${path}.${i}.target`}>
                  <input type="text" list={`${path}-sib`} value={x.target ?? ""} placeholder="rule id"
                    onChange={(e) => set(i, { target: e.target.value })} />
                  <datalist id={`${path}-sib`}>{siblings.map((s) => <option key={s} value={s} />)}</datalist>
                </F>
                <F path={`${path}.${i}.entry_id`} hint="blank = this entry">
                  <Text value={x.entry_id} onChange={(v) => set(i, { entry_id: v })} placeholder="entry id" />
                </F>
              </>
            )}
            <F path={`${path}.${i}.note`}><Text value={x.note} onChange={(v) => set(i, { note: v })} /></F>
            <RowTools i={i} n={xs.length} onMove={(d) => emit(move(xs, i, d))}
              onRemove={() => emit(xs.filter((_, j) => j !== i))} />
            <ErrorsAt path={`${path}.${i}`} />
          </div>
        );
      })}
      <button type="button" className="btn small"
        onClick={() => emit([...xs, { default_id: exemptable_defaults[0] }])}>+ add exemption</button>
    </div>
  );
}
