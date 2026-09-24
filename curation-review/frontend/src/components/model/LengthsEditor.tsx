import type { LengthBand } from "../../types";
import { ErrorsAt, F, Num, RowTools, move, put } from "../../model";

/** `lengths` — ORDERED ranges, FIRST MATCH WINS. `min_cm`/`max_cm` inclusive, blank = open at that
 *  end; `take` is how many of THESE you may keep (blank = the rule's own `take`). On a licensing
 *  `doing` a band names WHICH fish and never how many, so `take` is hidden there (`noTake`). */
export function LengthsEditor({ value, onChange, path, noTake = false }: {
  value?: LengthBand[] | null; onChange: (v: LengthBand[] | undefined) => void; path: string;
  noTake?: boolean;
}) {
  const bands = value ?? [];
  const emit = (next: LengthBand[]) => onChange(next.length ? next : undefined);
  const set = (i: number, patch: Partial<LengthBand>) =>
    emit(bands.map((b, j) => (j === i ? put(b, patch) : b)));
  return (
    <div className="lengths">
      {bands.map((b, i) => (
        <div className="band-row" key={i} data-path={`${path}.${i}`}>
          <span className="dim">#{i + 1}</span>
          <F path={`${path}.${i}.min_cm`}><Num label="min_cm" value={b.min_cm} step={1} width={60}
            onChange={(v) => set(i, { min_cm: v })} /></F>
          <F path={`${path}.${i}.max_cm`}><Num label="max_cm" value={b.max_cm} step={1} width={60}
            onChange={(v) => set(i, { max_cm: v })} /></F>
          {!noTake && (
            <F path={`${path}.${i}.take`}><Num label="take" value={b.take} step={1} width={50}
              onChange={(v) => set(i, { take: v })} /></F>
          )}
          <RowTools i={i} n={bands.length} onMove={(d) => emit(move(bands, i, d))}
            onRemove={() => emit(bands.filter((_, j) => j !== i))} />
          <ErrorsAt path={`${path}.${i}`} />
        </div>
      ))}
      <button type="button" className="btn small" onClick={() => emit([...bands, {}])}>+ add range</button>
      {bands.length === 0 && <span className="hint"> no size ranges</span>}
    </div>
  );
}
