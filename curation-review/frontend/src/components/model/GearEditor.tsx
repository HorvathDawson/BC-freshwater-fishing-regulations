import type { GearClause, GearSpec, GearWhen } from "../../types";
import { SpeciesPicker } from "../SpeciesPicker";
import {
  Check, ErrorsAt, F, Num, Pick, RowTools, Tags, Text, move, orNone, put, useVocab,
} from "../../model";

type Bound = "allow" | "only" | "ban";
const BOUNDS: Bound[] = ["allow", "only", "ban"];

/** `when` / `unless` on a clause — a CLOSED vocabulary (`GearWhen`); `note` is the escape and
 *  costs the rule a `review_reason`. */
function GearWhenEditor({ value, onChange, path }: {
  value: GearWhen; onChange: (w: GearWhen) => void; path: string;
}) {
  const v = useVocab();
  const set = (patch: Partial<GearWhen>) => onChange(put(value, patch));
  return (
    <div className="gear-when">
      <F path={`${path}.water`}><Pick value={value.water} options={v.water_kinds} onChange={(x) => set({ water: x })} /></F>
      <F path={`${path}.method`}><Pick value={value.method} options={v.methods} onChange={(x) => set({ method: x })} /></F>
      <F path={`${path}.angler`}><Pick value={value.angler} options={v.angler_states} onChange={(x) => set({ angler: x })} /></F>
      <F path={`${path}.targeting`} deep>
        <SpeciesPicker values={value.targeting} options={v.species} onChange={(x) => set({ targeting: x })} />
      </F>
      <F path={`${path}.gear_in_use`}><Text value={value.gear_in_use} onChange={(x) => set({ gear_in_use: x })} /></F>
      <F path={`${path}.note`} hint="the escape — needs a review_reason on the rule">
        <Text value={value.note} onChange={(x) => set({ note: x })} />
      </F>
    </div>
  );
}

function GearSpecEditor({ value, onChange, path }: {
  value: GearSpec; onChange: (s: GearSpec) => void; path: string;
}) {
  const set = (patch: Partial<GearSpec>) => onChange(put(value, patch));
  return (
    <div className="gear-when">
      <F path={`${path}.attached_to`}><Text value={value.attached_to} onChange={(x) => set({ attached_to: x })} /></F>
      <F path={`${path}.attachment`}><Text value={value.attachment} onChange={(x) => set({ attachment: x })} /></F>
      <F path={`${path}.within_m_of_hook`}><Num value={value.within_m_of_hook} onChange={(x) => set({ within_m_of_hook: x })} /></F>
      <F path={`${path}.opening_shape`}><Text value={value.opening_shape} onChange={(x) => set({ opening_shape: x })} /></F>
      <F path={`${path}.note`}><Text value={value.note} onChange={(x) => set({ note: x })} /></F>
    </div>
  );
}

function ClauseEditor({ c, onChange, path }: { c: GearClause; onChange: (c: GearClause) => void; path: string }) {
  const v = useVocab();
  const shape = v.slots.find((s) => s.slot === c.slot)?.shape ?? "count";
  const set = (patch: Partial<GearClause>) => onChange(put(c, patch));
  const members = v.gear_members[c.slot] ?? [];
  const bound: Bound | null = BOUNDS.find((b) => c[b] != null) ?? null;
  // A value a DIFFERENT shape would use is still shown, so the model's refusal of it is legible
  // and it can be cleared — hiding it would save a field the curator cannot see.
  const showSet = shape === "set" || bound != null || (c.except?.length ?? 0) > 0;
  const showCount = shape === "count" || shape === "measured" || c.max != null || c.min != null
    || !!c.unlimited || (c.members?.length ?? 0) > 0;
  const showSpec = shape === "spec" || (c.must_be?.length ?? 0) > 0 || c.requires != null;
  return (
    <div className="clause-body">
      <F path={`${path}.of`} deep hint="which members this clause speaks about; blank = all of them">
        <Tags values={c.of} suggestions={members} onChange={(x) => set({ of: x })} label="of" />
      </F>
      {showSet && (
        <>
          <F path={`${path}.${bound ?? "allow"}`} k="bound" deep
            hint="allow permits (says nothing of the rest) · only closes the slot · ban prohibits what it names">
            <span className="radio-row">
              {BOUNDS.map((b) => (
                <label key={b} className="check">
                  <input type="radio" name={`${path}-bound`} checked={bound === b}
                    onChange={() => set({ allow: undefined, only: undefined, ban: undefined,
                                          [b]: (bound ? c[bound] : null) ?? [] })} />
                  {b}
                </label>
              ))}
            </span>
            {bound && (
              <Tags values={c[bound]} keepEmpty suggestions={members} label={bound}
                onChange={(x) => set({ [bound]: x ?? [] } as Partial<GearClause>)} />
            )}
          </F>
          {(bound === "ban" || (c.except?.length ?? 0) > 0) && (
            <F path={`${path}.except`} deep hint="members the ban does not reach">
              <Tags values={c.except} suggestions={members} onChange={(x) => set({ except: x })} label="except" />
            </F>
          )}
        </>
      )}
      {showCount && (
        <>
          <F path={`${path}.max`}><Num label="max" value={c.max} onChange={(x) => set({ max: x })}
            step={shape === "measured" ? undefined : 1} /></F>
          <F path={`${path}.min`}><Num label="min" value={c.min} onChange={(x) => set({ min: x })}
            step={shape === "measured" ? undefined : 1} /></F>
          {(shape === "count" || c.unlimited) && (
            <F path={`${path}.unlimited`} hint="no ceiling, said outright">
              <Check value={c.unlimited} onChange={(x) => set({ unlimited: x })} label="unlimited" />
            </F>
          )}
          {(shape === "count" || (c.members?.length ?? 0) > 0) && (
            <F path={`${path}.members`} deep hint="a choice of ONE from several kinds — qualifies a bound">
              <Tags values={c.members} suggestions={members} onChange={(x) => set({ members: x })} label="members" />
            </F>
          )}
        </>
      )}
      {showSpec && (
        <>
          <F path={`${path}.must_be`} deep>
            <Tags values={c.must_be} suggestions={v.must_be[c.slot] ?? []} label="must_be"
              onChange={(x) => set({ must_be: x })} />
          </F>
          <F path={`${path}.requires`}>
            <Check value={c.requires != null} label="requires"
              onChange={(on) => set({ requires: on ? { note: "" } : undefined })}>state a spec</Check>
            {c.requires && <GearSpecEditor value={c.requires} path={`${path}.requires`}
              onChange={(s) => set({ requires: s })} />}
          </F>
        </>
      )}
      <F path={`${path}.when`} hint="blank = always; a clause with no `when` is the last word on its slot">
        <Check value={c.when != null} label="when" onChange={(on) => set({ when: on ? {} : undefined })}>
          only when…
        </Check>
        {c.when && <GearWhenEditor value={c.when} path={`${path}.when`} onChange={(w) => set({ when: w })} />}
      </F>
      <F path={`${path}.unless`} hint="what lifts this clause">
        {(c.unless ?? []).map((u, i) => (
          <div key={i} className="unless-row">
            <GearWhenEditor value={u} path={`${path}.unless.${i}`}
              onChange={(w) => set({ unless: (c.unless ?? []).map((x, j) => (j === i ? w : x)) })} />
            <button type="button" className="x" title="remove"
              onClick={() => set({ unless: orNone((c.unless ?? []).filter((_, j) => j !== i)) })}>×</button>
            <ErrorsAt path={`${path}.unless.${i}`} />
          </div>
        ))}
        <button type="button" className="btn small"
          onClick={() => set({ unless: [...(c.unless ?? []), {}] })}>+ unless</button>
      </F>
    </div>
  );
}

/** `gear` — ordered clauses. Clauses on DIFFERENT slots all apply; on the SAME slot, FIRST MATCH
 *  WINS, so the narrow case goes first (the arrows reorder). Each clause's inputs follow its
 *  slot's shape in the model: a set slot takes allow/only/ban, a counted or measured one
 *  max/min, a spec slot must_be. Slots and shapes come from the model (`Slot`). */
export function GearEditor({ value, onChange, path }: {
  value?: GearClause[]; onChange: (g: GearClause[] | undefined) => void; path: string;
}) {
  const v = useVocab();
  const clauses = value ?? [];
  const emit = (next: GearClause[]) => onChange(next.length ? next : undefined);
  return (
    <div className="gear">
      {clauses.map((c, i) => (
        <div className="clause" key={i} data-path={`${path}.${i}`}>
          <div className="clause-head">
            <span className="dim">#{i + 1}</span>
            <F path={`${path}.${i}.slot`}>
              <select aria-label="slot" value={c.slot}
                onChange={(e) => emit(clauses.map((x, j) => (j === i ? { ...x, slot: e.target.value } : x)))}>
                {v.slots.map((s) => <option key={s.slot} value={s.slot}>{s.slot} ({s.shape})</option>)}
              </select>
            </F>
            <RowTools i={i} n={clauses.length} onMove={(d) => emit(move(clauses, i, d))}
              onRemove={() => emit(clauses.filter((_, j) => j !== i))} />
          </div>
          <ErrorsAt path={`${path}.${i}`} />
          <ClauseEditor c={c} path={`${path}.${i}`}
            onChange={(n) => emit(clauses.map((x, j) => (j === i ? n : x)))} />
        </div>
      ))}
      <button type="button" className="btn small"
        onClick={() => emit([...clauses, { slot: "bait", ban: [] }])}>+ add gear clause</button>
    </div>
  );
}
