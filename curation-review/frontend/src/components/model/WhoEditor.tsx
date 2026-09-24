import type { Who } from "../../types";
import { orNone, put, useVocab } from "../../model";

/** `Who` — a set on every axis (catalogue `WHO_AXES`); an axis left out means ANY member. An empty
 *  `who` is everyone, which the model spells by leaving the field out, so clearing every box
 *  removes it. The axes and members come from the model. */
export function WhoEditor({ value, onChange }: { value?: Who | null; onChange: (w: Who | undefined) => void }) {
  const { who_axes } = useVocab();
  const w: Who = value ?? {};
  function toggle(axis: string, member: string, on: boolean) {
    const cur = (w as Record<string, string[] | undefined>)[axis] ?? [];
    const next = who_axes[axis].filter((m) => (m === member ? on : cur.includes(m)));
    const out = put(w, { [axis]: orNone(next) } as Partial<Who>);
    onChange(Object.keys(out).length ? out : undefined);
  }
  return (
    <div className="who">
      {Object.entries(who_axes).map(([axis, members]) => (
        <div key={axis} className="who-axis" data-path-axis={axis}>
          <span className="dim">{axis}:</span>
          {members.map((m) => (
            <label key={m} className="check">
              <input type="checkbox"
                checked={((w as Record<string, string[] | undefined>)[axis] ?? []).includes(m)}
                onChange={(e) => toggle(axis, m, e.target.checked)} />
              {m}
            </label>
          ))}
        </div>
      ))}
      {!value && <span className="hint">no axis named — everyone (the field is left out)</span>}
    </div>
  );
}
