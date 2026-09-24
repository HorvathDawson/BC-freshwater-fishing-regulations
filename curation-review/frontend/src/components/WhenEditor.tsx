import type { Clock, DateRange, When } from "../types";

/**
 * A rule's `when` — the catalogue's own shape (pipeline/regs/parsing/catalogue.py `When`):
 * `dates` (inclusive calendar ranges, no year), `hours` (a start and an end, each a clock time or
 * a solar time with an offset), `weekdays`, and `unparsed`.
 *
 * It replaced a free-text "windows" list that saved a field the model no longer has. There is no
 * "excepts" switch: a rule's days are always the days it HOLDS, and the complement of a printed
 * "except" is computed by the parser. `unparsed` is what the parser could not read — shown, never
 * edited here: the fix is to write the dates it means and remove it, which the model decides when
 * the entry is re-parsed or hand-authored.
 *
 * Empty parts are left out of what is saved, as the model dumps them (`_Terse`), and a `when` with
 * nothing in it is removed from the rule: no `when` is all year.
 */
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

function terse(w: When): When | undefined {
  const out: When = {};
  if (w.dates?.length) out.dates = w.dates;
  if (w.hours) out.hours = w.hours;
  if (w.weekdays?.length) out.weekdays = w.weekdays;
  if (w.unparsed?.length) out.unparsed = w.unparsed;
  return Object.keys(out).length ? out : undefined;
}

function ClockInput({ value, onChange }: { value: Clock; onChange: (c: Clock) => void }) {
  const solar = value.solar != null;
  return (
    <span className="clock">
      <select value={solar ? value.solar : "clock"}
        onChange={(e) => onChange(e.target.value === "clock" ? { at: "00:00" }
          : { solar: e.target.value as "sunrise" | "sunset", ...(value.offset_min ? { offset_min: value.offset_min } : {}) })}>
        <option value="clock">clock</option>
        <option value="sunrise">sunrise</option>
        <option value="sunset">sunset</option>
      </select>
      {solar ? (
        <input type="number" step={15} style={{ width: 70 }} title="minutes from the event; negative is BEFORE"
          value={value.offset_min ?? 0}
          onChange={(e) => {
            const n = Number(e.target.value) || 0;
            onChange(n ? { solar: value.solar, offset_min: n } : { solar: value.solar });
          }} />
      ) : (
        // A cleared time input reports "" — the model refuses `at: ""`, so a clear is ignored.
        <input type="time" value={value.at ?? ""}
          onChange={(e) => { if (e.target.value) onChange({ at: e.target.value.slice(0, 5) }); }} />
      )}
    </span>
  );
}

export function WhenEditor({ value, onChange }: { value?: When; onChange: (w: When | undefined) => void }) {
  const w: When = value ?? {};
  const dates = w.dates ?? [];
  const set = (patch: Partial<When>) => onChange(terse({ ...w, ...patch }));
  const setDate = (i: number, patch: Partial<DateRange>) =>
    set({ dates: dates.map((d, j) => (j === i ? { ...d, ...patch } : d)) });
  const month = (v: number, on: (m: number) => void) => (
    <select value={v} onChange={(e) => on(Number(e.target.value))}>
      {MONTHS.map((m, i) => <option key={m} value={i + 1}>{m}</option>)}
    </select>
  );
  const day = (v: number, on: (d: number) => void) => (
    <input type="number" min={1} max={31} style={{ width: 50 }} value={v}
      onChange={(e) => on(Number(e.target.value))} />
  );
  return (
    <div className="dates-editor">
      {dates.map((d, i) => (
        <span className="date-row" key={i}>
          {month(d.from_month, (m) => setDate(i, { from_month: m }))}
          {day(d.from_day, (n) => setDate(i, { from_day: n }))}
          {" – "}
          {month(d.to_month, (m) => setDate(i, { to_month: m }))}
          {day(d.to_day, (n) => setDate(i, { to_day: n }))}
          <button type="button" className="x" title="remove these dates"
            onClick={() => set({ dates: dates.filter((_, j) => j !== i) })}>×</button>
        </span>
      ))}
      <button type="button" className="btn" style={{ padding: "1px 8px" }}
        onClick={() => set({ dates: [...dates, { from_month: 1, from_day: 1, to_month: 12, to_day: 31 }] })}>
        + add dates
      </button>
      <div className="date-row">
        <label>
          <input type="checkbox" checked={w.hours != null}
            onChange={(e) => set({ hours: e.target.checked
              ? { start: { at: "00:00" }, end: { at: "23:59" } } : undefined })} />{" "}
          hours
        </label>
        {w.hours && (
          <>
            {" "}<ClockInput value={w.hours.start} onChange={(c) => set({ hours: { ...w.hours!, start: c } })} />
            {" to "}<ClockInput value={w.hours.end} onChange={(c) => set({ hours: { ...w.hours!, end: c } })} />
          </>
        )}
      </div>
      <div className="date-row">
        {WEEKDAYS.map((d) => (
          <label key={d} style={{ marginRight: 6 }}>
            <input type="checkbox" checked={(w.weekdays ?? []).includes(d)}
              onChange={(e) => set({ weekdays: WEEKDAYS.filter((x) =>
                x === d ? e.target.checked : (w.weekdays ?? []).includes(x)) })} />
            {d.slice(0, 3)}
          </label>
        ))}
      </div>
      {(w.unparsed ?? []).length > 0 && (
        <div className="review-flag" title="the parser could not read this season; it is not all year">
          unparsed (read-only): {(w.unparsed ?? []).join(" · ")}
        </div>
      )}
      {!terse(w) && <span className="hint">no dates, hours or weekdays — all year</span>}
    </div>
  );
}
