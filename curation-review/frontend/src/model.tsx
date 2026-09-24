// Shared plumbing for the model editors: the vocabulary (read off catalogue.py by the backend),
// the draft's addressed errors, and the handful of inputs every editor is built from.
//
// EVERY FIELD IS LABELLED WITH THE MODEL'S OWN KEY and carries its JSON path (`data-path`), so a
// validator error — `rules.2.gear.0.max`, or a rule-level "take=0 needs an explicit may_target"
// — lands on the control that edits it. A message addressed to a parent that NAMES a field
// (`may_target` above) highlights that field too.

import { createContext, useContext, useState, type ReactNode } from "react";
import type { FieldError, Vocab } from "./types";

export const VocabCtx = createContext<Vocab | null>(null);

export function useVocab(): Vocab {
  const v = useContext(VocabCtx);
  if (!v) throw new Error("the model vocabulary has not loaded");
  return v;
}

export const ErrorsCtx = createContext<FieldError[]>([]);

function mentions(msg: string, key: string): boolean {
  return new RegExp(`(^|[^a-z_])${key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}([^a-z_]|$)`).test(msg);
}

/** Errors at exactly `path` (or under it, when `deep`), and parent-level errors naming its key. */
export function useFieldErrors(path: string, deep = false): { own: FieldError[]; named: FieldError[] } {
  return fieldErrors(useContext(ErrorsCtx), path, deep);
}

export function fieldErrors(errors: FieldError[], path: string, deep = false):
    { own: FieldError[]; named: FieldError[] } {
  const cut = path.lastIndexOf(".");
  const parent = cut >= 0 ? path.slice(0, cut) : "";
  const key = path.slice(cut + 1);
  const own = errors.filter((e) => e.path === path || (deep && e.path.startsWith(path + ".")));
  const named = /^\d+$/.test(key) ? [] :
    errors.filter((e) => e.path === parent && mentions(e.msg, key));
  return { own, named };
}

/** Errors addressed to exactly this path — for a card (a rule, a clause, a record). */
export function ErrorsAt({ path }: { path: string }) {
  const errors = useContext(ErrorsCtx);
  const here = errors.filter((e) => e.path === path);
  if (!here.length) return null;
  return (
    <div className="field-errors" data-errors-for={path}>
      {here.map((e, i) => <div key={i} className="field-error">{e.msg}</div>)}
    </div>
  );
}

/** One labelled field. `k` is the model key shown; `path` is where its errors are addressed. */
export function F({ path, k, children, hint, deep = false }: {
  path: string; k?: string; children: ReactNode; hint?: ReactNode; deep?: boolean;
}) {
  const { own, named } = useFieldErrors(path, deep);
  const key = k ?? path.slice(path.lastIndexOf(".") + 1);
  const cls = own.length ? " has-error" : named.length ? " named-error" : "";
  return (
    <div className={`mfield${cls}`} data-path={path}
      title={named.length ? named.map((e) => e.msg).join("\n") : undefined}>
      <span className="k" title={path}>{key}</span>
      <div className="v">
        {children}
        {hint && <div className="hint">{hint}</div>}
        {own.map((e, i) => (
          <div key={i} className="field-error">
            {e.path !== path && <code>{e.path.slice(path.length + 1)}: </code>}{e.msg}
          </div>
        ))}
      </div>
    </div>
  );
}

/** Drop keys whose value is undefined, so a cleared control removes the key rather than
 *  writing a null the model would store. */
export function put<T extends object>(obj: T, patch: Partial<T>): T {
  const out = { ...obj, ...patch } as Record<string, unknown>;
  for (const k of Object.keys(out)) if (out[k] === undefined) delete out[k];
  return out as T;
}

/** An empty list is the model's default: leave the key out. */
export function orNone<T>(xs: T[] | undefined | null): T[] | undefined {
  return xs && xs.length ? xs : undefined;
}

export function Num({ value, onChange, step, label, width = 80 }: {
  value?: number | null; onChange: (v: number | undefined) => void; step?: number;
  label?: string; width?: number;
}) {
  return (
    <input type="number" aria-label={label} step={step ?? "any"} style={{ width }}
      value={value ?? ""}
      onChange={(e) => onChange(e.target.value === "" ? undefined : Number(e.target.value))} />
  );
}

export function Text({ value, onChange, area = false, label, placeholder, rows = 2 }: {
  value?: string | null; onChange: (v: string | undefined) => void; area?: boolean;
  label?: string; placeholder?: string; rows?: number;
}) {
  const on = (v: string) => onChange(v === "" ? undefined : v);
  return area ? (
    <textarea aria-label={label} className="grow" rows={rows} spellCheck={false}
      placeholder={placeholder} value={value ?? ""} onChange={(e) => on(e.target.value)} />
  ) : (
    <input type="text" aria-label={label} className="grow" placeholder={placeholder}
      value={value ?? ""} onChange={(e) => on(e.target.value)} />
  );
}

/** A choice from a closed set; the blank option leaves the key out. */
export function Pick({ value, options, onChange, none = "—", label, words }: {
  value?: string | null; options: string[]; onChange: (v: string | undefined) => void;
  none?: string | null; label?: string; words?: Record<string, string>;
}) {
  return (
    <select aria-label={label} value={value ?? ""}
      onChange={(e) => onChange(e.target.value === "" ? undefined : e.target.value)}>
      {none !== null && <option value="">{none}</option>}
      {value && !options.includes(value) && <option value={value}>{value} (not in the list)</option>}
      {options.map((o) => <option key={o} value={o}>{words?.[o] ? `${o} — ${words[o]}` : o}</option>)}
    </select>
  );
}

/** A three-valued boolean: unset (the default / inherit), true, false. */
export function Tri({ value, onChange, unset = "unset", yes = "true", no = "false", label }: {
  value?: boolean | null; onChange: (v: boolean | undefined) => void;
  unset?: string; yes?: string; no?: string; label?: string;
}) {
  return (
    <select aria-label={label} value={value == null ? "" : value ? "true" : "false"}
      onChange={(e) => onChange(e.target.value === "" ? undefined : e.target.value === "true")}>
      <option value="">{unset}</option>
      <option value="true">{yes}</option>
      <option value="false">{no}</option>
    </select>
  );
}

export function Check({ value, onChange, children, label }: {
  value?: boolean; onChange: (v: boolean | undefined) => void; children?: ReactNode; label?: string;
}) {
  return (
    <label className="check">
      <input type="checkbox" aria-label={label} checked={!!value}
        onChange={(e) => onChange(e.target.checked ? true : undefined)} />
      {children}
    </label>
  );
}

/** A subset of a closed set, as checkboxes; order follows the vocabulary. */
export function Checks({ values, options, onChange, words }: {
  values?: string[]; options: string[]; onChange: (v: string[] | undefined) => void;
  words?: Record<string, string>;
}) {
  const have = values ?? [];
  const stray = have.filter((v) => !options.includes(v));
  return (
    <div className="checks">
      {[...options, ...stray].map((o) => (
        <label key={o} className="check" title={words?.[o]}>
          <input type="checkbox" checked={have.includes(o)}
            onChange={(e) => onChange(orNone(
              [...options, ...stray].filter((x) => (x === o ? e.target.checked : have.includes(x)))))} />
          {o}{stray.includes(o) ? " (not in the model)" : ""}
        </label>
      ))}
    </div>
  );
}

let listSeq = 0;

/** An ordered list of OPEN tokens: type + Enter to add. `suggestions` are what the corpus already
 *  uses; any token may be typed. `keepEmpty` returns [] rather than removing the key — for a bound
 *  (`allow`/`only`/`ban`) whose empty list is a statement the model refuses, not a default. */
export function Tags({ values, onChange, suggestions = [], placeholder, keepEmpty = false, label }: {
  values?: string[] | null; onChange: (v: string[] | undefined) => void; suggestions?: string[];
  placeholder?: string; keepEmpty?: boolean; label?: string;
}) {
  const [draft, setDraft] = useState("");
  const [id] = useState(() => `tags-${++listSeq}`);
  const have = values ?? [];
  const emit = (next: string[]) => onChange(keepEmpty ? next : orNone(next));
  function commit(raw: string) {
    const v = raw.trim();
    if (v && !have.includes(v)) emit([...have, v]);
    setDraft("");
  }
  return (
    <div className="tag-input">
      {have.map((v) => (
        <span className="tag" key={v}>
          {v}
          <button type="button" onClick={() => emit(have.filter((x) => x !== v))}
            aria-label={`remove ${v}`}>×</button>
        </span>
      ))}
      <input value={draft} aria-label={label} placeholder={placeholder ?? "type + Enter"}
        list={suggestions.length ? id : undefined}
        onChange={(e) => {
          const v = e.target.value;
          // picking from the datalist fills the whole token at once: take it
          if (suggestions.includes(v) && !have.includes(v)) { emit([...have, v]); setDraft(""); }
          else setDraft(v);
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === ",") { e.preventDefault(); commit(draft); }
          else if (e.key === "Backspace" && !draft && have.length) emit(have.slice(0, -1));
        }}
        onBlur={() => draft && commit(draft)} />
      {suggestions.length > 0 && (
        <datalist id={id}>{suggestions.filter((s) => !have.includes(s)).map((s) => <option key={s} value={s} />)}</datalist>
      )}
    </div>
  );
}

/** Move item `i` of a list by `d` (−1 up, +1 down). Order is meaningful in `gear` and `lengths`:
 *  first match wins. */
export function move<T>(xs: T[], i: number, d: number): T[] {
  const j = i + d;
  if (j < 0 || j >= xs.length) return xs;
  const out = xs.slice();
  [out[i], out[j]] = [out[j], out[i]];
  return out;
}

export function RowTools({ i, n, onMove, onRemove }: {
  i: number; n: number; onMove: (d: number) => void; onRemove: () => void;
}) {
  return (
    <span className="row-tools">
      <button type="button" className="x" title="move up (first match wins)" disabled={i === 0}
        onClick={() => onMove(-1)}>↑</button>
      <button type="button" className="x" title="move down" disabled={i === n - 1}
        onClick={() => onMove(1)}>↓</button>
      <button type="button" className="x" title="remove" onClick={onRemove}>×</button>
    </span>
  );
}

/** A sub-object as editable JSON, validated live by the backend like every other control. The
 *  escape hatch for a shape no structured control covers (an extent key the Extent model does
 *  not declare), and a way to see exactly what will be written. */
export function RawJson<T>({ value, onChange, label = "raw JSON" }: {
  value: T; onChange: (v: T) => void; label?: string;
}) {
  const [text, setText] = useState<string | null>(null);
  const [bad, setBad] = useState("");
  const shown = text ?? JSON.stringify(value, null, 1);
  return (
    <details className="raw-json">
      <summary>{label}</summary>
      <textarea spellCheck={false} rows={Math.min(24, shown.split("\n").length + 1)} value={shown}
        onChange={(e) => {
          setText(e.target.value);
          try {
            const v = JSON.parse(e.target.value);
            setBad("");
            onChange(v);
          } catch (err) {
            setBad(String((err as Error).message));
          }
        }}
        onBlur={() => { if (!bad) setText(null); }} />
      {bad && <div className="field-error">not JSON: {bad}</div>}
    </details>
  );
}
