import { useMemo } from "react";
import type { SpeciesOption } from "../types";

interface Props {
  values: string[]; // species codes
  options: SpeciesOption[];
  onChange: (next: string[]) => void;
}

// Species editor: selected species show as common names (with the code), and a dropdown "+ add" appends
// from the full BC species/group list. Empty = ALL species.
export function SpeciesPicker({ values, options, onChange }: Props) {
  const nameByCode = useMemo(() => Object.fromEntries(options.map((o) => [o.code, o.name])), [options]);
  const available = options.filter((o) => !values.includes(o.code));

  function add(code: string) {
    if (code && !values.includes(code)) onChange([...values, code]);
  }

  return (
    <div className="species-picker">
      {values.length === 0 && <span className="dim">ALL species</span>}
      {values.map((c) => (
        <span className="species-chip" key={c}>
          {nameByCode[c] ?? c} <span className="dim">({c})</span>
          <button type="button" className="x" title="remove" onClick={() => onChange(values.filter((v) => v !== c))}>×</button>
        </span>
      ))}
      <select value="" onChange={(e) => add(e.target.value)} title="add a species">
        <option value="">+ add species…</option>
        {available.map((o) => (
          <option key={o.code} value={o.code}>
            {o.name}{o.is_group ? " (group)" : ""} — {o.code}
          </option>
        ))}
      </select>
    </div>
  );
}
