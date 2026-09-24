import { useMemo } from "react";
import type { SpeciesOption } from "../types";

interface Props {
  values?: string[]; // species codes
  options: SpeciesOption[]; // catalogue.KNOWN_SPECIES, from /api/vocab
  onChange: (next: string[] | undefined) => void;
  label?: string;
}

// Species editor over the MODEL's vocabulary (`KNOWN_SPECIES`, groups first-class): selected codes
// show with the model's own words, and "+ add" appends. An empty list is NOT "all species" — a
// retention rule refuses it; ALL_GAME_FISH / ALL_FIN_FISH say "everything" explicitly.
export function SpeciesPicker({ values, options, onChange, label }: Props) {
  const have = values ?? [];
  const nameByCode = useMemo(() => Object.fromEntries(options.map((o) => [o.code, o.name])), [options]);
  const available = options.filter((o) => !have.includes(o.code));
  const groups = available.filter((o) => o.is_group);
  const single = available.filter((o) => !o.is_group);
  const emit = (next: string[]) => onChange(next.length ? next : undefined);

  return (
    <div className="species-picker">
      {have.length === 0 && <span className="dim">none</span>}
      {have.map((c) => (
        <span className="species-chip" key={c}>
          {nameByCode[c] ?? `${c} (not a known code)`} <span className="dim">({c})</span>
          <button type="button" className="x" title="remove" onClick={() => emit(have.filter((v) => v !== c))}>×</button>
        </span>
      ))}
      <select aria-label={label} value="" onChange={(e) => e.target.value && emit([...have, e.target.value])} title="add a species">
        <option value="">+ add…</option>
        <optgroup label="groups — the word the page prints">
          {groups.map((o) => <option key={o.code} value={o.code}>{o.name} — {o.code}</option>)}
        </optgroup>
        <optgroup label="individual fish">
          {single.map((o) => <option key={o.code} value={o.code}>{o.name} — {o.code}</option>)}
        </optgroup>
      </select>
    </div>
  );
}
