import type { LengthBand, Rule } from "../types";

/** One size band as stored: `min_cm` / `max_cm` (inclusive; null = open) and its `take`. */
export function bandWords(b: LengthBand): string {
  const lo = b.min_cm ?? null;
  const hi = b.max_cm ?? null;
  const size = lo != null && hi != null ? `${lo}–${hi} cm`
    : lo != null ? `${lo} cm and over`
    : hi != null ? `up to ${hi} cm`
    : "any size";
  const take = b.take == null ? "take not stated" : b.take === 0 ? "release (take 0)" : `take ${b.take}`;
  return `${size} → ${take}`;
}

interface Props { rule: Pick<Rule, "species" | "species_except" | "lengths"> }

/* THE SPECIES AND SIZES A RULE STORES (T205): exactly the rule's `species`, `species_except` and
   `lengths` fields, one line per species with the size ranges that apply to it. `lengths` is one
   ordered list for the whole rule (the first matching band wins), so every named species carries
   the same ranges. Empty fields are said, not hidden: the reviewer checks the parse against the book. */
export function RuleSpecies({ rule }: Props) {
  const species = rule.species ?? [];
  const except = rule.species_except ?? [];
  const bands = rule.lengths ?? [];
  const ranges = bands.length
    ? <ol className="sp-bands">{bands.map((b, i) => <li key={i}>{bandWords(b)}</li>)}</ol>
    : <span className="dim">no size range stored — any size</span>;
  const names = species.length
    ? species
    : [except.length ? "all species" : "no species stored (all species)"];
  return (
    <div className="rule-species" data-testid="rule-species">
      <table>
        <tbody>
          {names.map((name) => (
            <tr key={name}>
              <td className="sp-name">{name}</td>
              <td>{ranges}</td>
            </tr>
          ))}
          {except.length > 0 && (
            <tr><td className="sp-name">except</td><td>{except.join(", ")}</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
