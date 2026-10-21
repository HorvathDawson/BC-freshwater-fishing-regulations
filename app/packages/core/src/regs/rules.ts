/**
 * A store rule as one part sees it. Replaces page v36's globals RULES, RIX, D.rid, DRULES, NORULE, RX and the per-part
 * rank of makePlace.R: the rules come in as a `stored` map (T04 `rules()`), the part's tributary list as `via`
 * (`viaOf(store.ruleset(akey.ruleset))` — page_data.py builds the page's `[[ix, via]]` from the same ruleset), and the
 * species names as a `SpeciesBook`.
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): the RULES decode loop, NORULE, RX, makePlace.R.
 * What changes: a view is a plain frozen object per part (the page uses Object.create(base) per place); its arrays are frozen.
 */
import type { ExportRule, RuleFacts, RuleIndex, RuleView } from "./inputs";
import { expand, type SpeciesBook } from "./names";
import { timeCond, whenDates, type RuleWhen } from "./time";
import { WORDS } from "./words";

export type Via = "reach" | "trib";

/** page makePlace.R: a rule reached by the tributary walk ranks 1 (the reader's place) unless its base rank is federal. */
const rankOf = (baseRank: number, via: Via): number => (via === "trib" && baseRank >= 0 ? 1 : baseRank);

interface ReadFields {
  readonly species?: readonly string[] | null; readonly species_except?: readonly string[] | null;
  readonly when?: RuleWhen | null; readonly undrawn_part?: string | null; readonly review_reason?: string | null;
}

export function ruleView(rix: number, exp: ExportRule, facts: RuleFacts | null, sp: SpeciesBook, via: Via): RuleView {
  const f = exp.fields as ReadFields;
  const notes: string[] = [];
  if (exp.prov.uncertain) notes.push(WORDS.UNCERTAIN + (exp.prov.why || WORDS.NO_REASON));
  if (f.review_reason) notes.push(WORDS.CURATOR + f.review_reason);
  const baseRank = exp.prov.rank;
  return Object.freeze({
    rix, key: `${exp.entry_id}::${exp.rule_id}`, export: exp, facts,
    f: exp.fields, kind: facts ? facts.kind : null,
    baseRank, via, rank: rankOf(baseRank, via),
    species: Object.freeze(expand(sp, f.species)), speciesExcept: Object.freeze(expand(sp, f.species_except)),
    wins: Object.freeze(whenDates(f.when).map((w) => Object.freeze(w))), tcond: timeCond(f.when),
    part: f.undrawn_part || null,
    notes: Object.freeze(notes),
    missing: false,
  });
}

const NONE: readonly never[] = Object.freeze([]);

/** page NORULE: a rule the inputs name that the data lacks — a visible stand-in, never a guess. Ranked as page R ranks it. */
export function noRule(rix: number, via: Via = "reach"): RuleView {
  const says = WORDS.MISSING_RULE(rix);
  const exp: ExportRule = Object.freeze({ entry_id: "", rule_id: "", type: "", family: "", label: says, verbatim: says,
    fields: Object.freeze({}), prov: Object.freeze({ rank: 4 }) });
  return Object.freeze({
    rix, key: `#${rix}`, export: exp, facts: null, f: exp.fields, kind: null,
    baseRank: 4, via, rank: rankOf(4, via),
    species: NONE, speciesExcept: NONE, wins: NONE, tcond: "", part: null,
    notes: Object.freeze([WORDS.MISSING_DATA(rix)]),
    missing: true,
  });
}

/** The part's via per rix, from its ruleset's `reach` and `trib` lists (no rix is in both). No ruleset: none. */
export function viaOf(ruleset: { readonly reach: readonly number[]; readonly trib: readonly number[] } | null): ReadonlyMap<number, Via> {
  const m = new Map<number, Via>();
  if (!ruleset) return m;
  for (const ix of ruleset.reach) m.set(ix, "reach");
  for (const ix of ruleset.trib) m.set(ix, "trib");
  return m;
}

/** page makePlace.R over a part: each rule viewed once, through the part's `via` (absent: "reach"); a rix not in the
 *  store is the noRule stand-in, so get() never misses. */
export function ruleIndex(stored: ReadonlyMap<number, { readonly export: ExportRule; readonly facts: RuleFacts | null }>,
  via: ReadonlyMap<number, Via>, sp: SpeciesBook): RuleIndex {
  const cache = new Map<number, RuleView>();
  let keys: Map<string, number> | undefined;   // key → rix, built on the first byKey (a sheet open), once per part
  return {
    get(rix: number): RuleView {
      let v = cache.get(rix);
      if (!v) {
        const s = stored.get(rix), how = via.get(rix) ?? "reach";
        v = s ? ruleView(rix, s.export, s.facts, sp, how) : noRule(rix, how);
        cache.set(rix, v);
      }
      return v;
    },
    has: (rix: number) => stored.has(rix),
    get size() { return stored.size; },
    byKey(key: string): RuleView | null {
      keys ??= new Map([...stored].map(([rix, s]) => [`${s.export.entry_id}::${s.export.rule_id}`, rix] as const));
      const rix = keys.get(key);
      return rix === undefined ? null : this.get(rix);
    },
  };
}
