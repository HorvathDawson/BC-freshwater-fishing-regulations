/**
 * The place a tap reads (T07): page v36's `makePlace` over a `RegsInputs` — the part's key, its rules viewed through the
 * part's via (T06 `ruleIndex`, built by T09), the ruleset's candidates, the key's segment starts and moments, the part's
 * facts, and whether the key is tidal. Everything is the store's, as loaded; nothing is computed.
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): makePlace, segStarts, momentsOf, isTidal. Hidden globals replaced:
 * D.waters / A.parts / A.keys / DWATERS (→ `inp.item`, `inp.key`, `inp.year`), `w.R` (→ `inp.rules`).
 */
import type { AkeyRow, Moment, PartFacts, RegsInputs, RuleIndex, RuleView } from "./inputs";

export interface RegsPlace {
  /** null: outside B.C. (page `w.key == null`). */
  readonly key: AkeyRow | null;
  readonly R: RuleIndex;
  /** Every rix of the part's ruleset, reach then trib (page `cands`), each viewed through the part. */
  readonly cands: readonly RuleView[];
  readonly starts: readonly number[];
  readonly moments: readonly Moment[] | null;
  /** page `disp`: the part's facts (null outside B.C., and for an unnamed section). */
  readonly part: PartFacts | null;
  /** page isTidal: the key's `tidal` flag. */
  readonly tidal: boolean;
}

const NONE: readonly never[] = Object.freeze([]);

export function makePlace(inp: RegsInputs): RegsPlace {
  const rs = inp.ruleset;
  return Object.freeze({
    key: inp.key,
    R: inp.rules,
    cands: rs ? Object.freeze([...rs.reach, ...rs.trib].map((ix) => inp.rules.get(ix))) : NONE,
    starts: inp.year?.segStarts ?? NONE,
    moments: inp.year?.moments ?? null,
    part: inp.item?.parts[inp.partIx] ?? null,
    tidal: !!inp.key?.tidal,
  });
}
