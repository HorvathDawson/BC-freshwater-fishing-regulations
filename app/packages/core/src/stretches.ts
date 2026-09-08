/**
 * A river as the stretches that DIFFER, not as every section it is cut into.
 *
 * THE FRASER SHOWED 201 ROWS. `WaterScreen` lists a water's reaches, and the atlas cuts a
 * long river into hundreds of sections for reasons that have nothing to do with regulation:
 * confluences, lake outlets, gauge splits, a 25 km length cap. A reader opening the Fraser
 * got two hundred identical lines, and the one line that mattered — the stretch where the
 * rules change — was indistinguishable from its neighbours.
 *
 * The bundle already knows which sections differ. Interning the rule sets gave every section
 * a `set_id`, and two sections with the same id are covered by exactly the same rules, for
 * exactly the same reasons (scope is part of the set's identity). So the stretches a person
 * should be shown are the RUNS of equal set id.
 *
 * CONSECUTIVE runs, not a group-by. A river can be closed, open, then closed again, and
 * those two closed pieces are in different places — collapsing them into one row because
 * they share a rule set would put a stretch on the screen that does not exist on the ground.
 * Sections arrive mouth-to-source, so adjacency in the list is adjacency on the water.
 */

/**
 * Whatever the caller uses to name a section. `core` never looks inside it — it compares
 * nothing, parses nothing, and only carries it through — so it must not have an opinion:
 * the bundle names sections by an integer handle, and a test or a fixture may name them
 * with a string. Pinning this to `string` made the id's SHAPE a core concern, which it is
 * not, and would have forced every caller of `runsOfSameRules` to launder it.
 */
export type SectionKey = string | number;

/** A section with the id of the rule set covering it — `null` where no rule reaches it. */
export interface SectionSet {
  section: SectionKey;
  setId: number | null;
}

/** One drawn stretch: a run of adjacent sections that all answer the same way. */
export interface Stretch<T extends SectionSet> {
  /** The sections it covers, in order. Never empty. */
  sections: readonly T[];
  /** The rule set they share. `null` means no rule reaches this stretch. */
  setId: number | null;
  /** Where this stretch starts, counting from the mouth. Used for "Stretch 2 of 5". */
  index: number;
}

/**
 * Collapse adjacent sections that share a rule set.
 *
 * A section with no rules (`setId === null`) groups with its neighbours that also have none:
 * "the rest of the river, under the general rules" is one answer and one stretch, not a
 * hundred. It never merges with a regulated run, because `null` only equals `null`.
 */
export function runsOfSameRules<T extends SectionSet>(sections: readonly T[]): Stretch<T>[] {
  const out: Stretch<T>[] = [];
  for (const s of sections) {
    const last = out[out.length - 1];
    if (last && last.setId === s.setId) (last.sections as T[]).push(s);
    else out.push({ sections: [s], setId: s.setId, index: out.length });
  }
  return out;
}

/** One rule regime on a water: the set, and every place it applies. */
export interface Regime<T extends SectionSet> {
  setId: number | null;
  /** The contiguous pieces this regime covers, in order from the mouth. Never empty. */
  runs: readonly Stretch<T>[];
  /** Every section under it, across all pieces. */
  sections: readonly T[];
  index: number;
}

/**
 * A water as its DISTINCT RULE REGIMES — the answer to "what rules apply here, and where".
 *
 * `runsOfSameRules` answers a different question: where along the water does the answer
 * change. Both are wanted, and they are not the same number. The Fraser is 248 sections in
 * 102 contiguous runs — because the item spans 151 separate blue lines, braids and side
 * channels included — but only 16 distinct rule sets. A list of 102 rows is not much better
 * than a list of 248; a list of 16 is the thing a person came to read.
 *
 * A regime can therefore cover several pieces of water that are not next to each other, and
 * `runs` keeps them apart so a row can say "in 12 places" rather than implying one stretch.
 * That is the honest version of the compression: the geography is not thrown away, it is
 * reported at the level the question was asked.
 */
export function regimesOf<T extends SectionSet>(sections: readonly T[]): Regime<T>[] {
  const runs = runsOfSameRules(sections);
  const by = new Map<number | null, Regime<T>>();
  for (const run of runs) {
    const got = by.get(run.setId);
    if (got) {
      (got.runs as Stretch<T>[]).push(run);
      (got.sections as T[]).push(...run.sections);
    } else {
      by.set(run.setId, { setId: run.setId, runs: [run],
                          sections: [...run.sections], index: by.size });
    }
  }
  return [...by.values()];
}

/**
 * What one stretch is called.
 *
 * `lower`/`upper` come from a rule's extent and are usually absent — the bundle does not
 * carry clause offsets yet — so most stretches have no name for their ENDS. Numbering
 * distinguishes them without inventing geography, and a river that really is one stretch
 * says so rather than calling itself "Stretch 1 of 1".
 */
export function stretchLabel(where: { lower?: string | null; upper?: string | null },
                             index: number, total: number): string {
  if (where.lower || where.upper)
    return `${where.lower ?? "the mouth"} → ${where.upper ?? "the source"}`;
  return total === 1 ? "the mouth → the source" : `Stretch ${index + 1} of ${total}`;
}
