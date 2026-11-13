/**
 * T06 parity: core's rule views and describe() against page v36's OWN decode layer (page_v36.js up to `segBar`, run here
 * by `pageDecode`, ruleSources.ts) over THE SAME RULES. Nothing here is a hand-copied string: every expected value is what
 * the page's code says of the rule core reads.
 *
 * Over two inputs (ruleSources.ts): page_v36.html's own #data/#answers (always — frozen inputs, frozen code: it never
 * moves), and the release paired with the oracles, read through `openRegsStore(loader)` as the app reads it (skipped
 * otherwise), EVERY rule of it:
 *   7. every rix: describe(ruleIndex(stored, new Map(), book).get(rix), book) === the page's describe()
 *   8. every rix: {key, kind, baseRank, species, speciesExcept, wins, tcond, part, notes} deep-equal the page's decode
 * A part's via and per-part rank (old item 9) are held per golden state by settle.parity's makePlace test (page-oracle.json).
 *
 * (T667 slice 5b) Until 2026-11-13 the release run compared with page v36's FROZEN output (hydrate.json: its 726 rules)
 * and so carried the rulings since (renames, retired rules, "on parts", a drawn part — v36-rulings.json); a second run
 * (item 10) held the 28 rules with notes to the page's code. The page's code over every release rule is both, with no
 * amendment.
 */
import { describe as group, expect, it } from "vitest";
import { describe, ruleIndex, type RegsPage, type RuleView } from "@app/core";
import { pageOracle } from "./oracles";
import { pageDecode, pageSource, storeSource, type RuleSource } from "./ruleSources";

const store = await storeSource();

/** The port target core's contract names (model.ts `RegsPage.version`): typed, so a contract bump fails tsc here. */
const TARGET: RegsPage["version"] = "regs-v38";

const factsOf = (r: RuleView) => ({ key: r.key, kind: r.kind, baseRank: r.baseRank, species: r.species, speciesExcept: r.speciesExcept,
  wins: r.wins, tcond: r.tcond, part: r.part, notes: r.notes });

it("the page oracle is the port target", () => {
  expect(`regs-${pageOracle().meta.version}`).toBe(TARGET);
});

function parity(src: () => RuleSource, least: number) {
  it("7. describe() says what page v36's code says, for every rule", () => {
    const s = src(), page = pageDecode(s);
    const ix = ruleIndex(s.stored, new Map(), s.book);
    const bad: string[] = [];
    for (const rix of s.stored.keys()) {
      const got = describe(ix.get(rix), s.book), want = page.describe(rix);
      if (got !== want) bad.push(`${rix}: ${JSON.stringify(got)} ≠ ${JSON.stringify(want)}`);
    }
    expect(bad).toEqual([]);
    expect(s.stored.size).toBeGreaterThanOrEqual(least);
  });

  it("8. every rule view decodes as page v36's code does (key, kind, ranks, species, windows, time, part, notes)", () => {
    const s = src(), page = pageDecode(s);
    const ix = ruleIndex(s.stored, new Map(), s.book);
    for (const rix of s.stored.keys()) {
      const r = page.rules.get(rix)!;
      expect(factsOf(ix.get(rix)), `rix ${rix}`).toEqual({ key: r.key, kind: r.k, baseRank: r.baseRank, species: r.species,
        speciesExcept: r.speciesExcept, wins: r.wins, tcond: r.tcond, part: r.part, notes: r.notes });
    }
  });
}

group("T06 parity: page_v36.html's own inputs", () => parity(pageSource, 726));

group.skipIf(!store)(`T06 parity: the regs store (${store?.name ?? "absent"})`, () => {
  parity(() => store!, 726);
  // the release's rules with prov.uncertain (or a review_reason) are the only ones that exercise the UNCERTAIN /
  // CURATOR wording (the page's own 726 have none): the run above must have met some
  it("the run covers rules with notes", () => {
    const noted = [...pageDecode(store!).rules.values()].filter((r) => r.notes.length > 0).length;
    expect(noted, "store rules with notes").toBeGreaterThan(0);
  });
});
