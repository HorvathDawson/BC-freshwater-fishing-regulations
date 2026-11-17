/**
 * (T770 / T796, owner 2026-11-16 19:09: "As a closure line (Recommended)") EVERY CHAIN OF THE RELEASE UNDER TEST THAT
 * CARRIES ANOTHER FISH'S CLOSURE (`o`, `deliver.salmon.closures_on`) SHOWS IT ON ITS CARD AS A CLOSURE LINE, in Python's
 * words, on every day it holds — and never among the card's gear lines. Asked of the whole release, chain by chain, so a
 * release with more such chains is covered with no edit; a release cut before `o` has none and asserts only that.
 * Where the release carries `o`, the Kitsumkalum River's card (Region 6 B(ii)) reads "Eulachon closed all year: No
 * fishing for eulachon" (r-1603e71cc92c: 37 chains carry `o`).
 */
import { describe, expect, it } from "vitest";
import { salmonCard, type SalmonFacts } from "@app/core";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
const EULACHON = "Eulachon closed all year: No fishing for eulachon";
// a day per month: a closure line that holds part of the year is asked on its days
const DAYS = Array.from({ length: 12 }, (_, m) => new Date(2026, m, 15));

describe.skipIf(!LIVE)(`T770 another fish's closure on its card (${LIVE ?? "no live release"})`, () => {
  it("every chain's `o` lines are its card's closure lines that day, never a gear line; Kitsumkalum reads the eulachon's", async () => {
    const facts = (await openReleaseDir(LIVE!).salmon()) as SalmonFacts;
    const P = facts.payload;
    const carrying = P.chains.flatMap((c, i) => (c.o ? [i] : []));
    const card = (i: number, on: Date) => salmonCard({ ...facts, chainOf: () => i }, { sids: [0], item: null }, on);
    const wrong: string[] = [];
    for (const i of carrying) for (const on of DAYS) {
      const c = card(i, on);
      const want = [...new Set([...new Set(P.chains[i]!.o!.filter((_, k) => k % 2).filter((x) => x >= 0))].flatMap((x) => P.gears[x]!))];
      const held = c.closures;
      if (held.some((l) => !want.includes(l)) || c.gear.some((g) => want.includes(g)) || c.gear.some((g) => /^\w+ closed\b/.test(g)))
        wrong.push(`chain ${i} ${P.chains[i]!.w} ${on.toDateString()}: closures ${JSON.stringify(held)} gear ${JSON.stringify(c.gear)}`);
    }
    expect(wrong).toEqual([]);
    // a chain without `o` draws no closure line
    const none = P.chains.findIndex((c, i) => i !== P.default && !c.o);
    expect(card(none, DAYS[0]!).closures).toEqual([]);
    if (!carrying.length) return;           // a release cut before `o`
    const kits = carrying.filter((i) => P.chains[i]!.w.startsWith("Kitsumkalum River"));
    expect(kits.length).toBeGreaterThan(0);
    for (const i of kits) for (const on of DAYS) {
      const c = card(i, on);
      expect([c.closures, c.gear.includes(EULACHON), c.gear.some((g) => /eulachon/i.test(g))], `chain ${i} ${on.toDateString()}`)
        .toEqual([[EULACHON], false, false]);
    }
  });
});
