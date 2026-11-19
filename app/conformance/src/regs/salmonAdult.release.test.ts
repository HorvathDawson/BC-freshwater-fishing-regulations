/**
 * (T869, owner 2026-11-18: "It is 10 max adult Chinook not over 50cm because some rivers adult is defined differently")
 * OVER THE RELEASE UNDER TEST, every water DFO's header gives another adult chinook length (R2's Vedder / Capilano /
 * Fraser / Chehalis: over 62 cm; R5B's Bella Coola: over 65 cm) and every page of a region whose own adult is longer
 * (R6: over 65 cm) says THAT length for adults — on the chinook row's sizes, on a day the length holds, and beside the
 * annual chinook limit ("(here an adult is over 62 cm …)") — and never "adult chinook over 50 cm". Where the release's
 * annual tier is in our words (a release cut after T869), it is "10 adult chinook a year from all fresh waters" exactly;
 * a release cut before keeps DFO's sentence there, which this test then leaves alone. Asked of the whole file, region by
 * region, so a new scoped length is covered with no edit.
 */
import { describe, expect, it } from "vitest";
import { salmonCard, salmonTable, type SalmonFacts } from "@app/core";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
/** a file day index (Jan 1 = 0, Feb 29 = 59) as a date of 2028, a leap year: every index is a real day */
const dateOf = (i: number) => new Date(2028, 0, 1 + i);

describe.skipIf(!LIVE)(`T869 adult chinook as DFO defines it on each water (${LIVE ?? "no live release"})`, () => {
  it("each water with another adult length, and each region with its own, says that length — never over 50 cm", async () => {
    const facts = (await openReleaseDir(LIVE!).salmon()) as SalmonFacts;
    const P = facts.payload;
    const wrong: string[] = [];
    let asked = 0;
    for (const [r, region] of Object.entries(P.regions)) {
      const annual = region.bucket?.tree.children.find((c) => c.node === "tier" && c.period === "annual" && c.species?.includes("CH"));
      const over = annual && annual.node === "tier" ? annual.over_cm ?? 50 : 50;
      const chain = P.chains.findIndex((c, i) => i !== P.default && c.r.length === 1 && c.r[0] === r);
      if (chain < 0) continue;
      const cardOf = (item: string | null, on: Date) => salmonCard({ ...facts, chainOf: () => chain }, { sids: [0], item }, on);
      const sizes = region.sizes.filter((e) => e.sp.includes("CH") && e.adult?.min_cm !== undefined);
      const check = (what: string, item: string | null, on: Date, cm: number) => {
        asked++;
        const c = cardOf(item, on);
        const limit = salmonTable(c).species.find((s) => s.code === "CH")?.limit ?? "";
        if (!limit.includes(`adult chinook over ${cm} cm`) || (cm !== 50 && limit.includes("adult chinook over 50 cm")))
          wrong.push(`${what}: sizes ${JSON.stringify(limit)}`);
        if (cm !== over && !c.adultHere?.includes(`over ${cm} cm`)) wrong.push(`${what}: annual line's adult ${JSON.stringify(c.adultHere)}`);
        if (cm === over && sizes.every((e) => e.adult!.min_cm === over) && c.adultHere) wrong.push(`${what}: says ${c.adultHere}`);
      };
      // a water's own length, on a day it holds (the first entry holding the item that day wins, as the pipeline ordered)
      for (const e of sizes.filter((x) => x.w?.length)) for (const key of e.w!) for (const item of (P.scopes?.[r]?.[key] ?? []).slice(0, 5)) {
        const day = e.on?.[0]?.[0] ?? 15;
        const first = sizes.find((x) => x.w?.some((k) => (P.scopes?.[r]?.[k] ?? []).includes(item))
          && (!x.on?.length || x.on.some(([a, b]) => day >= a! && day <= b!)));
        if (first !== e) continue;
        check(`region ${r} ${key} ${item}`, item, dateOf(day), e.adult!.min_cm!);
      }
      // the region's own length, on a page naming no water
      const own = sizes.find((e) => !e.w?.length);
      if (own) check(`region ${r} (no water)`, null, dateOf(15), own.adult!.min_cm!);
      // the annual tier in our words, where the release is cut after T869
      if (annual && annual.node === "tier" && !annual.says.startsWith("The total annual limit"))
        expect(annual.says, `region ${r}`).toBe(`${annual.n} adult chinook a year from all fresh waters`);
    }
    expect(wrong).toEqual([]);
    expect(asked).toBeGreaterThan(5);
  });
});
