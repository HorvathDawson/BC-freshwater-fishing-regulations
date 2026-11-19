/**
 * (T869, owner 2026-11-18: "It is 10 max adult Chinook not over 50cm because some rivers adult is defined differently")
 * OVER THE RELEASE UNDER TEST, every water DFO's header gives another adult chinook length (R2's Vedder / Capilano /
 * Fraser / Chehalis: over 62 cm; R5B's Bella Coola: over 65 cm) and every page of a region whose own adult is longer
 * (R6: over 65 cm) says THAT length for adults — on the chinook row's sizes, on a day the length holds, and beside the
 * annual chinook limit, the whole line ("(here an adult is over 62 cm, Sep 1–Dec 31)") — and never "adult chinook over 50
 * cm". The annual tier is "10 adult chinook a year from all fresh waters" exactly on every release cut with T869 — known
 * from the file itself: it carries the places' printed rows (`pr`, T870, the same change), so a release built with this
 * code that still printed DFO's sentence fails; a release cut before carries neither (r-fc137c4040fa) and keeps DFO's
 * sentence. (A list of the older release ids is a pin the anchor scanner refuses in a live-release test, AGENTS W46.)
 * Asked of the whole file, region by region, so a new scoped length is covered with no edit; a water with a shape this
 * oracle does not compose (two scoped lengths) fails, naming it.
 */
import { describe, expect, it } from "vitest";
import { salmonCard, salmonTable, type SalmonFacts } from "@app/core";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
const MON = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(" ");
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
      const reg = sizes.find((e) => !e.w?.length)?.adult?.min_cm;
      const day = (i: number) => { const d = dateOf(i); return `${MON[d.getMonth()]} ${d.getDate()}`; };
      /** the whole line the card must say for a water's one scoped length (`e`), or for the region's own (none) */
      const adultLine = (e: (typeof sizes)[number] | null): string | null => {
        const cm = e ? e.adult!.min_cm! : reg;
        const win = e?.on?.length ? `, ${e.on.map(([a, b]) => `${day(a!)}–${day(b!)}`).join(", ")}` : "";
        const parts = [...(cm !== undefined && cm !== over ? [`over ${cm} cm${e ? win : ""}`] : []),
                       ...(e?.on?.length && reg !== undefined && reg !== over ? [`over ${reg} cm the rest of the year`] : [])];
        return parts.length ? `(here an adult is ${parts.join("; ")})` : null;
      };
      const check = (what: string, item: string | null, on: Date, cm: number, e: (typeof sizes)[number] | null) => {
        asked++;
        const c = cardOf(item, on);
        const limit = salmonTable(c).species.find((s) => s.code === "CH")?.limit ?? "";
        if (!limit.includes(`adult chinook over ${cm} cm`) || (cm !== 50 && limit.includes("adult chinook over 50 cm")))
          wrong.push(`${what}: sizes ${JSON.stringify(limit)}`);
        if ((c.adultHere ?? null) !== adultLine(e)) wrong.push(`${what}: annual line's adult ${JSON.stringify(c.adultHere)}, not ${JSON.stringify(adultLine(e))}`);
      };
      // a water's own length, on a day it holds (the first entry holding the item that day wins, as the pipeline ordered)
      for (const e of sizes.filter((x) => x.w?.length)) for (const key of e.w!) for (const item of (P.scopes?.[r]?.[key] ?? []).slice(0, 5)) {
        const d0 = e.on?.[0]?.[0] ?? 15;
        const first = sizes.find((x) => x.w?.some((k) => (P.scopes?.[r]?.[k] ?? []).includes(item))
          && (!x.on?.length || x.on.some(([a, b]) => d0 >= a! && d0 <= b!)));
        if (first !== e) continue;
        const mine = sizes.filter((x) => x.w?.some((k) => (P.scopes?.[r]?.[k] ?? []).includes(item)));
        if (mine.length !== 1) { wrong.push(`region ${r} ${item}: ${mine.length} scoped adult lengths — extend this test's oracle`); continue; }
        check(`region ${r} ${key} ${item}`, item, dateOf(d0), e.adult!.min_cm!, e);
      }
      // the region's own length, on a page naming no water
      const own = sizes.find((e) => !e.w?.length);
      if (own) check(`region ${r} (no water)`, null, dateOf(15), own.adult!.min_cm!, null);
      // the annual tier in our words, on every release cut with T869 (it carries `pr`)
      if (annual && annual.node === "tier" && P.pr !== undefined)
        expect(annual.says, `region ${r}`).toBe(`${annual.n} adult chinook a year from all fresh waters`);
    }
    expect(wrong).toEqual([]);
    expect(asked).toBeGreaterThan(5);
  });
});
