/**
 * T190: on a piece in two regions (T183) the "How this was decided" ladder heads each region's rules with THAT region.
 * The golden cannot hold it (it predates T183; x15p14 and x27p1 are `gone` in taps.json), so it is pinned on the committed
 * regs fixture release, on the two real straddlers the ticket names.
 */
import { describe, expect, it } from "vitest";
import { buildModel, FISH, makePlace, plain, plainRowOf, plainRowSeams, settle, type PlainRowVM } from "@app/core";
import { loadRegsInputs, openRegsStore } from "@app/data/regs";
import { openRegsFixture, sectionNames } from "@app/data/release/node";

/** Every row of a part, through the plain-row presenter, Oct 10 2026 (the golden's day). */
async function rowsOf(item: string, ends: readonly [string, string]): Promise<PlainRowVM[]> {
  // (T667) the part by its ENDS (the tokens of its two ends), never its number: one more cut below it renumbers it
  const partIx = await sectionNames(openRegsFixture()).part(item, ends[0], ends[1]);
  const st = await openRegsStore(openRegsFixture());
  const inp = await loadRegsInputs(st, { kind: "part", item, partIx }, { month: 10, day: 10 }, "Saturday", 2026);
  const place = makePlace(inp), settled = settle(inp, place), card = buildModel(inp, place, settled), seams = plainRowSeams(inp, FISH);
  return card.rows.map((_, i) => plainRowOf(i, { inp, card, settled, seams }));
}
/** A row's ladder as "<question> | <step heading> | <lines>". */
const ladder = (r: PlainRowVM) => r.ladder.sections.flatMap((s) => s.steps.map((st) =>
  `${s.q} | ${st.level} | ${st.lines.map((l) => plain(l.text)).join(" // ")}`));

/** the two straddlers' parts: the Fraser from Region 5's line up to Region 3's; the West Road from Region 5's up to 7A's */
const FRASER_R5_R3 = ["region_line:5", "region_line:3"] as const, WEST_ROAD_R5_ZA = ["region_line:5", "region_line:7A"] as const;

describe("T190: a two-region piece's ladder names each region over its own rules", () => {
  // (T278, regs fixture on r-62e2f3a8c81e) the Fraser's tidal cut at the CPR Bridge at Mission took its old part 3: the Regions 3 and 5 part is 13 now (was 14)
  // (T517, regs fixture r-fc17c4373686) part_ix is river order: "Region 5 boundary → Region 3 boundary" is part 14
  it("the Fraser, part 14 (Regions 3 and 5): Region 5's possession line is under Region 5, not under Region 3", async () => {
    const tc = (await rowsOf("gnis:39325", FRASER_R5_R3)).find((r) => /^Brook trout/.test(r.title))!;
    expect(ladder(tc)).toEqual([
      "What you can keep | Fraser River | No fishing for trout and char, Oct 1–31.",
      "Other rules | Region 3 | Possession: 2 days’ quota. Fish at home don’t count.",
      "Other rules | Region 5 | Possession: 2 days’ quota. Fish at home don’t count."]);
  });

  it("every row of both straddlers: no region heading repeats within a question, and no heading is another region's", async () => {
    for (const [item, ends] of [["gnis:39325", FRASER_R5_R3], ["gnis:26104", WEST_ROAD_R5_ZA]] as const) {
      for (const r of await rowsOf(item, ends)) {
        for (const s of r.ladder.sections) {
          const heads = s.steps.map((st) => st.level).filter((h) => /^Region /.test(h));
          expect(heads, `${item} ${ends.join(" → ")} ${r.title} ${s.q}`).toEqual([...new Set(heads)]);
        }
      }
    }
  });

  // (T517) part_ix is river order: the West Road's Region 5 / Zone A straddler is part 1 (was 3)
  it("the West Road, part 1: its Region 5 rules under Region 5, Region 6's under its zone", async () => {
    const tc = (await rowsOf("gnis:26104", WEST_ROAD_R5_ZA)).find((r) => r.title === "Trout and char")!;
    expect(ladder(tc).filter((x) => x.startsWith("Other rules"))).toEqual([
      "Other rules | Region 5 | Possession: 2 days’ quota. Fish at home don’t count.",
      "Other rules | Zone A | Possession: 2 days’ quota for cutthroat trout, brook trout, brown trout, rainbow trout and steelhead. "
        + "Fish at home don’t count. // Possession: the daily quota for Dolly Varden/bull trout. Fish at home don’t count. "
        + "// Possession: the daily quota for lake trout. Fish at home don’t count."]);
  });
});
