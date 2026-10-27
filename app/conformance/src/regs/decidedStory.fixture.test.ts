/**
 * T216: "How this was decided" for an angler — the rules that applied, in layers (base → this water → exceptions), what
 * each did today and why, each step with its printed page. Pinned on real cases: on the committed regs fixture release
 * the Fraser's two-region part, a sub-cap (the Bella Coola) and a spring closure lifted by a listed water's row (Glenn
 * Creek, Region 4); on the live release (skipped by name without one) a listed Region 5 stream whose row lifts the
 * spring closure (Williams Lake River) and a closed water (Chimney Creek above Brunson Lake in the spring closure).
 */
import { describe, expect, it } from "vitest";
import { buildModel, FISH, makePlace, plain, plainRowOf, plainRowSeams, settle, type DecidedStoryVM, type PlainRowVM } from "@app/core";
import { loadRegsInputs, openRegsStore, type RegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

async function rowsOf(st: RegsStore, item: string, partIx: number, month: number, day: number): Promise<PlainRowVM[]> {
  const inp = await loadRegsInputs(st, { kind: "part", item, partIx }, { month, day }, "Saturday", 2026);
  const place = makePlace(inp), settled = settle(inp, place), card = buildModel(inp, place, settled), seams = plainRowSeams(inp, FISH);
  return card.rows.map((_, i) => plainRowOf(i, { inp, card, settled, seams }));
}
const storyOf = (rows: PlainRowVM[], title: RegExp): DecidedStoryVM => rows.find((r) => title.test(r.title))!.ladder.story!;
/** The layer heads, in order. */
const heads = (s: DecidedStoryVM) => s.layers.map((l) => l.head);
/** Every step as "<says> <pages> | <verdict> | <why>". */
const steps = (s: DecidedStoryVM, head?: RegExp) => s.layers.filter((l) => !head || head.test(l.head))
  .flatMap((l) => l.steps.map((t) => `${plain(t.says)} ${plain(t.book)} | ${t.verdict} | ${t.why}`));
const byOf = (s: DecidedStoryVM, says: RegExp) => plain(s.layers.flatMap((l) => l.steps).find((t) => says.test(plain(t.says)))!.by);

describe("T216 on the fixture release", () => {
  it("the Fraser, part 14 (Regions 3 and 5), Oct 10: both regions' bases, the river's own closure decides, each region's limit overridden by it", async () => {
    const rows = await rowsOf(await openRegsStore(openRegsFixture()), "gnis:39325", 14, 10, 10);
    const tc = storyOf(rows, /^Brook trout/);
    expect(tc.notes).toEqual(["This stretch is in Region 3 and Region 5, so both regions’ rules apply here."]);
    expect(heads(tc)).toEqual(["All of B.C.: the province’s base rules", "Region 3: the region’s base rules", "Region 5: the region’s base rules",
      "Fraser River: this water’s own rules (Region 3 table)", "Fraser River: this water’s own rules (Region 5 table)"]);
    expect(steps(tc, /Region 3 table/)).toEqual(["No fishing for trout and char, Oct 1–31. Rule book p.30 ↗ | Decides it | "
      + "A closure decides it: a closure beats any limit that would let you keep fish."]);
    expect(steps(tc, /Region 5 table/)).toEqual(["No fishing for trout and char, Oct 1–31. Rule book p.45 ↗ | Says the same | "
      + "Fraser River’s own rule says the same, so nothing changes."]);
    expect(steps(tc, /^Region 5/)[1]).toBe("Keep up to 5 trout and char a day, all kinds together. Rule book p.42 ↗ | Overridden | "
      + "Fraser River’s own rule closes it, and a closure beats an opening.");
    expect(byOf(tc, /^Keep up to 5 trout and char/)).toBe("The rule that beat it: No fishing for trout and char, Oct 1–31. Rule book p.30 ↗");
    expect(tc.answer).toBe("Today’s answer: Closed here.");
    // two regions, same rank: the stricter limit wins (Region 3's 2 burbot over Region 5's 5)
    const bb = storyOf(rows, /^Burbot/);
    expect(steps(bb, /^Region/)).toEqual([
      "Keep up to 2 burbot a day. Rule book p.28 ↗ | Decides it | Nothing on this water changes it, so the base rule stands.",
      "Possession: 2 days’ quota. Fish at home don’t count. Rule book p.28 ↗ | Also applies | How many you may have at once, counting fish kept on earlier days.",
      "Possession: 2 days’ quota. Fish at home don’t count. Rule book p.42 ↗ | Also applies | How many you may have at once, counting fish kept on earlier days.",
      "Keep up to 5 burbot a day. Rule book p.42 ↗ | Overridden | The Region 3 rule is stricter, and the stricter rule wins."]);
  });

  it("a sub-cap: the Bella Coola, part 1, May 1 — the region's 5 lowered to 2 in streams, smaller caps inside it, the river's own row decides", async () => {
    const s = storyOf(await rowsOf(await openRegsStore(openRegsFixture()), "gnis:11611", 1, 5, 1), /^Trout and char/);
    expect(heads(s)).toEqual(["All of B.C.: the province’s base rules", "Region 5: the region’s base rules", "Bella Coola River: this water’s own rules"]);
    expect(steps(s, /^Region 5/).slice(0, 4).map((x) => x.split(" | ").slice(1).join(" | "))).toEqual([
      "Also applies | The overall daily limit: these fish count toward it.",
      "Lowers the limit | A lower limit for this kind of water. When two limits apply, the stricter one wins.",
      "Caps some fish | A smaller cap inside the daily limit: these fish count toward the total, but no more than this.",
      "Caps some fish | A smaller cap inside the daily limit: these fish count toward the total, but no more than this."]);
    expect(steps(s, /^Region 5/)[3]).toMatch(/^Keep up to 1 Dolly Varden\/bull trout a day\. Rule book p\.42 ↗/);
    expect(steps(s, /^Bella Coola/)[0]).toBe("Keep up to 2 cutthroat trout, Dolly Varden/bull trout, brook trout, brown trout, lake trout and rainbow "
      + "trout a day, all kinds together, 25 cm (10 in) or longer, Apr 1–May 31. Rule book p.43 ↗ | Decides it | "
      + "This water’s own rule sets it, and a water’s own rule beats the region’s.");
    expect(s.answer).toBe("Today’s answer: 2 a day here.");
  });

  it("a spring closure lifted by a listed water's row: Glenn Creek (Region 4), May 1", async () => {
    const s = storyOf(await rowsOf(await openRegsStore(openRegsFixture()), "gnis:10457", 0, 5, 1), /^Rainbow trout/);
    const lift = s.layers.flatMap((l) => l.steps).find((t) => /no fishing, Apr 1–Jun 14/.test(plain(t.says)))!;
    expect([lift.verdict, lift.why, plain(lift.book)]).toEqual(["Lifted here",
      "Duncan River’s own rule opens it here: a water’s own rule beats the region’s.", "Rule book p.34 ↗"]);
    expect(plain(lift.by)).toMatch(/^The rule that opens it: .* Rule book p\.36 ↗$/);
  });

  it("every step of every row of every fixture case cites a printed page; every layer has a step", async () => {
    const st = await openRegsStore(openRegsFixture());
    for (const [item, partIx, m, d] of [["gnis:39325", 14, 10, 10], ["gnis:11611", 1, 5, 1], ["gnis:10457", 0, 5, 1], ["gnis:3062", 0, 5, 15]] as const) {
      for (const r of await rowsOf(st, item, partIx, m, d)) {
        const s = r.ladder.story!;
        for (const l of s.layers) {
          expect(l.steps.length, `${item} ${r.title} ${l.head}`).toBeGreaterThan(0);
          for (const t of l.steps) expect(t.book.some((x) => x.t === "book"), `${item} ${r.title} ${plain(t.says)}`).toBe(true);
        }
      }
    }
  });
});

const LIVE = liveRelease();
describe.skipIf(!LIVE)(`T216 on the live release (${LIVE ?? "absent"})`, () => {
  it("a listed Region 5 stream: Williams Lake River, May 15 — its own row lifts the spring closure", async () => {
    const s = storyOf(await rowsOf(await openRegsStore(openReleaseDir(LIVE!)), "gnis:27764", 0, 5, 15), /^Trout and char/);
    expect(heads(s)).toEqual(["All of B.C.: the province’s base rules", "Region 5: the region’s base rules",
      "Fraser and Thompson watersheds in Region 5: rules for this area of the region"]);
    const [lift] = s.layers[2]!.steps;
    expect([plain(lift!.says), lift!.verdict, lift!.why, plain(lift!.book), plain(lift!.by)]).toEqual([
      "No fishing in streams, Apr 1-Jun 30: Fraser and Thompson watersheds in Region 5, except the Fraser mainstem", "Lifted here",
      "Williams Lake River’s own rule opens it here: a water’s own rule beats the region’s.", "Rule book p.42 ↗",
      "The rule that opens it: Spring stream closure lifted Rule book p.47 ↗"]);
    expect(s.answer).toBe("Today’s answer: 2 a day here, and they count toward 5 a day in Region 5.");
  });

  it("a closed water: Chimney Creek above Brunson Lake, May 15 — the spring closure decides; the region's limits lose to it", async () => {
    const s = storyOf(await rowsOf(await openRegsStore(openReleaseDir(LIVE!)), "gnis:39269", 0, 5, 15), /^Brook trout/);
    expect(steps(s, /^Fraser and Thompson/)).toEqual(["No fishing in streams, Apr 1-Jun 30: Fraser and Thompson watersheds in Region 5, "
      + "except the Fraser mainstem Rule book p.42 ↗ | Decides it | A closure decides it: a closure beats any limit that would let you keep fish."]);
    expect(steps(s, /^Region 5/)).toContain("Keep up to 5 burbot a day. Rule book p.42 ↗ | Overridden | "
      + "The rule for Fraser and Thompson watersheds in Region 5 closes it, and a closure beats an opening.");
    expect(s.answer).toBe("Today’s answer: Closed here.");
  });
});
