/**
 * T53 — an unnamed stream has regulations. The water screen with no item and the tapped section draws the Regulations
 * page the STORE answers for that section (`RegsTarget` kind "section": its region's base plus whatever override reaches
 * it), on the committed regs fixture release. Every line checked here is read from the store at test time — the page
 * core builds from the same store's inputs, the rule keys from the store's own rule set — never a hand copy.
 *
 * The ticket's own examples (handles 1408, 64035, 59506) are not in the committed fixture (it was cut for the golden
 * states); they run on the live release in conformance/src/regs/unnamed.release.test.ts. Here: 634 (Region 1, bound by
 * three tributary rules of named waters) and 817454 (Region 1, the base alone).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import { buildRegsPage, plain, WORDS, type RegsPage } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import { loadRegsInputs, type RegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { DEFAULT_WHO, LayoutWidthContext, loadRegsStore } from "@app/ui";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

afterEach(cleanup);

const DAY = new Date(2026, 9, 10, 12);   // Sat Oct 10
const HARRIS = "Harris Creek";
/** The fixture source, with flow pointers for the tapped reach: 634 → 9001 (unnamed) → 9002 (a named water). */
function sourceFlowing(): RegsSource {
  const base = makeFixtureSource();
  return { ...base,
    traceToGauge: async (from: SectionId) => (from === 634 ? [634, 9001, 9002] as SectionId[] : [from]),
    waterFor: async (s: SectionId) => (s === 9002 ? { item: "gnis:9" as ItemId, name: HARRIS, kind: "stream" } : base.waterFor(s)),
  };
}

/** The page core builds from the store for the section, on the same day, with the same downstream name. */
async function storePage(handle: number, downstream: string | null): Promise<{ page: RegsPage; trib: string[]; reach: string[] }> {
  const st: RegsStore = await loadRegsStore(openRegsFixture());
  const inp = await loadRegsInputs(st, { kind: "section", handle }, { month: 10, day: 10 }, "Saturday", 2026);
  const key = (r: number) => { const e = inp.rules.get(r)!.export; return `${e.entry_id}::${e.rule_id}`; };
  return { page: buildRegsPage(inp, { today: { month: 10, day: 10 }, who: DEFAULT_WHO, downstream }),
           trib: inp.ruleset!.trib.map(key), reach: inp.ruleset!.reach.map(key) };
}

async function open(handle: number, source: RegsSource = makeFixtureSource(), width: number | null = null) {
  render(<LayoutWidthContext.Provider value={width}>
           <WaterScreen source={source} item={null} section={handle as SectionId} palette={LIGHT}
                        loader={openRegsFixture()} day={DAY} />
         </LayoutWidthContext.Provider>);
  await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
  await waitFor(() => expect(within(screen.getByTestId("regs-header")).getByRole("heading").textContent).not.toBe(""));
}

describe("(T53) <WaterScreen> for an unnamed reach: the store's page for the tapped section", () => {
  it("634, bound by tributary rules: the store's name, \"Tributary of …\", and the closure the tributary rule makes", async () => {
    const { page, trib } = await storePage(634, HARRIS);
    await open(634, sourceFlowing());
    await waitFor(() => expect(screen.getByTestId("regs-place").textContent).not.toBe(""));
    const header = screen.getByTestId("regs-header");
    // the header: the store's place name (core wording), the downstream water in the label slot, no Map / Parts action
    expect([within(header).getByRole("heading").textContent, screen.getByTestId("regs-place").textContent,
            screen.queryByTestId("regs-action")])
      .toEqual([page.header.name, page.header.place!.full, null]);
    expect([page.header.name, page.header.place]).toEqual(["Unnamed stream · Region 1",
      { full: WORDS.TRIBUTARY_OF(HARRIS), short: WORDS.TRIBUTARY_OF(HARRIS) }]);
    // the banner, whole, as the store's page has it; its closure is one of the store's TRIBUTARY rules for this section
    const b = page.keep.banner!;
    expect(screen.getByTestId("keep-banner").textContent).toBe(
      b.big.join("") + b.paras.join("") + b.chain.map((c) => `${c.text} ${c.scope}${WORDS.SOURCE}${plain(c.book)}`).join(""));
    expect(b.chain.map((c) => trib.includes(c.rule))).toEqual(b.chain.map(() => true));
    expect([b.big, b.chain.map((c) => c.rule.split("::")[0])]).toEqual([["Closed all year"], ["r1:harris_creek@1-3"]]);
    // the four sections of the page, as for any water
    expect(within(screen.getByRole("tablist", { name: "Section" })).getAllByRole("tab").map((t) => t.textContent))
      .toEqual(page.header.tabs.map((t) => t.label));
  }, 30_000);

  it("817454, the region's base alone: no override, no tributary line, every rule the region's or the province's", async () => {
    const { page, trib, reach } = await storePage(817454, null);
    await open(817454);
    expect([within(screen.getByTestId("regs-header")).getByRole("heading").textContent,
            screen.getByTestId("regs-place").textContent, screen.queryByTestId("keep-banner"),
            screen.getByTestId("keep-strip").textContent?.startsWith("✓Open")])
      .toEqual(["Unnamed stream · Region 1", "", null, true]);
    expect([page.header.place, trib, reach.filter((k) => !/^z(1|p):/.test(k))]).toEqual([null, [], []]);
    // the "Where you can't fish" lines, whole, as the store's page has them
    const w = page.keep.whereNot!;
    expect(screen.getByTestId("keep-wherenot").textContent)
      .toBe(w.heading + (w.scope ?? "") + w.items.map((x) => plain(x.text)).join(""));
  }, 30_000);

  it("634 on a desktop window: the same name and \"Tributary of …\" over the side column and the page", async () => {
    const { page } = await storePage(634, HARRIS);
    await open(634, sourceFlowing(), 1400);
    await waitFor(() => expect(screen.getByTestId("regs-place").textContent).not.toBe(""));
    const side = screen.getByTestId("regs-side");
    expect([within(screen.getByTestId("regs-header")).getByRole("heading").textContent, screen.getByTestId("regs-place").textContent,
            screen.queryByTestId("regs-action"), !!within(screen.getByTestId("regs-columns")).getByTestId("regs-reading"),
            page.header.tabs.map((t) => within(side).queryAllByText(t.label).length > 0)])
      .toEqual([page.header.name, WORDS.TRIBUTARY_OF(HARRIS), null, true, [true, true, true, true]]);
  }, 30_000);
});
