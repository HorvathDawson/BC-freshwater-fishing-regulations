/**
 * (T991 round 3, #63 review) THE SOURCE ROW CITES EXACTLY WHAT THE SCREEN CITES — a probe over the fixture release, through
 * the water screen's own wiring: for several waters, parts and days (one a day only salmon may be fished, when Gear and
 * Methods draw the day's gear "if fished", not `page.gear`), every fold opened, the set of synopsis pages the row links equals
 * the set of pages the drawn tabs link ("B.C. Synopsis page N"), plus the water's own rule-book row (T474), marked "this water".
 * The review found both ways wrong: pages lost on salmon-only days, and pages listed that nothing on the screen cites.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { SalmonFacts } from "@app/core";
import type { ItemId } from "@app/data";
import { makeReleaseSource, type ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { drawnRegsOf, gearTabsFor, regsSourcesOf, type RegsPage } from "@app/core";
import { WaterScreen } from "./WaterScreen";
import { WaterFrame } from "./WaterFrame";
import { RegsScreen } from "./regs/RegsScreen";
import { GearTab } from "./regs/GearTab";
import { MethodsTab } from "./regs/MethodsTab";
import { LicenceTab } from "./regs/LicenceTab";
import vedder1010 from "./regs/fixtures/vedder_1010.json";
import glenn615 from "./regs/fixtures/glenn_615.json";
import { LIGHT } from "./theme";
import { TEST_TROUT_SOURCES } from "./regs/testing/sources";

afterEach(cleanup);
const LOADED = { timeout: 20_000 } as const;
const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
/** Every section of the fixture on one DFO chain, open to salmon Sep 1–Dec 31 (as WaterScreen.salmon.test.tsx). */
const FACTS = {
  handles: "0".repeat(16), chains: 2, chainOf: () => 1,
  payload: {
    format: "salmon/1", credit: "Fisheries and Oceans Canada (DFO)", default: 0,
    regions: { "2": { name: "Lower Fraser", asof: "2026-08-28", url: "https://example/2", shared: [], sizes: [] } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "open", t: "4 per day (Sep 1–Dec 31)" }],
    gears: [],
    chains: [{ r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
             { r: ["2"], w: "Chilliwack River", l: [], sp: all(1), g: [0, -1], m: [0, 3] }],
    unmapped: [],
  },
} as unknown as SalmonFacts;
function withSalmon(): ReleaseLoader {
  const live = openRegsFixture();
  return Object.assign(Object.create(Object.getPrototypeOf(live)), live, {
    manifest: { ...live.manifest, files: { ...live.manifest.files, salmon: { url: "salmon.bin.gz" } } },
    salmon: async () => FACTS, method: async () => null, access: async () => null,
  });
}

/** Open every fold of the page (a fold's lines are on the tab, one tap away), until none is left closed. */
function openFolds(root: HTMLElement) {
  for (let round = 0; round < 12; round++) {
    const closed = [...root.querySelectorAll('[aria-expanded="false"]')].filter((e) => !e.closest('[data-testid="regs-sources"]'));
    if (!closed.length) return;
    act(() => { for (const e of closed) fireEvent.click(e); });
  }
}
const pagesOf = (els: Element[], re: RegExp) =>
  els.flatMap((e) => { const m = re.exec(e.getAttribute("aria-label") ?? ""); return m ? [Number(m[1])] : []; });

/** The probe: the row's pages, the pages the drawn tabs link, and the water's own (marked) pages. */
function probe() {
  const content = screen.getByTestId("regs-content"), row = screen.getByTestId("regs-sources");
  openFolds(content);
  // (#69 review) the pages the screen DRAWS: rule text's links and the blocks' own lines — never a Source line counting
  // itself in (the trout table's line is drawn from the table, so its links are not evidence of what the table says)
  const links = [...content.querySelectorAll("[role=link]")].filter((e) => !e.closest("[data-testid=trout-source]"));
  const drawn = new Set(pagesOf(links.filter((e) => !row.contains(e)), /^B\.C\. Synopsis page (\d+),/));
  const inRow = [...row.querySelectorAll("[role=link]")];
  const listed = pagesOf(inRow, /^B\.C\. Synopsis page (\d+),/);
  const own = pagesOf(inRow, /^B\.C\. Synopsis page (\d+), this water's own entry/);
  return { listed: [...listed].sort((a, b) => a - b), expected: [...new Set([...drawn, ...own])].sort((a, b) => a - b), own, drawn };
}

type Case = { item: string; part: number; day: [number, number]; salmon?: true };
const CASES: Case[] = [
  { item: "gnis:8634", part: 3, day: [9, 10], salmon: true },
  { item: "gnis:8634", part: 3, day: [9, 10] },
  { item: "gnis:8634", part: 0, day: [2, 15], salmon: true },
  { item: "gnis:2936", part: 11, day: [7, 15] },
  { item: "gnis:2936", part: 11, day: [10, 22] },
  { item: "gnis:2936", part: 5, day: [4, 1] },
  { item: "gnis:14619", part: 1, day: [8, 1] },
  { item: "gnis:12227", part: 2, day: [5, 20] },
  { item: "gnis:14097", part: 3, day: [0, 10] },
];

describe("the Regulations Source row cites exactly what the drawn screen cites (#63 review)", () => {
  it.each(CASES)("$item part $part, month/day $day, salmon feed $salmon", async (c) => {
    const loader = c.salmon ? withSalmon() : openRegsFixture();
    render(<WaterScreen source={makeReleaseSource(loader)} item={c.item as ItemId} palette={LIGHT} part={c.part}
                        loader={loader} day={new Date(2026, c.day[0], c.day[1], 12)} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
    if (c.salmon) await waitFor(() => expect(screen.getByTestId("salmon-card")).toBeTruthy(), LOADED);
    const p = probe();
    expect(p.listed).toEqual(p.expected);
    expect(p.listed.length).toBeGreaterThan(0);
  }, 40_000);
});

/**
 * (T1031 item 3) THE TROUT TABLE'S SOURCE LINE, WIRED FOR REAL: WaterScreen computes it (`troutSourcesOf`) beside the foot
 * row, RegsScreen hands it to KeepTab, KeepTab to the card. (#69 review) It cites what the table draws — no page: on the
 * Vedder (the fixture release, Oct 10) its every-rule sheet holds p.6 (the province's steelhead quota and record duty), which
 * the table never says, so neither the table's line nor the foot row lists p.6 — "Who says so ›" opens the sheet.
 */
describe("the trout table's Source line on a real page (T1031 item 3)", () => {
  it("the Vedder's table: Who says so › alone, opening src:trout; the foot row cites no page the screen does not draw", async () => {
    const loader = openRegsFixture();
    render(<WaterScreen source={makeReleaseSource(loader)} item={"gnis:3062" as ItemId} palette={LIGHT} part={0}
                        loader={loader} day={new Date(2026, 9, 10, 12)} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
    openFolds(screen.getByTestId("regs-content"));
    const line = await waitFor(() => screen.getByTestId("trout-source"), LOADED);
    expect([line.textContent, screen.getByTestId("trout-sources").getAttribute("aria-label")])
      .toEqual(["SourceWho says so ›", "Who says so: the rules behind Trout and char"]);
    const p = probe();
    expect(p.listed).toEqual(p.expected);
    expect(p.listed.includes(6)).toBe(false);
  }, 40_000);
});

/**
 * A DAY ONLY SALMON MAY BE FISHED (the review's gnis:2308 Jan–Jun, gnis:27804, gnis:10061 Apr–Sep): game fish closed, salmon
 * open, so Gear / Methods draw the day's gear "if fished" (`gearTabsFor`), never `page.gear`. The fixture release has no
 * such day (every closed-to-game-fish part there is closed to all fishing, salmon too: T796), so the day is built as
 * WaterScreen builds it — `gearTabsFor` over a closed day's page and another day's open gear — and drawn by RegsScreen.
 */
describe("a day only salmon may be fished: the row cites the Gear drawn, not the closed day's (#63 review, defect 1)", () => {
  it("the probe holds, and the drawn gear's pages are in the row while the closed day's own gear pages are not", () => {
    const open = vedder1010 as unknown as RegsPage, closedDay = glenn615 as unknown as RegsPage;
    const page = { ...closedDay, gear: { ...closedDay.gear!, closed: "Closed to fishing for game fish" } } as RegsPage;
    const tabs = gearTabsFor(page, { gear: open.gear, methods: open.methods }, { status: "open" } as never, null);
    expect(tabs.gear).toBe(open.gear);
    const sources = regsSourcesOf(drawnRegsOf(page, tabs, page.licence, []), { waterPages: [], inSeason: false, dfo: false,
      rules: { get: () => { throw new Error("no rule index: a frozen page"); }, has: () => false, byKey: () => null } });
    render(<WaterFrame palette={LIGHT} face="regulations">
             <RegsScreen troutSources={TEST_TROUT_SOURCES} page={page} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} } as never} onDate={() => {}} sources={sources}
                         gear={(a) => <GearTab vm={tabs.gear!} palette={LIGHT} actions={a} salmonGear={tabs.salmonGear} />}
                         methods={(a) => <MethodsTab vm={tabs.methods!} palette={LIGHT} actions={a} boats={page.boats} />}
                         licence={page.licence ? (a) => <LicenceTab vm={page.licence!} who={page.who} palette={LIGHT} actions={a} onWho={() => {}} /> : undefined} />
           </WaterFrame>);
    const p = probe();
    expect(p.listed).toEqual(p.expected);
    // vedder's gear cites p.21 and p.22 (drawn); glenn's own closed-day gear cites p.34 and p.36 (not drawn)
    expect([p.listed.includes(21), p.listed.includes(22), p.drawn.has(34), p.listed.includes(34)]).toEqual([true, true, false, false]);
  });
});
