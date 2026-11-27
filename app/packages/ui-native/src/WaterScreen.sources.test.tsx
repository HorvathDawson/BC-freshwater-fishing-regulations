/**
 * (T991 round 3, #63 review) THE SOURCE ROW CITES EXACTLY WHAT THE SCREEN CITES — a probe over the fixture release, through
 * the water screen's own wiring: for several waters, parts and days (one a day only salmon may be fished, when Gear and
 * Methods draw the day's gear "if fished", not `page.gear`), every fold opened, the set of synopsis pages the row links equals
 * the set of pages the drawn tabs link ("B.C. Synopsis page N"), plus the water's own rule-book row (T474), marked "this water".
 * The review found both ways wrong: pages lost on salmon-only days, and pages listed that nothing on the screen cites.
 * (T1036) The every-source sheet behind the row's "All sources ›" had the same two faults: on the same cases its rows' pages
 * equal the drawn tabs' pages and the drawn trout table's (its line's "Who says so ›" opens them), nothing else.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { SalmonFacts } from "@app/core";
import type { ItemId } from "@app/data";
import { makeReleaseSource, type ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { drawnRegsOf, gearTabsFor, reachableSheets, regsSourcesOf, type RegsPage, type SalmonCardVM } from "@app/core";
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
import { expectedKeys, expectedOtherKeys, pressedSheets, sheetExpected, sheetPages, type ProbeIndex } from "./regs/testing/allSources";
import { setFailureSink } from "@app/core";

/** (T1036) what the screen hands the Source row: the drawn view models and the part's rule index (the sheet's keys' oracle) */
const handed = vi.hoisted(() => ({ drawn: null as unknown, rules: null as unknown,
  /** (T1036) the page's own sheets, as the screen hands them to the every-source sheet */
  sheet: null as unknown as (k: string) => never,
  /** (review of PR 126) the salmon card handed beside them (its rows open sp: sheets) */
  salmon: null as unknown,
  /** the sheets the screen itself reads to draw an open sheet (RegsScreen's host page: `withSalmonSheets`), and the keys
   *  it was asked for since the last press (the press oracle, `pressedSheets`) */
  host: null as unknown as (k: string) => never, opened: { keys: [] as string[] } }));
vi.mock("@app/core", async (orig) => {
  const m = await orig<typeof import("@app/core")>();
  return { ...m, regsSourcesOf: (...a: Parameters<typeof m.regsSourcesOf>) => {
    handed.drawn = a[0]; handed.rules = a[1].rules;
    return m.regsSourcesOf(...a);
  }, withDrawnSources: (...a: Parameters<typeof m.withDrawnSources>) => {
    handed.sheet = a[0].sheet as never; handed.salmon = a[4] ?? null;
    return m.withDrawnSources(...a);
  }, withSalmonSheets: (...a: Parameters<typeof m.withSalmonSheets>) => {
    const f = m.withSalmonSheets(...a);
    handed.host = f as never;
    return (k: Parameters<typeof f>[0]) => { handed.opened.keys.push(k); return f(k); };
  } };
});
// (T1036, W46) a rule a drawn line cites that the index lacks is reported: none on these pages
let missing: string[] = [], undoSink = () => {};
beforeEach(() => { missing = []; undoSink = setFailureSink((e) => { if ((e as Error).name === "AllSourcesRuleMissing") missing.push((e as Error).message); }); });
afterEach(() => undoSink());

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

/** `bare`: (T1036) no sheet one tap away cites a rule the drawn lines do not (the second group is not drawn) */
type Case = { item: string; part: number; day: [number, number]; salmon?: true; bare?: true };
const CASES: Case[] = [
  { item: "gnis:8634", part: 3, day: [9, 10], salmon: true, bare: true },
  { item: "gnis:8634", part: 3, day: [9, 10], bare: true },
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
    // (T1036) the every-source sheet: the drawn tabs' pages and the drawn trout table's, nothing else
    const sheet = sheetPages({ act, fireEvent, screen });
    expect(sheet.all).toEqual(sheetExpected(p.drawn, sheet.trout));
    // and its rules: exactly those the drawn view models cite (a missed rule that shares a page, or has none, fails here)
    expect([sheet.keys, missing]).toEqual([expectedKeys(handed.drawn, handed.rules as ProbeIndex), []]);
    expect(sheet.keys.length).toBeGreaterThan(0);
    // (T1036, owner 2026-11-24 "Add back but maybe have them separate?") the second group, under its one head: what the
    // sheets one tap from the drawn screen cite (a fish row's, Possession's), none of the first group's
    // (review of PR 126) the sheets one tap away, read by pressing every press target of the screen, not by core's walk:
    // core's list is exactly them: (T1077) every Source sheet it lists is one a drawn Source line opens (the bait list's
    // own, which no line draws since T900, is not listed)
    const pressed = pressedSheets({ act, fireEvent, screen }, () => document.body, handed.opened);
    const core: string[] = reachableSheets(handed.drawn as unknown[], handed.salmon as SalmonCardVM | null);
    expect([pressed.filter((k) => !core.includes(k)), core.filter((k) => !pressed.includes(k))]).toEqual([[], []]);
    expect([sheet.other, sheet.heads]).toEqual([expectedOtherKeys(pressed, handed.host, handed.rules as ProbeIndex, sheet.keys), c.bare ? 0 : 1]);
    expect([sheet.other.length > 0, sheet.other.filter((k) => sheet.keys.includes(k))]).toEqual([!c.bare, []]);
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
    // (T1036) the every-source sheet lists the table's rules (p.6 among them: "Who says so ›" opens them) and the drawn tabs'
    const sheet = sheetPages({ act, fireEvent, screen });
    expect([sheet.trout.includes(6), sheet.all]).toEqual([true, sheetExpected(p.drawn, sheet.trout)]);
    expect([sheet.keys, missing]).toEqual([expectedKeys(handed.drawn, handed.rules as ProbeIndex), []]);
    expect(sheet.other).toEqual(expectedOtherKeys(pressedSheets({ act, fireEvent, screen }, () => document.body, handed.opened),
                                                  handed.host, handed.rules as ProbeIndex, sheet.keys));
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
