/**
 * (#63 re-review) THE WATER SCREEN'S WIRING ON A DAY ONLY SALMON MAY BE FISHED: game fish closed, the salmon feed open, so
 * Gear / Methods / Licence draw the day's "if fished" tabs (`gearTabsFor` / `licenceTabFor`), and the Source row must be
 * built from THOSE (`drawnRegsOf(page, tabs, licence)` in WaterScreen's `shown`), never from `page.gear` / `page.methods` /
 * `page.licence`. The fixture release has no such day (every part closed to game fish there is closed to salmon too, T796:
 * the salmon bar reads the book's closure), so the salmon feed's bar is stubbed OPEN for the day: `salmonStripOf` answers
 * "open" — the stubbed loader's feed — and everything else is the app's own wiring. (T1036) The every-source sheet behind
 * the row's "All sources ›" is built from the same drawn tabs: its rows' pages are the drawn ones (and the drawn trout
 * table's), so the "if fished" gear's pages are in it and the closed day's undrawn rules are not.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { SalmonFacts } from "@app/core";
import type { ItemId } from "@app/data";
import { makeReleaseSource, type ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";

/** (T1036) what the screen hands the Source row: the drawn view models and the part's rule index (the sheet's keys' oracle) */
const handed = vi.hoisted(() => ({ drawn: null as unknown, rules: null as unknown }));
vi.mock("@app/core", async (orig) => {
  const m = await orig<typeof import("@app/core")>();
  return { ...m, salmonStripOf: (...a: Parameters<typeof m.salmonStripOf>) => {
    const s = m.salmonStripOf(...a);
    return s && { ...s, status: "open" as const, by: "dfo" as const };
  }, regsSourcesOf: (...a: Parameters<typeof m.regsSourcesOf>) => {
    handed.drawn = a[0]; handed.rules = a[1].rules;
    return m.regsSourcesOf(...a);
  } };
});
const { WaterScreen } = await import("./WaterScreen");
const { LIGHT } = await import("./theme");
const { expectedKeys, sheetExpected, sheetPages } = await import("./regs/testing/allSources");
type ProbeIndex = import("./regs/testing/allSources").ProbeIndex;

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
function withSalmon(facts: SalmonFacts = FACTS): ReleaseLoader {
  const live = openRegsFixture();
  return Object.assign(Object.create(Object.getPrototypeOf(live)), live, {
    manifest: { ...live.manifest, files: { ...live.manifest.files, salmon: { url: "salmon.bin.gz" } } },
    salmon: async () => facts, method: async () => null, access: async () => null,
  });
}
/** (T1031, #69 re-review) the same chain closed every day by a BOOK closure (a `book:` place, T796): its page, p.77, is a
 *  page no other block of the Chilliwack's page cites, so only the salmon card's own Source line can bring it to the row. */
const BOOK = "book:zp:superior_closures::superior_closures.r3";
const FACTS_BOOK = { ...FACTS, payload: {
  ...FACTS.payload,
  verdicts: [...FACTS.payload.verdicts, { s: "closed", t: "Closed to all fishing by the B.C. Synopsis (p.77): No Fishing" }],
  chains: [FACTS.payload.chains[0]!, { r: ["2"], w: "Chilliwack River", l: [BOOK], sp: all(2), g: [0, -1], m: [0, 3], a: all(0) }],
  p: { [BOOK]: { water: "Ecological Reserves", static: 1, book: { rule: "zp:superior_closures::superior_closures.r3", pages: [77] } } },
  static: { [BOOK]: { sp: all(2), g: [0, -1] } },
} } as unknown as SalmonFacts;

/** (T1036) a book closure whose rule nothing else on the Chilliwack's page cites: Region 4's spring stream closure (p.34),
 *  its place carrying the rule's own page, as a release's places do */
const BOOK_R4 = "book:z4:spring_stream_closure::spring_stream_closure.r1";
const FACTS_R4 = { ...FACTS, payload: {
  ...FACTS.payload,
  verdicts: [...FACTS.payload.verdicts, { s: "closed", t: "Closed to all fishing by the B.C. Synopsis (p.34): No Fishing" }],
  chains: [FACTS.payload.chains[0]!, { r: ["2"], w: "Chilliwack River", l: [BOOK_R4], sp: all(2), g: [0, -1], m: [0, 3], a: all(0) }],
  p: { [BOOK_R4]: { water: "Every stream in Region 4", static: 1, book: { rule: "z4:spring_stream_closure::spring_stream_closure.r1", pages: [34] } } },
  static: { [BOOK_R4]: { sp: all(2), g: [0, -1] } },
} } as unknown as SalmonFacts;

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
  // (T1031) the pages the screen draws: rule text's links ("B.C. Synopsis page N, …") and the blocks' own lines, never the
  // trout table's line counting itself in
  const links = [...content.querySelectorAll("[role=link]")].filter((e) => !e.closest("[data-testid=trout-source]"));
  const drawn = new Set(pagesOf(links.filter((e) => !row.contains(e)), /^B\.C\. Synopsis page (\d+),/));
  const inRow = [...row.querySelectorAll("[role=link]")];
  const listed = pagesOf(inRow, /^B\.C\. Synopsis page (\d+),/);
  const own = pagesOf(inRow, /^B\.C\. Synopsis page (\d+), this water's own entry/);
  return { listed: [...listed].sort((a, b) => a - b), expected: [...new Set([...drawn, ...own])].sort((a, b) => a - b), own, drawn };
}


describe("a day only salmon may be fished, through WaterScreen (#63 re-review)", () => {
  it("the Chilliwack's closed part on Oct 10 with the salmon feed open: Gear drawn \"if fished\", and the row cites exactly the drawn tabs", async () => {
    const loader = withSalmon();
    render(<WaterScreen source={makeReleaseSource(loader)} item={"gnis:8634" as ItemId} palette={LIGHT} part={3}
                        loader={loader} day={new Date(2026, 9, 10, 12)} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
    await waitFor(() => expect(screen.getByTestId("salmon-card")).toBeTruthy(), LOADED);
    // the day the review found pages lost: game fish closed, yet Gear is drawn (the salmon day's gear)
    expect(screen.getByTestId("keep-strip").textContent).toMatch(/Game fish.{0,6}✕Closed/);
    await waitFor(() => expect(screen.getByTestId("regs-gear")).toBeTruthy(), LOADED);
    const p = probe();
    expect(p.listed).toEqual(p.expected);
    expect(p.listed).toEqual([8, 9, 21, 22]);
    // (T1036) the sheet: the drawn "if fished" gear's p.8 and p.21 are in it
    const sheet = sheetPages({ act, fireEvent, screen });
    expect(sheet.all).toEqual(sheetExpected(p.drawn, sheet.trout));
    expect([sheet.all.includes(8), sheet.all.includes(21)]).toEqual([true, true]);
    expect(sheet.keys).toEqual(expectedKeys(handed.drawn, handed.rules as ProbeIndex));
  }, 40_000);

  it("(T1031) a salmon BOOK closure's page, drawn on the card's own Source line, is in the foot row too", async () => {
    const loader = withSalmon(FACTS_BOOK);
    render(<WaterScreen source={makeReleaseSource(loader)} item={"gnis:8634" as ItemId} palette={LIGHT} part={3}
                        loader={loader} day={new Date(2026, 9, 10, 12)} />);
    await waitFor(() => expect(screen.getByTestId("salmon-card")).toBeTruthy(), LOADED);
    const p = probe();
    const card = pagesOf([...screen.getByTestId("salmon-book-pages").querySelectorAll("[role=link]")], /^B\.C\. Synopsis page (\d+),/);
    expect(card).toEqual([77]);
    expect(p.drawn.has(77) && p.listed.includes(77)).toBe(true);
    expect(p.listed).toEqual(p.expected);
    // (T1036) the every-source sheet: the drawn pages, but for the card's p.77 — the hand-made place above names p.77 for
    // a rule the book prints on p.9 (drawn on the closed banner here), and a sheet row is the rule's own, its pages the
    // rule's (a release's places carry their rule's own pages). Listing the closure's rule is pinned in allsources.test.ts
    const sheet = sheetPages({ act, fireEvent, screen });
    expect(sheet.all).toEqual(sheetExpected([...p.drawn].filter((x) => x !== 77), sheet.trout));
    expect(sheet.keys).toEqual(expectedKeys(handed.drawn, handed.rules as ProbeIndex));
  }, 40_000);

  it("(T1036) a book closure's rule that nothing else on the screen cites is in the every-source sheet, its page with it", async () => {
    const loader = withSalmon(FACTS_R4);
    render(<WaterScreen source={makeReleaseSource(loader)} item={"gnis:8634" as ItemId} palette={LIGHT} part={3}
                        loader={loader} day={new Date(2026, 9, 10, 12)} />);
    await waitFor(() => expect(screen.getByTestId("salmon-card")).toBeTruthy(), LOADED);
    const p = probe();
    const sheet = sheetPages({ act, fireEvent, screen });
    const rule = "z4:spring_stream_closure::spring_stream_closure.r1";
    // cited only by the card's closure line: not among the drawn tabs' rules, yet listed, and p.34 with it
    expect([expectedKeys(handed.drawn, handed.rules as ProbeIndex).includes(rule), sheet.keys.includes(rule), p.drawn.has(34)]).toEqual([false, true, true]);
    expect([sheet.all, sheet.keys]).toEqual([sheetExpected(p.drawn, sheet.trout), [...expectedKeys(handed.drawn, handed.rules as ProbeIndex), rule].sort()]);
  }, 40_000);
});
