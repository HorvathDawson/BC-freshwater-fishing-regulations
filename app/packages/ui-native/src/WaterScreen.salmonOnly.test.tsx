/**
 * (#63 re-review) THE WATER SCREEN'S WIRING ON A DAY ONLY SALMON MAY BE FISHED: game fish closed, the salmon feed open, so
 * Gear / Methods / Licence draw the day's "if fished" tabs (`gearTabsFor` / `licenceTabFor`), and the Source row must be
 * built from THOSE (`drawnRegsOf(page, tabs, licence)` in WaterScreen's `shown`), never from `page.gear` / `page.methods` /
 * `page.licence`. The fixture release has no such day (every part closed to game fish there is closed to salmon too, T796:
 * the salmon bar reads the book's closure), so the salmon feed's bar is stubbed OPEN for the day: `salmonStripOf` answers
 * "open" — the stubbed loader's feed — and everything else is the app's own wiring.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { SalmonFacts } from "@app/core";
import type { ItemId } from "@app/data";
import { makeReleaseSource, type ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";

vi.mock("@app/core", async (orig) => {
  const m = await orig<typeof import("@app/core")>();
  return { ...m, salmonStripOf: (...a: Parameters<typeof m.salmonStripOf>) => {
    const s = m.salmonStripOf(...a);
    return s && { ...s, status: "open" as const, by: "dfo" as const };
  } };
});
const { WaterScreen } = await import("./WaterScreen");
const { LIGHT } = await import("./theme");

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
  const links = [...content.querySelectorAll("[role=link]")];
  const drawn = new Set(pagesOf(links.filter((e) => !row.contains(e)), /^Rule book page (\d+), opens the PDF$/));
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
  }, 40_000);
});
