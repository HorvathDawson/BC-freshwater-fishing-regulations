/**
 * T1052 (owner 2026-11-24 ~12:33, "One block, both calendars (Recommended)") THROUGH THE WATER SCREEN, as the app mounts it
 * (`glance`): a water one closure shuts to all fishing, salmon included, all year, says it once — the closed banner, for
 * both ("Closed to all fishing all year, salmon included", the book row's line, its source) — and draws no Salmon row; the
 * two calendars stay. Every other case keeps today's two statements. The pages are the fixture release's (core's
 * `buildRegsPage`), the salmon cards core's own `salmonCard` over a hand-made salmon file whose book place names the
 * page's closure by its rule, as a release's places do (T796 A5; Nelson Creek's `r2:nelson_creek@2-8::nelson_creek.r1`).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { inSeasonBanner, type SalmonFacts } from "@app/core";
import type { ItemId } from "@app/data";
import { makeReleaseSource, type ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import type { RegsScreenProps } from "./regs/RegsScreen";

/** (the in-season case) a B.C. notice the screen is handed for the water, drawn first on the page */
const notice = vi.hoisted(() => ({ on: false }));
vi.mock("./regs/RegsScreen", async (orig) => {
  const m = await orig<typeof import("./regs/RegsScreen")>();
  const change = { id: "is-a", region: "4", water: "Wilkie Creek", from: "2026-11-01", to: "2026-12-31",
                   text: "No fishing for bull trout.", source: "https://www2.gov.bc.ca/x", waters: null, words: {} };
  return { ...m, RegsScreen: (p: RegsScreenProps) => m.RegsScreen(notice.on ? { ...p, inSeason: [inSeasonBanner(change, { where: null })] } : p) };
});
const { WaterScreen } = await import("./WaterScreen");
const { LIGHT } = await import("./theme");

afterEach(() => { cleanup(); notice.on = false; });
const LOADED = { timeout: 20_000 } as const;
const NOV_24 = new Date(2026, 10, 24, 12);
const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
const NOT_OPENED = { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 };

/** The salmon file: chain 0 the unlisted water, chain 1 every section of the page, first place the book's closure by
 *  `rule` (`entry::rule`), its year `year` (per species runs: 1 the book's closed verdict, -1 says nothing). */
function bookFacts(rule: string, water: string, page: number, year: readonly number[] = [0, 1]): SalmonFacts {
  const BOOK = `book:${rule}`;
  return {
    handles: "0".repeat(16), chains: 2, chainOf: () => 1, loaded: [],
    payload: {
      format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO)", default: 0,
      regions: { "4": { name: "Kootenays", asof: "2026-08-28", url: "https://example/4", notes: [], shared: [], sizes: [] } },
      scopes: {}, unmapped: [], gears: [],
      verdicts: [NOT_OPENED, { s: "closed", t: `Closed to all fishing by the B.C. Synopsis (p.${page}): No Fishing` }],
      chains: [{ r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
               { r: ["4"], w: water, l: [BOOK], sp: all(1), g: [0, -1], m: [0, 3], a: all(0) }],
      p: { [BOOK]: { r: "4", water, static: 1, book: { rule, pages: [page] } } },
      static: { [BOOK]: { sp: [year, year, year, year, year], g: [0, -1] } },
    },
  } as unknown as SalmonFacts;
}
/** The unlisted water only: every section on DFO's default chain ("Not opened by DFO") — a second source, DFO's */
const DFO_ONLY = {
  handles: "0".repeat(16), chains: 1, chainOf: () => 0, loaded: [],
  payload: { format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO)", default: 0, regions: {}, scopes: {}, unmapped: [], gears: [],
             verdicts: [NOT_OPENED], chains: [{ r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] }] },
} as unknown as SalmonFacts;

function withSalmon(facts: SalmonFacts | null): ReleaseLoader {
  const live = openRegsFixture();
  if (!facts) return live;
  return Object.assign(Object.create(Object.getPrototypeOf(live)), live, {
    manifest: { ...live.manifest, files: { ...live.manifest.files, salmon: { url: "salmon.bin.gz" } } },
    salmon: async () => facts, method: async () => null, access: async () => null,
  });
}
async function open(item: string, part: number, facts: SalmonFacts | null, glance = true) {
  const loader = withSalmon(facts);
  render(<WaterScreen source={makeReleaseSource(loader)} item={item as ItemId} palette={LIGHT} part={part} glance={glance}
                      loader={loader} day={NOV_24} />);
  await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
  if (facts) await waitFor(() => expect(screen.getByTestId("salmon-block")).toBeTruthy(), LOADED);
}
/** what the page says: the closed banner's words (null: none), whether a Salmon row is in the species list, the strips */
const said = () => ({
  banner: screen.queryByTestId("keep-banner")?.textContent ?? null,
  salmonRow: !!screen.queryByTestId("glance:salmon"),
  calendars: [!!screen.queryByTestId("salmon-block"), !!screen.queryByTestId("keep-strip")],
});

const WILKIE = "r4:wilkie_creek@4-30::wilkie_creek.r1";
const ONE_BLOCK = (line: string, page: number) => `Closed to all fishing all year, salmon included${line}SourceB.C. Synopsis p.${page} ↗Who says so ›`;

describe("<WaterScreen> (T1052) a water closed to all fishing says it once", () => {
  it("one closure, all year, salmon included (Wilkie Creek, its own row \"No Fishing\"): one block, no Salmon row, both calendars", async () => {
    await open("gnis:27735", 0, bookFacts(WILKIE, "Wilkie Creek", 40));
    expect(said()).toEqual({ banner: ONE_BLOCK("The B.C. Synopsis row for Wilkie Creek reads “No Fishing”.", 40), salmonRow: false, calendars: [true, true] });
  }, 40_000);

  it("a screen mounted without the at-a-glance table (the cards in full): no Salmon card under the one block either", async () => {
    await open("gnis:27735", 0, bookFacts(WILKIE, "Wilkie Creek", 40), false);
    expect([said().banner, !!screen.queryByTestId("salmon-card")])
      .toEqual([ONE_BLOCK("The B.C. Synopsis row for Wilkie Creek reads “No Fishing”.", 40), false]);
    cleanup();
    await open("gnis:27735", 0, DFO_ONLY, false);
    expect(!!screen.queryByTestId("salmon-card")).toBe(true);
  }, 60_000);

  it("a several-part water: its closed part (a walked tributary of Wahleach Lake) gets the block naming the row it inherits; its open part keeps its Salmon row", async () => {
    const facts = bookFacts("r2:wahleach_jones_lake_s_tributaries@2-3::wahleach_jones_lake_s_tributaries.r1", "Wahleach Lake", 26);
    await open("gnis:22472", 1, facts);
    expect(said()).toEqual({ banner: ONE_BLOCK("Wahleach Lake’s row covers its tributaries: “No Fishing”.", 26), salmonRow: false, calendars: [true, true] });
    cleanup();
    await open("gnis:22472", 0, facts);
    expect([said().banner, said().salmonRow]).toEqual([null, true]);
  }, 60_000);

  it("two sources: salmon closed by DFO (\"Not opened by DFO\"), game fish by the book: both statements stay, neither source dropped", async () => {
    await open("gnis:27735", 0, DFO_ONLY);
    const s = said();
    expect([s.banner?.startsWith("Closed all yearNo fishing for any game fish. This waterSource"), s.salmonRow, s.calendars]).toEqual([true, true, [true, true]]);
    expect(screen.getByTestId("glance:salmon").textContent).toContain("closed");
  }, 40_000);

  it("salmon closed by ANOTHER book rule than the banner's: both statements stay", async () => {
    await open("gnis:27735", 0, bookFacts("r2:brunette_river@2-8::brunette_river.r1b", "Brunette River", 22));
    expect([said().banner?.startsWith("Closed all yearNo fishing for any game fish."), said().salmonRow]).toEqual([true, true]);
  }, 40_000);

  it("closed to all fishing for part of the year only (the Cowichan, Nov 15–Apr 15, the same rule): today's two statements", async () => {
    // the book place's year: closed Jan 1–Apr 15 and Nov 15–Dec 31 (file days), the rule's own dates
    await open("gnis:12227", 5, bookFacts("r1:cowichan_river_see_map_below@1-4::cowichan_river.r3", "Cowichan River", 15, [0, 1, 106, -1, 319, 1]));
    expect([said().banner?.includes("salmon included"), said().salmonRow]).toEqual([false, true]);
  }, 40_000);

  it("a live in-season notice on top: drawn first, as always; the one block under the calendars as without it", async () => {
    notice.on = true;
    await open("gnis:27735", 0, bookFacts(WILKIE, "Wilkie Creek", 40));
    expect([!!screen.queryByTestId("inseason-banners"), said()])
      .toEqual([true, { banner: ONE_BLOCK("The B.C. Synopsis row for Wilkie Creek reads “No Fishing”.", 40), salmonRow: false, calendars: [true, true] }]);
  }, 40_000);

  it("a release with no salmon: the game-fish banner as it was, no \"salmon included\"", async () => {
    await open("gnis:27735", 0, null);
    expect([said().banner?.startsWith("Closed all yearNo fishing for any game fish. This water"), said().banner?.includes("salmon")]).toEqual([true, false]);
  }, 40_000);
});
