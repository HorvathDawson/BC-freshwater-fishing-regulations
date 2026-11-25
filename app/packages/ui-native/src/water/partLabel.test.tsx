/**
 * (T1140, owner 2026-11-25: https://canifishthis.ca/water/squamish-river opened its Regulations tab on part 1 of 2, a reach
 * inside an ecological reserve, "Closed all year", and nothing said it was one part of two) A WATER OF SEVERAL PARTS NEVER
 * SHOWS ONE PART'S ANSWER WITHOUT SAYING WHICH PART OF HOW MANY — on the regs fixture release, through the app's own
 * views: the Regulations view (`WaterScreen`) by a plain link (no part: `targetFor`'s lead, the `/water/<slug>` way in) and
 * on every part (a search pick, a step, a tap and "Show regulations" all hand the frame its part), and the Conditions view
 * (`WaterConditions`). Each says "Part n of m" — the Map tab's count for the same frame, read off its own bar
 * (`WaterMapView`) — and, where other parts have other rules, the line to them, whose press opens the Map with its list of
 * parts open. A water of ONE part is pinned byte for byte to what origin/main drew before T1140.
 *
 * The split lake's area (Kootenay Lake — Main Body: one part of its own, the lake's other areas in its group) is a water
 * of several parts on the map, so it says "Part 1 of 3" too.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { ItemId } from "@app/data";
import type { MapProps } from "@app/map";
import { makeReleaseSource } from "@app/data/release";
import { openRegsFixtureWithLines } from "@app/data/release/node";
import { openRegsStore } from "@app/data/regs";
import { liveParts } from "@app/data/regs";
import { WATER_MAP_WORDS } from "@app/core";
import { LIGHT } from "../theme";
import { WaterViewsContext, type WaterViews } from "../ViewSwipe";
// the single-part pin: written on origin/main (before T1140) by this same render, never by hand (its `about`)
import PIN from "./fixtures/t1140-single-part.json";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: (_: MapProps) => null }));
// the Conditions panel is not this test's (its own net is ConditionsPanel.test): it says it is drawn
vi.mock("../ConditionsPanel", async (orig) => {
  const real = await orig<typeof import("../ConditionsPanel")>();
  const { Text } = await import("react-native");
  return { ...real, ConditionsPanel: () => <Text testID="panel">panel</Text> };
});
const { WaterScreen } = await import("../WaterScreen");
const { WaterConditions } = await import("../WaterViews");
const { WaterMapView } = await import("./WaterMapView");

afterEach(cleanup);

const loader = openRegsFixtureWithLines();
const source = makeReleaseSource(loader);
const store = openRegsStore(loader);
const DAY = new Date(2026, 6, 15, 12);
const MAP = { at: { atlas: "a", basemap: "b" } as never, theme: "light", camera: { lon: -122, lat: 49, zoom: 8 }, view: "plain" } as never;
const NOOP = () => {};

/** (T369, load-proof) poll in real time until `ok`, each wait inside `act`; `each` runs on every poll (an invariant) */
async function until(ok: () => boolean, each: () => void = NOOP, ms = 30_000): Promise<void> {
  const end = Date.now() + ms;
  for (;;) {
    each();
    if (ok()) return;
    if (Date.now() > end) throw new Error(`until: not settled in ${ms} ms:\n${document.body.textContent?.slice(0, 400)}`);
    await act(() => new Promise((d) => setTimeout(d, 20)));
  }
}

/** The water frame's views as the Shell hands them; `onParts` logs its press. */
function views(log: string[]): WaterViews {
  return { views: ["regs", "conditions", "map"], on: "regs", onView: NOOP, onBack: NOOP, backLabel: "Map", close: NOOP,
           closeLabel: "Map", swipe: null, bodySwipe: null, onParts: () => log.push("parts") };
}

/** The Map tab's own words for the frame (`water-map-where`, "Part 2 of 5 · Chilliwack River"), its count alone. */
async function mapSays(item: string, part: number | null): Promise<string> {
  render(<WaterMapView loader={loader} item={item as ItemId} part={part} tapSid={null} palette={LIGHT} map={MAP}
                       onOpenWater={NOOP} onShowRegs={NOOP} onOtherSection={NOOP} onCross={NOOP} />);
  await until(() => !!screen.queryByTestId("water-map-where"));
  const said = screen.getByTestId("water-map-where").textContent!;
  cleanup();
  return said.split(" · ")[0]!;
}

/** What the Regulations view heads with, once its page is drawn — and, on EVERY poll, that no page is drawn without it. */
async function regsHead(item: string, part: number | null, via: typeof loader = loader,
                        /** (the gate) must hold on every poll the page is drawn */
                        drawnOnlyIf: () => boolean = () => true): Promise<{ where: string | null; others: string | null; pressed: string[] }> {
  const log: string[] = [];
  const bare: string[] = [];
  render(<WaterViewsContext.Provider value={views(log)}>
           <WaterScreen source={source} loader={via} item={item as ItemId} part={part} day={DAY} palette={LIGHT} />
         </WaterViewsContext.Provider>);
  await until(() => !!screen.queryByTestId("regs-view") && !!screen.queryByTestId("regs-part-where"),
              () => { if (screen.queryByTestId("regs-view") && (!screen.queryByTestId("regs-part-where") || !drawnOnlyIf())) bare.push(item); });
  expect([item, part, bare.length]).toEqual([item, part, 0]);
  const where = screen.getByTestId("regs-part-where").textContent;
  const line = screen.queryByTestId("regs-parts-others");
  if (line) fireEvent.click(line);
  const out = { where, others: line?.getAttribute("aria-label") ?? null, pressed: log };
  cleanup();
  return out;
}

/** Each listed part of `item` (the release's keyed, non-tidal parts): the frame's part on a search pick or a step. */
async function partsOf(item: string): Promise<number[]> {
  const st = await store;
  return liveParts(await st.parts((await st.itemOrd(item))!)).filter((p) => !p.tidal).map((p) => p.partIx);
}

describe("(T1140) the Regulations head says which part of how many, as the Map tab does", () => {
  // the Chilliwack (five parts, two rule sets), the Coquitlam (its mouth tidal, never listed), Kootenay Lake's Main Body
  // (one part of its own, the lake's other areas in its group)
  it.each(["gnis:8634", "gnis:19986", "wbk:-20"])("%s: by a plain link and on every part, the map's count and the line", async (item) => {
    for (const part of [null, ...await partsOf(item)]) {
      const want = await mapSays(item, part);
      const head = await regsHead(item, part);
      expect({ item, part, where: head.where }).toEqual({ item, part, where: want });
      expect(want).toMatch(/^Part \d+ of \d+$/);
      // these waters' parts all differ from another (a rule set, or another area of the lake): the line, and its press
      expect({ item, part, others: head.others, pressed: head.pressed })
        .toEqual({ item, part, others: WATER_MAP_WORDS.partsOthers, pressed: ["parts"] });
    }
  }, 240_000);

  it("the page WAITS for the water's parts: with their read slow to come, no page is drawn without 'Part n of m'", async () => {
    // the group's read of the water's areas (the one query only it asks: `siblingOrds`) answers 1.5 s late
    let came = false;
    const late = <T,>(v: Promise<T>) => new Promise<T>((res) => setTimeout(() => { came = true; res(v); }, 1500));
    const slow = openRegsFixtureWithLines(undefined, { db: (name, d) => name !== "core" ? d : {
      all: (sql, ...ps) => (/part_of = \(SELECT COALESCE/.test(sql) ? late(d.all(sql, ...ps)) : d.all(sql, ...ps)),
      get: (sql, ...ps) => d.get(sql, ...ps) } });
    const head = await regsHead("gnis:8634", null, slow, () => came);
    expect(head.where).toBe(await mapSays("gnis:8634", null));
  }, 60_000);

  it("the page does NOT wait for the live salmon (T1049): drawn while it is coming, already saying 'Part n of m'", async () => {
    // the salmon slices answer 3 s late: the page is drawn before them, its count the map's of that moment (nothing joined)
    let came = false;
    const slow = new Proxy(loader, { get(t, k) {
      if (k === "salmon") return (r?: readonly number[]) => new Promise((res) => setTimeout(() => { came = true; res(t.salmon(r as never)); }, 3000));
      const v = Reflect.get(t, k, t);
      return typeof v === "function" ? v.bind(t) : v;
    } });
    const head = await regsHead("gnis:8634", null, slow);
    expect([head.where, came]).toEqual([await mapSays("gnis:8634", null), false]);
  }, 60_000);

  it("the Conditions head says the same, and its line opens the parts too", async () => {
    const log: string[] = [];
    render(<WaterViewsContext.Provider value={views(log)}>
             <WaterConditions source={source} loader={loader} item={"gnis:8634" as ItemId} tapSid={null} part={null} palette={LIGHT} />
           </WaterViewsContext.Provider>);
    await until(() => !!screen.queryByTestId("panel") && !!screen.queryByTestId("regs-part-where"));
    const where = screen.getByTestId("regs-part-where").textContent;
    fireEvent.click(screen.getByTestId("regs-parts-others"));
    cleanup();
    expect([where, log]).toEqual([await mapSays("gnis:8634", null), ["parts"]]);
  }, 60_000);
});

describe("(T1140) the Map opened from the line has its list of parts open", () => {
  it("listOpen: the list is open at once; without it, closed (as before)", async () => {
    for (const listOpen of [true, false]) {
      render(<WaterMapView loader={loader} item={"gnis:8634" as ItemId} part={null} tapSid={null} palette={LIGHT} map={MAP}
                           onOpenWater={NOOP} onShowRegs={NOOP} onOtherSection={NOOP} onCross={NOOP} listOpen={listOpen} />);
      await until(() => !!screen.queryByTestId("water-map-parts-toggle"));
      expect([listOpen, !!screen.queryByTestId("water-map-parts-list"),
              screen.getByTestId("water-map-parts-toggle").getAttribute("aria-expanded")])
        .toEqual([listOpen, listOpen, String(listOpen)]);
      cleanup();
    }
  }, 60_000);
});

/** The pin's text: what was drawn, its colour literals written ‹rgb r, g, b› / ‹hex RRGGBB› — a record of a render, not a
 *  place colours live (tools/check-colours.mjs scans the app's JSON for literals). One-to-one: nothing else is changed. */
const pinText = (s: string) => s.replace(/\b(rgba?|hsla?)\(([^)]*)\)/g, "‹$1 $2›")
  .replace(/(?<![\w&$/])#([0-9a-fA-F]{8}|[0-9a-fA-F]{6}|[0-9a-fA-F]{3,4})(?![\w-])/g, "‹hex $1›");

describe("(T1140) a water of ONE part is unchanged, byte for byte", () => {
  it("Glenn Creek's Regulations header and page text are what origin/main drew before T1140", async () => {
    render(<WaterScreen source={source} loader={loader} item={"gnis:10457" as ItemId} part={null} day={DAY} palette={LIGHT} />);
    let was = "";
    await until(() => { const v = screen.queryByTestId("regs-view"); const now = v?.textContent ?? ""; const same = !!v && now === was; was = now; return same; });
    const drawn = `${screen.getByTestId("regs-header").outerHTML}\n---\n${screen.getByTestId("regs-view").textContent}\n`;
    expect(screen.queryByTestId("regs-part-where")).toBeNull();
    expect(pinText(drawn)).toBe(PIN.drawn);
  }, 60_000);
});
