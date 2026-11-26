/**
 * (T1140, owner 2026-11-25: https://canifishthis.ca/water/squamish-river opened its Regulations tab on one part, a reach
 * inside an ecological reserve, "Closed all year", and nothing said it was one part; then, on the pictures: "there should
 * be no partt x of n nameas anywhewre in the app") A WATER OF SEVERAL PARTS NEVER NUMBERS ONE, and never shows one part's
 * answer before its parts are read — on the regs fixture release, through the app's own views: the Regulations view
 * (`WaterScreen`) by a plain link (no part: `targetFor`'s lead, the `/water/<slug>` way in) and on every part (a search
 * pick, a step, a tap and "Show regulations" all hand the frame its part), the Conditions view (`WaterConditions`) and
 * the Map tab's bar (`WaterMapView`). The head names the part (its place row) and, where other parts have other rules,
 * says "Other parts of this water have different rules", whose press opens the Map with its list of parts open. A water
 * of ONE part is pinned byte for byte to what origin/main drew before T1140.
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

const COUNT = /\bpart \d+ of \d+\b/i;

/** The Map tab's bar for the frame: the line over the name (`water-map-where`) and the part's name (`water-map-name`). */
async function mapSays(item: string, part: number | null): Promise<{ where: string; name: string }> {
  render(<WaterMapView loader={loader} item={item as ItemId} part={part} tapSid={null} palette={LIGHT} map={MAP}
                       onOpenWater={NOOP} onShowRegs={NOOP} onOtherSection={NOOP} onCross={NOOP} />);
  await until(() => !!screen.queryByTestId("water-map-where"));
  const out = { where: screen.getByTestId("water-map-where").textContent!, name: screen.getByTestId("water-map-name").textContent! };
  expect(COUNT.test(screen.getByTestId("water-map").textContent ?? "")).toBe(false);
  cleanup();
  return out;
}

/** What the Regulations view heads with once its page is drawn — and, on EVERY poll, that no page is drawn before the
 *  water's parts are read (the line to the other parts is then there: these waters' parts all differ) and `drawnOnlyIf`. */
async function regsHead(item: string, part: number | null, via: typeof loader = loader,
                        /** (the gate) must hold on every poll the page is drawn */
                        drawnOnlyIf: () => boolean = () => true): Promise<{ page: string; others: string | null; pressed: string[] }> {
  const log: string[] = [];
  const bare: string[] = [];
  render(<WaterViewsContext.Provider value={views(log)}>
           <WaterScreen source={source} loader={via} item={item as ItemId} part={part} day={DAY} palette={LIGHT} />
         </WaterViewsContext.Provider>);
  await until(() => !!screen.queryByTestId("regs-view") && !!screen.queryByTestId("regs-parts-others"),
              () => { if (screen.queryByTestId("regs-view") && (!screen.queryByTestId("regs-parts-others") || !drawnOnlyIf())) bare.push(item); });
  expect([item, part, bare.length]).toEqual([item, part, 0]);
  const page = screen.getByTestId("regs-view").textContent ?? "";
  const line = screen.getByTestId("regs-parts-others");
  fireEvent.click(line);
  const out = { page, others: line.getAttribute("aria-label"), pressed: log };
  cleanup();
  return out;
}

/** Each listed part of `item` (the release's keyed, non-tidal parts): the frame's part on a search pick or a step. */
async function partsOf(item: string): Promise<number[]> {
  const st = await store;
  return liveParts(await st.parts((await st.itemOrd(item))!)).filter((p) => !p.tidal).map((p) => p.partIx);
}

describe("(T1140) no part is numbered; the head names it and points to the others", () => {
  // the Chilliwack (five parts, two rule sets), the Coquitlam (its mouth tidal, never listed), Kootenay Lake's Main Body
  // (one part of its own, the lake's other areas in its group)
  it.each(["gnis:8634", "gnis:19986", "wbk:-20"])("%s: by a plain link and on every part — the map's bar and the page say no count; the line", async (item) => {
    const st = await store;
    const water = (await st.item((await st.itemOrd(item))!)).name;
    for (const part of [null, ...await partsOf(item)]) {
      const bar = await mapSays(item, part);
      const head = await regsHead(item, part);
      // the bar's line over the name is the water's; the page never numbers a part
      expect({ item, part, where: bar.where, counted: COUNT.test(head.page) }).toEqual({ item, part, where: water, counted: false });
      // these waters' parts all differ from another (a rule set, or another area of the lake): the line, and its press
      expect({ item, part, others: head.others, pressed: head.pressed })
        .toEqual({ item, part, others: WATER_MAP_WORDS.partsOthers, pressed: ["parts"] });
    }
  }, 240_000);

  it("the page WAITS for the water's parts: with their read slow to come, no page is drawn before it", async () => {
    // the group's read of the water's areas (the one query only it asks: `siblingOrds`) answers 1.5 s late
    let came = false;
    const late = <T,>(v: Promise<T>) => new Promise<T>((res) => setTimeout(() => { came = true; res(v); }, 1500));
    const slow = openRegsFixtureWithLines(undefined, { db: (name, d) => name !== "core" ? d : {
      all: (sql, ...ps) => (/part_of = \(SELECT COALESCE/.test(sql) ? late(d.all(sql, ...ps)) : d.all(sql, ...ps)),
      get: (sql, ...ps) => d.get(sql, ...ps) } });
    const head = await regsHead("gnis:8634", null, slow, () => came);
    expect(head.others).toBe(WATER_MAP_WORDS.partsOthers);
  }, 60_000);

  it("the page does NOT wait for the live salmon (T1049): drawn while it is coming, the line already there", async () => {
    let came = false;
    const slow = new Proxy(loader, { get(t, k) {
      if (k === "salmon") return (r?: readonly number[]) => new Promise((res) => setTimeout(() => { came = true; res(t.salmon(r as never)); }, 3000));
      const v = Reflect.get(t, k, t);
      return typeof v === "function" ? v.bind(t) : v;
    } });
    const head = await regsHead("gnis:8634", null, slow);
    expect([head.others, came]).toEqual([WATER_MAP_WORDS.partsOthers, false]);
  }, 60_000);

  it("the Conditions head says the same, and its line opens the parts too", async () => {
    const log: string[] = [];
    render(<WaterViewsContext.Provider value={views(log)}>
             <WaterConditions source={source} loader={loader} item={"gnis:8634" as ItemId} tapSid={null} part={null} palette={LIGHT} />
           </WaterViewsContext.Provider>);
    await until(() => !!screen.queryByTestId("panel") && !!screen.queryByTestId("regs-parts-others"));
    const counted = COUNT.test(screen.getByTestId("water-frame").textContent ?? "");
    fireEvent.click(screen.getByTestId("regs-parts-others"));
    cleanup();
    expect([counted, log]).toEqual([false, ["parts"]]);
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
    expect(screen.queryByTestId("regs-parts-others")).toBeNull();
    expect(pinText(drawn)).toBe(PIN.drawn);
  }, 60_000);
});
