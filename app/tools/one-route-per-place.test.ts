/**
 * ONE ROUTE PER PLACE (T60 check 1; owner 2026-10-22: "every route into a screen reaches the SAME screen, e.g. a water's
 * Regulations/Conditions opened from the Map tab vs the Conditions map"). Two halves:
 *
 *   the frame    every builder of a way into a water that @app/ui exports — a map tap (`openTap`, from either map), a
 *                search result (`waterOpener.searched`), a spot or deep path (`waterOpener.item`), "Flows into"
 *                (`exitAction`), ▲ / ▼ across a name change (`crossAction`), a Parts row (`partAction`,
 *                `showRegsActions`) — lands the SAME frame for the same subject (water, part, section, point),
 *                differing in `view` alone. A route reducer is the one place a frame is made, so a second entry that
 *                forgot the part, or the tapped section, would be a second place for the same water.
 *   the screen   each PLACE in the app — a water's three views, a spot, the capture, the four tabs, search, the menu's
 *                pages — is mounted by exactly ONE element in the app's source. A second mount of a screen component is
 *                a second route to the same screen, which is how the Conditions drill once lived beside the water
 *                screen (one-water-screen.test.ts) and the hamburger beside the More tab (one-menu.test.ts).
 *
 * Mutation-pinned: a planted second mount, and a planted entry builder that drops the part, both fail.
 */
import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { HOME, crossAction, exitAction, openTap, openWater, partAction, reduce, showRegsActions, waterFrame, waterOpener,
         type Route, type RouteAction, type WaterFrame } from "@app/ui";
import { code, files, read } from "./consistency";

const VEDDER = "gnis:3062" as ItemId, CHILLIWACK = "gnis:8634" as ItemId;
const SID = 77 as SectionId, UNNAMED = 501 as SectionId;
const AT = { lat: 49.1, lon: -121.9 };
const source = { itemForSection: async (s: SectionId) => (s === SID ? VEDDER : s === UNNAMED ? null : CHILLIWACK) };
const COND: Route = reduce(HOME, { type: "tab", tab: "conditions" });

/** A frame's PLACE: everything but the view in front. */
const place = (f: WaterFrame | null) => (f === null ? null : (({ view: _v, ...rest }) => rest)(f));
/** Where each route ends: the frame on screen, as a place. */
const landed = (r: Route) => place(waterFrame(r));
/** Every way into a water, named, each as a function from a start route to the route it lands on. */
type Entry = [name: string, run: (start: Route) => Promise<Route>];
const dispatch = async (start: Route, build: (go: (a: RouteAction) => void) => void | Promise<void>): Promise<Route> => {
  let r = start;
  await build((a) => { r = reduce(r, a); });
  return r;
};

describe("one route per place: the frame (T60)", () => {
  it("a tap on a named water: the Map tab, the Conditions map and a water's own map land ONE place", async () => {
    const entries: Entry[] = [
      ["Map tab tap", (s) => dispatch(s, async (go) => go((await openTap(source, SID, AT, "regs"))!))],
      ["Conditions map tap", (s) => dispatch(s, async (go) => go((await openTap(source, SID, AT, "conditions"))!))],
      ["opener.tap", (s) => dispatch(s, (go) => waterOpener(source, go).tap(SID, AT, "regs"))],
      ["opener.tap from Conditions", (s) => dispatch(s, (go) => waterOpener(source, go).tap(SID, AT, "conditions"))],
      // a tap on another water from inside a water (the Chilliwack's map): the same place, over the first water
      ["tap from inside a water", (s) => dispatch(reduce(s, openWater({ item: CHILLIWACK, view: "map" })),
                                                  (go) => waterOpener(source, go).tap(SID, AT, "regs", CHILLIWACK))],
    ];
    const seen = await Promise.all(entries.map(async ([n, run]) => [n, landed(await run(HOME)), landed(await run(COND))]));
    const want = { item: VEDDER, part: null, tapSid: SID, at: AT };
    expect(seen).toEqual(entries.map(([n]) => [n, want, want]));
  });

  it("an unnamed reach (T53): the same place from either map; the water Map is refused, never a different frame", async () => {
    const a = landed(reduce(HOME, (await openTap(source, UNNAMED, AT, "regs"))!));
    const b = landed(reduce(COND, (await openTap(source, UNNAMED, AT, "conditions"))!));
    const c = landed(reduce(HOME, (await openTap(source, UNNAMED, AT, "map"))!));
    expect([a, b, c]).toEqual([{ item: null, part: null, tapSid: UNNAMED, at: AT }, a, a]);
    expect(waterFrame(reduce(HOME, (await openTap(source, UNNAMED, AT, "map"))!))!.view).toBe("regs");
  });

  it("a water by name — search, a spot, a deep path, 'Flows into', a Parts row, ▲ / ▼ — is ONE place per (water, part)", async () => {
    const byName: Entry[] = [
      ["searched", (s) => dispatch({ ...s, search: true } as Route, (go) => waterOpener(source, go).searched(VEDDER))],
      ["item (a spot, a deep path)", (s) => dispatch(s, (go) => waterOpener(source, go).item(VEDDER))],
      ["item, Conditions in front", (s) => dispatch(s, (go) => waterOpener(source, go).item(VEDDER, "conditions"))],
      ["openWater", (s) => dispatch(s, (go) => go(openWater({ item: VEDDER })))],
    ];
    const seen = await Promise.all(byName.map(async ([n, run]) => [n, landed(await run(HOME))]));
    expect(seen).toEqual(byName.map(([n]) => [n, { item: VEDDER, part: null, tapSid: null, at: null }]));

    // at a part, from inside another water: the exit, the cross, the opener's flow — one place, over or in place of it
    const inside = reduce(HOME, openWater({ item: CHILLIWACK, view: "map" }));
    const atPart: Entry[] = [
      ["exitAction (Flows into)", (s) => dispatch(s, (go) => go(exitAction(VEDDER, 2)))],
      ["opener.flow", (s) => dispatch(s, (go) => waterOpener(source, go).flow(VEDDER, (g) => exitAction(VEDDER, 2, g)))],
      ["crossAction (▲ / ▼)", (s) => dispatch(s, (go) => go(crossAction(VEDDER, 2)))],
      ["openWater at a part", (s) => dispatch(s, (go) => go(openWater({ item: VEDDER, part: 2 })))],
    ];
    const seenPart = await Promise.all(atPart.map(async ([n, run]) => [n, landed(await run(inside))]));
    expect(seenPart).toEqual(atPart.map(([n]) => [n, { item: VEDDER, part: 2, tapSid: null, at: null }]));

    // a Parts row and "Show regulations for this part" step the frame's part: the same water at the same part
    const onVedder = reduce(HOME, openWater({ item: VEDDER, tapSid: SID, at: AT }));
    const stepped = reduce(onVedder, partAction(VEDDER, VEDDER, 3));
    let shown = onVedder;
    for (const a of showRegsActions(VEDDER, VEDDER, 3)) shown = reduce(shown, a);
    expect([landed(stepped), landed(shown), waterFrame(shown)!.view])
      .toEqual([{ item: VEDDER, part: 3, tapSid: SID, at: AT }, { item: VEDDER, part: 3, tapSid: SID, at: AT }, "regs"]);
  });

  it("MUTATION PIN: an entry that forgets the part, or the tapped section, is a different place and is caught", async () => {
    const forgotPart = (item: ItemId, _part: number): RouteAction => openWater({ item });          // drops the part
    const forgotTap = async (sid: SectionId): Promise<RouteAction> => openWater({ item: await source.itemForSection(sid) });
    const inside = reduce(HOME, openWater({ item: CHILLIWACK, view: "map" }));
    expect(landed(reduce(inside, forgotPart(VEDDER, 2)))).not.toEqual(landed(reduce(inside, exitAction(VEDDER, 2))));
    expect(landed(reduce(HOME, await forgotTap(SID)))).not.toEqual(landed(reduce(HOME, (await openTap(source, SID, AT, "regs"))!)));
  });
});

/**
 * THE SCREENS: each is mounted by ONE element in the app's source. A value here is why a component is mounted more
 * than once and is still one place; adding a line is a decision, said in words, not a way past the scan.
 */
const SCREENS: Record<string, string | null> = {
  Shell: null,
  WaterScreen: null, WaterConditions: null, WaterMap: null, WaterMapView: null, RegsScreen: null, ConditionsPanel: null,
  SpotsScreen: null, SpotCapture: null, MoreScreen: null, MenuHost: null,
  GlossaryPage: null, SourcesPage: null, AboutPage: null, ReportPage: null,
  SearchSheet: null, TopBar: null, LayersSheet: null, SpotPickBanner: null, SpotDropSheet: null,
  SpotScreen: "the capture's details step (SpotCapture) is the spot form in `mode=\"draft\"` — a form being filled, not a saved "
    + "spot's screen; a saved spot is reached only through the Shell's `openSpot` route",
  TabBar: "the bottom bar is drawn under the browse body and under an open spot (two screens, one bar each) — a tab change "
    + "is one action per screen, reduced by the one route",
  DaySheet: "\"Which day?\" is one sheet asked from two screens (the map's calendar button and the regs page's date pill); "
    + "each mounts it once and only one can be open",
  MapScreen: "a canvas, not a place: the browse maps, the water's Map view and the capture's point step each draw one",
};
/** Where `<Name` is mounted: the components (.tsx) — a hook (.ts) draws nothing, and `Async<WaterMap |` is a type. */
const mountsOf = (name: string) =>
  files().filter((f) => f.endsWith(".tsx"))
    .flatMap((f) => { const n = read(f).match(new RegExp(`<${name}\\b`, "g"))?.length ?? 0; return n ? [[f, n] as const] : []; });

describe("one route per place: the screens (T60)", () => {
  it("every screen component is mounted once across the app, or says why not", () => {
    const over: string[] = [];
    for (const [name, why] of Object.entries(SCREENS)) {
      const mounts = mountsOf(name);
      const n = mounts.reduce((s, [, k]) => s + k, 0);
      if (n === 0) over.push(`${name}: mounted nowhere (a screen nothing reaches, or a rename this list did not follow)`);
      else if (n > 1 && why === null) over.push(`${name}: mounted ${n} times — ${mounts.map(([f, k]) => `${f} ×${k}`).join(", ")}`);
      else if (n === 1 && why !== null) over.push(`${name}: mounted once; its allowance is stale`);
    }
    expect(over).toEqual([]);
  });

  it("MUTATION PIN: a second mount is counted (a comment is not)", () => {
    const planted = code(`export const A = () => <MoreScreen />;\n/* <MoreScreen /> */\nexport const B = () => (<MoreScreen palette={p} />);`);
    expect(planted.match(/<MoreScreen\b/g)?.length).toBe(2);
  });
});
