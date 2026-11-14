/**
 * (T369, owner 2026-11-03) THE NET: ONE LOAD PATH PER SCREEN. Each way into a water's Regulations and Conditions views —
 * a tap on the Map tab, the same tap on the Conditions map, a search Open then a parts-list pick, a step from another
 * part, a tap on another part then a step, the Runs sheet's open — and the reload of the address each leaves, is driven
 * through the app's own functions (the opener `waterOpener` with the release's `partOf`, the route reducer, `linkSearch`
 * → `parseStartLink` → `resolveStartLink`), then BOTH views are rendered on the regs fixture release and what they
 * draw is compared: the Regulations page's whole text (target part, access notices, Salmon card) and the Conditions
 * view's reach and "you are here" (the panel is stubbed to say what it was handed). The water Map view's own net is
 * `WaterMapView.test.tsx` (highlight, fit, ⤢).
 *
 * MUTATION PINS: the old divergences, put back by hand, make the comparison fail (a step that kept the tap: D1/D2; a
 * tap reloaded without its tap marker: D5) — so the net can see what it is for.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import type { ItemId, SectionId } from "@app/data";
import { makeReleaseSource } from "@app/data/release";
import { openRegsFixtureWithLines, sectionNames } from "@app/data/release/node";
import { openRegsStore } from "@app/data/regs";
import { HOME, parseStartLink, partAction, partOfTap, reduce, regsPartOf, waterFrame, waterOpener, type Route,
         type RouteAction, type View } from "@app/ui";
import { linkSearch, placeReads, resolveStartLink } from "@app/ui/place";
import { LIGHT } from "../theme";

// the Conditions panel says which reach and which "you are here" it was handed (its own net is ConditionsPanel.test)
vi.mock("../ConditionsPanel", async (orig) => {
  const real = await orig<typeof import("../ConditionsPanel")>();
  const { Text } = await import("react-native");
  return { ...real, ConditionsPanel: ({ section, from }: { section: number; from?: { lat: number; lon: number } | null }) =>
    <Text testID="panel">{`reach ${section} here ${from ? `${from.lat.toFixed(5)},${from.lon.toFixed(5)}` : "none"}`}</Text> };
});
const { WaterConditions } = await import("../WaterViews");
const { WaterScreen } = await import("../WaterScreen");

afterEach(cleanup);

const lined = openRegsFixtureWithLines();
const sections = sectionNames(lined);
const store = openRegsStore(lined);
const source = makeReleaseSource(lined);
const places = store.then((st) => placeReads(st, lined));
const itemFor = { itemForSection: async (sid: SectionId) => {
  const r = await (await (await store).core()).get(
    "SELECT i.item_id FROM item_section s JOIN item i ON i.ord = s.ord WHERE s.sid = ? LIMIT 1", sid);
  return r ? (String(r.item_id) as ItemId) : null;
} };
const C = "gnis:19986" as ItemId;          // the Coquitlam: part_ix 1 is Mary Hill → Lougheed (356364584 at 312 and 6590; T517: river order, part 0 the tidal mouth)
/** (T441) named by what it IS — the Coquitlam's section on stream 356364584 that starts at 312 m — never by id */
const TAP = await sections.stream(356364584, 312) as SectionId;
const AT = { lat: 49.23, lon: -122.79 };
const DAY = new Date(2026, 6, 15, 12);

/** The app's opener on a route: every open the app makes goes through it (with the release's `partOf`). */
async function viaOpener(from: Route, act: (o: ReturnType<typeof waterOpener>) => Promise<unknown> | void): Promise<Route> {
  let r = from;
  const o = waterOpener(itemFor, (a) => { r = reduce(r, a); }, { now: () => r, partOf: regsPartOf(lined) });
  await act(o);
  return r;
}
const run = (r: Route, ...as: RouteAction[]) => as.reduce(reduce, r);
const onView = (r: Route, view: View) => reduce(r, { type: "view", view });
/** The address a route leaves, reloaded the web's way. */
async function reload(r: Route): Promise<Route> {
  const url = await linkSearch(await places, r);
  const link = await resolveStartLink(parseStartLink(url)!, await places, itemFor);
  expect(link.notice).toBeNull();
  return link.route;
}

/** (T369, load-proof) poll in real time until `ok`, each wait inside `act`: no first-check race, a generous ceiling. */
async function until(ok: () => boolean, ms = 30_000): Promise<void> {
  const end = Date.now() + ms;
  while (!ok()) {
    if (Date.now() > end) throw new Error(`until: not settled in ${ms} ms:\n${document.body.textContent?.slice(0, 400)}`);
    await act(() => new Promise((d) => setTimeout(d, 25)));
  }
}

/** What the two views of the route's frame DRAW. */
async function drawn(r: Route): Promise<{ conditions: string; regs: string }> {
  const f = waterFrame(r)!;
  render(<WaterConditions source={source} loader={lined} item={f.item} tapSid={f.tapSid} part={f.part}
                          from={f.at} palette={LIGHT} />);
  await until(() => !!screen.queryByTestId("panel"));
  const conditions = screen.getByTestId("panel").textContent!;
  cleanup();
  render(<WaterScreen source={source} loader={lined} item={f.item} section={f.tapSid} part={f.part}
                      day={DAY} palette={LIGHT} />);
  await until(() => !!screen.queryByTestId("regs-view"));
  // the Salmon card and the access notices come after the page: settled (unchanged across two polls) before it is read
  let was = "";
  await until(() => { const now = screen.getByTestId("regs-view").textContent ?? ""; const same = now === was; was = now;
                      return same && !screen.queryByTestId("salmon-pending"); });
  const regs = screen.getByTestId("regs-view").textContent!;
  cleanup();
  return { conditions, regs };
}

describe("(T369) the net: every way into a water's Regulations and Conditions views draws the same", () => {
  it("the Coquitlam's Mary Hill → Lougheed part, NOT tapped: search + pick, a step, a tap elsewhere + a step — and each reloaded", async () => {
    const st = await store;
    const nav = await st.navRows((await st.itemOrd(C))!);
    const other = nav.sections.find((s) => s.partIx === 3)!.sid as SectionId;
    const ways: Record<string, Route> = {
      "search Open, the list's part": run(await viaOpener(HOME, (o) => o.searched(C)), partAction(C, C, 1)),
      "▼ from part 2": run(await viaOpener(HOME, (o) => o.item(C, "map")), partAction(C, C, 2), partAction(C, C, 1)),
      "a tap on part 3, then the step": run(await viaOpener(HOME, (o) => o.tap(other, AT, "regs")), partAction(C, C, 1)),
    };
    const first = await drawn(onView(ways["search Open, the list's part"]!, "regs"));
    // the PART's middle (its `mid_sid`, never the water's first section), and "you are here" there (owner Q1)
    const mid = nav.geo.find((g) => g.partIx === 1)!;
    expect(first.conditions).toBe(`reach ${mid.midSid} here ${mid.mid[1].toFixed(5)},${mid.mid[0].toFixed(5)}`);
    for (const [way, r] of Object.entries(ways)) {
      const back = await reload(r);
      expect({ way, live: await drawn(r), reloaded: await drawn(back) }).toEqual({ way, live: first, reloaded: first });
    }
  }, 120_000);

  it("the same part TAPPED (356364584:312): from the Map tab, from the Conditions map, from the Runs sheet — and each reloaded", async () => {
    const ways: Record<string, Route> = {
      "a tap on the Map tab": await viaOpener(HOME, (o) => o.tap(TAP, AT, "regs")),
      "the same tap on the Conditions map": await viaOpener(run(HOME, { type: "tab", tab: "conditions" }), (o) => o.tap(TAP, AT, "conditions")),
      // (D15) the Runs sheet hands on the tap's point
      "the Runs sheet's open": await viaOpener(run(HOME, { type: "tab", tab: "runs" }), (o) => o.tap(TAP, AT, "regs")),
    };
    // (D3) every tapped frame OPENS at its part: nothing adopts one later
    expect(Object.values(ways).map((r) => waterFrame(r)!.part)).toEqual([1, 1, 1]);
    const first = await drawn(ways["a tap on the Map tab"]!);
    expect(first.conditions).toBe(`reach ${TAP} here ${AT.lat.toFixed(5)},${AT.lon.toFixed(5)}`);
    for (const [way, r] of Object.entries(ways)) {
      const back = await reload(r);
      expect(waterFrame(back)).toMatchObject({ item: C, part: 1, tapSid: TAP });
      expect({ way, live: await drawn(r), reloaded: await drawn(back) }).toEqual({ way, live: first, reloaded: first });
    }
  }, 120_000);

  it("MUTATION PINS: a step that kept the tap (D1/D2), and a tapped address reloaded without its tap (D5), draw otherwise", async () => {
    const st = await store;
    const tapped = await viaOpener(HOME, (o) => o.tap(TAP, AT, "regs"));
    // (T517) a part that is not the water's first map part (part 1, where a frame with its part unread lands): part 2
    const part2 = run(await viaOpener(HOME, (o) => o.searched(C)), partAction(C, C, 2), { type: "view", view: "regs" });
    const nav = await st.navRows((await st.itemOrd(C))!);
    const other = nav.sections.find((s) => s.partIx === 3)!.sid as SectionId;
    // (D1) the old Conditions view never read the frame's part: the same frame with its part unread draws otherwise
    const partUnread: Route = { ...part2, mode: "water", stack: [{ ...waterFrame(part2)!, part: null }] } as Route;
    expect((await drawn(partUnread)).conditions).not.toEqual((await drawn(part2)).conditions);
    // (D2) the old `part` action kept the tap of ANOTHER part on the stepped frame: the route now drops it
    const tappedElsewhere = await viaOpener(HOME, (o) => o.tap(other, AT, "regs"));
    expect(waterFrame(run(tappedElsewhere, partAction(C, C, 1)))).toMatchObject({ part: 1, tapSid: null, at: null });
    // the old address of a tapped frame: the part's, no tap — the reload is not the tapped frame
    const untapped = (await resolveStartLink(parseStartLink((await linkSearch(await places, tapped)).replace(/&here=[^&]*/, ""))!,
                                             await places, itemFor)).route;
    expect(await drawn(untapped)).not.toEqual(await drawn(tapped));
    // and the release's own answer for the tap is the part the frame opened at
    expect(await partOfTap(st, C, TAP)).toEqual({ part: 1 });
  }, 120_000);
});
