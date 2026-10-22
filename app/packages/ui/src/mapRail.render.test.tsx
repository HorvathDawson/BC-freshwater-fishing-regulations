/**
 * The map rail (owner 2026-10-21): the Spots menu's view model, whole, and the hook behind the rail — the provisional
 * pin a long press drops (Cancel leaves nothing; Continue is the ONE capture, at the pin), the ways to add a spot, and
 * where "Show" sends the map. Every add ends as the route's own `addSpot` at a point.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { Spot } from "@app/data/spots";
import { spotsMenu, useMapRail, type Locate, type LocatePermission, type MapRail } from "./mapRail";
import { HOME, reduce, type Route, type RouteAction } from "./route";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });

const VISIT = new Date(2026, 7, 30, 23, 30).getTime();
const spot = (over: Partial<Spot> = {}): Spot => ({
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0, lat: 49.0974, lon: -121.9675, item: null,
  waterName: null, title: "Tamihi run", notes: "", photos: [], reading: null, weather: null, trace: null, panel: null,
  ...over,
});

describe("spotsMenu", () => {
  it("your spots as rows, the two ways to add (your location, granted; pick on the map), the stale line", () => {
    const vm = spotsMenu([spot({ photos: ["photos/s1-1.jpg"] }), spot({ id: "s2", title: "", waterName: "Vedder River",
                                                                        weather: { at: "x" } as never })],
                         { canAdd: true, canRefresh: true, permission: "granted" });
    expect(vm).toEqual({
      title: "Spots", error: null, empty: null,
      rows: [
        { id: "s1", title: "Tamihi run", lines: "2026-08-30 · no gauge · no weather\n49.0974, -121.9675",
          photo: "photos/s1-1.jpg", showLabel: "Show Tamihi run on the map" },
        { id: "s2", title: "Vedder River", lines: "2026-08-30 · no gauge · no weather\n49.0974, -121.9675",
          photo: null, showLabel: "Show Vedder River on the map" },
      ],
      stale: "2 spots are missing the weather from the day they were pinned, or the week before it.",
      adds: [{ k: "mine", label: "At my location", detail: "" },
             { k: "pick", label: "Pick on the map", detail: "Go to the map and tap the place" }],
      note: null,
    });
  });

  it("your location: offered when granted, said to ask when the platform would; refused or absent, not offered AND " +
     "said why; still asking, nothing said; pick on the map always", () => {
    const kinds = (["granted", "prompt", "denied", "unavailable", null] as (LocatePermission | null)[])
      .map((permission) => {
        const vm = spotsMenu([], { canAdd: true, canRefresh: false, permission });
        return `${vm.adds.map((a) => `${a.k}${a.detail ? `(${a.detail})` : ""}`).join(" ")} | ${vm.note ?? "-"}`;
      });
    expect(kinds).toEqual([
      "mine pick(Go to the map and tap the place) | -",
      "mine(Asks for your location first) pick(Go to the map and tap the place) | -",
      "pick(Go to the map and tap the place) | Location access is turned off for this app, so At my location is not offered.",
      "pick(Go to the map and tap the place) | Your location is not available here, so At my location is not offered.",
      "pick(Go to the map and tap the place) | -"]);
  });

  it("none yet: what a spot is; unreadable: said, never an empty list; no saving: no way to add", () => {
    expect([spotsMenu([], { canAdd: true, canRefresh: true, permission: null }).empty,
            spotsMenu([spot()], { error: "The saved spots could not be read.", canAdd: true, canRefresh: true,
                                  permission: null }),
            spotsMenu([spot()], { canAdd: false, canRefresh: false, permission: "granted" }).adds]).toEqual([
      { title: "No spots yet",
        body: "Pin an exact point on a stream and the app records the gauge, its standing against the record and the " +
              "weather that day. Notes and photographs go on top. Press and hold the map to drop one." },
      { title: "Spots", error: "The saved spots could not be read.", empty: null, rows: [], stale: null,
        adds: [{ k: "pick", label: "Pick on the map", detail: "Go to the map and tap the place" }], note: null },
      [],
    ]);
  });
});

function Probe({ go, locate, out, route = HOME, seen }: {
  go: (a: RouteAction) => void; locate?: Locate | null; out: { r?: MapRail }; route?: Route; seen?: string[];
}) {
  out.r = useMapRail(go, locate ?? null, route);
  const r = out.r;
  const line = `drop:${r.drop ? `${r.drop.lat},${r.drop.lon}` : "-"} perm:${r.permission ?? "-"}` +
               ` err:${r.error ?? "-"} fly:${r.fly ? r.fly.key : "-"}`;
  seen?.push(line);
  return <p>{line}</p>;
}
const SPOTS = reduce(HOME, { type: "tab", tab: "spots" });
const rig = (locate?: Locate | null, route: Route = HOME) => {
  const acts: RouteAction[] = [];
  const out: { r?: MapRail } = {};
  const view = render(<Probe go={(a) => acts.push(a)} locate={locate} out={out} route={route} />);
  const at = (r: Route) => view.rerender(<Probe go={(a) => acts.push(a)} locate={locate} out={out} route={r} />);
  return { acts, out, at, text: () => view.container.textContent };
};

describe("useMapRail", () => {
  it("a long press drops a provisional pin; Cancel removes it and nothing is added", () => {
    const { acts, out, text } = rig();
    act(() => out.r!.pressLong(49.1, -121.9));
    const dropped = text();
    act(() => out.r!.cancelDrop());
    expect([dropped, text(), acts]).toEqual([
      "drop:49.1,-121.9 perm:- err:- fly:-", "drop:- perm:- err:- fly:-", []]);
  });

  it("Continue is the one capture, started at the pin, and the pin goes with it", () => {
    const { acts, out, text } = rig();
    act(() => out.r!.pressLong(49.1, -121.9));
    act(() => out.r!.continueDrop());
    expect([text(), acts]).toEqual(["drop:- perm:- err:- fly:-",
                                    [{ type: "addSpot", at: { lat: 49.1, lon: -121.9 } }]]);
  });

  it("(T48) the Spots TAB asks the platform's permission each time it is entered, never the location itself", async () => {
    const asked: string[] = [];
    const locate: Locate = { permission: async () => { asked.push("permission"); return "prompt"; },
                             here: async () => { asked.push("here"); return { lat: 0, lon: 0 }; } };
    const { at, text } = rig(locate);
    await settle();
    const onMap = [text(), [...asked]];
    at(SPOTS);
    await settle();
    at(HOME);
    at(SPOTS);
    await settle();
    expect([onMap, text(), asked]).toEqual([["drop:- perm:- err:- fly:-", []],
                                            "drop:- perm:prompt err:- fly:-", ["permission", "permission"]]);
  });

  it("At my location: the capture where the device is; a failure is said on the tab and nothing starts", async () => {
    let fail = false;
    const locate: Locate = { permission: async () => "granted",
                             here: async () => { if (fail) throw new Error("timeout"); return { lat: 50, lon: -120 }; } };
    const { acts, out, text } = rig(locate, SPOTS);
    await act(async () => { await out.r!.addMine(); });
    const ok = [text(), [...acts]];
    fail = true;
    await act(async () => { await out.r!.addMine(); });
    expect([ok, text(), acts.length]).toEqual([
      ["drop:- perm:granted err:- fly:-", [{ type: "addSpot", at: { lat: 50, lon: -120 } }]],
      "drop:- perm:granted err:Your location could not be read, so no spot was started. fly:-", 1]);
  });

  it("Show goes to the Map tab, frames the spot, and moves again each time", () => {
    const { acts, out, text } = rig(null, SPOTS);
    act(() => out.r!.show({ id: "s1", lat: 49.0974, lon: -121.9675 }));
    const first = text();
    const box = out.r!.fly!.bbox.map((v) => v.toFixed(4));
    act(() => out.r!.show({ id: "s1", lat: 49.0974, lon: -121.9675 }));
    expect([acts, first, box, text()]).toEqual([
      [{ type: "tab", tab: "map" }, { type: "tab", tab: "map" }],
      "drop:- perm:- err:- fly:spot:s1:1",
      ["-121.9730", "49.0938", "-121.9620", "49.1010"],
      "drop:- perm:- err:- fly:spot:s1:2"]);
  });

  it("PICK ON THE MAP (owner 2026-10-22): from the Spots tab, the Map tab in pick mode; the first tap there places the " +
     "provisional pin and ends pick mode; Continue is the one capture at that point", () => {
    const { acts, out, at, text } = rig(null, SPOTS);
    act(() => out.r!.pick());
    const asked = [...acts];
    at(HOME);
    const picking = [out.r!.picking, text()];
    act(() => out.r!.tapPick(49.2, -122.1));
    const tapped = [out.r!.picking, text()];
    act(() => out.r!.tapPick(49.3, -122.3));        // pick mode is over: a later tap is the map's own again
    act(() => out.r!.continueDrop());
    expect([asked, picking, tapped, out.r!.picking, acts]).toEqual([
      [{ type: "tab", tab: "map" }],
      [true, "drop:- perm:- err:- fly:-"],
      [false, "drop:49.2,-122.1 perm:- err:- fly:-"],
      false,
      [{ type: "tab", tab: "map" }, { type: "addSpot", at: { lat: 49.2, lon: -122.1 } }]]);
  });

  it("pick mode: Cancel leaves it with nothing; the route moving anywhere else forgets it; a long press ends it too", () => {
    const { acts, out, at, text } = rig(null, HOME);
    act(() => out.r!.pick());                       // already on the Map tab: no move
    const on = out.r!.picking;
    act(() => out.r!.cancelPick());
    const cancelled = [out.r!.picking, text()];
    act(() => out.r!.pick());
    at(reduce(HOME, { type: "tab", tab: "conditions" }));
    const moved = out.r!.picking;
    at(HOME);
    const back = out.r!.picking;
    act(() => out.r!.pick());
    act(() => out.r!.pressLong(49.1, -121.9));
    expect([on, cancelled, moved, back, [out.r!.picking, text()], acts]).toEqual([
      true, [false, "drop:- perm:- err:- fly:-"], false, false, [false, "drop:49.1,-121.9 perm:- err:- fly:-"], []]);
  });

  it("the route moving anywhere forgets the pin and the framing — on that same render, so a map remounted on return is " +
     "never framed on an old spot again — EXCEPT the Show it was asked for, which lands on the Map tab", () => {
    const seen: string[] = [];
    const out: { r?: MapRail } = {};
    const at = (route: Route) => <Probe go={() => {}} out={out} route={route} seen={seen} />;
    const { rerender } = render(at(SPOTS));
    act(() => out.r!.show({ id: "s1", lat: 49.0974, lon: -121.9675 }));
    rerender(at(HOME));                                       // Show's own move: the framing survives it
    const landed = seen[seen.length - 1];
    act(() => out.r!.pressLong(49.1, -121.9));
    const before = seen[seen.length - 1];
    seen.length = 0;
    const spotOpen = reduce(HOME, { type: "openSpot", id: "s2" });
    rerender(at(spotOpen));
    rerender(at(reduce(spotOpen, { type: "back" })));
    const conditions = reduce(HOME, { type: "tab", tab: "conditions" });
    rerender(at(conditions));
    expect([landed, before, [...new Set(seen)]]).toEqual([
      "drop:- perm:- err:- fly:spot:s1:1",
      "drop:49.1,-121.9 perm:- err:- fly:spot:s1:1",
      ["drop:- perm:- err:- fly:-"]]);
  });

  it("a spot's own Show (its screen, over any tab) lands on the Map tab the same way", () => {
    const seen: string[] = [];
    const out: { r?: MapRail } = {};
    const acts: RouteAction[] = [];
    const at = (route: Route) => <Probe go={(a) => acts.push(a)} out={out} route={route} seen={seen} />;
    const spotOnCond = reduce(reduce(HOME, { type: "tab", tab: "conditions" }), { type: "openSpot", id: "s1" });
    const { rerender } = render(at(spotOnCond));
    act(() => out.r!.show({ id: "s1", lat: 49.0974, lon: -121.9675 }));
    rerender(at(reduce(spotOnCond, acts[0]!)));
    expect([acts, seen[seen.length - 1]]).toEqual([[{ type: "tab", tab: "map" }], "drop:- perm:- err:- fly:spot:s1:1"]);
  });
});
