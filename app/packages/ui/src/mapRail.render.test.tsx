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
  it("your spots as rows, the map's middle then your location (granted), the stale line", () => {
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
      adds: [{ k: "here", label: "Add a spot here", detail: "At the middle of the map" },
             { k: "mine", label: "At my location", detail: "" }],
    });
  });

  it("your location: offered when granted, said to ask when the platform would, never when refused or absent", () => {
    const kinds = (["granted", "prompt", "denied", "unavailable", null] as (LocatePermission | null)[])
      .map((permission) => spotsMenu([], { canAdd: true, canRefresh: false, permission }).adds
        .map((a) => `${a.k}${a.detail ? `(${a.detail})` : ""}`).join(" "));
    expect(kinds).toEqual([
      "here(At the middle of the map) mine",
      "here(At the middle of the map) mine(Asks for your location first)",
      "here(At the middle of the map)", "here(At the middle of the map)", "here(At the middle of the map)"]);
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
        adds: [{ k: "here", label: "Add a spot here", detail: "At the middle of the map" }] },
      [],
    ]);
  });
});

function Probe({ go, locate, out, route = HOME, seen }: {
  go: (a: RouteAction) => void; locate?: Locate | null; out: { r?: MapRail }; route?: Route; seen?: string[];
}) {
  out.r = useMapRail(go, locate ?? null, route);
  const r = out.r;
  const line = `open:${r.spotsOpen} drop:${r.drop ? `${r.drop.lat},${r.drop.lon}` : "-"} perm:${r.permission ?? "-"}` +
               ` err:${r.error ?? "-"} fly:${r.fly ? r.fly.key : "-"}`;
  seen?.push(line);
  return <p>{line}</p>;
}
const rig = (locate?: Locate | null) => {
  const acts: RouteAction[] = [];
  const out: { r?: MapRail } = {};
  const { container } = render(<Probe go={(a) => acts.push(a)} locate={locate} out={out} />);
  return { acts, out, text: () => container.textContent };
};

describe("useMapRail", () => {
  it("a long press drops a provisional pin; Cancel removes it and nothing is added", () => {
    const { acts, out, text } = rig();
    act(() => out.r!.pressLong(49.1, -121.9));
    const dropped = text();
    act(() => out.r!.cancelDrop());
    expect([dropped, text(), acts]).toEqual([
      "open:false drop:49.1,-121.9 perm:- err:- fly:-", "open:false drop:- perm:- err:- fly:-", []]);
  });

  it("Continue is the one capture, started at the pin, and the pin goes with it", () => {
    const { acts, out, text } = rig();
    act(() => out.r!.pressLong(49.1, -121.9));
    act(() => out.r!.continueDrop());
    expect([text(), acts]).toEqual(["open:false drop:- perm:- err:- fly:-",
                                    [{ type: "addSpot", at: { lat: 49.1, lon: -121.9 } }]]);
  });

  it("the menu asks the platform's permission each time it opens, never the location itself", async () => {
    const asked: string[] = [];
    const locate: Locate = { permission: async () => { asked.push("permission"); return "prompt"; },
                             here: async () => { asked.push("here"); return { lat: 0, lon: 0 }; } };
    const { out, text } = rig(locate);
    const before = text();
    act(() => out.r!.openSpots());
    await settle();
    act(() => out.r!.closeSpots());
    act(() => out.r!.openSpots());
    await settle();
    expect([before, text(), asked]).toEqual(["open:false drop:- perm:- err:- fly:-",
                                             "open:true drop:- perm:prompt err:- fly:-", ["permission", "permission"]]);
  });

  it("At my location: the capture where the device is; a failure is said and nothing starts", async () => {
    let fail = false;
    const locate: Locate = { permission: async () => "granted",
                             here: async () => { if (fail) throw new Error("timeout"); return { lat: 50, lon: -120 }; } };
    const { acts, out, text } = rig(locate);
    act(() => out.r!.openSpots());
    await act(async () => { await out.r!.addMine(); });
    const ok = [text(), [...acts]];
    act(() => out.r!.openSpots());
    fail = true;
    await act(async () => { await out.r!.addMine(); });
    expect([ok, text(), acts.length]).toEqual([
      ["open:false drop:- perm:granted err:- fly:-", [{ type: "addSpot", at: { lat: 50, lon: -120 } }]],
      "open:true drop:- perm:granted err:Your location could not be read, so no spot was started. fly:-", 1]);
  });

  it("Add a spot here: the capture at the point given; Show frames the spot, closes the menu, and moves again each time", () => {
    const { acts, out, text } = rig();
    act(() => out.r!.openSpots());
    act(() => out.r!.addAt({ lat: 49, lon: -122 }));
    act(() => out.r!.openSpots());
    act(() => out.r!.show({ id: "s1", lat: 49.0974, lon: -121.9675 }));
    const first = text();
    const box = out.r!.fly!.bbox.map((v) => v.toFixed(4));
    act(() => out.r!.show({ id: "s1", lat: 49.0974, lon: -121.9675 }));
    expect([acts, first, box, text()]).toEqual([
      [{ type: "addSpot", at: { lat: 49, lon: -122 } }],
      "open:false drop:- perm:- err:- fly:spot:s1:1",
      ["-121.9730", "49.0938", "-121.9620", "49.1010"],
      "open:false drop:- perm:- err:- fly:spot:s1:2"]);
  });

  it("the route moving anywhere forgets the pin, the framing and the menu — on that same render, so a map remounted " +
     "on return is never framed on an old spot again", () => {
    const seen: string[] = [];
    const out: { r?: MapRail } = {};
    const at = (route: Route) => <Probe go={() => {}} out={out} route={route} seen={seen} />;
    const { rerender } = render(at(HOME));
    act(() => out.r!.show({ id: "s1", lat: 49.0974, lon: -121.9675 }));
    act(() => out.r!.pressLong(49.1, -121.9));
    act(() => out.r!.openSpots());
    const before = seen[seen.length - 1];
    seen.length = 0;
    const spotOpen = reduce(HOME, { type: "openSpot", id: "s2" });
    rerender(at(spotOpen));
    rerender(at(reduce(spotOpen, { type: "back" })));
    const conditions = reduce(HOME, { type: "tab", tab: "conditions" });
    act(() => out.r!.show({ id: "s1", lat: 49.0974, lon: -121.9675 }));
    rerender(at(conditions));
    expect([before, [...new Set(seen)]]).toEqual([
      "open:true drop:49.1,-121.9 perm:- err:- fly:spot:s1:1",
      ["open:false drop:- perm:- err:- fly:-", "open:false drop:- perm:- err:- fly:spot:s1:2"]]);
  });
});
