/**
 * T131 — YOUR SPOTS ON THE MAP: which marks a map draws at a zoom (a spot alone, or several close spots as one group),
 * which carry their name, which is selected, what each says aloud, and what a tap on each does. Whole values only.
 */
import { describe, expect, it } from "vitest";
import { CAMERA_MAX_ZOOM } from "./ladder";
import { SPOT_PIN, SPOT_PIN_WORDS, spotBack, spotFace, spotMarkers, spotPress, spotUnder, type SpotMarker,
         type SpotPoint } from "./spotPins";

const TAMIHI: SpotPoint = { id: "s1", lat: 49.0974, lon: -121.9675, title: "Tamihi run" };
const VEDDER: SpotPoint = { id: "s2", lat: 49.1, lon: -122.0, title: "Vedder River" };
/** Far from both: the Thompson at Spences Bridge. */
const THOMPSON: SpotPoint = { id: "s3", lat: 50.4189, lon: -121.3482, title: "Spences Bridge" };
/** Exactly where Tamihi run is: no zoom ever splits these two. */
const TWIN: SpotPoint = { id: "s4", lat: 49.0974, lon: -121.9675, title: "Tamihi again" };

const one = (p: SpotPoint, label: string | null, selected = false): SpotMarker => ({
  key: `spot:${p.id}`, ids: [p.id], lat: p.lat, lon: p.lon, label, selected, count: 1, splitZoom: null,
  a11y: SPOT_PIN_WORDS.open(p.title),
});

describe("the policy", () => {
  it("is one set of numbers: a finger-sized (invisible) target, SMALL faces under a cap, names from the reach zoom " +
     "in, close spots grouped (T145)", () => {
    expect(SPOT_PIN).toEqual({ hit: 44, face: 12, groupFace: 18, frame: 2, ring: 4, max: 26, groupPx: 32,
                               labelZoom: 12, labelMaxWidth: 120, labelHeight: 15, labelGap: 3 });
    expect(SPOT_PIN.hit).toBeGreaterThanOrEqual(44);
  });

  it("says each mark in words: a spot opens; a group zooms in, or lists when no zoom parts it", () => {
    expect([SPOT_PIN_WORDS.open("Tamihi run"), SPOT_PIN_WORDS.zoom(3), SPOT_PIN_WORDS.list(2)]).toEqual([
      "Open spot Tamihi run", "3 spots here. Zoom in to see them", "2 spots at this place. Open the Spots list"]);
  });
});

describe("spotMarkers", () => {
  it("CLOSE UP: every spot its own mark with its name, in id order; the selected one says so", () => {
    expect(spotMarkers([VEDDER, TAMIHI, THOMPSON], 12, "s2")).toEqual([
      one(TAMIHI, "Tamihi run"), one(VEDDER, "Vedder River", true), one(THOMPSON, "Spences Bridge")]);
  });

  it("FURTHER OUT, BELOW THE NAME ZOOM: the same marks with no names", () => {
    expect(spotMarkers([TAMIHI, VEDDER, THOMPSON], 10, null)).toEqual([
      one(TAMIHI, null), one(VEDDER, null), one(THOMPSON, null)]);
  });

  it("FAR OUT: two spots under 32 px apart are ONE group at their middle, with the zoom that parts them", () => {
    expect(spotMarkers([TAMIHI, VEDDER, THOMPSON], 9, null)).toEqual([
      { key: "group:s1,s2", ids: ["s1", "s2"], lat: (TAMIHI.lat + VEDDER.lat) / 2, lon: (TAMIHI.lon + VEDDER.lon) / 2,
        label: null, selected: false, count: 2, splitZoom: 9.5, a11y: "2 spots here. Zoom in to see them" },
      one(THOMPSON, null)]);
  });

  it("a group holding the selected spot is selected", () => {
    expect(spotMarkers([TAMIHI, VEDDER], 9, "s2").map((m) => [m.key, m.selected])).toEqual([["group:s1,s2", true]]);
  });

  it("the zoom that parts a group really parts it, and one half-step less does not", () => {
    const g = spotMarkers([TAMIHI, VEDDER], 9, null)[0]!;
    expect([spotMarkers([TAMIHI, VEDDER], g.splitZoom!, null).length,
            spotMarkers([TAMIHI, VEDDER], g.splitZoom! - 0.5, null).length]).toEqual([2, 1]);
  });

  it("TWO SPOTS ON ONE POINT: a group no zoom parts (splitZoom null), even at the camera's deepest", () => {
    expect(spotMarkers([TAMIHI, TWIN], CAMERA_MAX_ZOOM, null)).toEqual([
      { key: "group:s1,s4", ids: ["s1", "s4"], lat: TAMIHI.lat, lon: TAMIHI.lon, label: null, selected: false, count: 2,
        splitZoom: null, a11y: "2 spots at this place. Open the Spots list" }]);
  });

  it("NAMES DO NOT OVERLAP: of two spots whose names would cross, the selected keeps its name", () => {
    // 60 px apart at z12: the marks are apart (> 32 px), their names (~110 px wide) are not
    const a: SpotPoint = { id: "a", lat: 49.5, lon: -122, title: "Upper canyon pool" };
    const b: SpotPoint = { id: "b", lat: 49.5, lon: -122 + 60 / (512 * 2 ** 12) * 360, title: "Lower canyon pool" };
    expect([spotMarkers([a, b], 12, null).map((m) => m.label), spotMarkers([a, b], 12, "b").map((m) => m.label)])
      .toEqual([["Upper canyon pool", null], [null, "Lower canyon pool"]]);
  });

  it("THE SELECTED MARK IS ITS DRAWN SIZE: a name that would cross the selected (ringed) mark is dropped, one that " +
     "would cross only a plain face's size is not mistaken for clear", () => {
    // N sits 33.5 px straight below S at z12 (two marks: > 32 px). N's name (15 px tall, 3 px above N's 6 px half-face)
    // tops out 8.5 px below S's centre — inside the selected mark (half 10), outside a plain face (half 6).
    const z = 12, world = 512 * 2 ** z;
    const at = (x: number, y: number) => ({
      lon: (x / world) * 360 - 180,
      lat: (Math.atan(Math.sinh(Math.PI * (1 - (2 * y) / world))) * 180) / Math.PI });
    const x0 = world * 0.16, y0 = world * 0.35;
    const s: SpotPoint = { id: "a", ...at(x0, y0), title: "S" };
    const n: SpotPoint = { id: "b", ...at(x0, y0 + 33.5), title: "A name long enough to sit over S" };
    expect([spotMarkers([s, n], z, null).map((m) => m.label), spotMarkers([s, n], z, "a").map((m) => m.label)])
      .toEqual([["S", n.title], ["S", null]]);
  });

  it("spotFace: a spot 12, a group 18, the selected ringed 8 px wider — whole values", () => {
    expect([spotFace({ count: 1, selected: false }), spotFace({ count: 1, selected: true }),
            spotFace({ count: 3, selected: false }), spotFace({ count: 3, selected: true })]).toEqual([
      { face: 12, extent: 12 }, { face: 12, extent: 20 }, { face: 18, extent: 18 }, { face: 18, extent: 26 }]);
  });

  it("CAPPED AT EVERY ZOOM (owner: \"they get way too big\"): no mark, at any zoom from the province to the camera's " +
     "deepest, selected or not, alone or grouped, is drawn past SPOT_PIN.max", () => {
    const pts = [TAMIHI, VEDDER, THOMPSON, TWIN];
    const extents = new Set<number>();
    for (let z = 4; z <= CAMERA_MAX_ZOOM; z += 0.5)
      for (const sel of [null, "s1", "s3"])
        for (const m of spotMarkers(pts, z, sel)) extents.add(spotFace(m).extent);
    expect([[...extents].sort((a, b) => a - b), Math.max(...extents) <= SPOT_PIN.max]).toEqual([[12, 18, 20, 26], true]);
  });

  it("a long name is the label whole (the face clips nothing); none at all for no spots", () => {
    const long: SpotPoint = { ...THOMPSON, title: "The long run below the railway bridge at Spences Bridge" };
    expect([spotMarkers([long], 14, null)[0]!.label, spotMarkers([], 14, null)]).toEqual([long.title, []]);
  });

  it("is the same whatever order the spots come in", () => {
    expect(spotMarkers([THOMPSON, VEDDER, TAMIHI], 9, null)).toEqual(spotMarkers([TAMIHI, VEDDER, THOMPSON], 9, null));
  });
});

describe("spotUnder: the mark a tap lands on (T145: a tap on a spot is never also the map's)", () => {
  it("on a mark, or anywhere in its 44 px target: that mark; just outside, none; between two, the nearer", () => {
    const z = 14, world = 512 * 2 ** z;
    const shift = (p: SpotPoint, dx: number) => ({ lat: p.lat, lon: p.lon + (dx / world) * 360 });
    const marks = spotMarkers([TAMIHI, VEDDER, THOMPSON], z, null);
    const under = (q: { lat: number; lon: number }) => spotUnder(marks, q.lat, q.lon, z)?.key ?? null;
    expect([under(TAMIHI), under(shift(TAMIHI, 21)), under(shift(TAMIHI, 23)), under(shift(TAMIHI, -21)),
            spotUnder([], TAMIHI.lat, TAMIHI.lon, z)]).toEqual(["spot:s1", "spot:s1", null, "spot:s1", null]);
  });

  it("two marks whose targets overlap: the tap goes to the nearer", () => {
    const z = 14, world = 512 * 2 ** z;
    const b: SpotPoint = { id: "b", lat: TAMIHI.lat, lon: TAMIHI.lon + (36 / world) * 360, title: "B" };
    const marks = spotMarkers([TAMIHI, b], z, null);
    const at = (dx: number) => spotUnder(marks, TAMIHI.lat, TAMIHI.lon + (dx / world) * 360, z)?.key ?? null;
    expect([marks.length, at(10), at(26)]).toEqual([2, "spot:s1", "spot:b"]);
  });
});

describe("spotBack: a spot's ‹ names where back goes", () => {
  it("the tab it is over, by name; over a water, Back", () => {
    expect([spotBack("Spots"), spotBack("Map"), spotBack("Conditions"), spotBack(null)])
      .toEqual(["Spots", "Map", "Conditions", "Back"]);
  });
});

describe("spotPress: what a tap on a mark does", () => {
  it("a spot opens; a group zooms to where it parts, at its middle; an unpartable group lists", () => {
    const [group, alone] = spotMarkers([TAMIHI, VEDDER, THOMPSON], 9, null);
    const twin = spotMarkers([TAMIHI, TWIN], 14, null)[0]!;
    expect([spotPress(alone!), spotPress(group!), spotPress(twin)]).toEqual([
      { kind: "open", id: "s3" },
      { kind: "zoom", camera: { lat: group!.lat, lon: group!.lon, zoom: 9.5 } },
      { kind: "list", ids: ["s1", "s4"] },
    ]);
  });
});
