/**
 * T131 — YOUR SPOTS ON THE MAP: which marks a map draws at a zoom (a spot alone, or several close spots as one group),
 * which carry their name, which is selected, what each says aloud, and what a tap on each does. Whole values only.
 */
import { describe, expect, it } from "vitest";
import { CAMERA_MAX_ZOOM } from "./ladder";
import { SPOT_PIN, SPOT_PIN_WORDS, spotBack, spotMarkers, spotPress, type SpotMarker, type SpotPoint } from "./spotPins";

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
  it("is one set of numbers: a finger-sized target, names from the valley zoom in, close spots grouped", () => {
    expect(SPOT_PIN).toEqual({ hit: 44, face: 30, selectedFace: 36, icon: 18, groupPx: 40, labelZoom: 11,
                               labelMaxWidth: 160, labelGap: 6 });
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

  it("FAR OUT: two spots under 40 px apart are ONE group at their middle, with the zoom that parts them", () => {
    expect(spotMarkers([TAMIHI, VEDDER, THOMPSON], 9, null)).toEqual([
      { key: "group:s1,s2", ids: ["s1", "s2"], lat: (TAMIHI.lat + VEDDER.lat) / 2, lon: (TAMIHI.lon + VEDDER.lon) / 2,
        label: null, selected: false, count: 2, splitZoom: 10, a11y: "2 spots here. Zoom in to see them" },
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
    // 60 px apart at z12: the marks are apart (> 40 px), their names (~100 px wide) are not
    const a: SpotPoint = { id: "a", lat: 49.5, lon: -122, title: "Upper canyon pool" };
    const b: SpotPoint = { id: "b", lat: 49.5, lon: -122 + 60 / (512 * 2 ** 12) * 360, title: "Lower canyon pool" };
    expect([spotMarkers([a, b], 12, null).map((m) => m.label), spotMarkers([a, b], 12, "b").map((m) => m.label)])
      .toEqual([["Upper canyon pool", null], [null, "Lower canyon pool"]]);
  });

  it("a long name is the label whole (the face clips nothing); none at all for no spots", () => {
    const long: SpotPoint = { ...THOMPSON, title: "The long run below the railway bridge at Spences Bridge" };
    expect([spotMarkers([long], 14, null)[0]!.label, spotMarkers([], 14, null)]).toEqual([long.title, []]);
  });

  it("is the same whatever order the spots come in", () => {
    expect(spotMarkers([THOMPSON, VEDDER, TAMIHI], 9, null)).toEqual(spotMarkers([TAMIHI, VEDDER, THOMPSON], 9, null));
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
      { kind: "zoom", camera: { lat: group!.lat, lon: group!.lon, zoom: 10 } },
      { kind: "list", ids: ["s1", "s4"] },
    ]);
  });
});
