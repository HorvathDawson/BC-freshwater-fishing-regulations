import { describe, expect, it } from "vitest";
import { CAMERA_BOUNDS } from "./ladder";
import { NEAR_BC_MARGIN_KM, nearBC } from "./nearBc";
import { NEAR_BC_RING } from "./nearBcOutline";

// distances to the province's edge measured in BC Albers against data/source/bc_boundary.geojson (T864)
const INSIDE = { Vancouver: [49.28, -123.12], Masset: [54.01, -132.15], Atlin: [59.58, -133.69], "Fort Nelson": [58.81, -122.69] };
const BORDER = {
  "Bellingham (28 km)": [48.75, -122.48], "Jasper (21 km)": [52.87, -118.08], "Watson Lake (7 km)": [60.06, -128.71],
  "Banff (17 km)": [51.18, -115.57], "Oroville (7 km)": [48.94, -119.43], "Port Angeles (14 km)": [48.12, -123.43],
  "Skagway (12 km)": [59.46, -135.31],
};
const FAR = {
  "Calgary (85 km)": [51.05, -114.07], "Whitehorse (80 km)": [60.72, -135.06], "Seattle (102 km)": [47.61, -122.33],
  "Spokane (149 km)": [47.66, -117.43], Toronto: [43.65, -79.38], London: [51.51, -0.13], Sydney: [-33.87, 151.21],
  "date line, east side": [52.0, 179.95], "date line, west side": [-17.0, -179.95], "Attu, Alaska": [52.9, 173.2],
  "null island": [0, 0],
};
const at = ([lat, lon]: number[]) => ({ lat: lat!, lon: lon! });

describe("(T864) nearBC: is the device somewhere the app can help", () => {
  it("the outline: one closed ring, ~50 km margin, inside the camera's bounds", () => {
    expect(NEAR_BC_MARGIN_KM).toBe(50);
    expect(NEAR_BC_RING[0]).toEqual(NEAR_BC_RING[NEAR_BC_RING.length - 1]);
    expect(NEAR_BC_RING.length).toBeLessThan(120);
    for (const [lon, lat] of NEAR_BC_RING) {
      expect(lon).toBeGreaterThan(CAMERA_BOUNDS[0][0]); expect(lon).toBeLessThan(CAMERA_BOUNDS[1][0]);
      expect(lat).toBeGreaterThan(CAMERA_BOUNDS[0][1]); expect(lat).toBeLessThan(CAMERA_BOUNDS[1][1]);
    }
  });

  it("in B.C. and in border towns within ~50 km: near", () => {
    const got = Object.fromEntries(Object.entries({ ...INSIDE, ...BORDER }).map(([k, p]) => [k, nearBC(at(p))]));
    expect(Object.values(got).every(Boolean)).toBe(true);
  });

  it("further than the margin, across the world, over the date line, and null island: not near", () => {
    const got = Object.fromEntries(Object.entries(FAR).map(([k, p]) => [k, nearBC(at(p))]));
    expect(Object.entries(got).filter(([, v]) => v)).toEqual([]);
  });

  it("nothing, not a number, or off the globe: not near (never throws)", () => {
    for (const p of [null, undefined, { lat: NaN, lon: -123 }, { lat: 49, lon: Infinity }, { lat: 91, lon: -123 },
                     { lat: 49, lon: -483 }]) expect(nearBC(p)).toBe(false);
  });
});
