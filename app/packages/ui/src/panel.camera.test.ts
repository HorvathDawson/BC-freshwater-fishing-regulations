import { describe, expect, it } from "vitest";
import { metresApart, panelCamera, SAME_PLACE_M } from "@app/core";

describe("framing a whole panel", () => {
  it("frames nothing when no donor has a coordinate", () => {
    expect(panelCamera([{ lat: null, lon: null }])).toBeNull();
    expect(panelCamera([])).toBeNull();
  });

  it("centres on the spread of every donor, not on the first", () => {
    // Three gauges strung west to east; the camera must sit in the middle of them, which a
    // single-gauge camera would not.
    const c = panelCamera([{ lat: 49, lon: -123 }, { lat: 49, lon: -121 },
                           { lat: 49, lon: -122 }])!;
    expect(c.lon).toBeCloseTo(-122, 6);
    expect(c.lat).toBeCloseTo(49, 6);
  });

  it("includes the spot itself in the frame", () => {
    const gauges = [{ lat: 49, lon: -122 }];
    const alone = panelCamera(gauges)!;
    const withSpot = panelCamera(gauges, { lat: 50, lon: -122 })!;
    expect(withSpot.lat).toBeCloseTo(49.5, 6);
    expect(withSpot.zoom).toBeLessThan(alone.zoom);   // pulled back to hold both
  });

  it("zooms out as the panel spreads", () => {
    const tight = panelCamera([{ lat: 49, lon: -122 }, { lat: 49.05, lon: -122.05 }])!;
    const wide = panelCamera([{ lat: 49, lon: -122 }, { lat: 52, lon: -126 }])!;
    expect(wide.zoom).toBeLessThan(tight.zoom);
  });

  it("stays inside the tile range at both extremes", () => {
    const same = panelCamera([{ lat: 49, lon: -122 }, { lat: 49, lon: -122 }])!;
    const huge = panelCamera([{ lat: 20, lon: -170 }, { lat: 70, lon: -50 }])!;
    for (const c of [same, huge]) {
      expect(c.zoom).toBeGreaterThanOrEqual(5);
      expect(c.zoom).toBeLessThanOrEqual(13);
    }
  });

  it("ignores donors with no coordinate rather than framing zero", () => {
    // A station ECCC published no position for must not drag the camera to 0,0 — the
    // Gulf of Guinea is a long way from the Chilliwack.
    const c = panelCamera([{ lat: 49, lon: -122 }, { lat: null, lon: null }])!;
    expect(c.lat).toBeCloseTo(49, 6);
    expect(c.lon).toBeCloseTo(-122, 6);
  });
});


describe("telling two marks apart", () => {
  it("calls a gauge on the tapped spot the same place", () => {
    // The common case the map got wrong: you tap the river AT the station, and two pins
    // land on one pixel so the map looks like it lost one.
    const spot = { lat: 49.0961, lon: -121.9583 };
    const gauge = { lat: 49.0962, lon: -121.9584 };
    expect(metresApart(spot, gauge)).toBeLessThan(SAME_PLACE_M);
  });

  it("keeps a gauge a few kilometres off as its own mark", () => {
    expect(metresApart({ lat: 49.0, lon: -122.0 }, { lat: 49.03, lon: -122.0 }))
      .toBeGreaterThan(SAME_PLACE_M);
  });

  it("is symmetric and zero at a point", () => {
    const a = { lat: 50, lon: -120 }, b = { lat: 51, lon: -121 };
    expect(metresApart(a, b)).toBeCloseTo(metresApart(b, a), 6);
    expect(metresApart(a, a)).toBe(0);
  });

  it("narrows longitude by latitude, as the map does", () => {
    // A degree of longitude is ~111 km at the equator and ~72 km at BC's latitude. Treating
    // them alike would put the "same place" radius half again too wide up here.
    const nearEq = metresApart({ lat: 0, lon: 0 }, { lat: 0, lon: 1 });
    const inBC = metresApart({ lat: 50, lon: -122 }, { lat: 50, lon: -121 });
    expect(inBC).toBeLessThan(nearEq * 0.7);
  });
});
