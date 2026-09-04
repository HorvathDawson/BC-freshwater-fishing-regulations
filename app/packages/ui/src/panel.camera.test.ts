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
    // The MERCATOR midpoint, not the arithmetic one — a degree of latitude is taller in
    // pixels the further north it sits, so the centre that actually splits the frame in
    // half is a little north of the average.
    expect(withSpot.lat).toBeCloseTo(49.503, 3);
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
      expect(c.zoom).toBeGreaterThanOrEqual(4);
      expect(c.zoom).toBeLessThanOrEqual(13);
    }
  });

  /**
   * Project a point the way MapLibre does and say where it lands, in CSS pixels from the
   * top-left of the map. This is the assertion the old camera could not pass: it fitted
   * `max(latitude span, longitude span)` in DEGREES against one assumed viewport size, and
   * a donor on the Fraser landed 136 px above the top of a 210 px map.
   */
  function screenXY(cam: { lon: number; lat: number; zoom: number },
                    p: { lat: number; lon: number },
                    size: { width: number; height: number }) {
    const toY = (lat: number) => {
      const f = (lat * Math.PI) / 180;
      return (1 - Math.log(Math.tan(f) + 1 / Math.cos(f)) / Math.PI) / 2;
    };
    // 512 is MapLibre's vector tile size, so its world is `512 * 2^zoom`. This helper
    // read 256 and so agreed with a camera that was one zoom level too close — the test
    // passed while the pins fell off the map. A model of the renderer has to be the
    // renderer's own arithmetic or it certifies the bug.
    const world = 512 * 2 ** cam.zoom;
    return {
      x: size.width / 2 + ((p.lon + 180) / 360 - (cam.lon + 180) / 360) * world,
      y: size.height / 2 + (toY(p.lat) - toY(cam.lat)) * world,
    };
  }

  it("puts every donor inside a map that is wider than it is tall", () => {
    // The real shape: the Fraser's panel spans Spences Bridge to Shelley, which is a
    // north-south panel on a 380x210 map — the axis with the least room.
    const size = { width: 380, height: 210 };
    const pts = [{ lat: 50.42, lon: -121.35 }, { lat: 54.0, lon: -122.65 },
                 { lat: 52.1, lon: -122.1 }];
    const spot = { lat: 49.38, lon: -121.44 };
    const cam = panelCamera(pts, spot, size)!;
    for (const p of [...pts, spot]) {
      const { x, y } = screenXY(cam, p, size);
      expect(x).toBeGreaterThanOrEqual(0);
      expect(x).toBeLessThanOrEqual(size.width);
      expect(y).toBeGreaterThanOrEqual(0);
      expect(y).toBeLessThanOrEqual(size.height);
    }
  });

  it("puts every donor inside a map that is taller than it is wide", () => {
    // The other axis has to be the constraint when the panel runs east-west.
    const size = { width: 200, height: 400 };
    const pts = [{ lat: 49.2, lon: -125.0 }, { lat: 49.3, lon: -118.0 }];
    const cam = panelCamera(pts, { lat: 49.25, lon: -121.5 }, size)!;
    for (const p of pts) {
      const { x, y } = screenXY(cam, p, size);
      expect(x).toBeGreaterThanOrEqual(0);
      expect(x).toBeLessThanOrEqual(size.width);
      expect(y).toBeGreaterThanOrEqual(0);
      expect(y).toBeLessThanOrEqual(size.height);
    }
  });

  it("accounts for Mercator stretch, which grows with latitude", () => {
    // The same span of latitude needs a lower zoom the further north it is drawn. The old
    // camera treated degrees as pixels and drew a northern panel about half again too big.
    const size = { width: 380, height: 210 };
    const south = panelCamera([{ lat: 0, lon: 0 }, { lat: 2, lon: 0 }], null, size)!;
    const north = panelCamera([{ lat: 58, lon: 0 }, { lat: 60, lon: 0 }], null, size)!;
    expect(north.zoom).toBeLessThan(south.zoom);
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
