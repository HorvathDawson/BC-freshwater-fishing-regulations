import { describe, expect, it } from "vitest";
import {
  bboxOfGeometry, boxAround, describeBy, distanceKm, duplicateNames, fixOf, matchTier,
  normalise, rankWaters, refineFit, trilaterate, wherePhrase, type Bbox,
} from "./search";

const w = (name: string, size: number | null, pieces = 1, matchedAs: string | null = null) =>
  ({ name, size, pieces, matchedAs });

describe("the match ladder", () => {
  it("normalises case, accents and spacing", () => {
    expect(normalise("  Chilliwack   RIVER ")).toBe("chilliwack river");
    expect(normalise("Th'ewá:lí")).toBe("thewa:li");
  });

  it("orders exact, prefix, word, anywhere — name ahead of alias at each step", () => {
    expect(matchTier("chilliwack river", "Chilliwack River")).toBe(0);
    expect(matchTier("chilliwack riv", "Chilliwack River")).toBe(2);
    expect(matchTier("chilliwack", "Upper Chilliwack River")).toBe(4);
    expect(matchTier("illiwa", "Chilliwack River")).toBe(6);
    expect(matchTier("vedder", "Chilliwack River", "Vedder River")).toBe(3);
    expect(matchTier("kettle", "Nowhere Creek")).toBeNull();
  });

  it("an exact alias ties with a name prefix instead of beating it", () => {
    // Two ponds carry the alias "Elk"; the query "elk" must not put them above the river.
    expect(matchTier("elk", "Elk Lake", "Elk")).toBe(matchTier("elk", "Elk River"));
  });
});

describe("rankWaters", () => {
  it("prefers the better match over the bigger water", () => {
    const got = rankWaters("elk", [w("Welkin Creek", 90000), w("Elk Lake", null)]);
    expect(got.map((h) => h.name)).toEqual(["Elk Lake", "Welkin Creek"]);
    const got2 = rankWaters("elk", [w("Big Elk Creek", 90000), w("Elk Lake", null)]);
    expect(got2.map((h) => h.name)).toEqual(["Elk Lake", "Big Elk Creek"]);
  });

  it("among equal matches, the bigger water first — duplicates included", () => {
    const got = rankWaters("elk", [
      w("Elk Lake", null, 1, "Elk"), w("Elk River", 796, 14), w("Elk River", 7525, 62),
      w("Elkin Creek", 148, 10),
    ]);
    expect(got.map((h) => `${h.name}:${h.size}`)).toEqual(
      ["Elk River:7525", "Elk River:796", "Elkin Creek:148", "Elk Lake:null"]);
  });

  it("the exact name wins whatever its size", () => {
    const got = rankWaters("fraser lake", [w("Fraser Lake Creek", 400), w("Fraser Lake", null)]);
    expect(got[0]!.name).toBe("Fraser Lake");
  });

  it("is total: equal candidates sort the same way every time", () => {
    const a = [w("Twin Lakes", null), w("Twin Lake", null)];
    expect(rankWaters("twin", a)).toEqual(rankWaters("twin", [...a].reverse()));
    expect(rankWaters("twin", a)[0]!.name).toBe("Twin Lake");
  });

  it("flags names shown more than once, so each copy carries a where", () => {
    expect(duplicateNames([{ name: "Elk River" }, { name: "elk river" }, { name: "Elk Lake" }]))
      .toEqual(new Set(["elk river"]));
  });
});

describe("where a water is described from", () => {
  const p = (name: string, kind: string, km: number) => ({ name, kind, km, lat: 0, lon: 0 });

  it("a real town a little further off beats a locality on the bank", () => {
    // The Elk River's nearest names, from the province bundle.
    const got = describeBy([p("Mosquito Flats", "locality", 0.1), p("Elko", "village", 0.23),
                            p("Fernie", "city", 0.44)]);
    expect(got!.name).toBe("Fernie");
  });

  it("but not one twenty kilometres away", () => {
    expect(describeBy([p("Stump Flat", "locality", 0.2), p("Big City", "city", 20)])!.name)
      .toBe("Stump Flat");
  });

  it("says so in one composed phrase", () => {
    expect(wherePhrase({ name: "Fernie", km: 0.44 })).toBe("near Fernie · under 1 km");
    expect(wherePhrase({ name: "Gold River", km: 12.06 })).toBe("near Gold River · 12 km");
    expect(wherePhrase({ name: "Hope", km: 0 })).toBe("at Hope");
    expect(wherePhrase(null)).toBeNull();
  });
});

describe("placing a water from what the bundle knows", () => {
  it("points on the water win outright", () => {
    const e = fixOf({ on: [{ lat: 49.1, lon: -121.9 }, { lat: 49.05, lon: -121.5 }],
                      near: [{ lat: 55, lon: -127, km: 3 }] });
    expect(e!.basis).toBe("exact");
    expect(e!.bbox[0]).toBeLessThanOrEqual(-121.9);
    expect(e!.bbox[2]).toBeGreaterThanOrEqual(-121.5);
  });

  it("town rings recover a water's position to within a couple of km", () => {
    // A lake at a known point, and four towns at their true distances from it.
    const lake = { lat: 54.0, lon: -126.0 };
    const towns = [
      { lat: 54.05, lon: -126.02 }, { lat: 53.93, lon: -125.9 },
      { lat: 54.1, lon: -125.85 }, { lat: 53.98, lon: -126.2 },
    ].map((t) => ({ ...t, km: distanceKm(t, lake) }));
    const got = trilaterate(towns)!;
    expect(distanceKm(got, lake)).toBeLessThan(1);
    const e = fixOf({ on: [], near: towns })!;
    expect(e.basis).toBe("estimate");
    expect(e.bbox[0] < lake.lon && e.bbox[2] > lake.lon).toBe(true);
    expect(e.bbox[1] < lake.lat && e.bbox[3] > lake.lat).toBe(true);
  });

  it("no evidence is no box — never a guess at the province", () => {
    expect(fixOf({ on: [], near: [] })).toBeNull();
  });

  it("measures a geometry at any depth", () => {
    expect(bboxOfGeometry({ type: "MultiLineString",
                            coordinates: [[[-122, 49], [-121, 49.5]], [[-121.5, 48.8], [-121.2, 49]]] }))
      .toEqual([-122, 48.8, -121, 49.5]);
    expect(bboxOfGeometry({ type: "Polygon", coordinates: [] })).toBeNull();
  });
});

describe("refining the camera against loaded geometry", () => {
  const view: Bbox = [-122, 49, -121, 50];

  it("re-fits when the water runs off the edge of the view", () => {
    expect(refineFit(view, [-122, 49.2, -121.4, 49.6], 0)).toEqual([-122, 49.2, -121.4, 49.6]);
  });

  it("re-fits when the water is a speck in the view", () => {
    expect(refineFit(view, [-121.52, 49.48, -121.5, 49.5], 0)).not.toBeNull();
  });

  it("stops once the water sits comfortably inside", () => {
    expect(refineFit(view, [-121.8, 49.2, -121.2, 49.8], 0)).toBeNull();
  });

  it("stops after a bounded number of rounds, and when nothing is loaded", () => {
    expect(refineFit(view, [-122, 49, -121, 50], 4)).toBeNull();
    expect(refineFit(view, null, 0)).toBeNull();
  });

  it("a box around a point is square on the ground", () => {
    const b = boxAround({ lat: 54, lon: -126 }, 10);
    const ew = distanceKm({ lat: 54, lon: b[0] }, { lat: 54, lon: b[2] });
    const ns = distanceKm({ lat: b[1], lon: -126 }, { lat: b[3], lon: -126 });
    expect(Math.abs(ew - ns)).toBeLessThan(0.01);
  });
});
