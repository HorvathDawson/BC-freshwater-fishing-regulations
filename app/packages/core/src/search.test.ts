import { describe, expect, it } from "vitest";
import {
  bboxOfGeometry, boxAround, boxOfParts, describeBy, distanceKm, duplicateNames, extentOf, fixOf,
  matchTier, outlineOf, PLACEHOLDER_MAX, SEARCH_BAR, placeholderFor, waterCaption,
  searchKey, rankWaters, refineFit, trilaterate, wherePhrase, type Bbox,
  NEAR_KM, kmWords, nearCount, nearRowSub, spokenName, waterRowSub, watersNear, rankPointers, seeLine,
} from "./search";

const w = (name: string, size: number | null, pieces = 1, matchedAs: string | null = null) =>
  ({ name, kind: /lake/i.test(name) ? "lake" : "stream", size, areaHa: null as number | null,
     pieces, matchedAs });
const lake = (name: string, areaHa: number | null, pieces = 1) =>
  ({ ...w(name, null, pieces), kind: "lake", areaHa });

describe("the match ladder", () => {
  it("normalises case, accents and spacing", () => {
    expect(searchKey("  Chilliwack   RIVER ")).toBe("chilliwack river");
    expect(searchKey("Th'ewá:lí")).toBe("thewa:li");
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

  it("a lake is ranked by its AREA, on the scale magnitude uses — not as a trickle", () => {
    // "kamloops": Kamloops Lake (4,975 ha) above a creek of magnitude 300, below a river of
    // magnitude 16,000 — one hectare weighs as one headwater (NEAR_RANK.REF).
    const got = rankWaters("kam", [w("Kamloops Creek", 300), lake("Kamloops Lake", 4975),
                                   w("Kam River", 16_000)]);
    expect(got.map((h) => h.name)).toEqual(["Kam River", "Kamloops Lake", "Kamloops Creek"]);
    // Williston (172,669 ha) and the Fraser both reach the top of the scale: pieces decide.
    const top = rankWaters("w", [w("W River", 296_885, 251), lake("W Lake", 172_669, 1)]);
    expect(top.map((h) => h.name)).toEqual(["W River", "W Lake"]);
  });

  it("a lake with no area is never ranked above one with any", () => {
    const got = rankWaters("twin", [lake("Twin Lake", null), lake("Twin Lakes", 3)]);
    expect(got[0]!.name).toBe("Twin Lakes");
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

describe("the search map's words", () => {
  it("a placed water is captioned by its name; an unplaced one says so", () => {
    expect([waterCaption("Kooryet Lake", true), waterCaption("Kooryet Lake", false)])
      .toEqual(["Kooryet Lake", "Kooryet Lake — the map cannot place it yet"]);
  });
});

describe("(T48) the top bar's search field and its results sheet", () => {
  it("the words, whole: what the field is called aloud, its close, its clear, a row's open action, the sheet's lines", () => {
    expect({ ...SEARCH_BAR, open: SEARCH_BAR.open("Chilliwack River"), placeholders: SEARCH_BAR.placeholders.length })
      .toEqual({
        label: "Search waters and towns",
        close: "Close search",
        clear: "Clear search text",
        open: "Open Chilliwack River",
        seeHead: "In the regulations",
        start: "Type two letters — a river, a lake or a town. Aliases count: “Vedder” finds the Chilliwack.",
        none: "Nothing by that name in this bundle.",
        failed: "Search could not read the bundle.",
        placeholders: 9,
      });
  });

  it("the placeholder lines: the approved six, then three that name towns; none promises fish", () => {
    expect(SEARCH_BAR.placeholders).toEqual([
      "Cast your search — any lake or river",
      "Drop a line: river, lake or creek…",
      "Hook a water by name",
      "Reel in a river…",
      "Where to next? Search waters",
      "Find your next water",
      "Search a lake, river or town",
      "Name a town to find water near it",
      "Fishing near a town? Type its name",
    ]);
    expect(SEARCH_BAR.placeholders.filter((l) => /biting|catch|fish are/i.test(l))).toEqual([]);
  });

  it("every line fits the field at 390 px: at most PLACEHOLDER_MAX characters (measured in Chrome, T48-shots)", () => {
    expect([PLACEHOLDER_MAX, SEARCH_BAR.placeholders.filter((l) => [...l].length > PLACEHOLDER_MAX)]).toEqual([36, []]);
  });

  it("ONE line per opening: a seed picks it, the same seed always the same line, every line reachable", () => {
    const n = SEARCH_BAR.placeholders.length;
    expect([placeholderFor(0), placeholderFor(0.999), placeholderFor(n), placeholderFor(-1), placeholderFor(Number.NaN)])
      .toEqual(["Cast your search — any lake or river", "Cast your search — any lake or river",
                "Cast your search — any lake or river", "Drop a line: river, lake or creek…",
                "Cast your search — any lake or river"]);
    expect(new Set(Array.from({ length: n }, (_, i) => placeholderFor(i))).size).toBe(n);
  });
});

describe("placing a water by its own geometry (the bundle's part_geo)", () => {
  // Kooryet Lake (wbk:328972518) in release r-6960fe8201e3: one part, no gauge, no town ring — the water the search
  // map used to call unplaceable.
  const KOORYET = { w: -130.043061, s: 53.333981, e: -129.902673, n: 53.361987, midLon: -129.984551, midLat: 53.344782 };
  // Kootenay Lake's three closure areas: the parent owns no section, so its box is its parts'.
  const MAIN = { w: -117.130272, s: 49.246325, e: -116.479453, n: 50.180355, midLon: -116.808854, midLat: 49.64012 };
  const UPPER_WEST = { w: -117.219097, s: 49.551213, e: -116.967467, n: 49.639755, midLon: -117.10497, midLat: 49.601084 };
  const LOWER_WEST = { w: -117.468881, s: 49.447024, e: -117.213168, n: 49.592476, midLon: -117.295876, midLat: 49.504886 };

  it("one part: the water's box is that part's box, exactly", () => {
    expect(boxOfParts([KOORYET])).toEqual([-130.043061, 53.333981, -129.902673, 53.361987]);
  });

  it("several parts: the union of their boxes", () => {
    expect(boxOfParts([MAIN, UPPER_WEST, LOWER_WEST])).toEqual([-117.468881, 49.246325, -116.479453, 50.180355]);
  });

  it("a part with no box counts as its midpoint; no parts is no box", () => {
    expect(boxOfParts([{ w: null, s: null, e: null, n: null, midLon: -121, midLat: 49 }]))
      .toEqual([-121, 49, -121, 49]);
    expect(boxOfParts([KOORYET, { w: null, s: null, e: null, n: null, midLon: -129, midLat: 54 }]))
      .toEqual([-130.043061, 53.333981, -129, 54]);
    expect(boxOfParts([])).toBeNull();
  });

  it("an outline opens on the water itself, and is never narrower than a few hundred metres", () => {
    expect(outlineOf([-130.043061, 53.333981, -129.902673, 53.361987]))
      .toEqual({ bbox: [-130.043061, 53.333981, -129.902673, 53.361987], basis: "outline" });
    const pond = outlineOf([-121, 49, -121, 49])!;
    expect(pond.basis).toBe("outline");
    expect(distanceKm({ lat: pond.bbox[1], lon: pond.bbox[0] }, { lat: pond.bbox[1], lon: pond.bbox[2] }))
      .toBeCloseTo(0.6, 3);
    expect(outlineOf(null)).toBeNull();
  });

  it("the outline wins over the gauges and towns; without one the evidence still places it", () => {
    const fix = { on: [{ lat: 49.1, lon: -121.9 }], near: [{ lat: 55, lon: -127, km: 3 }] };
    expect(extentOf([-122, 49, -121, 49.5], fix))
      .toEqual({ bbox: [-122, 49, -121, 49.5], basis: "outline" });
    expect(extentOf(null, fix)).toEqual(fixOf(fix));
    expect(extentOf(null, null)).toBeNull();
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

describe("distance words (P12): the search rows' sub-lines, composed whole in core", () => {
  it("a distance in a near-list row", () => {
    expect(kmWords(0.44)).toBe("under 1 km");
    expect(kmWords(1)).toBe("1.0 km");
    expect(kmWords(7.93)).toBe("7.9 km");
  });

  it("the line under a water in 'Water near <town>'", () => {
    expect(nearRowSub("stream", 1.48)).toBe("Stream · 1.5 km");
    expect(nearRowSub("lake", 0.2)).toBe("Lake · under 1 km");
    expect(nearRowSub("wetland", 3)).toBe("Wetland · 3.0 km");
    expect(nearRowSub("canal", 3)).toBe("canal · 3.0 km");
  });

  it("the line under a searched water", () => {
    expect(waterRowSub("stream", { name: "Fernie", km: 3.2 }, false)).toBe("Stream · near Fernie · 3 km");
    expect(waterRowSub("lake", { name: "Hope", km: 0 }, true)).toBe("Lake · at Hope");
    expect(waterRowSub("stream", null, true)).toBe("Stream · no town within 25 km");
    expect(waterRowSub("stream", null, false)).toBe("Stream");
  });

  it("a searched water's name, as spoken", () => {
    expect(spokenName("Elk River", { name: "Fernie", km: 3.2 })).toBe("Elk River, near Fernie, 3 km");
    expect(spokenName("Coquihalla River", { name: "Hope", km: 0 })).toBe("Coquihalla River, at Hope");
    expect(spokenName("Elk River", null)).toBe("Elk River");
  });

  it("the near-list header and its empty note", () => {
    expect(NEAR_KM).toBe(25);
    expect(nearCount(12)).toBe("12 within 25 km");
    expect(watersNear(1, "Smithers")).toBe("1 water within 25 km of Smithers");
    expect(watersNear(2, "Smithers")).toBe("2 waters within 25 km of Smithers");
    expect(watersNear(0, "Smithers")).toBe("No named water within 25 km of Smithers");
  });
});


describe("(owner 2026-10-22) a group name that only points", () => {
  const tee = { name: "Tee Pee Lakes", see: [{ name: "Friday Lake" }, { name: "Saturday Lake" }, { name: "Sunday Lake" }] };

  it("is said whole: the group, then each water it names, in the book's order", () => {
    expect([seeLine(tee), seeLine({ name: "Garbutt Lake", see: [{ name: "Norbury (Garbutt) Lake" }] })]).toEqual([
      "Tee Pee Lakes: see Friday Lake, Saturday Lake, Sunday Lake", "Garbutt Lake: see Norbury (Garbutt) Lake"]);
  });

  it("is found by its name or the start of it, exact first, never by a word inside it", () => {
    const hits = [{ name: "Cameron Lakes" }, { name: "Cameron Lakes Area" }, { name: "Garbutt Lake" }, tee];
    expect([rankPointers("tee pee", hits), rankPointers("cameron lakes", hits).map((h) => h.name),
            rankPointers("lake", hits), rankPointers("pee", hits)])
      .toEqual([[tee], ["Cameron Lakes", "Cameron Lakes Area"], [], []]);
    // MUTATION: the word-inside tier (4) would have answered "lake" with every group; the cut at tier 2 is the pin
    expect(matchTier("lake", "Garbutt Lake")).toBe(4);
  });
});
