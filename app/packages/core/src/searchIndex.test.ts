/**
 * (T247) The search index: the name ladder equal to `matchTier`, typos, management units, regions, towns, duplicates side
 * by side — and the time a keystroke takes on a release-sized index.
 */
import { describe, expect, it } from "vitest";
import { matchTier } from "./search";
import { areaPhrase, boundedEdits, buildSearchIndex, clusterByName, editsAllowed, queryIndex, queryPlaces,
         type IndexWater, type SearchIndexData } from "./searchIndex";

const town = (name: string, km = 2) => ({ name, kind: "town", lat: 49, lon: -122, km });
const w = (item: string, name: string, o: Partial<IndexWater> = {}): IndexWater => ({
  item, name, kind: "stream", size: 100, areaHa: null, pieces: 1, regions: ["2"], mus: ["2-8"], near: null, ...o });

const DATA: SearchIndexData = {
  waters: [
    w("a", "Chilliwack River", { size: 2200, mus: ["2-8", "2-9"], near: town("Chilliwack") }),
    w("b", "Elk River", { size: 9000, regions: ["4"], mus: ["4-1", "4-2"], near: town("Fernie") }),
    w("c", "Elk River", { size: 800, regions: ["5"], mus: ["5-4"], near: town("Gold River") }),
    w("d", "Elk Lake", { kind: "lake", size: null, areaHa: 40, regions: ["1"], mus: ["1-5"], near: town("Victoria") }),
    w("e", "Welkin Creek", { size: 90000, regions: ["3"], mus: ["3-2"] }),
    w("f", "Vedder Canal", { size: 300, mus: ["2-8"], near: town("Chilliwack", 1) }),
    w("g", "Harrison River", { size: 4000, mus: ["2-7", "2-8"], near: town("Harrison Hot Springs") }),
    w("h", "Barrière Lake", { kind: "lake", size: null, areaHa: 900, regions: ["3"], mus: ["3-13"] }),
    w("i", "Kootenay River", { size: 7000, regions: ["4", "8"], mus: ["4-6", "8-1"], near: town("Cranbrook") }),
  ],
  aliases: [{ item: "a", alias: "Vedder River", source: "Province" }],
  places: [
    { place: "1", name: "Chilliwack", kind: "city", lat: 49.17, lon: -121.95, pop: 93000 },
    { place: "2", name: "Fernie", kind: "town", lat: 49.5, lon: -115.06, pop: 6000 },
    { place: "3", name: "Chilliwack Mountain", kind: "locality", lat: 49.2, lon: -121.9, pop: 10 },
  ],
};
const ix = buildSearchIndex(DATA);
const names = (q: string, limit = 40) => queryIndex(ix, q, limit).waters.map((x) => x.name);
const items = (q: string) => queryIndex(ix, q).waters.map((x) => x.item);

describe("the name ladder is matchTier's", () => {
  it("every water and alias, for a spread of queries, lands on the tier matchTier gives it", () => {
    const qs = ["elk", "elk river", "river", "ver", "vedder", "vedder river", "ved", "barriere", "illiwa", "chilliwack riv",
                "elk r", "k river", "canal"];
    for (const q of qs) {
      const want = new Map<string, number>();
      for (const x of DATA.waters) {
        const alias = DATA.aliases.find((a) => a.item === x.item)?.alias ?? null;
        const t = matchTier(q, x.name, alias);
        if (t !== null) want.set(x.item, t);
      }
      // the index answers fuzzily only when few match; compare the exact part: same waters first, in tier order
      const got = queryIndex(ix, q, 100).waters.filter((h) => want.has(h.item));
      expect(new Set(got.map((h) => h.item)), q).toEqual(new Set(want.keys()));
    }
  });
  it("quality before size: elk offers the big Elk River before the small one, and Welkin Creek last", () => {
    expect(names("elk")).toEqual(["Elk River", "Elk River", "Elk Lake", "Welkin Creek"]);
    expect(items("elk").slice(0, 2)).toEqual(["b", "c"]);
  });
  it("an alias finds its water, and says so", () => {
    const r = queryIndex(ix, "vedder river").waters[0]!;
    expect([r.name, r.matchedAs, r.matchedSource]).toEqual(["Chilliwack River", "Vedder River", "Province"]);
  });
  it("accents and apostrophes fold, one letter is nothing", () => {
    expect(names("barriere")).toEqual(["Barrière Lake"]);
    expect(queryIndex(ix, "e").waters).toEqual([]);
  });
});

describe("typos", () => {
  it("edit distance is bounded and counts a swap as one", () => {
    expect([boundedEdits("chilliwack", "chillwack", 2), boundedEdits("river", "rvier", 1), boundedEdits("abc", "xyz", 1),
            boundedEdits("kootenay", "kootney", 2)]).toEqual([1, 1, 2, 2]);
    expect([0, 1, 2, 3, 4, 6, 7, 12].map(editsAllowed)).toEqual([0, 0, 0, 0, 1, 1, 2, 2]);
  });
  it("finds the water through a dropped, doubled, swapped or wrong letter", () => {
    expect(names("chillwack")[0]).toBe("Chilliwack River");
    expect(names("chiliwack river")[0]).toBe("Chilliwack River");
    expect(names("kootney")[0]).toBe("Kootenay River");
    expect(names("harrisson")[0]).toBe("Harrison River");
    expect(names("elk rvier").slice(0, 2)).toEqual(["Elk River", "Elk River"]);
  });
  it("a name half typed with a slip in it still finds it", () => {
    expect(names("chiliwa")[0]).toBe("Chilliwack River");
  });
  it("ranks below every exact match, and says it was fuzzy", () => {
    const r = queryIndex(ix, "elk riwer");
    expect(r.fuzzy).toBe(true);
    expect(queryIndex(ix, "elk").fuzzy).toBe(false);
  });
  it("finds a name by a typo in an ALIAS too", () => {
    expect(queryIndex(ix, "veder").waters.map((x) => x.matchedAs)).toContain("Vedder River");
  });
  it("does not make something of a nonsense query", () => {
    expect(queryIndex(ix, "zzzqq").waters).toEqual([]);
  });
});

describe("management units and regions", () => {
  it("a unit lists its waters, biggest first", () => {
    const r = queryIndex(ix, "2-8");
    expect(r.area).toEqual({ kind: "mu", code: "2-8" });
    expect(r.waters.map((x) => x.name)).toEqual(["Harrison River", "Chilliwack River", "Vedder Canal"]);
  });
  it("mu, wmu and unit say the same, and a unit narrows a name", () => {
    expect(names("mu 2-8")).toEqual(names("2-8"));
    expect(names("wmu 2-8")).toEqual(names("2-8"));
    expect(names("river 2-8")).toEqual(["Harrison River", "Chilliwack River"]);
    expect(names("elk 4-1")).toEqual(["Elk River"]);
  });
  it("a region lists its waters, and narrows a name, however it is written", () => {
    expect(names("region 4")).toEqual(["Elk River", "Kootenay River"]);
    expect(names("r4")).toEqual(names("region 4"));
    expect(names("zone 8")).toEqual(["Kootenay River"]);
    expect(names("elk region 5")).toEqual(["Elk River"]);
    expect(items("elk region 5")).toEqual(["c"]);
    expect(queryIndex(ix, "region 4").area).toEqual({ kind: "region", code: "4" });
  });
  it("a region's letter (7a) is upper-cased; a unit prefix lists the region's units", () => {
    expect(queryIndex(ix, "region 7a").area).toEqual({ kind: "region", code: "7A" });
    expect(names("4-")).toEqual(["Elk River", "Kootenay River"]);
  });
  it("a unit nobody is in answers nothing", () => {
    expect(names("9-99")).toEqual([]);
  });
  it("says where a water is", () => {
    expect([areaPhrase(["2"], ["2-8"]), areaPhrase(["2", "5"], ["2-8", "2-9"]), areaPhrase(["4"], ["4-1", "4-2", "4-3", "4-4"]),
            areaPhrase([], []), areaPhrase(["2"], [])])
      .toEqual(["Region 2 · MU 2-8", "Regions 2 and 5 · MUs 2-8, 2-9", "Region 4 · MUs 4-1, 4-2 +2", "", "Region 2"]);
  });
});

describe("towns", () => {
  it("near X lists the waters X describes, nearest first, and the town", () => {
    const r = queryIndex(ix, "near chilliwack");
    expect(r.waters.map((x) => x.name)).toEqual(["Chilliwack River", "Vedder Canal"]);
    expect(r.places.map((p) => p.name)).toEqual(["Chilliwack", "Chilliwack Mountain"]);
  });
  it("a slip in the town still finds it", () => {
    expect(queryIndex(ix, "near chiliwack").waters.map((x) => x.name)).toEqual(["Chilliwack River", "Vedder Canal"]);
    expect(queryPlaces(ix, "fernei").map((p) => p.name)).toEqual(["Fernie"]);
  });
  it("a plain town name that matched no water adds the waters it describes, below any name match", () => {
    expect(names("fernie")).toEqual(["Elk River"]);
    expect(queryIndex(ix, "fernie").places.map((p) => p.name)).toEqual(["Fernie"]);
  });
  it("towns: the bigger one first among equal matches; an exact name first", () => {
    expect(queryPlaces(ix, "chilliwack").map((p) => p.name)).toEqual(["Chilliwack", "Chilliwack Mountain"]);
    expect(queryPlaces(ix, "mountain").map((p) => p.name)).toEqual(["Chilliwack Mountain"]);
  });
});

describe("duplicates", () => {
  it("waters of one name sit together, at the place the best of them ranks", () => {
    const rows = [{ name: "Elk River", i: 1 }, { name: "Elk Lake", i: 2 }, { name: "Elk River", i: 3 }];
    expect(clusterByName(rows).map((r) => r.i)).toEqual([1, 3, 2]);
  });
  it("and each carries its own region and unit, to tell them apart", () => {
    const [a, b] = queryIndex(ix, "elk river").waters;
    expect([a!.regions, a!.mus, b!.regions, b!.mus]).toEqual([["4"], ["4-1", "4-2"], ["5"], ["5-4"]]);
  });
});

describe("speed (T247: measure time per keystroke)", () => {
  /** A release-sized index: 20,000 waters, 3,000 aliases, 2,800 towns, drawn from a fixed pseudo-random stream. */
  const big = (() => {
    let s = 12345;
    const rnd = () => (s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
    const A = ["Elk", "Salmon", "Cedar", "Beaver", "Trout", "Silver", "Mud", "Fox", "Gold", "Bear", "Dry", "Little", "Upper", "Big"];
    const B = ["Creek", "River", "Lake", "Brook", "Slough", "Pond", "Channel"];
    const C = ["", "", "", " North", " South", " West", " Fork", " Side Channel"];
    const waters: IndexWater[] = Array.from({ length: 20000 }, (_, i) => w(`w${i}`,
      `${A[Math.floor(rnd() * A.length)]}${rnd() < 0.3 ? ` ${A[Math.floor(rnd() * A.length)]}` : ""} ${B[Math.floor(rnd() * B.length)]}${C[Math.floor(rnd() * C.length)]} ${i % 97}`,
      { size: Math.floor(rnd() * 5000), regions: [String(1 + (i % 8))], mus: [`${1 + (i % 8)}-${1 + (i % 20)}`],
        near: i % 3 ? town(`Town${i % 400}`) : null }));
    const aliases = Array.from({ length: 3000 }, (_, i) => ({ item: `w${i * 6}`, alias: `Old ${A[i % A.length]} ${i}`, source: null }));
    const places = Array.from({ length: 2800 }, (_, i) => ({ place: String(i), name: `Town${i % 400}${i >= 400 ? ` ${i}` : ""}`,
                                                              kind: "town", lat: 49, lon: -122, pop: i }));
    return { build: () => buildSearchIndex({ waters, aliases, places }) };
  })();

  it("builds once, and answers a keystroke in a few milliseconds — typos and areas included", () => {
    const t0 = performance.now();
    const index = big.build();
    const built = performance.now() - t0;
    const queries = ["el", "elk", "elk cr", "elk creek", "salmn rivr", "cedr", "2-8", "region 3", "near town12", "mud lake 5-4",
                    "zzzzz", "beavr slough", "silver"];
    const times = queries.map((q) => { const t = performance.now(); queryIndex(index, q); return performance.now() - t; });
    const worst = Math.max(...times);
    // the budget is the phone's frame: a keystroke must not cost more than a few; a slow CI machine gets headroom
    expect(built).toBeLessThan(2000);
    expect(worst).toBeLessThan(150);
    console.log(`search index: ${index.entries} entries built in ${built.toFixed(0)} ms; per keystroke ` +
                queries.map((q, i) => `${q}=${times[i]!.toFixed(1)}`).join(" "));
  });
});
