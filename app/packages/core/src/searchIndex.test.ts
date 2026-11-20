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

describe("T926: every name variant finds its water", () => {
  // twelve big Beaver Creeks, then two small lakes a stocking record calls "BEAVER"
  const many: SearchIndexData = {
    waters: [
      ...Array.from({ length: 12 }, (_, i) => w(`bc${i}`, `Beaver Creek ${i}`, { size: 1000 + i })),
      w("erie", "Erie Lake", { kind: "lake", size: null, areaHa: 5 }),
      w("shane", "Shane Lake", { kind: "lake", size: null, areaHa: 3 }),
      w("u320", "Unnamed 3-20", { kind: "lake", size: null, areaHa: 2, regions: ["3"], mus: ["3-12"] }),
      w("iver", "Iver Lake", { kind: "lake", size: null, areaHa: 2 }),
    ],
    aliases: [{ item: "erie", alias: "BEAVER", source: null }, { item: "shane", alias: "BEAVER", source: null },
              { item: "iver", alias: "SNOW LAKE *", source: null }],
    places: [],
  };
  const mix = buildSearchIndex(many);
  it("a name typed exactly is never cut off by the limit; the top of the list is unchanged", () => {
    const all = queryIndex(mix, "beaver", 100).waters.map((x) => x.item);
    const cut = queryIndex(mix, "beaver", 8).waters.map((x) => x.item);
    expect(all.slice(0, 6)).toEqual(cut.slice(0, 6));
    expect(cut.slice(6).sort()).toEqual(["erie", "shane"]);
  });
  it("more waters of the exact name than the limit: the biggest of them, in order, and never a non-exact row", () => {
    const lakes: SearchIndexData = {
      waters: [
        ...Array.from({ length: 50 }, (_, i) => w(`ll${i}`, "Long Lake", { kind: "lake", size: null, areaHa: 1000 - i })),
        ...Array.from({ length: 10 }, (_, i) => w(`lc${i}`, `Long Lake Creek ${i}`, { size: 90000 })),
      ],
      aliases: [], places: [],
    };
    const lx = buildSearchIndex(lakes);
    const got = queryIndex(lx, "long lake", 40).waters.map((x) => x.item);
    expect(got).toEqual(Array.from({ length: 40 }, (_, i) => `ll${i}`));
    // 80+: still exactly the limit, the biggest first
    const many = buildSearchIndex({ ...lakes, waters: [...lakes.waters,
      ...Array.from({ length: 40 }, (_, i) => w(`lm${i}`, "Long Lake", { kind: "lake", size: null, areaHa: 10 - i / 10 }))] });
    const m = queryIndex(many, "long lake", 40).waters.map((x) => x.item);
    expect(m).toEqual(Array.from({ length: 40 }, (_, i) => `ll${i}`));
  });
  it("a whole query that is a name is found though part of it reads as a unit", () => {
    expect(queryIndex(mix, "UNNAMED 3-20").waters[0]?.item).toBe("u320");
  });
  it("a print mark the file gives is typed as it stands", () => {
    expect(queryIndex(mix, "SNOW LAKE *").waters.map((x) => [x.item, x.matchedAs])).toEqual([["iver", "SNOW LAKE *"]]);
  });
});

describe("T927: the book's boundary places", () => {
  const D: SearchIndexData = {
    waters: [
      w("cw", "Cowichan River", { size: 5000 }), w("ck", "Chuckwalla River", { size: 900 }),
      w("vd", "Vedder River", { size: 4000 }), w("ch", "Chilliwack River", { size: 4500 }),
      w("zy", "Zymoetz River", { size: 3000 }), w("lm", "Limonite Creek", { size: 20 }),
      w("li", "Liumchen Creek", { size: 50 }), w("gt", "Greendale Trestle Creek", { size: 1 }),
    ],
    aliases: [],
    places: [{ place: "9", name: "Vedder Crossing", kind: "village", lat: 49.1, lon: -121.96, pop: 500 }],
    cuts: [
      { item: "cw", words: "Greendale Trestle", alt: [], generic: false, blk: 1, m: 48408, lat: 48.8, lon: -124 },
      { item: "ck", words: "Ten Mile Pool (100 m upstream)", alt: ["fishing boundary signs at Ten Mile Pool (100 m upstream)"],
        generic: false, blk: 2, m: 13632, lat: 51.77, lon: -127.19 },
      { item: "ck", words: "Ten Mile Pool (200 m downstream)", alt: [], generic: false, blk: 2, m: 13332, lat: 51.77, lon: -127.19 },
      { item: "vd", words: "Vedder Crossing Bridge", alt: [], generic: false, blk: 3, m: 12950, lat: null, lon: null },
      { item: "vd", words: "Cable car crossing (200 m downstream of Vedder Crossing bridge)", alt: [], generic: true,
        blk: 3, m: 12723, lat: 49.09, lon: -121.96 },
      { item: "zy", words: "Limonite Creek confluence", alt: [], generic: false, blk: 4, m: 46924, lat: 54.5, lon: -127.9 },
      { item: "li", words: "Lower falls", alt: [], generic: true, blk: 5, m: 800, lat: 49.0, lon: -122.0 },
    ],
  };
  const cx = buildSearchIndex(D);
  const cuts = (q: string) => queryIndex(cx, q).cuts.map((c) => `${c.words} / ${c.water.name}`);
  it("a place is found by its words, first among places, and names its water", () => {
    expect(cuts("Greendale Trestle")).toEqual(["Greendale Trestle / Cowichan River"]);
    expect(cuts("Vedder Crossing")[0]).toBe("Vedder Crossing Bridge / Vedder River");
    expect(cuts("Limonite Creek")[0]).toBe("Limonite Creek confluence / Zymoetz River");
  });
  it("two places on one line within 500 m are one row; a place's other name finds it and says so", () => {
    expect(cuts("ten mile pool")).toEqual(["Ten Mile Pool (100 m upstream) / Chuckwalla River"]);
    expect(queryIndex(cx, "fishing boundary signs").cuts.map((c) => c.matchedAs))
      .toEqual(["fishing boundary signs at Ten Mile Pool (100 m upstream)"]);
  });
  it("a place whose words name nothing of their own only with its water's name; a kind word is not a name", () => {
    expect(cuts("lower falls")).toEqual([]);
    expect(cuts("liumchen lower falls")).toEqual(["Lower falls / Liumchen Creek"]);
    expect(cuts("creek falls")).toEqual([]);
    expect(cuts("cowichan greendale")).toEqual(["Greendale Trestle / Cowichan River"]);
  });
  it("the waters and the town are answered beside the places; nothing by a typo or from inside a word", () => {
    const r = queryIndex(cx, "Vedder Crossing");
    expect(r.places.map((p) => p.name)).toEqual(["Vedder Crossing"]);
    expect(queryIndex(cx, "Greendale Trestle").waters.map((x) => x.name)).toEqual(["Greendale Trestle Creek"]);
    expect(cuts("Greendale Trestel")).toEqual([]);
    expect(cuts("rest")).toEqual([]);
  });
  it("a one-word query that is only a kind of place finds no place by a word in it; a place starting with it still", () => {
    const k = buildSearchIndex({ ...D, cuts: [...D.cuts!,
      { item: "cw", words: "Hwy 19 bridge", alt: [], generic: false, blk: 9, m: 10, lat: null, lon: null },
      { item: "ck", words: "Bridge River confluence", alt: [], generic: false, blk: 8, m: 10, lat: null, lon: null }] });
    for (const q of ["bridge", "falls", "confluence", "signs", "boundary", "mouth", "outlet", "inlet", "creek"])
      expect(queryIndex(k, q).cuts.map((c) => c.words).filter((x) => x !== "Bridge River confluence"), q).toEqual([]);
    expect(queryIndex(k, "bridge").cuts.map((c) => c.words)).toEqual(["Bridge River confluence"]);
    expect(queryIndex(k, "hwy 19 bridge").cuts.map((c) => c.words)).toEqual(["Hwy 19 bridge"]);
  });
  it("an index without places answers none", () => {
    expect(queryIndex(ix, "elk").cuts).toEqual([]);
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
