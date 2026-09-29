import { describe, expect, it } from "vitest";
import type { ItemId, PlaceId, SectionId } from "@app/data";
import { focusFor, townToPreview, type Located, type TownView } from "./search";

const sid = (n: number) => n as SectionId;
const chilliwack: Located = {
  item: "gnis:8634" as ItemId, name: "Chilliwack River",
  sections: [sid(1), sid(2), sid(3)],
  extent: { bbox: [-122, 49.05, -121.4, 49.12], basis: "exact" },
};
const smithers: TownView = {
  place: { place: "76" as PlaceId, name: "Smithers", kind: "town", lat: 54.779,
           lon: -127.176, pop: 5316 },
  near: [{ item: "gnis:1" as ItemId, name: "Bulkley River", kind: "stream", km: 1.48 },
         { item: "gnis:2" as ItemId, name: "Lake Kathlyn", kind: "lake", km: 7.9 }],
  sections: [sid(10), sid(11), sid(12)],
};

describe("what the search map shows", () => {
  it("nothing chosen, nothing matched: the map stays put", () => {
    expect(focusFor({ town: null, best: null })).toBeNull();
  });

  it("a water: the WHOLE water lit, its box, and a refine", () => {
    const f = focusFor({ town: null, best: chilliwack })!;
    expect(f.key).toBe("item:gnis:8634");
    expect(f.highlight).toEqual([1, 2, 3]);
    expect(f.bbox).toEqual(chilliwack.extent!.bbox);
    expect(f.refine).toBe(true);
    expect(f.marker).toBeNull();
    expect(f.caption).toBe("Chilliwack River");
  });

  it("a water the bundle cannot place is still lit, and says so", () => {
    const f = focusFor({ town: null, best: { ...chilliwack, extent: null } })!;
    expect(f.bbox).toBeNull();
    expect(f.highlight.length).toBe(3);
    expect(f.caption).toBe("Chilliwack River — the map cannot place it yet");
  });

  it("a town wins over the water: every water near it lit, the town marked", () => {
    const f = focusFor({ town: smithers, best: chilliwack })!;
    expect(f.key).toBe("place:76");
    expect(f.highlight).toEqual([10, 11, 12]);
    expect(f.refine).toBe(false);
    expect(f.marker).toEqual({ lat: 54.779, lon: -127.176 });
    expect(f.caption).toBe("2 waters within 25 km of Smithers");
    // The box holds the farthest water listed.
    const [w, s, e, n] = f.bbox!;
    expect(n - s).toBeGreaterThan((2 * 7.9) / 111.32);
    expect(w < -127.176 && e > -127.176).toBe(true);
  });

  it("a town with nothing near it says that, rather than showing an empty map", () => {
    const f = focusFor({ town: { ...smithers, near: [], sections: [] }, best: null })!;
    expect(f.caption).toBe("No named water within 25 km of Smithers");
    expect(f.highlight).toEqual([]);
  });

  it("the key names the subject, so the same water never re-moves the camera", () => {
    const a = focusFor({ town: null, best: chilliwack })!;
    const b = focusFor({ town: null, best: { ...chilliwack, sections: [...chilliwack.sections] } })!;
    expect(a.key).toBe(b.key);
  });
});

describe("a town typed in full", () => {
  const town = (name: string) => ({ place: name as PlaceId, name, kind: "town", lat: 54,
                                    lon: -127, pop: null });

  it("previews the town when no water carries that exact name", () => {
    expect(townToPreview("smithers", [], [town("Smithers"), town("Smithers Landing")])!.name)
      .toBe("Smithers");
    // "Chilliwack" is a city; the waters are Chilliwack RIVER and LAKE, not Chilliwack.
    expect(townToPreview("Chilliwack ", [{ name: "Chilliwack River" }], [town("Chilliwack")])!
      .name).toBe("Chilliwack");
  });

  it("leaves the water alone when one has exactly that name", () => {
    expect(townToPreview("fraser lake", [{ name: "Fraser Lake" }], [town("Fraser Lake")]))
      .toBeNull();
  });

  it("does not guess from a prefix", () => {
    expect(townToPreview("smith", [], [town("Smithers")])).toBeNull();
    expect(townToPreview("s", [], [town("S")])).toBeNull();
  });
});
