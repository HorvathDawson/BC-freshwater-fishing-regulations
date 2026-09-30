import { describe, expect, it } from "vitest";
import type { ItemId, PlaceId, SectionId } from "@app/data";
import type { NameHit } from "@app/data";
import {
  NO_PICK, focusFor, isSelected, resultGroups, searchStep, subjectOf, targetKey, townToPreview,
  type Located, type SearchPick, type SearchTarget, type TownView,
} from "./search";

const sid = (n: number) => n as SectionId;
const chilliwack: Located = {
  item: "gnis:8634" as ItemId, name: "Chilliwack River",
  sections: [sid(1), sid(2), sid(3)],
  extent: { bbox: [-122, 49.05, -121.4, 49.12], basis: "exact" },
};
const smithers: TownView = {
  place: { place: "76" as PlaceId, name: "Smithers", kind: "town", lat: 54.779,
           lon: -127.176, pop: 5316 },
  near: [{ item: "gnis:1" as ItemId, name: "Bulkley River", kind: "stream", km: 1.48,
           signals: { mag: 16673, areaHa: null, pieces: 76, towns: 42, gauged: true, stocked: false,
                      listed: true } },
         { item: "gnis:2" as ItemId, name: "Lake Kathlyn", kind: "lake", km: 7.9,
           signals: { mag: null, areaHa: 240, pieces: 1, towns: 7, gauged: true, stocked: false,
                      listed: true } }],
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

describe("select, then go", () => {
  const water: SearchTarget = { kind: "water", item: "gnis:8634" as ItemId,
                                name: "Chilliwack River" };
  const other: SearchTarget = { kind: "water", item: "gnis:1" as ItemId, name: "Bulkley River" };
  const place: SearchTarget = { kind: "place", place: smithers.place };
  const sel = (t: SearchTarget) => searchStep(NO_PICK, { t: "select", target: t }).pick;

  it("the first tap on a water row selects it and never leaves", () => {
    const r = searchStep(NO_PICK, { t: "select", target: water });
    expect(r.leave).toBeNull();
    expect(r.pick.selected).toEqual(water);
    expect(r.pick.town).toBeNull();
    expect(isSelected(r.pick, water)).toBe(true);
  });

  it("the first tap on a place row selects the town and never leaves or opens its list", () => {
    const r = searchStep(NO_PICK, { t: "select", target: place });
    expect(r.leave).toBeNull();
    expect(r.pick).toEqual({ selected: place, town: null });
  });

  it("tapping the selected row again keeps it selected and does not navigate", () => {
    const r = searchStep(sel(water), { t: "select", target: water });
    expect(r).toEqual({ pick: { selected: water, town: null }, leave: null });
  });

  it("tapping another row moves the selection", () => {
    const r = searchStep(sel(water), { t: "select", target: other });
    expect(isSelected(r.pick, other)).toBe(true);
    expect(isSelected(r.pick, water)).toBe(false);
    expect(r.leave).toBeNull();
  });

  it("only the selected row shows the go button", () => {
    const p = sel(water);
    expect([water, other, place].map((t) => isSelected(p, t))).toEqual([true, false, false]);
    expect(isSelected(NO_PICK, water)).toBe(false);
  });

  it("go on the selected water leaves for that water", () => {
    const r = searchStep(sel(water), { t: "go", target: water });
    expect(r.leave).toBe("gnis:8634");
  });

  it("go on a row that is NOT selected only selects it — nothing opens by accident", () => {
    const r = searchStep(sel(water), { t: "go", target: other });
    expect(r.leave).toBeNull();
    expect(isSelected(r.pick, other)).toBe(true);
    expect(searchStep(NO_PICK, { t: "go", target: water }).leave).toBeNull();
  });

  it("go on the selected place stays, and opens what is near it", () => {
    const r = searchStep(sel(place), { t: "go", target: place });
    expect(r.leave).toBeNull();
    // The selection is dropped: the map now shows the town and its waters.
    expect(r.pick).toEqual({ selected: null, town: smithers.place });
  });

  it("inside a town's list, a tap selects the water and keeps the town", () => {
    const inTown: SearchPick = { selected: null, town: smithers.place };
    const r = searchStep(inTown, { t: "select", target: water });
    expect(r.leave).toBeNull();
    expect(r.pick.town).toBe(smithers.place);
    expect(r.pick.selected).toEqual(water);
  });

  it("typing and going back forget both", () => {
    const busy: SearchPick = { selected: water, town: smithers.place };
    expect(searchStep(busy, { t: "typed" })).toEqual({ pick: NO_PICK, leave: null });
    expect(searchStep(busy, { t: "back" })).toEqual({ pick: NO_PICK, leave: null });
  });

  it("a row's key is the key of the focus it produces, so the lit row is the shown one", () => {
    expect(targetKey(water)).toBe(focusFor({ town: null, best: chilliwack })!.key);
    expect(targetKey(place)).toBe(focusFor({ town: smithers, best: null })!.key);
  });
});

describe("what the pinned map is about", () => {
  const auto = { best: "gnis:9" as ItemId, typedTown: null };
  const water: SearchTarget = { kind: "water", item: "gnis:1" as ItemId, name: "Bulkley River" };

  it("nothing tapped: the best match for the typing", () => {
    expect(subjectOf(NO_PICK, auto)).toEqual({ item: "gnis:9", place: null, pin: null });
  });

  it("a town typed in full beats the best water", () => {
    expect(subjectOf(NO_PICK, { ...auto, typedTown: smithers.place }))
      .toEqual({ item: null, place: smithers.place, pin: null });
  });

  it("a tapped row beats the typing", () => {
    expect(subjectOf({ selected: water, town: null }, auto).item).toBe("gnis:1");
    expect(subjectOf({ selected: { kind: "place", place: smithers.place }, town: null }, auto))
      .toEqual({ item: null, place: smithers.place, pin: null });
  });

  it("an open town is the subject until one of its waters is tapped, which keeps it pinned", () => {
    const inTown: SearchPick = { selected: null, town: smithers.place };
    expect(subjectOf(inTown, auto)).toEqual({ item: null, place: smithers.place, pin: null });
    expect(subjectOf({ ...inTown, selected: water }, auto))
      .toEqual({ item: "gnis:1", place: null, pin: smithers.place });
  });

  it("a water shown with its town pinned marks the town", () => {
    const f = focusFor({ town: null, best: chilliwack, pin: smithers.place })!;
    expect(f.key).toBe("item:gnis:8634");
    expect(f.marker).toEqual({ lat: 54.779, lon: -127.176 });
  });
});

describe("places are their own group, above the waters", () => {
  const hit = (item: string, name: string): NameHit => ({
    item: item as ItemId, name, kind: "stream", matchedAs: null, pieces: 1, size: null,
    areaHa: null, near: null,
  } as NameHit);

  it("places first, then waters, each with its own heading", () => {
    const g = resultGroups({ waters: [hit("a", "Bulkley River")],
                             places: [smithers.place, { ...smithers.place, place: "77" as PlaceId,
                                                        name: "Smithers Landing" }] });
    expect(g.map((x) => x.kind)).toEqual(["places", "waters"]);
    expect(g[0]!.title).toBe("Places");
    expect(g[0]!.kind === "places" && g[0]!.places.map((p) => p.name))
      .toEqual(["Smithers", "Smithers Landing"]);
    expect(g[1]!.title).toBe("1 named water");
  });

  it("no places, no places group — and one place is singular", () => {
    expect(resultGroups({ waters: [hit("a", "X Creek")], places: [] }).map((x) => x.kind))
      .toEqual(["waters"]);
    const one = resultGroups({ waters: [], places: [smithers.place] });
    expect(one.map((x) => x.title)).toEqual(["Place"]);
  });

  it("flags the waters whose names repeat, so they carry a where", () => {
    const g = resultGroups({ waters: [hit("a", "Elk River"), hit("b", "Elk River"),
                                      hit("c", "Bull River")], places: [] });
    const w = g[0]!.kind === "waters" ? g[0]!.waters : [];
    expect(w.map((x) => x.dup)).toEqual([true, true, false]);
  });
});
