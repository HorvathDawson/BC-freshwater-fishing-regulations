/**
 * (T1140, owner 2026-11-25: "there should be no partt x of n nameas anywhewre in the app. that is a fault") NO PART IS
 * NUMBERED: the map's bar says the water a part is of over its NAME, never "Part n of m", and a parts-list row says its
 * name; a part outside B.C. is "<water> outside B.C." ("\"<water> outside B.C.\" (Recommended)"). And the heads' line
 * "Other parts of this water have different rules" (`entryPlaces` `differs`) is said only where another entry has
 * another rule set, another water of the group, or another salmon reading — over the same `mergeNav(group, live salmon
 * key)` the map draws: two runs of one rule set are not "different".
 */
import { describe, expect, it } from "vitest";
import { entryPlaces, mergeNav, nameUnder, partName, partRowAria, partWhere, waterGroup, type ItemNavRows } from "./waterNav";

const ENDS = { mouth: ["Mouth"], sp: ["Sandy Pool sign"], cb: ["Canyon Bridge"], ws: ["Watershed boundary"], source: ["Source"] };
const piece = (partIx: number, lo: string, hi: string, grp = 0, run = 0) => ({ partIx, label: "", grp, run, lo, hi });
/** one river: parts 0-2 one run of rule set 0 (the DFO chain cuts it), part 3 rule set 1 above it */
const river = (top: { grp: number; run: number } = { grp: 1, run: 0 }): ItemNavRows => ({
  ord: 1, item: "gnis:1", name: "Test River", kind: "stream", tidal: [],
  parts: [piece(0, "mouth", "sp"), piece(1, "sp", "cb"), piece(2, "cb", "ws"), piece(3, "ws", "source", top.grp, top.run)],
  geo: [0, 1, 2, 3].map((partIx) => ({ partIx, box: [partIx, 0, partIx + 1, 1] as const, mid: [partIx + 0.5, 0.5] as const })),
  marks: [{ lon: 1, lat: 0.5, token: "sp", down: 0, up: [1] }, { lon: 2, lat: 0.5, token: "cb", down: 1, up: [2] },
          { lon: 3, lat: 0.5, token: "ws", down: 2, up: [3] }],
  sections: [[1, 0], [2, 0], [3, 1], [4, 2], [5, 3], [6, 3]].map(([sid, partIx]) => ({ sid: sid!, partIx: partIx! })),
  exit: null,
});
const feed = (open: ReadonlySet<number>) => (sid: number) => (open.has(sid) ? "open" : "closed");
const COUNT = /\bpart \d+ of \d+\b/i;

describe("(T1140) no part is numbered, anywhere the map says one", () => {
  it("the bar's line over the name is the water's; the list rows say the names; never 'Part n of m'", () => {
    for (const key of [null, feed(new Set()), feed(new Set([3]))]) {
      const g = mergeNav(waterGroup([river()], ENDS), key);
      g.parts.forEach((_, i) => {
        expect(partWhere(g, i)).toBe("Test River");
        expect(partRowAria(g, i)).toBe(partName(g, i));
        expect(COUNT.test(`${partWhere(g, i)} ${partRowAria(g, i)}`)).toBe(false);
      });
    }
  });

  it("(owner: \"Drop the repeat\") a name that starts with the water's is shown by its words under it; others as they are", () => {
    expect(nameUnder("Squamish River", "Squamish River inside Baynes Island Ecological Reserve")).toBe("Inside Baynes Island Ecological Reserve");
    expect(nameUnder("Capilano River", "CN Railway Bridge (tidal boundary) → Cable Pool Bridge footbridge"))
      .toBe("CN Railway Bridge (tidal boundary) → Cable Pool Bridge footbridge");
    expect(nameUnder("Comox Creek", "Side channel, joining 12 km from the mouth")).toBe("Side channel, joining 12 km from the mouth");
    // (T1140 proposal) a part of added city lines with its own name: the dash goes with the water's name
    expect(nameUnder("Noons Creek", "Noons Creek — Turner Creek (City of Port Moody ESA streams layer)"))
      .toBe("Turner Creek (City of Port Moody ESA streams layer)");
    expect(nameUnder("Vedder River", "Vedder River")).toBe("Vedder River");          // the water's own name: no words left
    expect(nameUnder("Squamish River", "Squamish Rivers end")).toBe("Squamish Rivers end");   // the whole name, then a space
    // a part named by its ends whose first end begins with the water's name is a place, never a repeat (the Skeena's reserve)
    expect(nameUnder("Skeena River", "Skeena River Ecological Reserve boundary (km 63.5) → Exchamsiks River confluence"))
      .toBe("Skeena River Ecological Reserve boundary (km 63.5) → Exchamsiks River confluence");
  });

  it("a water of one part is still the whole water", () => {
    const one: ItemNavRows = { ...river(), parts: [piece(0, "mouth", "sp"), piece(1, "sp", "cb")],
                               geo: river().geo.slice(0, 2), marks: river().marks.slice(0, 1),
                               sections: [{ sid: 1, partIx: 0 }, { sid: 2, partIx: 1 }] };
    const g = mergeNav(waterGroup([one], ENDS), feed(new Set()));
    expect([g.parts.length, partWhere(g, 0)]).toEqual([1, "Whole water"]);
    expect(entryPlaces(g, "gnis:1", feed(new Set())).size).toBe(0);
  });

  it("a part with no ends of its own (side channels only) is the release's own words for it, never the bare water", () => {
    // (owner 2026-11-25 17:31: "The data's own label (Recommended)", e.g. "Side channel, joining 12 km from the mouth")
    const r = river();
    const side: ItemNavRows = { ...r, parts: [...r.parts.slice(0, 3),
      { ...r.parts[3]!, lo: null, hi: null, said: "Side channel, joining 12 km from the mouth" }] };
    const g = waterGroup([side], ENDS);
    expect([partName(g, 3), partRowAria(g, 3)]).toEqual(["Side channel, joining 12 km from the mouth",
                                                       "Side channel, joining 12 km from the mouth"]);
    // with no words of its own it stays its water's (nothing invented)
    const bare: ItemNavRows = { ...r, parts: [...r.parts.slice(0, 3), { ...r.parts[3]!, lo: null, hi: null }] };
    expect(partName(waterGroup([bare], ENDS), 3)).toBe("Test River");
  });

  it("a part outside B.C. (no key, no facts) is '<water> outside B.C.'", () => {
    const r = river();
    const out: ItemNavRows = { ...r, parts: [...r.parts.slice(0, 3), { ...r.parts[3]!, lo: null, hi: null, outside: true }] };
    const g = waterGroup([out], ENDS);
    expect(partName(g, 3)).toBe("Test River outside B.C.");
    expect(partName(g, 0)).toBe("Mouth → Sandy Pool sign");
  });
});

describe("(T1140) entryPlaces: whether another part of the map's list has other rules", () => {
  it("after the live salmon joins the run: the run and the rule set above it differ from each other", () => {
    const key = feed(new Set());
    const g = mergeNav(waterGroup([river()], ENDS), key);
    expect([...entryPlaces(g, "gnis:1", key)]).toEqual([[0, { differs: true }], [1, { differs: true }],
                                                       [2, { differs: true }], [3, { differs: true }]]);
  });

  it("'different rules' is never said of entries of one rule set and one salmon reading (two runs a lake parts)", () => {
    const key = feed(new Set());
    const g = mergeNav(waterGroup([river({ grp: 0, run: 1 })], ENDS), key);
    expect(g.parts.length).toBe(2);
    expect([...entryPlaces(g, "gnis:1", key).values()].map((p) => p.differs)).toEqual([false, false, false, false]);
    // a salmon reading that differs between them IS different rules
    const opened = feed(new Set([5, 6]));
    const g2 = mergeNav(waterGroup([river({ grp: 0, run: 1 })], ENDS), opened);
    expect([...entryPlaces(g2, "gnis:1", opened).values()].map((p) => p.differs)).toEqual([true, true, true, true]);
    // and with no salmon read (nothing joins), only the rule sets speak: one rule set, nothing "different"
    const raw = mergeNav(waterGroup([river({ grp: 0, run: 1 })], ENDS), null);
    expect([...entryPlaces(raw, "gnis:1", null).values()].map((p) => p.differs)).toEqual([false, false, false, false]);
  });
});
