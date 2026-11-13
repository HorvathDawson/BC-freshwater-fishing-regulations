/**
 * P15: the water map's navigation over the release's own tables. The rows are copied from release r-1bb06fa28daf
 * (water `part` / `part_geo` / `part_mark` / `water_exit` / `part_section`, core `item.part_of`), microdegrees as
 * degrees; the same five waters are in the committed regs fixture release, where `@app/data`'s
 * `waterGroup.release.test.ts` reads them through the store. (T517, water/2) `part_ix` IS river order, mouth first: the
 * rows were cut before it (their `part_geo.rank` ordered the parts), so each water is renumbered by that rank
 * (`riverOrder`) — the shape a water/2 release stores.
 */
import { describe, expect, it } from "vitest";
import {
  crossLabel, crossDown, crossUp, exitLabel, groupBounds, isConfidence, markA11y, itemNote, markLabel, mergeNav, navSections, partAt, partBounds, partName,
  partOfSection, partRowAria, partWhere, stepDown, stepsDown, stepsUp, waterGroup, wholeAria, wholeLabel, WATER_MAP_WORDS, type ItemNavRows,
} from "./waterNav";
import { WORDS } from "./regs/words";
import type { EndsTable } from "./ends";

/** (T517) The rows below were copied from a release that shipped words, not tokens: here each end's words ARE its token,
 *  and this test-only `display.ends` says every token as itself. */
const NO_ENDS: EndsTable = new Proxy({}, { get: (_t, k) => (typeof k === "string" ? [k] : undefined) });
/** A part as the rows below hold it: its two ends from its old short name ("A → B": tokens A and B; none — a lake, a
 *  side channel — no ends, said as its water), its own run (never joined). */
const own = (partIx: number, label: string, short: string) => {
  const [lo, hi] = short.includes(" → ") ? short.split(" → ") as [string, string] : [null, null];
  return { partIx, label, lo, hi, grp: partIx, run: 0 };
};
const deg = (u: number) => u / 1e6;
const geo = (partIx: number, rank: number, mid: [number, number], w: number, s: number, e: number, n: number) =>
  ({ partIx, rank, mid: [deg(mid[0]), deg(mid[1])] as const, box: [deg(w), deg(s), deg(e), deg(n)] as const });
/** A boundary: its end token (here, its words — `NO_ENDS`), or null (no named place). */
const mark = (lon: number, lat: number, token: string | null, down: number, up: number[]) =>
  ({ lon: deg(lon), lat: deg(lat), token, down, up });
const secs = (o: Record<number, number[]>) =>
  Object.entries(o).flatMap(([p, ids]) => ids.map((sid) => ({ sid, partIx: Number(p) })));
type Ranked = Omit<ItemNavRows, "geo"> & { readonly geo: readonly ReturnType<typeof geo>[] };
/** A water's rows as a release cut before water/2 stored them, renumbered so `part_ix` is the old `rank` (river order). */
function riverOrder(r: Ranked): ItemNavRows {
  const to = new Map(r.geo.map((g) => [g.partIx, g.rank]));
  const m = (p: number) => to.get(p) ?? p;
  return { ...r, parts: r.parts.map((p) => ({ ...p, partIx: m(p.partIx) })).sort((a, b) => a.partIx - b.partIx),
           geo: r.geo.map(({ rank, ...g }) => ({ ...g, partIx: rank })),
           marks: r.marks.map((k) => ({ ...k, down: m(k.down), up: k.up.map(m) })),
           sections: r.sections.map((x) => ({ ...x, partIx: m(x.partIx) })) };
}

/** Chilliwack River (ord 11095): four parts; its mouth meets the Vedder River's part 0. */
export const CHILLIWACK: ItemNavRows = riverOrder({
  ord: 11095, item: "gnis:8634", name: "Chilliwack River", kind: "stream", tidal: [],
  parts: [
    own(0, "From the fishing boundary signs 100 m below the Slesse Creek confluence up to Chilliwack River Ecological Reserve boundary (through 1 lake)", "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary"),
    own(1, "From the Vedder Crossing Bridge up to the Tamihi Rapids Bridge", "Vedder Crossing Bridge → Tamihi Rapids Bridge"),
    own(2, "From the Tamihi Rapids Bridge up to the fishing boundary signs 100 m below the Slesse Creek confluence", "Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence"),
    own(3, "From Chilliwack River Ecological Reserve boundary up to the top of the river", "Chilliwack River Ecological Reserve boundary → Source"),
  ],
  geo: [
    geo(1, 0, [-121909275, 49079445], -121966394, 49071523, -121836034, 49097385),
    geo(2, 1, [-121778339, 49076177], -121837917, 49066457, -121710461, 49084975),
    geo(0, 2, [-121566532, 49098225], -121715605, 49011250, -121396120, 49114995),
    geo(3, 3, [-121412861, 49006538], -121415021, 48999855, -121406893, 49011627),
  ],
  marks: [
    mark(-121407380, 49011348, "Chilliwack River Ecological Reserve boundary", 0, [3]),
    mark(-121837880, 49071839, "Tamihi Rapids Bridge", 1, [2]),
    mark(-121710609, 49078392, "fishing boundary signs 100 m below the Slesse Creek confluence", 2, [0]),
  ],
  sections: secs({ 0: [59894, 59900, 61178, 62859, 1235782, 1235783, 1235784],
                    1: [60210, 60211, 60212, 62453, 62571, 62823, 1235779, 1235780, 1235825],
                    2: [1235781, 1235826, 1235827], 3: [1235785] }),
  exit: { ord: 7670, part: 0, item: "gnis:3062", name: "Vedder River" },
});

/** Vedder River (ord 7670): one part; its mouth meets the Sumas River. */
export const VEDDER: ItemNavRows = riverOrder({
  ord: 7670, item: "gnis:3062", name: "Vedder River", kind: "stream", tidal: [],
  parts: [own(0, "From the mouth up to the Vedder Crossing Bridge", "Mouth → Vedder Crossing Bridge")],
  geo: [geo(0, 0, [-122018012, 49098870], -122102138, 49084156, -121962123, 49126994)],
  marks: [],
  sections: secs({ 0: [62410, 313837, 313838, 313839, 1957095] }),
  exit: { ord: 6093, part: 0, item: "gnis:26216", name: "Sumas River" },
});

/** Cowichan River (ord 792): four parts in a line; no exit stored (the sea). */
export const COWICHAN: ItemNavRows = riverOrder({
  ord: 792, item: "gnis:12227", name: "Cowichan River", kind: "stream", tidal: [],
  parts: [
    own(0, "From the mouth up to the CNR Mile 66 Trestle", "Mouth → CNR Mile 66 Trestle"),
    own(1, "From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)", "CNR Mile 66 Trestle → CNR Bridge (mile 70.2)"),
    own(2, "From the CNR Bridge (mile 70.2) up to the Greendale Trestle", "CNR Bridge (mile 70.2) → Greendale Trestle"),
    own(3, "From the Greendale Trestle up to the Cowichan Lake outlet", "Greendale Trestle → Cowichan Lake outlet"),
  ],
  geo: [
    geo(0, 0, [-123755466, 48770496], -123929188, 48751368, -123631598, 48782855),
    geo(1, 1, [-123970749, 48782089], -124005658, 48773314, -123927177, 48803777),
    geo(2, 2, [-124012323, 48824151], -124047737, 48803692, -123998757, 48831693),
    geo(3, 3, [-124057074, 48826106], -124061185, 48823921, -124046750, 48828529),
  ],
  marks: [
    mark(-123928327, 48776340, "CNR Mile 66 Trestle", 0, [1]),
    mark(-124003497, 48803759, "CNR Bridge (mile 70.2)", 1, [2]),
    mark(-124046750, 48828270, "Greendale Trestle", 2, [3]),
  ],
  sections: secs({ 0: [381, 514, 515, 619, 716, 717, 818, 59287, 59288, 59289], 1: [59290], 2: [59291], 3: [59292, 59293] }),
  exit: null,
});

const lakePart = (ord: number, item: string, arm: string, sid: number, g: ReturnType<typeof geo>,
                  exit: ItemNavRows["exit"]): ItemNavRows => ({
  ord, item, name: `Kootenay Lake — ${arm}`, kind: "lake", tidal: [],
  parts: [own(0, `Main Body, Upper West Arm and Lower West Arm / For location see map on page 34 · Kootenay Lake — ${arm}`,
              `Kootenay Lake — ${arm}`)],
  geo: [g], marks: [], sections: [{ sid, partIx: 0 }], exit,
});
/** Kootenay Lake's three closure areas (ords 11618–11620, `part_of` wbk:328974235): Main Body → Upper → Lower West Arm. */
export const KL_MAIN = lakePart(11618, "wbk:-20", "Main Body", 1235839,
  geo(0, 0, [-116808854, 49640120], -117130272, 49246325, -116479453, 50180355),
  { ord: 11619, part: 0, item: "wbk:-21", name: "Kootenay Lake — Upper West Arm" });
export const KL_UPPER = lakePart(11619, "wbk:-21", "Upper West Arm", 1235838,
  geo(0, 0, [-117104970, 49601084], -117219097, 49551213, -116967467, 49639755),
  { ord: 11620, part: 0, item: "wbk:-22", name: "Kootenay Lake — Lower West Arm" });
export const KL_LOWER = lakePart(11620, "wbk:-22", "Lower West Arm", 1235837,
  geo(0, 0, [-117295876, 49504886], -117468881, 49447024, -117213168, 49592476),
  { ord: 1496, part: 4, item: "gnis:14097", name: "Kootenay River" });

/** Zymoetz River (ord 7827): a side channel (part 5) leaves at the canyon's upper entrance and rejoins part 0. */
export const ZYMOETZ: ItemNavRows = riverOrder({
  ord: 7827, item: "gnis:3137", name: "Zymoetz River", kind: "stream", tidal: [],
  parts: [
    own(0, "From the Zymoetz Canyon upper entrance up to the Limonite Creek confluence", "Zymoetz Canyon upper entrance → Limonite Creek confluence"),
    own(1, "From the mouth up to the Zymoetz Canyon lower entrance = transmission-line sign", "Mouth → Zymoetz Canyon lower entrance = transmission-line sign"),
    own(2, "From a lake up to the McDonell Lake outlet", "A lake → McDonell Lake outlet"),
    own(3, "From the Limonite Creek confluence up to the top of the river (through 9 lakes)", "Limonite Creek confluence → Source"),
    own(4, "From the Zymoetz Canyon lower entrance = transmission-line sign up to the Zymoetz Canyon upper entrance", "Zymoetz Canyon lower entrance = transmission-line sign → Zymoetz Canyon upper entrance"),
    own(5, "Side channel, joining 19.4 km from the mouth", "Side channel"),
  ],
  geo: [
    geo(1, 0, [-128407198, 54519922], -128483935, 54471059, -128259522, 54549597),
    geo(4, 1, [-128256234, 54479375], -128260446, 54477462, -128248676, 54480194),
    geo(5, 2, [-128245890, 54479208], -128248819, 54478270, -128243108, 54480380),
    geo(0, 3, [-128086700, 54463795], -128249581, 54448626, -127930672, 54499575),
    geo(2, 4, [-127654616, 54784742], -127676586, 54780896, -127635407, 54786179),
    geo(3, 5, [-127795316, 54788722], -127988302, 54498919, -127337003, 54805514),
  ],
  marks: [
    mark(-127942782, 54499417, "Limonite Creek confluence", 0, [3]),
    mark(-128260178, 54477465, "Zymoetz Canyon lower entrance = transmission-line sign", 1, [4]),
    mark(-127676496, 54783443, "Limonite Creek confluence", 2, [3]),
    mark(-128248724, 54478539, "Zymoetz Canyon upper entrance", 4, [0, 5]),
    mark(-128248724, 54478539, "Zymoetz Canyon upper entrance", 5, [0]),
  ],
  // the first and last section of each part (the release lists 44 / 11 / 4 / 36 / 2 / 1)
  sections: secs({ 0: [761938, 1225853], 1: [762705, 1225850], 2: [763513, 1225857], 3: [765911, 1225866], 4: [767028, 1225851],
                   5: [773265] }),
  exit: { ord: 7279, part: 0, item: "gnis:2936", name: "Skeena River" },
});

/** (T460) The Fraser River (ord 9871) above the Chilcotin, marks as r-67b456b5e64b stores them: part 1 also meets part 0
 *  (mark 0, 53.27 N), past part 15. Boxes are the marks' span (no part_geo copied: only rank orders here). */
const fraserPart = (partIx: number, short: string) => own(partIx, short, short);
const box = (partIx: number, rank: number, s: number, n: number) => geo(partIx, rank, [-122500000, (s + n) / 2], -123000000, s, -122000000, n);
export const FRASER_UP: ItemNavRows = riverOrder({
  ord: 9871, item: "gnis:39325", name: "Fraser River", kind: "stream", tidal: [],
  parts: [fraserPart(0, "Region 5 boundary → Source"), fraserPart(1, "Williams Lake River confluence → Region 7A boundary"),
          fraserPart(14, "Chilcotin River confluence → Williams Lake River confluence"),
          fraserPart(15, "Region 7A boundary → Region 5 boundary")],
  geo: [box(14, 0, 51739993, 52163781), box(1, 1, 52163781, 53273342), box(15, 2, 53122530, 53307117), box(0, 3, 53273342, 54000000)],
  marks: [
    mark(-122273377, 52163781, "Williams Lake River confluence", 14, [1]),
    mark(-122619847, 53122530, null, 1, [15]),
    mark(-122843561, 53273342, null, 1, [0]),
    mark(-122867878, 53307117, null, 15, [0]),
  ],
  sections: secs({ 0: [900], 1: [901], 14: [914], 15: [915] }), exit: null,
});
/** (T460) The Fraser round Lillooet (same release): parts 11 and 12 leave part 3 and come back to it. */
export const FRASER_LILLOOET: ItemNavRows = riverOrder({
  ord: 9871, item: "gnis:39325", name: "Fraser River", kind: "stream", tidal: [],
  parts: [fraserPart(3, "Thompson River confluence → Region 5 boundary (3 stretches)"), fraserPart(10, "CNR bridge ~1km below Thompson → Thompson River confluence"),
          fraserPart(11, "BC Hydro's tail race outflow channel → Hwy 99 bridge at Lillooet"),
          fraserPart(12, "BC Railway → Lower entrance of the fish ladder at Bridge River Rapids"),
          fraserPart(13, "Region 5 boundary → Region 3 boundary")],
  geo: [box(10, 0, 50224240, 50235508), box(3, 1, 50235508, 51257392), box(11, 2, 50672350, 50683561),
        box(12, 3, 50717739, 50756940), box(13, 4, 51257392, 51517203)],
  marks: [
    mark(-121584880, 50235508, "Thompson River confluence", 10, [3]),
    mark(-121920469, 50672350, "BC Hydro's tail race outflow channel", 3, [11]),
    mark(-121913911, 50717739, "BC Railway", 3, [12]),
    mark(-122185864, 51257392, null, 3, [13]),
    mark(-121929168, 50683561, "Hwy 99 bridge at Lillooet", 11, [3]),
    mark(-121930970, 50756940, "lower entrance of the fish ladder at Bridge River Rapids", 12, [3]),
  ],
  sections: secs({ 3: [903], 10: [910], 11: [911], 12: [912], 13: [913] }), exit: null,
});

const order = (g: ReturnType<typeof waterGroup>) => g.parts.map((p, i) => `${i}: ${partName(g, i)}`);
/** Each part's ways ▼ | ▲ ("-": none), as the conformance test prints them. */
const steps = (g: ReturnType<typeof waterGroup>) =>
  g.parts.map((_, i) => `${stepsDown(g, i).join(",") || "-"}|${stepsUp(g, i).join(",") || "-"}`);

/** The release's `water_continue` row: the Chilliwack (its part 0, mouth first) goes on as the Vedder (its part 0). */
const CHW_AS_VEDDER = { ord: 7670, part: 0, otherPart: 0, item: "gnis:3062", name: "Vedder River" };
const VEDDER_FROM_CHW = { ord: 11095, part: 0, otherPart: 0, item: "gnis:8634", name: "Chilliwack River" };

describe("the group's parts, mouth first (the release's `part_ix`)", () => {
  it("Chilliwack: the four parts in river order, each named by its short name, its water beside it", () => {
    const g = waterGroup([CHILLIWACK], NO_ENDS);
    expect(order(g)).toEqual([
      "0: Vedder Crossing Bridge → Tamihi Rapids Bridge",
      "1: Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence",
      "2: 100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary",
      "3: Chilliwack River Ecological Reserve boundary → Source",
    ]);
    expect(g.parts.map((p) => [p.partIx, p.pieces])).toEqual([[0, [0]], [1, [1]], [2, [2]], [3, [3]]]);
    expect([partWhere(g, 0), partWhere(g, 3)]).toEqual(["Part 1 of 4 · Chilliwack River", "Part 4 of 4 · Chilliwack River"]);
  });

  it("(T150 review) a parts-list row as a screen reader says it, whole; the list's title is the one PARTS_TITLE", () => {
    const g = waterGroup([CHILLIWACK], NO_ENDS);
    expect([partRowAria(g, 0), partRowAria(g, 3), partRowAria(waterGroup([VEDDER], NO_ENDS), 0)]).toEqual([
      "Vedder Crossing Bridge → Tamihi Rapids Bridge, part 1 of 4 · Chilliwack River",
      "Chilliwack River Ecological Reserve boundary → Source, part 4 of 4 · Chilliwack River",
      "Vedder River, part 1 of 1"]);
    // one copy of "Parts of this water" (that it is SPELLED once, in regs/words.ts, is a source scan:
    // tools/one-parts-list.test.ts — core may not read files; the Parts sheet that also read it is removed, T698)
    expect([WATER_MAP_WORDS.parts, WORDS.PARTS_TITLE]).toEqual(["Parts of this water", "Parts of this water"]);
  });

  it("Cowichan: four parts in a line, the marks as stored", () => {
    const g = waterGroup([COWICHAN], NO_ENDS);
    expect(order(g)).toEqual([
      "0: Mouth → CNR Mile 66 Trestle", "1: CNR Mile 66 Trestle → CNR Bridge (mile 70.2)",
      "2: CNR Bridge (mile 70.2) → Greendale Trestle", "3: Greendale Trestle → Cowichan Lake outlet"]);
    expect(g.marks.map((m) => `${markLabel(g, m)}: ${m.down} → ${m.up.join(",")} at ${m.lon},${m.lat}`)).toEqual([
      "CNR Mile 66 Trestle: 0 → 1 at -123.928327,48.77634",
      "CNR Bridge (mile 70.2): 1 → 2 at -124.003497,48.803759",
      "Greendale Trestle: 2 → 3 at -124.04675,48.82827"]);
  });

  it("a one-part water is named by its water, with no note", () => {
    const g = waterGroup([VEDDER], NO_ENDS);
    expect([partName(g, 0), itemNote(g, 0), partWhere(g, 0)]).toEqual(["Vedder River", "", "Part 1 of 1"]);
  });

  it("KLWA: a split lake's three items are one water, ordered by their exits (as the prototype's klwa)", () => {
    // opened from the Main Body; the siblings in any order
    const g = waterGroup([KL_MAIN, KL_UPPER, KL_LOWER], NO_ENDS);
    expect(order(g)).toEqual([
      "0: Kootenay Lake — Lower West Arm", "1: Kootenay Lake — Upper West Arm", "2: Kootenay Lake — Main Body"]);
    expect(waterGroup([KL_UPPER, KL_MAIN, KL_LOWER], NO_ENDS).parts.map((p) => p.item)).toEqual(["wbk:-22", "wbk:-21", "wbk:-20"]);
    // the boundaries between them have no place (not drawn) and the prototype's words
    expect(g.marks.map((m) => `${markLabel(g, m)}: ${m.down} → ${m.up.join(",")} at ${m.lon},${m.lat}`)).toEqual([
      "null: 0 → 1 at null,null", "null: 1 → 2 at null,null"]);
    // the group's mouth is the Lower West Arm's: into the Kootenay River at its part 4
    expect(g.exit).toEqual({ item: "gnis:14097", name: "Kootenay River", part: 4 });
    expect(wholeLabel(g, 0, false)).toBe("⤢ Whole lake");
    // (T116) a marsh's button says what it is: waterNoun's word, not "river"
    const marsh = waterGroup([{ ...KL_MAIN, kind: "wetland" }], NO_ENDS);
    expect([wholeLabel(marsh, 0, false), wholeAria(marsh, 0, false)]).toEqual(["⤢ Whole wetland", "Whole wetland"]);
  });

  it("KLWA opened as the split lake itself (no part of its own): its closure areas, and its mouth is theirs", () => {
    const lake: ItemNavRows = { ord: 11862, item: "wbk:328974235", name: "Kootenay Lake", kind: "lake", tidal: [], parts: [],
                                geo: [], marks: [], sections: [], exit: null };
    const g = waterGroup([lake, KL_UPPER, KL_MAIN, KL_LOWER], NO_ENDS);
    expect([order(g), g.exit]).toEqual([
      ["0: Kootenay Lake — Lower West Arm", "1: Kootenay Lake — Upper West Arm", "2: Kootenay Lake — Main Body"],
      { item: "gnis:14097", name: "Kootenay River", part: 4 }]);
  });
});

describe("▼ / ▲ over the marks", () => {
  it("Cowichan: one step each way; the ends stop", () => {
    const g = waterGroup([COWICHAN], NO_ENDS);
    expect([0, 1, 2, 3].map((i) => [stepDown(g, i), stepsUp(g, i)])).toEqual([
      [null, [1]], [0, [2]], [1, [3]], [2, []]]);
  });

  it("Chilliwack: by the marks, which name partIx", () => {
    const g = waterGroup([CHILLIWACK], NO_ENDS);
    expect([0, 1, 2, 3].map((i) => [stepDown(g, i), stepsUp(g, i)])).toEqual([
      [null, [1]], [0, [2]], [1, [3]], [2, []]]);
  });

  it("a side channel: ▲ from below the fork gives the main stem first and offers the side channel", () => {
    const g = waterGroup([ZYMOETZ], NO_ENDS);
    const canyon = partAt(g, "gnis:3137", 1), main = partAt(g, "gnis:3137", 3), side = partAt(g, "gnis:3137", 2);
    expect([canyon, main, side]).toEqual([1, 3, 2]);
    expect(stepsUp(g, canyon)).toEqual([main, side]);
    // the side channel rejoins the main stem going up, and going down returns to the canyon
    expect([stepsUp(g, side), stepDown(g, side)]).toEqual([[main], canyon]);
    // ▼ from the main stem takes the canyon, not the side channel
    expect(stepDown(g, main)).toBe(canyon);
  });

  it("where the marks say nothing, the order does", () => {
    const g = waterGroup([ZYMOETZ], NO_ENDS);
    const lake = partAt(g, "gnis:3137", 4);       // "A lake → McDonell Lake outlet": no mark below it
    expect([lake, stepDown(g, lake), stepsUp(g, lake)]).toEqual([4, 3, [5]]);
  });

  it("a fork going DOWN: below the Zymoetz's source part are the main stem and the lake part, main stem first", () => {
    // the lake part's own mark says it sits below the source (`down 2, up [3]`), and nothing else leads into it
    const g = waterGroup([ZYMOETZ], NO_ENDS);
    const source = partAt(g, "gnis:3137", 5), main = partAt(g, "gnis:3137", 3), lake = partAt(g, "gnis:3137", 4);
    expect([stepsDown(g, source), stepDown(g, source)]).toEqual([[main, lake], main]);
    // and above the canyon, ▼ offers the canyon first and the side channel that rejoins there
    expect(steps(g)).toEqual(["-|1", "0|3,2", "1|3", "1,2|5", "3|5", "3,4|-"]);
  });

  it("(T460) the Fraser above Williams Lake: ▲ never skips the Region 7A → Region 5 part; ▼ never skips back", () => {
    const g = waterGroup([FRASER_UP], NO_ENDS);
    const p = (ix: number) => partAt(g, "gnis:39325", ix);
    const [chilcotin, williams, border, source] = [p(0), p(1), p(2), p(3)];
    // the release's marks offer part 0 from part 1 too (a side channel of part 1 touches it 53.27 N): not a step
    expect([stepsUp(g, williams), stepsDown(g, source)]).toEqual([[border], [border]]);
    expect([stepsUp(g, chilcotin), stepsUp(g, border), stepsDown(g, border), stepsDown(g, williams)])
      .toEqual([[williams], [source], [williams], [chilcotin]]);
  });

  it("(T460) channels that leave a part and come back to it are ways on, not skips (the Fraser at Lillooet)", () => {
    const g = waterGroup([FRASER_LILLOOET], NO_ENDS);
    const p = (ix: number) => partAt(g, "gnis:39325", ix);
    expect(stepsUp(g, p(1)).slice().sort()).toEqual([p(2), p(3), p(4)].sort());
    expect([stepsUp(g, p(2)), stepsUp(g, p(3))]).toEqual([[p(1)], [p(1)]]);
  });

  it("EVERY part of every water is stepped INTO by ▼ or ▲ from another part, and all are reached from the mouth", () => {
    const waters = { CHILLIWACK: [CHILLIWACK], VEDDER: [VEDDER], COWICHAN: [COWICHAN], KLWA: [KL_MAIN, KL_UPPER, KL_LOWER],
                     ZYMOETZ: [ZYMOETZ] };
    const missed: string[] = [];
    for (const [name, rows] of Object.entries(waters)) {
      const g = waterGroup(rows, NO_ENDS);
      const into = new Set(g.parts.flatMap((_, i) => [...stepsDown(g, i), ...stepsUp(g, i)].filter((j) => j !== i)));
      const seen = new Set([0]);
      const queue = [0];
      while (queue.length) {
        const i = queue.shift()!;
        for (const j of [...stepsDown(g, i), ...stepsUp(g, i)]) if (!seen.has(j)) { seen.add(j); queue.push(j); }
      }
      g.parts.forEach((p, j) => {
        if (g.parts.length > 1 && !into.has(j)) missed.push(`${name} ${j} (${p.partIx}): nothing steps into it`);
        if (!seen.has(j)) missed.push(`${name} ${j} (${p.partIx}): not reached from the mouth`);
      });
    }
    expect(missed).toEqual([]);
  });

  it("KLWA: the sibling boundaries are steps; the ends stop", () => {
    const g = waterGroup([KL_MAIN, KL_UPPER, KL_LOWER], NO_ENDS);
    expect([0, 1, 2].map((i) => [stepDown(g, i), stepsUp(g, i)])).toEqual([[null, [1]], [0, [2]], [1, []]]);
  });
});

describe("across waters: a stream that goes on as another (`water_continue`)", () => {
  const chw = waterGroup([{ ...CHILLIWACK, continuesAs: CHW_AS_VEDDER }], NO_ENDS);
  const ved = waterGroup([{ ...VEDDER, continuesFrom: VEDDER_FROM_CHW }], NO_ENDS);

  it("the Vedder: ▲ from its part into the Chilliwack's lowest part (its part 0, Vedder Crossing → Tamihi)", () => {
    expect([ved.prev, ved.next, crossUp(ved, 0), crossDown(ved, 0)]).toEqual([
      { item: "gnis:8634", name: "Chilliwack River", part: 0, at: 0 }, null,
      { item: "gnis:8634", name: "Chilliwack River", part: 0, at: 0 }, null]);
    expect(crossLabel("up", crossUp(ved, 0)!)).toBe("Upstream into Chilliwack River");
  });

  it("the Chilliwack: ▼ at its lowest part into the Vedder's part 0; nowhere else crosses", () => {
    expect([chw.next, chw.prev]).toEqual([{ item: "gnis:3062", name: "Vedder River", part: 0, at: 0 }, null]);
    expect([0, 1, 2, 3].map((i) => [crossDown(chw, i), crossUp(chw, i)])).toEqual([
      [{ item: "gnis:3062", name: "Vedder River", part: 0, at: 0 }, null], [null, null], [null, null], [null, null]]);
    expect(crossLabel("down", chw.next!)).toBe("Downstream into Vedder River");
    // the mouth's exit is still there (the "Flows into" link stays)
    expect(chw.exit).toEqual({ item: "gnis:3062", name: "Vedder River", part: 0 });
  });

  it("a tributary-only water gets no cross-water step: the Cowichan, the KLWA lake, and a water with only an exit", () => {
    for (const g of [waterGroup([COWICHAN], NO_ENDS), waterGroup([KL_MAIN, KL_UPPER, KL_LOWER], NO_ENDS), waterGroup([CHILLIWACK], NO_ENDS),
                     waterGroup([VEDDER], NO_ENDS)])
      expect([g.next, g.prev, ...g.parts.flatMap((_, i) => [crossDown(g, i), crossUp(g, i)])].every((x) => x === null))
        .toBe(true);
  });

  it("a cross only where this water has no step that way; a junction part the water does not have THROWS", () => {
    // the junction on a part with a part above it: ▲ stays inside the water
    const mid = waterGroup([{ ...CHILLIWACK, continuesFrom: { ...VEDDER_FROM_CHW, ord: 1, part: 0 } }], NO_ENDS);
    expect([mid.prev?.at, crossUp(mid, 0)]).toEqual([0, null]);
    // (no silent fallbacks) a release defect is said, never a cross quietly left out
    expect(() => waterGroup([{ ...VEDDER, continuesFrom: { ...VEDDER_FROM_CHW, part: 7 } }], NO_ENDS))
      .toThrow("water map: gnis:3062 (ord 7670) its water_continue row names part 7, which it does not have");
  });
});

describe("the exit, the highlight and the frame", () => {
  it("the exit is the receiving water at the part the mouth meets", () => {
    const g = waterGroup([CHILLIWACK], NO_ENDS);
    expect(g.exit).toEqual({ item: "gnis:3062", name: "Vedder River", part: 0 });
    expect(exitLabel(g.exit!)).toBe("Flows into Vedder River ›");
    expect(waterGroup([COWICHAN], NO_ENDS).exit).toBeNull();
  });

  it("(no silent fallbacks) a release defect in the rows THROWS, naming it — never a part, mark or exit quietly dropped", () => {
    const bad = (o: Partial<ItemNavRows>) => () => waterGroup([{ ...COWICHAN, ...o }], NO_ENDS);
    expect(bad({ geo: COWICHAN.geo.slice(1) })).toThrow("water map: gnis:12227 (ord 792) part 0 has no part_geo row");
    expect(bad({ sections: COWICHAN.sections.filter((x) => x.partIx !== 2) }))
      .toThrow("water map: gnis:12227 (ord 792) part 2 has no part_section row");
    expect(bad({ marks: [mark(0, 0, "x", 0, [9])] })).toThrow("water map: gnis:12227 (ord 792) a part_mark names part 9, which it does not have");
    // a split lake whose areas flow into each other in a circle has no mouth
    expect(() => waterGroup([{ ...KL_MAIN, exit: { ...KL_MAIN.exit!, ord: 11619 } }, { ...KL_UPPER, exit: { ...KL_UPPER.exit!, ord: 11618 } }], NO_ENDS))
      .toThrow("water map: wbk:-20 (ord 11618) and its siblings flow into each other in a cycle: no mouth");
    // a sibling flowing into a sibling at no stored part
    expect(() => waterGroup([KL_MAIN, { ...KL_UPPER, exit: { ...KL_UPPER.exit!, part: null } }, KL_LOWER], NO_ENDS))
      .toThrow("water map: wbk:-21 (ord 11619) flows into its sibling wbk:-22 at no stored part (water_exit.down_part)");
  });

  it("(T278) a TIDAL piece (never listed) takes the marks that reach it with it — the one mark the group drops, by rule", () => {
    const g = waterGroup([{ ...COWICHAN, tidal: [9], marks: [...COWICHAN.marks, mark(0, 0, "tidal boundary", 9, [0])] }], NO_ENDS);
    expect(g.marks.map((m) => m.token)).toEqual(["CNR Mile 66 Trestle", "CNR Bridge (mile 70.2)", "Greendale Trestle"]);
  });

  it("(T345) a mouth that meets TIDAL water keeps its exit, said as plain words with no chevron", () => {
    const g = waterGroup([{ ...CHILLIWACK, exit: { ord: 9871, part: 16, item: "gnis:39325", name: "Fraser River", tidal: true } }], NO_ENDS);
    expect(g.exit).toEqual({ item: "gnis:39325", name: "Fraser River", part: 16, tidal: true });
    expect(exitLabel(g.exit!)).toBe("Flows into the tidal Fraser River");
  });

  it("selected is exactly the part's part_section rows; water is the rest of the group", () => {
    const g = waterGroup([CHILLIWACK], NO_ENDS);
    expect(navSections(g, 0)).toEqual({
      selected: [60210, 60211, 60212, 62453, 62571, 62823, 1235779, 1235780, 1235825],
      water: [1235781, 1235826, 1235827, 59894, 59900, 61178, 62859, 1235782, 1235783, 1235784, 1235785],
    });
    expect([partOfSection(g, 1235827), partOfSection(g, 1), partAt(g, "gnis:8634", 9)]).toEqual([1, -1, -1]);
  });

  it("frames a part by its box, the whole by the union, and a part with no box by its midpoint", () => {
    const g = waterGroup([COWICHAN], NO_ENDS);
    expect(partBounds(g, 1)).toEqual([-124.005658, 48.773314, -123.927177, 48.803777]);
    expect(groupBounds(g)).toEqual([-124.061185, 48.751368, -123.631598, 48.831693]);
    const nobox = waterGroup([{ ...VEDDER, geo: [{ ...VEDDER.geo[0]!, box: null }] }], NO_ENDS);
    const b = partBounds(nobox, 0)!;
    expect(b.map((x) => Math.round(x * 1e4) / 1e4)).toEqual([-122.0249, 49.0944, -122.0112, 49.1034]);
    expect(wholeLabel(g, 0, false)).toBe("⤢ Whole river");
    expect(wholeLabel(g, 0, true)).toBe("This part");
  });
});

describe("(T517, water/3) a boundary is said by the end token its parts share — the one end resolver", () => {
  const ENDS: EndsTable = { cnr: ["CNR Mile 66 Trestle, approximate location (low confidence)"], br: ["CNR Bridge (mile 70.2)"],
                            gt: ["Greendale Trestle"] };
  const rows: ItemNavRows = { ...COWICHAN, marks: COWICHAN.marks.map((m, i) => ({ ...m, token: ["cnr", "br", "gt"][i]!,
                                                                                    ...(i === 0 ? { confidence: "low" as const } : {}) })) };
  it("its words are the token's in `display.ends` — an approximate cut's confidence is IN them, never appended again", () => {
    const g = waterGroup([rows], ENDS);
    expect(g.marks.map((m) => markLabel(g, m))).toEqual([
      "CNR Mile 66 Trestle, approximate location (low confidence)", "CNR Bridge (mile 70.2)", "Greendale Trestle"]);
    expect(markA11y(g, g.marks[0]!)).toBe("Boundary: CNR Mile 66 Trestle, approximate location (low confidence)");
  });
  it("no token (no named place: a side channel meets, a split lake's areas): no words, and \"Boundary\" alone", () => {
    const g = waterGroup([{ ...rows, marks: rows.marks.map((m, i) => (i === 1 ? { ...m, token: null } : m)) }], ENDS);
    expect([markLabel(g, g.marks[1]!), markA11y(g, g.marks[1]!)]).toEqual([null, "Boundary"]);
  });
  it("a token the release does not hold throws, naming it", () => {
    const g = waterGroup([{ ...rows, marks: rows.marks.map((m, i) => (i === 2 ? { ...m, token: "gone" } : m)) }], ENDS);
    expect(() => markLabel(g, g.marks[2]!)).toThrow("ends: token gone is not in this release's display.ends");
  });
  it("a confidence is read as the pipeline's values", () => {
    expect([isConfidence("low"), isConfidence("certain"), isConfidence(null)]).toEqual([true, false, false]);
  });
});

describe("T517: one part per connected run, a salmon-cut run joined on the map", () => {
  // the Dean's shape: one run (grp 0, run 0) cut where the DFO salmon chain changes into parts 0 and 1; part 2 its own
  // (T517, owner 2026-11-09) each part ships its two end tokens, said through the release's `display.ends`
  const ENDS: EndsTable = { mouth: ["Mouth"], "split:tc": ["Tanswanket Creek confluence"], "lake:anahim": ["Anahim Lake"],
                            source: ["Source"] };
  const piece = (n: number, label: string, lo: string, hi: string) => ({ partIx: n, label, short: null, grp: 0, run: 0, lo, hi });
  const DEAN: ItemNavRows = riverOrder({
    ord: 77, item: "gnis:16075", name: "Dean River", kind: "stream", tidal: [],
    parts: [piece(0, "Mouth → Tanswanket Creek confluence", "mouth", "split:tc"),
            piece(1, "Tanswanket Creek confluence → Anahim Lake", "split:tc", "lake:anahim"),
            { partIx: 2, label: "Above", short: null, lo: "lake:anahim", hi: "source", grp: 1, run: 0 }],
    geo: [geo(0, 0, [-127000000, 52000000], -127100000, 51900000, -126900000, 52100000),
          geo(1, 1, [-126500000, 52200000], -126600000, 52100000, -126400000, 52300000),
          geo(2, 2, [-125500000, 52500000], -125600000, 52400000, -125400000, 52600000)],
    marks: [mark(-126800000, 52150000, "split:tc", 0, [1]),
            mark(-126000000, 52400000, "lake:anahim", 1, [2])],
    sections: secs({ 0: [10, 11], 1: [12], 2: [13] }), exit: null });

  it("ONE mode (owner 2026-11-08): the run is listed once by its own name where the live salmon reads it alike", () => {
    const g = mergeNav(waterGroup([DEAN], ENDS), () => "alike");
    expect(g.parts.map((_, i) => partName(g, i))).toEqual(["Mouth → Anahim Lake", "Anahim Lake → Source"]);
    expect(g.parts[0]!.sids).toEqual([10, 11, 12]);
  });

  it("the same water and the same live salmon give the identical list, names and marks whatever the map's mode", () => {
    // the list is a function of the group and the live reading only: no mode argument exists to differ on
    const key = (sid: number) => (sid === 12 ? "coho open" : "closed");
    const regular = mergeNav(waterGroup([DEAN], ENDS), key), salmon = mergeNav(waterGroup([DEAN], ENDS), key);
    expect(regular).toEqual(salmon);
    expect(regular.parts.map((_, i) => partName(regular, i))).toEqual([
      "Mouth → Tanswanket Creek confluence", "Tanswanket Creek confluence → Anahim Lake", "Anahim Lake → Source"]);
  });

  it("with no salmon reading yet, no cut is hidden: every part of the run is its own entry", () => {
    const g = mergeNav(waterGroup([DEAN], ENDS), null);
    expect(g.parts.length).toBe(3);
  });

  it("a DORMANT DFO place the live feed turns active separates on the map with no release", () => {
    // the cut is in the release whatever DFO lists today: the chain holds every curated place, dormant ones too
    // (pipeline `salmon.build`), so the parts and the mark between them are already there
    const dormant = mergeNav(waterGroup([DEAN], ENDS), () => "same");           // the feed: nothing on piece 1 today
    expect(dormant.parts.map((_, i) => partName(dormant, i))).toEqual(["Mouth → Anahim Lake", "Anahim Lake → Source"]);
    const active = mergeNav(waterGroup([DEAN], ENDS), (sid) => (sid === 12 ? "coho open" : "same"));   // DFO lists it again
    expect(active.parts.map((_, i) => partName(active, i))).toEqual([
      "Mouth → Tanswanket Creek confluence", "Tanswanket Creek confluence → Anahim Lake", "Anahim Lake → Source"]);
    expect(active.marks.map((m) => [markLabel(active, m), m.down, m.up])).toEqual([
      ["Tanswanket Creek confluence", 0, [1]], ["Anahim Lake", 1, [2]]]);
  });

  it("salmon mode keeps the pieces apart where their salmon differs, each by its own ends", () => {
    const g = mergeNav(waterGroup([DEAN], ENDS), (sid) => (sid === 12 ? "b" : "a"));
    expect(g.parts.map((_, i) => partName(g, i))).toEqual([
      "Mouth → Tanswanket Creek confluence", "Tanswanket Creek confluence → Anahim Lake", "Anahim Lake → Source"]);
  });
});
