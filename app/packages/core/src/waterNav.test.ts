/**
 * P15: the water map's navigation over the release's own tables. The rows are copied from release r-1bb06fa28daf
 * (water `part` / `part_geo` / `part_mark` / `water_exit` / `part_section`, core `item.part_of`), microdegrees as
 * degrees; the same five waters are in the committed regs fixture release, where `@app/data`'s
 * `waterGroup.release.test.ts` reads them through the store.
 */
import { describe, expect, it } from "vitest";
import {
  confidenceWords, crossLabel, crossDown, crossUp, exitLabel, groupBounds, isConfidence, markA11y, itemNote, markLabel, navSections, partAt, partBounds, partName,
  partOfSection, partRowAria, partWhere, stepDown, stepsDown, stepsUp, waterGroup, wholeAria, wholeLabel, WATER_MAP_WORDS, type ItemNavRows,
} from "./waterNav";
import { WORDS } from "./regs/words";

const deg = (u: number) => u / 1e6;
const geo = (partIx: number, rank: number, mid: [number, number], w: number, s: number, e: number, n: number) =>
  ({ partIx, rank, mid: [deg(mid[0]), deg(mid[1])] as const, box: [deg(w), deg(s), deg(e), deg(n)] as const });
const mark = (lon: number, lat: number, label: string | null, down: number, up: number[]) =>
  ({ lon: deg(lon), lat: deg(lat), label, down, up });
const secs = (o: Record<number, number[]>) =>
  Object.entries(o).flatMap(([p, ids]) => ids.map((sid) => ({ sid, partIx: Number(p) })));

/** Chilliwack River (ord 11095): four parts; its mouth meets the Vedder River's part 0. */
export const CHILLIWACK: ItemNavRows = {
  ord: 11095, item: "gnis:8634", name: "Chilliwack River", kind: "stream",
  parts: [
    { partIx: 0, label: "From the fishing boundary signs 100 m below the Slesse Creek confluence up to Chilliwack River Ecological Reserve boundary (through 1 lake)", short: "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary" },
    { partIx: 1, label: "From the Vedder Crossing Bridge up to the Tamihi Rapids Bridge", short: "Vedder Crossing Bridge → Tamihi Rapids Bridge" },
    { partIx: 2, label: "From the Tamihi Rapids Bridge up to the fishing boundary signs 100 m below the Slesse Creek confluence", short: "Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence" },
    { partIx: 3, label: "From Chilliwack River Ecological Reserve boundary up to the top of the river", short: "Chilliwack River Ecological Reserve boundary → Source" },
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
};

/** Vedder River (ord 7670): one part; its mouth meets the Sumas River. */
export const VEDDER: ItemNavRows = {
  ord: 7670, item: "gnis:3062", name: "Vedder River", kind: "stream",
  parts: [{ partIx: 0, label: "From the mouth up to the Vedder Crossing Bridge", short: "Mouth → Vedder Crossing Bridge" }],
  geo: [geo(0, 0, [-122018012, 49098870], -122102138, 49084156, -121962123, 49126994)],
  marks: [],
  sections: secs({ 0: [62410, 313837, 313838, 313839, 1957095] }),
  exit: { ord: 6093, part: 0, item: "gnis:26216", name: "Sumas River" },
};

/** Cowichan River (ord 792): four parts in a line; no exit stored (the sea). */
export const COWICHAN: ItemNavRows = {
  ord: 792, item: "gnis:12227", name: "Cowichan River", kind: "stream",
  parts: [
    { partIx: 0, label: "From the mouth up to the CNR Mile 66 Trestle", short: "Mouth → CNR Mile 66 Trestle" },
    { partIx: 1, label: "From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)", short: "CNR Mile 66 Trestle → CNR Bridge (mile 70.2)" },
    { partIx: 2, label: "From the CNR Bridge (mile 70.2) up to the Greendale Trestle", short: "CNR Bridge (mile 70.2) → Greendale Trestle" },
    { partIx: 3, label: "From the Greendale Trestle up to the Cowichan Lake outlet", short: "Greendale Trestle → Cowichan Lake outlet" },
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
};

const lakePart = (ord: number, item: string, arm: string, sid: number, g: ReturnType<typeof geo>,
                  exit: ItemNavRows["exit"]): ItemNavRows => ({
  ord, item, name: `Kootenay Lake — ${arm}`, kind: "lake",
  parts: [{ partIx: 0, label: `Main Body, Upper West Arm and Lower West Arm / For location see map on page 34 · Kootenay Lake — ${arm}`,
            short: `Kootenay Lake — ${arm}` }],
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
export const ZYMOETZ: ItemNavRows = {
  ord: 7827, item: "gnis:3137", name: "Zymoetz River", kind: "stream",
  parts: [
    { partIx: 0, label: "From the Zymoetz Canyon upper entrance up to the Limonite Creek confluence", short: "Zymoetz Canyon upper entrance → Limonite Creek confluence" },
    { partIx: 1, label: "From the mouth up to the Zymoetz Canyon lower entrance = transmission-line sign", short: "Mouth → Zymoetz Canyon lower entrance = transmission-line sign" },
    { partIx: 2, label: "From a lake up to the McDonell Lake outlet", short: "A lake → McDonell Lake outlet" },
    { partIx: 3, label: "From the Limonite Creek confluence up to the top of the river (through 9 lakes)", short: "Limonite Creek confluence → Source" },
    { partIx: 4, label: "From the Zymoetz Canyon lower entrance = transmission-line sign up to the Zymoetz Canyon upper entrance", short: "Zymoetz Canyon lower entrance = transmission-line sign → Zymoetz Canyon upper entrance" },
    { partIx: 5, label: "Side channel, joining 19.4 km from the mouth", short: "Side channel" },
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
};

const order = (g: ReturnType<typeof waterGroup>) => g.parts.map((p, i) => `${i}: ${partName(g, i)}`);
/** Each part's ways ▼ | ▲ ("-": none), as the conformance test prints them. */
const steps = (g: ReturnType<typeof waterGroup>) =>
  g.parts.map((_, i) => `${stepsDown(g, i).join(",") || "-"}|${stepsUp(g, i).join(",") || "-"}`);

/** The release's `water_continue` row 11095|1|7670|0: the Chilliwack (its part 1) goes on as the Vedder (its part 0). */
const CHW_AS_VEDDER = { ord: 7670, part: 1, otherPart: 0, item: "gnis:3062", name: "Vedder River" };
const VEDDER_FROM_CHW = { ord: 11095, part: 0, otherPart: 1, item: "gnis:8634", name: "Chilliwack River" };

describe("the group's parts, mouth first (the release's rank)", () => {
  it("Chilliwack: the four parts by rank, each named by its short name, its water beside it", () => {
    const g = waterGroup([CHILLIWACK]);
    expect(order(g)).toEqual([
      "0: Vedder Crossing Bridge → Tamihi Rapids Bridge",
      "1: Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence",
      "2: 100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary",
      "3: Chilliwack River Ecological Reserve boundary → Source",
    ]);
    expect(g.parts.map((p) => p.partIx)).toEqual([1, 2, 0, 3]);
    expect([partWhere(g, 0), partWhere(g, 3)]).toEqual(["Part 1 of 4 · Chilliwack River", "Part 4 of 4 · Chilliwack River"]);
  });

  it("(T150 review) a parts-list row as a screen reader says it, whole; the list's title is the Parts sheet's one word", () => {
    const g = waterGroup([CHILLIWACK]);
    expect([partRowAria(g, 0), partRowAria(g, 3), partRowAria(waterGroup([VEDDER]), 0)]).toEqual([
      "Vedder Crossing Bridge → Tamihi Rapids Bridge, part 1 of 4 · Chilliwack River",
      "Chilliwack River Ecological Reserve boundary → Source, part 4 of 4 · Chilliwack River",
      "Vedder River, part 1 of 1"]);
    // one copy of "Parts of this water": the Map view's list says what the (start-link) Parts sheet says (that it is
    // SPELLED once, in regs/words.ts, is a source scan: tools/one-parts-list.test.ts — core may not read files)
    expect([WATER_MAP_WORDS.parts, WORDS.PARTS_TITLE]).toEqual(["Parts of this water", "Parts of this water"]);
  });

  it("Cowichan: four parts in a line, the marks as stored", () => {
    const g = waterGroup([COWICHAN]);
    expect(order(g)).toEqual([
      "0: Mouth → CNR Mile 66 Trestle", "1: CNR Mile 66 Trestle → CNR Bridge (mile 70.2)",
      "2: CNR Bridge (mile 70.2) → Greendale Trestle", "3: Greendale Trestle → Cowichan Lake outlet"]);
    expect(g.marks.map((m) => `${markLabel(g, m)}: ${m.down} → ${m.up.join(",")} at ${m.lon},${m.lat}`)).toEqual([
      "CNR Mile 66 Trestle: 0 → 1 at -123.928327,48.77634",
      "CNR Bridge (mile 70.2): 1 → 2 at -124.003497,48.803759",
      "Greendale Trestle: 2 → 3 at -124.04675,48.82827"]);
  });

  it("a one-part water is named by its water, with no note", () => {
    const g = waterGroup([VEDDER]);
    expect([partName(g, 0), itemNote(g, 0), partWhere(g, 0)]).toEqual(["Vedder River", "", "Part 1 of 1"]);
  });

  it("KLWA: a split lake's three items are one water, ordered by their exits (as the prototype's klwa)", () => {
    // opened from the Main Body; the siblings in any order
    const g = waterGroup([KL_MAIN, KL_UPPER, KL_LOWER]);
    expect(order(g)).toEqual([
      "0: Kootenay Lake — Lower West Arm", "1: Kootenay Lake — Upper West Arm", "2: Kootenay Lake — Main Body"]);
    expect(waterGroup([KL_UPPER, KL_MAIN, KL_LOWER]).parts.map((p) => p.item)).toEqual(["wbk:-22", "wbk:-21", "wbk:-20"]);
    // the boundaries between them have no place (not drawn) and the prototype's words
    expect(g.marks.map((m) => `${markLabel(g, m)}: ${m.down} → ${m.up.join(",")} at ${m.lon},${m.lat}`)).toEqual([
      "Lower West Arm / Upper West Arm: 0 → 1 at null,null", "Upper West Arm / Main Body: 1 → 2 at null,null"]);
    // the group's mouth is the Lower West Arm's: into the Kootenay River at its part 4
    expect(g.exit).toEqual({ item: "gnis:14097", name: "Kootenay River", part: 4 });
    expect(wholeLabel(g, 0, false)).toBe("⤢ Whole lake");
    // (T116) a marsh's button says what it is: waterNoun's word, not "river"
    const marsh = waterGroup([{ ...KL_MAIN, kind: "wetland" }]);
    expect([wholeLabel(marsh, 0, false), wholeAria(marsh, 0, false)]).toEqual(["⤢ Whole wetland", "Whole wetland"]);
  });

  it("KLWA opened as the split lake itself (no part of its own): its closure areas, and its mouth is theirs", () => {
    const lake: ItemNavRows = { ord: 11862, item: "wbk:328974235", name: "Kootenay Lake", kind: "lake", parts: [],
                                geo: [], marks: [], sections: [], exit: null };
    const g = waterGroup([lake, KL_UPPER, KL_MAIN, KL_LOWER]);
    expect([order(g), g.exit]).toEqual([
      ["0: Kootenay Lake — Lower West Arm", "1: Kootenay Lake — Upper West Arm", "2: Kootenay Lake — Main Body"],
      { item: "gnis:14097", name: "Kootenay River", part: 4 }]);
  });
});

describe("▼ / ▲ over the marks", () => {
  it("Cowichan: one step each way; the ends stop", () => {
    const g = waterGroup([COWICHAN]);
    expect([0, 1, 2, 3].map((i) => [stepDown(g, i), stepsUp(g, i)])).toEqual([
      [null, [1]], [0, [2]], [1, [3]], [2, []]]);
  });

  it("Chilliwack: by the marks, which name partIx — never the table's order", () => {
    const g = waterGroup([CHILLIWACK]);
    expect([0, 1, 2, 3].map((i) => [stepDown(g, i), stepsUp(g, i)])).toEqual([
      [null, [1]], [0, [2]], [1, [3]], [2, []]]);
  });

  it("a side channel: ▲ from below the fork gives the main stem first and offers the side channel", () => {
    const g = waterGroup([ZYMOETZ]);
    const canyon = partAt(g, "gnis:3137", 4), main = partAt(g, "gnis:3137", 0), side = partAt(g, "gnis:3137", 5);
    expect([canyon, main, side]).toEqual([1, 3, 2]);
    expect(stepsUp(g, canyon)).toEqual([main, side]);
    // the side channel rejoins the main stem going up, and going down returns to the canyon
    expect([stepsUp(g, side), stepDown(g, side)]).toEqual([[main], canyon]);
    // ▼ from the main stem takes the canyon, not the side channel
    expect(stepDown(g, main)).toBe(canyon);
  });

  it("where the marks say nothing, the order does", () => {
    const g = waterGroup([ZYMOETZ]);
    const lake = partAt(g, "gnis:3137", 2);       // "A lake → McDonell Lake outlet": no mark below it
    expect([lake, stepDown(g, lake), stepsUp(g, lake)]).toEqual([4, 3, [5]]);
  });

  it("a fork going DOWN: below the Zymoetz's source part are the main stem and the lake part, main stem first", () => {
    // the lake part's own mark says it sits below the source (`down 2, up [3]`), and nothing else leads into it
    const g = waterGroup([ZYMOETZ]);
    const source = partAt(g, "gnis:3137", 3), main = partAt(g, "gnis:3137", 0), lake = partAt(g, "gnis:3137", 2);
    expect([stepsDown(g, source), stepDown(g, source)]).toEqual([[main, lake], main]);
    // and above the canyon, ▼ offers the canyon first and the side channel that rejoins there
    expect(steps(g)).toEqual(["-|1", "0|3,2", "1|3", "1,2|5", "3|5", "3,4|-"]);
  });

  it("EVERY part of every water is stepped INTO by ▼ or ▲ from another part, and all are reached from the mouth", () => {
    const waters = { CHILLIWACK: [CHILLIWACK], VEDDER: [VEDDER], COWICHAN: [COWICHAN], KLWA: [KL_MAIN, KL_UPPER, KL_LOWER],
                     ZYMOETZ: [ZYMOETZ] };
    const missed: string[] = [];
    for (const [name, rows] of Object.entries(waters)) {
      const g = waterGroup(rows);
      const into = new Set(g.parts.flatMap((_, i) => [...stepsDown(g, i), ...stepsUp(g, i)].filter((j) => j !== i)));
      const seen = new Set([0]);
      const queue = [0];
      while (queue.length) {
        const i = queue.shift()!;
        for (const j of [...stepsDown(g, i), ...stepsUp(g, i)]) if (!seen.has(j)) { seen.add(j); queue.push(j); }
      }
      g.parts.forEach((p, j) => {
        if (g.parts.length > 1 && !into.has(j)) missed.push(`${name} ${j} (${p.short}): nothing steps into it`);
        if (!seen.has(j)) missed.push(`${name} ${j} (${p.short}): not reached from the mouth`);
      });
    }
    expect(missed).toEqual([]);
  });

  it("KLWA: the sibling boundaries are steps; the ends stop", () => {
    const g = waterGroup([KL_MAIN, KL_UPPER, KL_LOWER]);
    expect([0, 1, 2].map((i) => [stepDown(g, i), stepsUp(g, i)])).toEqual([[null, [1]], [0, [2]], [1, []]]);
  });
});

describe("across waters: a stream that goes on as another (`water_continue`)", () => {
  const chw = waterGroup([{ ...CHILLIWACK, continuesAs: CHW_AS_VEDDER }]);
  const ved = waterGroup([{ ...VEDDER, continuesFrom: VEDDER_FROM_CHW }]);

  it("the Vedder: ▲ from its part into the Chilliwack's lowest part (its part 1, Vedder Crossing → Tamihi)", () => {
    expect([ved.prev, ved.next, crossUp(ved, 0), crossDown(ved, 0)]).toEqual([
      { item: "gnis:8634", name: "Chilliwack River", part: 1, at: 0 }, null,
      { item: "gnis:8634", name: "Chilliwack River", part: 1, at: 0 }, null]);
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
    for (const g of [waterGroup([COWICHAN]), waterGroup([KL_MAIN, KL_UPPER, KL_LOWER]), waterGroup([CHILLIWACK]),
                     waterGroup([VEDDER])])
      expect([g.next, g.prev, ...g.parts.flatMap((_, i) => [crossDown(g, i), crossUp(g, i)])].every((x) => x === null))
        .toBe(true);
  });

  it("a cross only where this water has no step that way, and only into a water the release names", () => {
    // the junction on a part with a part above it: ▲ stays inside the water
    const mid = waterGroup([{ ...CHILLIWACK, continuesFrom: { ...VEDDER_FROM_CHW, ord: 1, part: 1 } }]);
    expect([mid.prev?.at, crossUp(mid, 0)]).toEqual([0, null]);
    expect(waterGroup([{ ...VEDDER, continuesFrom: { ...VEDDER_FROM_CHW, item: null, name: null } }]).prev).toBeNull();
    // a junction part the water does not have
    expect(waterGroup([{ ...VEDDER, continuesFrom: { ...VEDDER_FROM_CHW, part: 7 } }]).prev).toBeNull();
  });
});

describe("the exit, the highlight and the frame", () => {
  it("the exit is the receiving water at the part the mouth meets", () => {
    const g = waterGroup([CHILLIWACK]);
    expect(g.exit).toEqual({ item: "gnis:3062", name: "Vedder River", part: 0 });
    expect(exitLabel(g.exit!)).toBe("Flows into Vedder River ›");
    expect(waterGroup([COWICHAN]).exit).toBeNull();
    // a receiving water the release does not name is no exit (nothing to open)
    expect(waterGroup([{ ...VEDDER, exit: { ord: 6093, part: 0, item: null, name: null } }]).exit).toBeNull();
  });

  it("selected is exactly the part's part_section rows; water is the rest of the group", () => {
    const g = waterGroup([CHILLIWACK]);
    expect(navSections(g, 0)).toEqual({
      selected: [60210, 60211, 60212, 62453, 62571, 62823, 1235779, 1235780, 1235825],
      water: [1235781, 1235826, 1235827, 59894, 59900, 61178, 62859, 1235782, 1235783, 1235784, 1235785],
    });
    expect([partOfSection(g, 1235827), partOfSection(g, 1), partAt(g, "gnis:8634", 9)]).toEqual([1, -1, -1]);
  });

  it("frames a part by its box, the whole by the union, and a part with no box by its midpoint", () => {
    const g = waterGroup([COWICHAN]);
    expect(partBounds(g, 1)).toEqual([-124.005658, 48.773314, -123.927177, 48.803777]);
    expect(groupBounds(g)).toEqual([-124.061185, 48.751368, -123.631598, 48.831693]);
    const nobox = waterGroup([{ ...VEDDER, geo: [{ ...VEDDER.geo[0]!, box: null }] }]);
    const b = partBounds(nobox, 0)!;
    expect(b.map((x) => Math.round(x * 1e4) / 1e4)).toEqual([-122.0249, 49.0944, -122.0112, 49.1034]);
    expect(wholeLabel(g, 0, false)).toBe("⤢ Whole river");
    expect(wholeLabel(g, 0, true)).toBe("This part");
  });
});

describe("T207: a cut placed approximately says so on its boundary", () => {
  it("a low-confidence mark's label and its words for a screen reader carry the whole sentence", () => {
    const rows: ItemNavRows = { ...COWICHAN, marks: COWICHAN.marks.map((m, i) => (i === 0 ? { ...m, confidence: "low" as const } : m)) };
    const g = waterGroup([rows]);
    expect(g.marks.map((m) => markLabel(g, m))).toEqual([
      "CNR Mile 66 Trestle, approximate location (low confidence)", "CNR Bridge (mile 70.2)", "Greendale Trestle"]);
    expect(markA11y(g, g.marks[0]!)).toBe("Boundary: CNR Mile 66 Trestle, approximate location (low confidence)");
  });

  it("the one sentence, as the pipeline's `confidence_words` says it", () => {
    expect([confidenceWords(undefined), confidenceWords("high"), confidenceWords("medium"), confidenceWords("low")])
      .toEqual(["", "", "approximate location (medium confidence)", "approximate location (low confidence)"]);
    expect([isConfidence("low"), isConfidence("certain"), isConfidence(null)]).toEqual([true, false, false]);
  });
});

/** (T204) Kanaka Creek (ord 455) as the T204 bundle writes it: part 0 is below the 112 Avenue bridge AND above Cliff
 *  Park Falls (two stretches), part 1 between them; the pair meets at BOTH places. */
const KANAKA: ItemNavRows = {
  ord: 455, item: "gnis:11318", name: "Kanaka Creek", kind: "stream",
  parts: [
    { partIx: 0, label: "Two stretches: from the mouth up to the 112 Avenue bridge, and from Cliff Park Falls up to the top of the river", short: "Mouth → Source (2 stretches)" },
    { partIx: 1, label: "From the 112 Avenue bridge up to Cliff Park Falls", short: "112 Avenue bridge → Cliff Park Falls" },
  ],
  geo: [geo(1, 0, [-122540000, 49205000], -122550000, 49200000, -122530000, 49210000),
        geo(0, 1, [-122500000, 49220000], -122650000, 49190000, -122400000, 49250000)],
  marks: [
    { ...mark(-122550000, 49200000, "112 Avenue bridge", 0, [1]), downStretch: 0 },
    { ...mark(-122536222, 49207142, "Cliff Park Falls", 1, [0]), upStretches: [1] },
  ],
  sections: secs({ 0: [294874, 294876, 294877], 1: [294875] }),
  stretches: [
    { partIx: 0, stretch: 0, rank: 0, box: [-122.65, 49.19, -122.55, 49.2], mid: [-122.6, 49.195], sids: [294874] },
    { partIx: 1, stretch: 0, rank: 1, box: [-122.55, 49.2, -122.53, 49.21], mid: [-122.54, 49.205], sids: [294875] },
    { partIx: 0, stretch: 1, rank: 2, box: [-122.53, 49.21, -122.4, 49.25], mid: [-122.45, 49.23], sids: [294876, 294877] },
  ],
  exit: null,
};

describe("T204: a part's separate stretches", () => {
  it("Kanaka: three entries mouth first — part 0's lower stretch, part 1, part 0's upper stretch — each its own name", () => {
    const g = waterGroup([KANAKA]);
    expect(g.parts.map((p) => [p.partIx, p.stretch ?? 0, p.sids])).toEqual([
      [0, 0, [294874]], [1, 0, [294875]], [0, 1, [294876, 294877]]]);
    expect([0, 1, 2].map((i) => partName(g, i))).toEqual([
      "Mouth → Source (2 stretches) · stretch 1 of 2", "112 Avenue bridge → Cliff Park Falls",
      "Mouth → Source (2 stretches) · stretch 2 of 2"]);
  });

  it("Kanaka: the bridge is below part 1 and the falls above it — each mark on the stretch that meets there", () => {
    const g = waterGroup([KANAKA]);
    expect(g.marks.map((m) => [m.label, m.down, m.up])).toEqual([["112 Avenue bridge", 0, [1]], ["Cliff Park Falls", 1, [2]]]);
    expect(stepsUp(g, 0)).toEqual([1]);
    expect(stepsUp(g, 1)).toEqual([2]);
    expect(stepDown(g, 2)).toBe(1);
    // each stretch is lit and framed on its own
    expect(navSections(g, 2)).toEqual({ selected: [294876, 294877], water: [294874, 294875] });
    expect(partBounds(g, 0)).toEqual([-122.65, 49.19, -122.55, 49.2]);
    // the regulations are the part's: both stretches open part 0
    expect(partAt(g, "gnis:11318", 0)).toBe(0);
    expect(partOfSection(g, 294877)).toBe(2);
  });

  it("Chilliwack (a chain, no stretch rows) is unchanged", () => {
    const g = waterGroup([CHILLIWACK]);
    expect(g.parts.map((p) => [p.partIx, p.stretch])).toEqual([[1, undefined], [2, undefined], [0, undefined], [3, undefined]]);
    expect(g.marks.map((m) => [m.down, m.up])).toEqual([[2, [3]], [0, [1]], [1, [2]]]);
  });
});
