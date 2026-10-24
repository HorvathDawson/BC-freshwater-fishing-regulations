/**
 * T12: the exclusion list stays honest — every entry says why, names states that exist, and every keyed entry still
 * matches something in the golden (a dead exclusion hides nothing and must go).
 */
import { describe, expect, it } from "vitest";
import type { GoldenState } from "@app/core/regs/testing";
import { comparableGroups, D19_GROUPS, D19_POOLED_IDS, droppedLines, EXCLUDE, excluded, GOLDEN_MAP_ACTION_IDS, GOLDEN_PARTS_ACTION_IDS, KEEP_STAMP_LINE_IDS, keepLines, NO_MAP_LABEL_IDS, SET_LINE_NOTE_IDS, STAMP_LINE, STAMP_LINE_IDS, TROUT_LINE_IDS,
         YEAR_WINDOW_IDS, YEAR_WINDOW_LINE, OTHER_FISH_STAMP_LINE } from "./exclusions";
import { golden, taps, ZONE7_REWRITES } from "./oracles";

const G = golden(), T = taps();

/** The keys a block of one golden state has: sheet keys, else every dotted path under the block. */
const keysOf = (s: GoldenState, block: string): string[] => {
  if (block === "sheets") return Object.keys(s.sheets);
  const out: string[] = [];
  const walk = (o: unknown, p: string) => {
    if (o === null || typeof o !== "object") return;   // (T18) arrays too, by index: "groups.0.tree"
    for (const [k, v] of Object.entries(o)) { const q = p ? `${p}.${k}` : k; out.push(q); walk(v, q); }
  };
  walk((s as unknown as Record<string, unknown>)[block], "");
  return out;
};

describe("EXCLUDE", () => {
  it("every entry has a non-empty reason", () => {
    for (const e of EXCLUDE) expect(e.reason.trim().length, `${e.block} ${e.key ?? ""}`).toBeGreaterThan(20);
  });

  it("every listed id is a taps.json state (tapped, or declared gone)", () => {
    for (const e of EXCLUDE) if (e.ids !== "*") for (const id of e.ids) expect(T.states[id] ?? T.gone[id], id).toBeDefined();
  });

  it("(T115) a state whose part is gone is excluded in every block, with its taps.json reason — and only those states are", () => {
    const blocks = ["header", "keep", "gear", "methods", "licence", "sheets", "pins"] as const;
    const gone = Object.keys(T.gone).sort();
    expect(gone).toEqual(["x15p14_1010", "x15p15_1010", "x15p17_1010", "x15p18_1010", "x19p5_1010", "x22p2_1010"]);
    for (const b of blocks) {
      for (const id of gone) expect(excluded(b, id)?.reason, `${b} ${id}`).toBe(`part gone — ${T.gone[id]!.reason}`);
      expect(Object.keys(T.states).filter((id) => excluded(b, id)?.reason.startsWith("part gone")), b).toEqual([]);
    }
    for (const id of gone) expect(T.states[id], id).toBeUndefined();
  });

  it("every keyed entry matches a key some golden state has", () => {
    for (const e of EXCLUDE.filter((x) => x.key)) {
      const hit = Object.values(G.states).some((s) => keysOf(s, e.block).some((k) => e.key!.test(k)));
      expect([e.block, e.key!.source, hit]).toEqual([e.block, e.key!.source, true]);
    }
  });

  it("the list, by block and key", () => {
    // (T115) first: the 4 gone states × 7 blocks, whole-state; the 4 grown parts' labels; the Parts sheet of the re-cut
    // Fraser (14 states, T115) and Skeena (7, T67); T67's two closed states gone
    const gone = ["x15p14_1010", "x15p15_1010", "x15p17_1010", "x15p18_1010", "x19p5_1010", "x22p2_1010"].flatMap(() =>
      ["header 1 -", "keep 1 -", "gear 1 -", "methods 1 -", "licence 1 -", "sheets 1 -", "pins 1 -"]);
    expect(EXCLUDE.map((e) => `${e.block} ${e.ids === "*" ? "*" : e.ids.length} ${e.key?.source ?? "-"}`))
      .toEqual([...gone, "header 4 ^place\\.(full|short)$", "sheets 21 ^parts$", "sheets 1 ^(other:x18p5_1010\\|o0)$", "sheets 1 ^(other:x22p1_1010\\|o1)$", "sheets 1 ^(other:x22p3_1010\\|o1)$",
        "sheets * ^rule:", `header ${NO_MAP_LABEL_IDS.length} ^place$`, `header ${GOLDEN_MAP_ACTION_IDS.length} ^action$`,
        `header ${GOLDEN_PARTS_ACTION_IDS.length} ^action$`, "header * ^place\\.shown$",
        "keep 3 ^groups\\.\\d+\\.(tree|worked|lines)$",
        "pins 3 ^anchors\\.\\d+\\.tiers$", "keep 1 ^plainRows\\.0$", "gear 4 ^bait\\.src\\.who$",
        `methods ${SET_LINE_NOTE_IDS.length} ^ways\\.list\\.0\\.items\\.\\d+\\.note$`,
        "licence 1 -"]);
    // the four pipeline needs shipped (rows 32, licence 7, static export.pdf_pages): no entry names them any more
    expect(EXCLUDE.filter((e) => /PIPELINE NEED|PENDING/.test(e.reason))).toEqual([]);
    expect(SET_LINE_NOTE_IDS).toEqual(["x3p_1010", "x12p_1010", "x30p_1010", "x35p_1010"]);
  });
});

describe("TROUT_LINE_IDS", () => {
  it("the 11 golden states whose keep.strip.line carries a 'Trout …' line (the only strip part a per-row year strip answers)", () => {
    expect(TROUT_LINE_IDS).toEqual(["x5p1_1010", "x11p1_1010", "x15p8_1010", "x15p5_1010", "x15p13_1010", "x15p4_1010",
                                    "x15p14_1010", "x15p15_1010", "x15p6_1010", "x21p0_1010", "x32p0_1010"]);
    expect(Object.values(G.states).filter((s) => (s.keep as { strip?: unknown } | null)?.strip).length).toBe(133);
  });
});

describe("(T71 / T150) the golden header's actions", () => {
  it("every golden state with an action is excluded, by its kind's entry: the app's header has no action (owner call 3)", () => {
    const withAction = Object.entries(G.states).filter(([, s]) => (s.header as { action: unknown } | null)?.action).map(([id]) => id);
    expect([...GOLDEN_MAP_ACTION_IDS, ...GOLDEN_PARTS_ACTION_IDS].sort()).toEqual([...withAction].sort());
    expect(GOLDEN_PARTS_ACTION_IDS.filter((id) => GOLDEN_MAP_ACTION_IDS.includes(id))).toEqual([]);
    expect(GOLDEN_PARTS_ACTION_IDS.length).toBeGreaterThan(0);
    expect(excluded("header", GOLDEN_PARTS_ACTION_IDS[0]!, "action")?.reason).toMatch(/^owner call 3 \(2026-10-23, T150, Q122\)/);
    expect(excluded("header", GOLDEN_MAP_ACTION_IDS[0]!, "action")?.reason).toMatch(/^owner call 3 \(2026-10-22, T71\)/);
  });
});

describe("NO_MAP_LABEL_IDS", () => {
  it("the 22 golden states with no part label and no Map action (the prototype had no water map of the water)", () => {
    expect(NO_MAP_LABEL_IDS).toEqual(["wilkes_1010", "anderson_1010", "ape_501", "ape_1010", "glenn_501", "glenn_615", "glenn_1010", "hamill_615", "hamill_1010",
      "iron_720", "iron_1010", "x2p_1010", "x3p_1010", "x4p_1010", "x12p_1010", "x25p_1010", "x26p_1010", "x30p_1010", "x31p_1010",
      "x34p_1010", "x35p_1010", "x41p_1010"]);
    expect(excluded("header", "wilkes_1010", "place")?.reason).toMatch(/no water map/);
    expect(excluded("header", "vedder_1010", "place")).toBeNull();
  });
});

describe("(T17–T19 review) dropped golden lines", () => {
  it("the 8 states with a stamp line (compared since rows 31), the 11 with a .gyear line", () => {
    expect(STAMP_LINE_IDS).toEqual(["anderson_1010", "x4p_1010", "x13p4_1010", "x15p16_1010", "x15p1_1010", "x15p17_1010", "x20p1_1010", "x23p0_1010"]);
    expect(YEAR_WINDOW_IDS).toEqual(["glenn_501", "glenn_615", "glenn_1010", "hamill_615", "hamill_1010", "x9p0_1010", "x9p1_1010", "x9p5_1010",
      "x21p2_1010", "x21p1_1010", "x32p1_1010"]);
  });

  it("a lines entry never excludes its key; it drops only its lines, only in its states, only from its keys", () => {
    expect(excluded("keep", "anderson_1010", "groups.0.lines")).toBeNull();
    expect(excluded("keep", "x4p_1010", "groups.0.tree")).toBeNull();
    // rows 31: the stamp lines are the store's `stamps`, drawn and compared — no state drops them
    expect(STAMP_LINE_IDS.flatMap((id) => [...droppedLines("keep", id, "groups.0.lines"), ...droppedLines("keep", id, "groups.0.tree")])).toEqual([]);
    expect(droppedLines("keep", "glenn_501", "groups.0.lines")).toEqual([]);   // (T21 review) the .gyear line is drawn
    expect(droppedLines("keep", "glenn_501", "groups.0.tree")).toEqual([]);
    expect(droppedLines("keep", "vedder_1010", "groups.0.lines")).toEqual([]);
    expect(droppedLines("keep", "anderson_1010", "groups.0.notKept")).toEqual([]);
    expect(keepLines(["a", "You need X ›", "Through the year b", "c"], [YEAR_WINDOW_LINE])).toEqual(["a", "You need X ›", "c"]);
    expect(STAMP_LINE.test("You need Steelhead Conservation Surcharge Stamp › only if you fish for steelhead, even to release them")).toBe(true);
  });

  it("the Kitimat-ruled states' group lines are excluded whole (their tree and worked are held to the store)", () => {
    expect(excluded("keep", "x14p0_1010", "groups.0.lines")?.reason).toMatch(/^OWNER RULING 2026-10-21 \(a\)/);
    expect(excluded("keep", "vedder_1010", "groups.0.lines")).toBeNull();
  });
});

describe("(T21) the whole Keep tab", () => {
  it("the 14 states whose keep.lines carry an OTHER FISH stamp line — compared since rows 32, never dropped; the D19 row", () => {
    expect(KEEP_STAMP_LINE_IDS.length).toBe(14);
    expect(KEEP_STAMP_LINE_IDS.flatMap((id) => droppedLines("keep", id, "lines"))).toEqual([]);
    expect(OTHER_FISH_STAMP_LINE.test("You need White Sturgeon Conservation Licence › only if you fish for white sturgeon, even to release them")).toBe(true);
    // a group's stamp line is never dropped: the 8 group-stamp states keep "You need … Steelhead/Shuswap" compared
    expect(OTHER_FISH_STAMP_LINE.test("You need Steelhead Conservation Surcharge Stamp › only if you fish for steelhead")).toBe(false);
    expect(droppedLines("keep", "glenn_501", "lines")).toEqual([]);   // (T21 review) the .gyear line is drawn
    expect(EXCLUDE.some((x) => x.lines === YEAR_WINDOW_LINE)).toBe(false);
    expect(droppedLines("keep", "cow1_1010", "lines")).toEqual([]);
    expect(D19_POOLED_IDS).toEqual(["klmb_1010"]);
    expect(excluded("keep", "klmb_1010", "plainRows.0")?.reason).toMatch(/^decisions D19/);
    expect(excluded("keep", "klmb_1010", "order")).toBeNull();
    expect(excluded("keep", "klmb_1010", "lines")).toBeNull();
  });

  it("D19 (a) as built (T80): the app's klmb_1010 group is set aside only while it is there — once, on its row", () => {
    expect(D19_GROUPS).toEqual([{ id: "klmb_1010", row: 2, title: "Rainbow trout" }]);
    const g = (rowIx: number) => ({ ctx: { rowIx } });
    expect(comparableGroups("klmb_1010", [g(0), g(2)])).toEqual([g(0)]);
    expect(comparableGroups("vedder_1010", [g(0), g(2)])).toEqual([g(0), g(2)]);      // other states untouched
    expect(() => comparableGroups("klmb_1010", [g(0)])).toThrow(/D19_GROUPS: klmb_1010 draws 0 groups on row r2/);
    expect(() => comparableGroups("klmb_1010", [g(2), g(2)])).toThrow(/draws 2 groups/);
  });
});

describe("pins (T28, IMPLEMENT-HANDOFF C5)", () => {
  it("no golden state carries the v33 pinUpdate throw at v38 @ bdda3586; no state is dropped, only the Kitimat ruling's tier lines", () => {
    const errs = Object.entries(G.states).filter(([, s]) => !!(s.pins as { error?: unknown } | null)?.error).map(([id]) => id);
    expect([G.meta.version, G.meta.sha, errs]).toEqual(["v38", "bdda3586", []]);
    // no state is dropped for a pins reason; (T115) the 4 whose part the release merged away are dropped from every block
    expect(Object.keys(G.states).filter((id) => excluded("pins", id) !== null).sort()).toEqual(Object.keys(T.gone).sort());
    expect(excluded("pins", "x14p1_1010", "anchors.0.tiers")?.reason).toMatch(/^OWNER RULING 2026-10-21 \(a\)/);
    expect(excluded("pins", "x14p1_1010", "anchors.0.groupLine")).toBeNull();
    expect(excluded("pins", "vedder_1010", "anchors.0.tiers")).toBeNull();
  });
});

describe("excluded", () => {
  it("a keyed entry excludes its key only, never the whole state", () => {
    expect(excluded("sheets", "vedder_515", "parts")).toBeNull();   // built (T36: choicesOnDay); its D1 note is a SHEET_SUBST
    expect(excluded("sheets", "x22p1_1010", "other:x22p1_1010|o1")?.reason).toMatch(/^decisions D19: /);
    expect(excluded("sheets", "x22p1_1010", "other:x22p1_1010|o0")).toBeNull();
    expect(excluded("sheets", "vedder_515", "rule:r2:x::y.r1")?.reason).toMatch(/^'Technical detail'/);
    expect(excluded("sheets", "vedder_515", "pos:pos")).toBeNull();
    expect(excluded("sheets", "vedder_515")).toBeNull();
    expect(excluded("header", "vedder_515", "place.shown")?.reason).toMatch(/fitLabel/);
    expect(excluded("header", "vedder_515", "place.full")).toBeNull();
  });

  it("keep.strip is compared whole on every state (T13): the trout line reads the stored per-row year strip, rows 30", () => {
    for (const k of ["strip", "strip.line", "strip.lines", "strip.bar", "strip.legend", "strip.months", "strip.today", "strip.status", "strip.statusClass"])
      expect(excluded("keep", "x5p1_1010", k), k).toBeNull();
    expect(excluded("keep", "vedder_515", "strip.line")).toBeNull();
    expect(excluded("keep", "vedder_515", "groups")).toBeNull();
    expect(excluded("keep", "x5p1_1010")).toBeNull();
  });

  it("(T18/T19) the Kitimat ruling excludes only the 3 ruled states' group trees and worked lines, never their heads, not-kept lines or roots", () => {
    expect(excluded("keep", "x14p0_1010", "groups.0.tree")?.reason).toMatch(/^OWNER RULING 2026-10-21 \(a\)/);
    expect(excluded("keep", "x14p0_1010", "groups.0.worked")?.reason).toMatch(/^OWNER RULING 2026-10-21 \(a\)/);
    for (const k of ["groups", "groups.0", "groups.0.head", "groups.0.notKept", "groups.0.treeRoot", "groups.0.quiet"]) expect(excluded("keep", "x14p0_1010", k), k).toBeNull();
    expect(excluded("keep", "vedder_1010", "groups.0.tree")).toBeNull();
  });
});

describe("T109 zone-level ruling (oracles.ts zone7Ruled)", () => {
  it("rewrites exactly the golden's 'Region 7' level words in the 8 zone-7 states, each to its one printed zone", () => {
    expect(ZONE7_REWRITES).toEqual({ x15p0_1010: 3, x15p18_1010: 3, x22p0_1010: 9, x22p1_1010: 9, x22p3_1010: 9, x22p6_1010: 9,
      x27p2_1010: 3, x41p_1010: 9 });
    const all = JSON.stringify(G.states);
    expect(all.match(/Region 7(?= ›| From the Region 7[AB] table)/g)).toBeNull();
    // the place words the ruling does not touch (a header sub, the decided-by line of an r7 water row) are still the golden's
    expect(all.match(/Region 7 · for this water \(Peace River\)|Where Region 5 and Region 7 meet|In Region 7 · /g)?.length).toBe(5);
  });
});
