/**
 * T12: the exclusion list stays honest — every entry says why, names states that exist, and every keyed entry still
 * matches something in the golden (a dead exclusion hides nothing and must go).
 */
import { describe, expect, it } from "vitest";
import type { GoldenState } from "@app/core/regs/testing";
import { EXCLUDE, excluded, NO_MAP_LABEL_IDS, PINS_ERROR_IDS, TROUT_LINE_IDS } from "./exclusions";
import { golden, taps } from "./oracles";

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

  it("every listed id is a taps.json state", () => {
    for (const e of EXCLUDE) if (e.ids !== "*") for (const id of e.ids) expect(T.states[id], id).toBeDefined();
  });

  it("every keyed entry matches a key some golden state has", () => {
    for (const e of EXCLUDE.filter((x) => x.key)) {
      const hit = Object.values(G.states).some((s) => keysOf(s, e.block).some((k) => e.key!.test(k)));
      expect([e.block, e.key!.source, hit]).toEqual([e.block, e.key!.source, true]);
    }
  });

  it("the list, by block and key", () => {
    expect(EXCLUDE.map((e) => `${e.block} ${e.ids === "*" ? "*" : e.ids.length} ${e.key?.source ?? "-"}`))
      .toEqual(["sheets * ^parts$", "sheets * ^rule:", `header ${NO_MAP_LABEL_IDS.length} ^place$`, "header * ^place\\.shown$",
        "keep 3 ^groups\\.\\d+\\.(tree|worked)$"]);
  });
});

describe("TROUT_LINE_IDS", () => {
  it("the 11 golden states whose keep.strip.line carries a 'Trout …' line (the only strip part a per-row year strip answers)", () => {
    expect(TROUT_LINE_IDS).toEqual(["x5p1_1010", "x11p1_1010", "x15p8_1010", "x15p5_1010", "x15p13_1010", "x15p4_1010",
                                    "x15p14_1010", "x15p15_1010", "x15p6_1010", "x21p0_1010", "x32p0_1010"]);
    expect(Object.values(G.states).filter((s) => (s.keep as { strip?: unknown } | null)?.strip).length).toBe(133);
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

describe("PINS_ERROR_IDS", () => {
  it("none at golden v38 @ bdda3586: the prototype's pinUpdate throw (25 states at v33, facts A#25) is fixed", () => {
    expect([G.meta.version, G.meta.sha, PINS_ERROR_IDS]).toEqual(["v38", "bdda3586", []]);
  });
});

describe("excluded", () => {
  it("a keyed entry excludes its key only, never the whole state", () => {
    expect(excluded("sheets", "vedder_515", "parts")?.reason).toMatch(/^the prototype's parts sheet/);
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
