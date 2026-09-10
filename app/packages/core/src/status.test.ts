import { describe, expect, it } from "vitest";
import type { PlainDate, Window } from "./dates";
import { inForce, inWindow } from "./dates";
import { evaluate, statusWord, FAMILY_OF, type Rule } from "./status";

const on = (month: number, day: number): PlainDate => ({ year: 2026, month, day });
const w = (a: [number, number], b: [number, number]): Window => ({
  from: { month: a[0], day: a[1] },
  to: { month: b[0], day: b[1] },
});
const rule = (p: Partial<Rule> & Pick<Rule, "id" | "type">): Rule => ({
  scope: "section", via: "reach", group: "provincial", windows: [],
  family: FAMILY_OF[p.type], dimension: p.type, label: "", ...p,
});

/** A closure is a retention limit of zero you may not fish for — not a kind of rule. */
const closure = (p: Partial<Rule> & Pick<Rule, "id">): Rule =>
  rule({ type: "retention_limit", take: 0, mayTarget: false, ...p });

describe("season windows", () => {
  it("no window means all year, never never", () => {
    // Reading an empty window list as "never" renders every year-round closure as open.
    expect(inForce([], on(1, 1))).toBe(true);
    expect(inForce([], on(8, 30))).toBe(true);
  });

  it("includes both ends", () => {
    const jun = w([6, 1], [6, 30]);
    expect(inWindow(jun, on(6, 1))).toBe(true);
    expect(inWindow(jun, on(6, 30))).toBe(true);
    expect(inWindow(jun, on(5, 31))).toBe(false);
    expect(inWindow(jun, on(7, 1))).toBe(false);
  });

  it("handles a window that wraps the new year", () => {
    const winter = w([10, 15], [4, 15]);   // Oct 15 - Apr 15
    expect(inWindow(winter, on(12, 25))).toBe(true);
    expect(inWindow(winter, on(1, 2))).toBe(true);
    expect(inWindow(winter, on(4, 15))).toBe(true);
    expect(inWindow(winter, on(8, 30))).toBe(false);
  });
});

describe("outcome and provenance are two channels", () => {
  it("water nobody wrote a rule about is OPEN and general, not unknown", () => {
    const s = evaluate({ rules: [], on: on(8, 30), group: "provincial" });
    expect(s.outcome).toBe("open");
    expect(s.provenance).toBe("general");
    expect(statusWord(s)).toBe("OPEN · GENERAL RULES");
  });

  it("a rule written for this water is OPEN and specific when nothing is in force", () => {
    const s = evaluate({
      rules: [rule({ id: "r1", type: "retention_limit", take: 0, mayTarget: false, windows: [w([6, 1], [6, 30])] })],
      on: on(8, 30), group: "provincial",
    });
    expect(s.outcome).toBe("open");
    expect(s.provenance).toBe("specific");
  });
});

describe("the date decides", () => {
  const chilliwack = rule({ id: "cw.r5", type: "retention_limit", take: 0, mayTarget: false, windows: [w([6, 1], [6, 30])] });
  it("closed inside the window", () => {
    expect(evaluate({ rules: [chilliwack], on: on(6, 15), group: "provincial" }).outcome)
      .toBe("closed");
  });
  it("open outside it", () => {
    expect(evaluate({ rules: [chilliwack], on: on(8, 30), group: "provincial" }).outcome)
      .toBe("open");
  });
});

describe("scopes", () => {
  it("a zone rule closes water nobody wrote about", () => {
    const spring = rule({ id: "mu.spring", type: "retention_limit", take: 0, mayTarget: false,
                          scope: "mu", via: "reach", windows: [w([4, 1], [6, 15])] });
    expect(evaluate({ rules: [spring], on: on(5, 1), group: "provincial" }).outcome)
      .toBe("closed");
  });

  it("a rule for THIS water replaces the zone default about the same thing", () => {
    // The point of the key: a lake may deliberately be more permissive than its MU — a
    // quota of 6 where the management unit says 2. BOTH have to be daily retention limits
    // to compete; that is what (type, dimension) means, and it is why the lake's quota does
    // not silently displace the MU's bait ban.
    const zone = rule({ id: "mu.quota", type: "retention_limit", take: 2, scope: "mu",
                        via: "reach", dimension: "daily" });
    const lake = rule({ id: "lake.quota", type: "retention_limit", take: 6, scope: "section",
                        via: "reach", dimension: "daily" });
    const s = evaluate({ rules: [zone, lake], on: on(8, 30), group: "provincial" });
    // A quota is a restriction whichever number it carries, so the OUTCOME is unchanged;
    // what changes is which rule the reader is shown as the answer.
    expect(s.outcome).toBe("restricted");
    expect(s.from.map((r) => r.id)).not.toContain("mu.quota");
    expect(s.from.map((r) => r.id)).toContain("lake.quota");
  });

  it("a possession quota does not displace a daily one", () => {
    // Same type, different dimension. Without the second half of the key every retention
    // rule on a water would collide with every other.
    const daily = rule({ id: "mu.daily", type: "retention_limit", take: 2, scope: "mu",
                         via: "reach", dimension: "daily" });
    const poss = rule({ id: "lake.poss", type: "retention_limit", take: 8, scope: "section",
                        via: "reach", dimension: "possession" });
    const s = evaluate({ rules: [daily, poss], on: on(8, 30), group: "provincial" });
    expect(s.from.map((r) => r.id)).toContain("mu.daily");
  });

  it("but it does not replace a zone rule about something else", () => {
    const bait = rule({ id: "mu.bait", type: "bait_restriction", scope: "mu", via: "reach",
                        dimension: "bait:any" });
    const quota = rule({ id: "lake.quota", type: "advisory", scope: "section", via: "reach",
                         dimension: "daily" });
    expect(evaluate({ rules: [bait, quota], on: on(8, 30), group: "provincial" }).outcome)
      .toBe("restricted");
  });

  it("an area closure is absolute — a permissive local rule cannot open it", () => {
    const reserve = closure({ id: "eco", scope: "area", via: "reach", absolute: true });
    const local = rule({ id: "lake.quota", type: "advisory", scope: "section", via: "reach" });
    expect(evaluate({ rules: [reserve, local], on: on(8, 30), group: "provincial" }).outcome)
      .toBe("closed");
  });
});

describe("salmon is a parallel authority, not a precedence level", () => {
  const provincialOpen = rule({ id: "bc", type: "advisory", scope: "section", via: "reach" });
  const dfoClosed = rule({ id: "dfo", type: "retention_limit", take: 0, mayTarget: false, scope: "section", via: "reach",
                           group: "salmon", absolute: true });
  const both = [provincialOpen, dfoClosed];

  it("open for trout", () => {
    expect(evaluate({ rules: both, on: on(8, 30), group: "provincial" }).outcome).toBe("open");
  });
  it("and closed for salmon, on the same day", () => {
    expect(evaluate({ rules: both, on: on(8, 30), group: "salmon" }).outcome).toBe("closed");
  });
});

describe("unknown is never rendered as open", () => {
  it("a rule we could not place makes the water unknown", () => {
    const s = evaluate({
      rules: [rule({ id: "fraser.r4", type: "retention_limit", take: 0, mayTarget: false, uncertain: true })],
      on: on(8, 30), group: "provincial",
    });
    expect(s.outcome).toBe("unknown");
    expect(s.because).toBe("unplaceable");
  });

  it("an unreachable override feed makes it unknown even with no rules", () => {
    const s = evaluate({ rules: [], on: on(8, 30), group: "provincial", feedUnreachable: true });
    expect(s.outcome).toBe("unknown");
    expect(s.because).toBe("feed-unreachable");
  });

  it("but a closure we DID place still reads closed, not unknown", () => {
    const s = evaluate({
      rules: [rule({ id: "a", type: "retention_limit", take: 0, mayTarget: false }), rule({ id: "b", type: "retention_limit", take: 0, mayTarget: false, uncertain: true })],
      on: on(8, 30), group: "provincial",
    });
    expect(s.outcome).toBe("closed");
  });
});

describe("a zero limit is not always a closed river", () => {
  /*
   * MEASURED, NOT HYPOTHETICAL. "Only non-game fish (such as carp) may be speared" is a
   * retention limit of zero on every game fish — correctly, that is what it says — and it
   * carries `method: "spear_fishing"`. It reaches 1,674 of the bundle's 1,693 rulesets, so a
   * severity that reads `take === 0 && mayTarget === false` and stops there returns "closed"
   * for essentially every section in British Columbia, every day of the year.
   */
  const spear = rule({
    id: "zp:spear_fishing.r1", type: "retention_limit", take: 0, mayTarget: false,
    method: "spear_fishing", species: ["ALL_GAME_FISH"], scope: "area", via: "reach",
  });

  it("a closure of one METHOD leaves the water open", () => {
    const s = evaluate({ rules: [spear], on: on(9, 10), group: "provincial" });
    expect(s.outcome).not.toBe("closed");
  });

  it("a closure of one SPECIES is not the whole water", () => {
    const sturgeon = rule({ id: "z4.sturgeon", type: "retention_limit", take: 0,
                            mayTarget: false, species: ["WSG"], scope: "area", via: "reach" });
    expect(evaluate({ rules: [sturgeon], on: on(9, 10), group: "provincial" }).outcome)
      .not.toBe("closed");
  });

  it("but a closure naming the whole closed list still shuts it", () => {
    const shut = rule({ id: "r2.no_fishing", type: "retention_limit", take: 0,
                        mayTarget: false, species: ["ALL_GAME_FISH"] });
    expect(evaluate({ rules: [shut], on: on(9, 10), group: "provincial" }).outcome)
      .toBe("closed");
  });

  it("and so does one that names no species at all", () => {
    const shut = rule({ id: "r2.no_fishing", type: "retention_limit", take: 0,
                        mayTarget: false });
    expect(evaluate({ rules: [shut], on: on(9, 10), group: "provincial" }).outcome)
      .toBe("closed");
  });

  it("the spear rule beside a real closure still reads closed", () => {
    const shut = rule({ id: "r2.no_fishing", type: "retention_limit", take: 0,
                        mayTarget: false, species: ["ALL_GAME_FISH"] });
    expect(evaluate({ rules: [spear, shut], on: on(9, 10), group: "provincial" }).outcome)
      .toBe("closed");
  });
});
