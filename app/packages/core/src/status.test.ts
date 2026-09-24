import { describe, expect, it } from "vitest";
import type { PlainDate, When, Window } from "./dates";
import { ALL_YEAR, holdsOn, inForce, inWindow, weekdayOf } from "./dates";
import { evaluate, statusWord, FAMILY_OF, type Rule } from "./status";

const on = (month: number, day: number): PlainDate => ({ year: 2026, month, day });
const w = (a: [number, number], b: [number, number]): Window => ({
  from: { month: a[0], day: a[1] },
  to: { month: b[0], day: b[1] },
});
const rule = (p: Partial<Rule> & Pick<Rule, "id" | "type">): Rule => ({
  scope: "section", via: "reach", group: "provincial", when: ALL_YEAR,
  family: FAMILY_OF[p.type], dimension: p.type, label: "", ...p,
});

const season = (...dates: Window[]): When => ({ ...ALL_YEAR, dates });

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
      rules: [rule({ id: "r1", type: "retention_limit", take: 0, mayTarget: false, when: season(w([6, 1], [6, 30])) })],
      on: on(8, 30), group: "provincial",
    });
    expect(s.outcome).toBe("open");
    expect(s.provenance).toBe("specific");
  });
});

describe("the date decides", () => {
  const chilliwack = rule({ id: "cw.r5", type: "retention_limit", take: 0, mayTarget: false, when: season(w([6, 1], [6, 30])) });
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
                          scope: "mu", via: "reach", when: season(w([4, 1], [6, 15])) });
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
   * carries `while: ["spear_fishing"]`. It reaches 1,674 of the bundle's 1,693 rulesets, so a
   * severity that reads `take === 0 && mayTarget === false` and stops there returns "closed"
   * for essentially every section in British Columbia, every day of the year.
   */
  const spear = rule({
    id: "zp:spear_fishing.r1", type: "retention_limit", take: 0, mayTarget: false,
    while: ["spear_fishing"], species: ["ALL_GAME_FISH"], scope: "area", via: "reach",
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

describe("when: weekdays, hours and a season nobody could read", () => {
  it("knows its weekdays", () => {
    // 2026-09-12 is a Saturday.
    expect(weekdayOf({ year: 2026, month: 9, day: 12 })).toBe("Saturday");
    expect(weekdayOf({ year: 2026, month: 9, day: 14 })).toBe("Monday");
    expect(weekdayOf({ year: 2024, month: 2, day: 29 })).toBe("Thursday");
  });

  it("a weekend-only rule binds on the weekend, inside its dates, and not otherwise", () => {
    const weekends: When = { ...ALL_YEAR, dates: [w([9, 1], [10, 31])],
                             weekdays: ["Saturday", "Sunday"] };
    expect(holdsOn(weekends, { year: 2026, month: 9, day: 12 })).toBe(true);    // Sat
    expect(holdsOn(weekends, { year: 2026, month: 9, day: 14 })).toBe(false);   // Mon
    expect(holdsOn(weekends, { year: 2026, month: 11, day: 7 })).toBe(false);   // Sat, out of dates
    const shut = closure({ id: "r5.weekend", when: weekends });
    expect(evaluate({ rules: [shut], on: { year: 2026, month: 9, day: 12 },
                      group: "provincial" }).outcome).toBe("closed");
    expect(evaluate({ rules: [shut], on: { year: 2026, month: 9, day: 14 },
                      group: "provincial" }).outcome).toBe("open");
  });

  it("a closure for part of the day restricts the day, it does not close it", () => {
    /* campbell_river.r4: "No Fishing from 21:00 hours to 05:00 hours each day, Aug 1-Dec 31".
       A day-level status that read it as closed shut the river at noon. */
    const night = closure({ id: "r2.campbell.r4", when: {
      ...ALL_YEAR, dates: [w([8, 1], [12, 31])],
      hours: { start: { at: "21:00" }, end: { at: "05:00" } } } });
    const s = evaluate({ rules: [night], on: on(8, 30), group: "provincial" });
    expect(s.outcome).toBe("restricted");
    expect(evaluate({ rules: [night], on: on(7, 30), group: "provincial" }).outcome)
      .toBe("open");
  });

  it("an unreadable season is uncertain — never all year, never never", () => {
    const tbd = closure({ id: "dfo.r1", when: { ...ALL_YEAR, unparsed: ["To be determined"] } });
    const s = evaluate({ rules: [tbd], on: on(8, 30), group: "provincial" });
    expect(s.outcome).toBe("unknown");
    expect(s.because).toBe("unreadable-season");
    // …and it cannot open a water another placed closure shuts.
    const shut = closure({ id: "r2.shut" });
    expect(evaluate({ rules: [tbd, shut], on: on(8, 30), group: "provincial" }).outcome)
      .toBe("closed");
  });
});

describe("a `while`-scoped zero is not a closed water", () => {
  it("take 0 on every game fish WHILE spear fishing leaves the water open to angling", () => {
    const spear = closure({ id: "zp:spear_fishing.r1", species: ["ALL_GAME_FISH"],
                            while: ["spear_fishing"], scope: "area" });
    expect(evaluate({ rules: [spear], on: on(8, 30), group: "provincial" }).outcome)
      .toBe("restricted");
    // The same rule with the `while` dropped IS a closure — the field is what decides it.
    const { while: _gone, ...bare } = spear;
    expect(evaluate({ rules: [bare as Rule], on: on(8, 30), group: "provincial" }).outcome)
      .toBe("closed");
  });
});

describe("a standing rule is shown everywhere and decides nothing", () => {
  it("'no fishing within 23 m of any fishway' does not close every water", () => {
    const buffer = closure({ id: "zp:no_fishing_buffers.r1", species: ["ALL_GAME_FISH"],
                             standing: true, scope: "area" });
    const s = evaluate({ rules: [buffer], on: on(8, 15), group: "provincial" });
    expect(s.outcome).toBe("open");
    expect(s.from.map((r) => r.id)).toEqual(["zp:no_fishing_buffers.r1"]);   // still shown
    const { standing: _s, ...placed } = buffer;
    expect(evaluate({ rules: [placed as Rule], on: on(8, 15), group: "provincial" }).outcome)
      .toBe("closed");
  });
});

describe("exemptions: a lifted rule does not count", () => {
  /* The North Thompson case: the region's spring closure binds every stream in Region 3, and the
     river's own "Exempt from spring closure" lifts it. Applied nowhere, the river read CLOSED on
     May 1 beside its own exemption. */
  const SPRING = { entry: "z3:spring_stream_closure", rule: "spring_stream_closure.r1" };
  const spring = closure({ id: `${SPRING.entry}.${SPRING.rule}`, scope: "area",
                           when: season(w([1, 1], [6, 30])) });
  const exempt = rule({ id: "r3:north_thompson_river@3-27.north_thompson_river.r1",
                        type: "retention_limit", species: ["ALL_GAME_FISH"], exempts: [SPRING] });
  const quota = rule({ id: "r3:north_thompson_river@3-27.north_thompson_river.r2",
                       type: "retention_limit", take: 2 });

  it("a lifted rule is lifted where the exemption is in force", () => {
    expect(evaluate({ rules: [spring], on: on(5, 1), group: "provincial" }).outcome)
      .toBe("closed");
    const s = evaluate({ rules: [spring, exempt, quota], on: on(5, 1), group: "provincial" });
    expect(s.outcome).toBe("restricted");
    expect(s.from.map((r) => r.id)).not.toContain(spring.id);
  });

  it("a lift and nothing else restricts nobody", () => {
    expect(evaluate({ rules: [spring, exempt], on: on(5, 1), group: "provincial" }).outcome)
      .toBe("open");
  });

  it("a lift names one rule of one entry, and lifts no other rule of that entry", () => {
    const r5 = closure({ id: "z4:species_quotas.species_quotas.r5", scope: "area" });
    const r1 = closure({ id: "z4:species_quotas.species_quotas.r1", scope: "area" });
    const lift = rule({ id: "r4:upper_arrow@4-31.upper_arrow.r4", type: "retention_limit",
                        exempts: [{ entry: "z4:species_quotas", rule: "species_quotas.r5" }] });
    const s = evaluate({ rules: [r5, r1, lift], on: on(5, 1), group: "provincial" });
    expect(s.from.map((r) => r.id)).toContain(r1.id);
    expect(s.from.map((r) => r.id)).not.toContain(r5.id);
    expect(s.outcome).toBe("closed");
  });

  it("a lift out of season lifts nothing", () => {
    const summer = rule({ ...exempt, when: season(w([7, 1], [8, 31])) });
    const s = evaluate({ rules: [spring, summer, quota], on: on(5, 1), group: "provincial" });
    expect(s.outcome).toBe("closed");
    expect(s.from.find((r) => r.id === spring.id)?.liftedFor).toBeUndefined();
  });

  it("an uncertain lift lifts nothing", () => {
    const unsure = rule({ ...exempt, uncertain: true });
    const s = evaluate({ rules: [spring, unsure, quota], on: on(5, 1), group: "provincial" });
    expect(s.outcome).toBe("closed");
  });

  it("a rule never lifts itself (the z6 steelhead self-lift)", () => {
    const id = { entry: "z6:steelhead_stream_closure", rule: "steelhead_stream_closure.r1" };
    const self = closure({ id: `${id.entry}.${id.rule}`, scope: "area", exempts: [id] });
    expect(evaluate({ rules: [self], on: on(5, 20), group: "provincial" }).outcome)
      .toBe("closed");
  });

  it("a lift in another species group does not reach this one", () => {
    const salmonLift = rule({ ...exempt, group: "salmon" });
    expect(evaluate({ rules: [spring, salmonLift, quota], on: on(5, 1), group: "provincial" })
      .outcome).toBe("closed");
  });
});

describe("a partial lift keeps the rule, marked, and never removes it", () => {
  /* A LIFT IS NEVER WIDER THAN ITS LIFTER. Before this, all three of these lifted the whole rule. */
  const TCW = { entry: "z4:trout_char_winter_release", rule: "trout_char_winter_release.r1" };
  const winter = rule({ id: `${TCW.entry}.${TCW.rule}`, type: "retention_limit", scope: "area",
                        species: ["TROUT_CHAR"], take: 0, mayTarget: true,
                        when: season(w([11, 1], [3, 31])) });
  const duncan = rule({ id: "r4:duncan_river@4-19.duncan_river.r2", type: "retention_limit",
                        species: ["BT"], exempts: [{ ...TCW, species: ["BT"] }] });

  it("SPECIES: Duncan's bull-trout exemption leaves the trout/char release on the others", () => {
    const s = evaluate({ rules: [winter, duncan], on: on(12, 1), group: "provincial" });
    const kept = s.from.find((r) => r.id === winter.id);
    expect(kept, "the release still binds rainbow and cutthroat").toBeDefined();
    expect(kept!.liftedFor).toEqual([{ by: duncan.id, species: ["BT"] }]);
    expect(s.outcome).toBe("restricted");
    // the whole-species lift beside it (Columbia's TROUT_CHAR) still lifts it outright
    const columbia = rule({ id: "r4:columbia_river@4-15.columbia_river.r3", type: "retention_limit",
                            species: ["TROUT_CHAR"], exempts: [TCW] });
    const c = evaluate({ rules: [winter, columbia], on: on(12, 1), group: "provincial" });
    expect(c.from.map((r) => r.id)).not.toContain(winter.id);
    expect(c.outcome).toBe("open");
  });

  const BAN = { entry: "zp:bait", rule: "bait.r1" };
  const ban = rule({ id: `${BAN.entry}.${BAN.rule}`, type: "bait_restriction", scope: "area" });

  it("WHEN_TARGETING: sturgeon bait does not lift the fin-fish ban for every angler", () => {
    const sturgeon = rule({ id: "zp:bait.bait.r3", type: "bait_restriction", scope: "area",
                            exempts: [{ ...BAN, whenTargeting: ["WSG"] }] });
    const s = evaluate({ rules: [ban, sturgeon], on: on(8, 1), group: "provincial" });
    const kept = s.from.find((r) => r.id === ban.id);
    expect(kept?.liftedFor).toEqual([{ by: sturgeon.id, whenTargeting: ["WSG"] }]);
    expect(s.outcome).toBe("restricted");
  });

  it("WHILE: a lift while set lining leaves the rule for everyone else", () => {
    const setLine = rule({ id: "zp:bait.bait.r2", type: "bait_restriction", scope: "area",
                           while: ["set_lining"], exempts: [{ ...BAN, while: ["set_lining"] }] });
    const kept = evaluate({ rules: [ban, setLine], on: on(8, 1), group: "provincial" })
      .from.find((r) => r.id === ban.id);
    expect(kept?.liftedFor).toEqual([{ by: setLine.id, while: ["set_lining"] }]);
  });

  it("a partly lifted CLOSURE no longer closes the water, and is never removed", () => {
    const spring = closure({ id: "z3:spring_stream_closure.spring_stream_closure.r1",
                             scope: "area" });
    const lift = { entry: "z3:spring_stream_closure", rule: "spring_stream_closure.r1" };
    for (const partial of [
      rule({ id: "r3:x.x.r1", type: "retention_limit", exempts: [{ ...lift, species: ["BT"] }] }),
      rule({ id: "r3:x.x.r1", type: "retention_limit", exempts: [{ ...lift, whenTargeting: ["WSG"] }] }),
      rule({ id: "r3:x.x.r1", type: "retention_limit", exempts: [{ ...lift, while: ["set_lining"] }] }),
      // a lifter for part of the day lifts only those hours
      rule({ id: "r3:x.x.r1", type: "retention_limit", exempts: [lift],
             when: { ...ALL_YEAR, hours: { start: { at: "21:00" }, end: { at: "05:00" } } } }),
    ]) {
      const s = evaluate({ rules: [spring, partial], on: on(5, 1), group: "provincial" });
      expect(s.from.map((r) => r.id), JSON.stringify(partial.exempts)).toContain(spring.id);
      expect(s.outcome).toBe("restricted");
    }
  });

  it("a whole lift beside a partial one still lifts the rule", () => {
    const whole = rule({ id: "r4:x.x.r3", type: "retention_limit", exempts: [TCW] });
    const s = evaluate({ rules: [winter, duncan, whole], on: on(12, 1), group: "provincial" });
    expect(s.from.map((r) => r.id)).not.toContain(winter.id);
  });
});
