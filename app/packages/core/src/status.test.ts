import { describe, expect, it } from "vitest";
import type { PlainDate, Window } from "./dates";
import { inForce, inWindow } from "./dates";
import { evaluate, statusWord, type Rule } from "./status";

const on = (month: number, day: number): PlainDate => ({ year: 2026, month, day });
const w = (a: [number, number], b: [number, number]): Window => ({
  from: { month: a[0], day: a[1] },
  to: { month: b[0], day: b[1] },
});
const rule = (p: Partial<Rule> & Pick<Rule, "id" | "kind">): Rule => ({
  scope: "section", via: "reach", group: "provincial", windows: [], ...p,
});

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
      rules: [rule({ id: "r1", kind: "closure", windows: [w([6, 1], [6, 30])] })],
      on: on(8, 30), group: "provincial",
    });
    expect(s.outcome).toBe("open");
    expect(s.provenance).toBe("specific");
  });
});

describe("the date decides", () => {
  const chilliwack = rule({ id: "cw.r5", kind: "closure", windows: [w([6, 1], [6, 30])] });
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
    const spring = rule({ id: "mu.spring", kind: "closure", scope: "mu", via: "reach",
                          windows: [w([4, 1], [6, 15])] });
    expect(evaluate({ rules: [spring], on: on(5, 1), group: "provincial" }).outcome)
      .toBe("closed");
  });

  it("a rule for THIS water replaces the zone default about the same subject", () => {
    // The point of the subject key: a lake may deliberately be more permissive than its MU.
    const zone = rule({ id: "mu.quota", kind: "harvest", scope: "mu", via: "reach", subject: "trout-quota" });
    const lake = rule({ id: "lake.quota", kind: "note", scope: "section", via: "reach",
                        subject: "trout-quota" });
    const s = evaluate({ rules: [zone, lake], on: on(8, 30), group: "provincial" });
    expect(s.outcome).toBe("open");
    expect(s.from.map((r) => r.id)).not.toContain("mu.quota");
  });

  it("but it does not replace a zone rule about something else", () => {
    const bait = rule({ id: "mu.bait", kind: "gear_restriction", scope: "mu", via: "reach", subject: "bait" });
    const quota = rule({ id: "lake.quota", kind: "note", scope: "section", via: "reach",
                         subject: "trout-quota" });
    expect(evaluate({ rules: [bait, quota], on: on(8, 30), group: "provincial" }).outcome)
      .toBe("restricted");
  });

  it("an area closure is absolute — a permissive local rule cannot open it", () => {
    const reserve = rule({ id: "eco", kind: "closure", scope: "area", via: "reach", absolute: true });
    const local = rule({ id: "lake.quota", kind: "note", scope: "section", via: "reach" });
    expect(evaluate({ rules: [reserve, local], on: on(8, 30), group: "provincial" }).outcome)
      .toBe("closed");
  });
});

describe("salmon is a parallel authority, not a precedence level", () => {
  const provincialOpen = rule({ id: "bc", kind: "note", scope: "section", via: "reach" });
  const dfoClosed = rule({ id: "dfo", kind: "closure", scope: "section", via: "reach",
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
      rules: [rule({ id: "fraser.r4", kind: "closure", uncertain: true })],
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
      rules: [rule({ id: "a", kind: "closure" }), rule({ id: "b", kind: "closure", uncertain: true })],
      on: on(8, 30), group: "provincial",
    });
    expect(s.outcome).toBe("closed");
  });
});
