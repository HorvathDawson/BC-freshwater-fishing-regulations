import { describe, expect, it } from "vitest";
import { DAY_LABELS, FORECAST_WINDOW, regsDayLink } from "./dayLink";

// Tue 10 Nov 2026
const NOW = new Date(2026, 10, 10, 9);
const plus = (n: number) => new Date(2026, 10, 10 + n, 12);

describe("T582: the regulations day against the Conditions window", () => {
  it("today and no day link nothing", () => {
    expect(regsDayLink(null, NOW).kind).toBe("today");
    expect(regsDayLink(plus(0), NOW).kind).toBe("today");
  });
  it("a day inside the window is marked on the chart", () => {
    const l = regsDayLink(plus(4), NOW);
    expect(l.kind).toBe("inside");
    if (l.kind !== "inside") return;
    expect(l.name).toBe("Sat 14 Nov");
    expect(l.mark).toBe("SAT 14 NOV");
    expect(new Date(l.at).getDate()).toBe(14);
  });
  it("the last forecast day is inside, the next is past, with the one line", () => {
    expect(regsDayLink(plus(FORECAST_WINDOW), NOW).kind).toBe("inside");
    const l = regsDayLink(plus(FORECAST_WINDOW + 1), NOW);
    expect(l.kind).toBe("past");
    if (l.kind === "past") expect(l.line).toBe("Sat 21 Nov is past the forecast.");
  });
  it("each tab names its own day", () => {
    expect(DAY_LABELS.conditions(0, NOW)).toBe("Conditions: today");
    expect(DAY_LABELS.conditions(3, NOW)).toBe("Conditions: Fri 13 Nov");
    expect(DAY_LABELS.regulations(plus(20), NOW)).toBe("Regulations: Mon 30 Nov");
    expect(DAY_LABELS.map(null, NOW)).toBe("Map: today's rules");
  });
});
