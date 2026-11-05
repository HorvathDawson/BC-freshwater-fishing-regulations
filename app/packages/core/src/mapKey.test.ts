import { describe, expect, it } from "vitest";
import { isolateDay, KEY_ISOLATE_WORDS } from "./mapKey";

describe("(T377) isolateDay: one class alone, the rest to none", () => {
  const day = { closed: ["p1"], own: ["p2", "p3"], base: ["b"], no_access: ["n"], none: ["p9"] };

  it("keeps the class asked and moves every other label to none, none's own kept", () => {
    expect(isolateDay(day, "own")).toEqual({ closed: [], own: ["p2", "p3"], base: [], no_access: [], none: ["p9", "p1", "b", "n"] });
  });

  it("every label stays in exactly one set", () => {
    const out = isolateDay(day, "closed");
    const all = Object.values(out).flat();
    expect(new Set(all).size).toBe(all.length);
    expect(all.sort()).toEqual(Object.values(day).flat().sort());
  });

  it("null, none or a class the day does not hold returns the day itself", () => {
    expect(isolateDay(day, null)).toBe(day);
    expect(isolateDay(day, "none")).toBe(day);
    expect(isolateDay(day, "release" as never)).toBe(day);
  });

  it("the salmon day the same way", () => {
    const s = { open: ["s0"], release: ["s1"], closed: ["s2"], check: [], none: [] };
    expect(isolateDay(s, "release")).toEqual({ open: [], release: ["s1"], closed: [], check: [], none: ["s0", "s2"] });
  });

  it("a reader hears the whole entry", () => {
    expect(KEY_ISOLATE_WORDS.entry("Closed", false)).toBe("Closed. Tap to show only this on the map.");
    expect(KEY_ISOLATE_WORDS.entry("Closed", true)).toBe("Closed, shown alone on the map. Tap to show every class.");
  });
});
