/**
 * T377 — the key's tap reaches the map as the status (or salmon) day with one class alone: the input's values are
 * @app/core `isolateDay`'s, its key changes (so both renderers repaint), and the gear marks never ride a greyed water.
 */
import { describe, expect, it } from "vitest";
import { dayProfiles, isolateDay, type StatusIndex } from "@app/core";
import { statusInput } from "./colouring";
import { gearShown } from "./paint";
import { NO_VALUES } from "./style";

const index = {
  handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 3, waters: 0, profiles: 3,
  profileOf: (s: unknown) => Number(s) - 10, profileCodeOn: (p: number) => (["closed", "own", "base"] as const)[p]!,
  codeOn: () => "base", waterCodeOn: () => "base",
} as unknown as StatusIndex;
const DAY = new Date(2026, 5, 1);

describe("(T377) one class alone on the map", () => {
  it("the status input's values are the day with that class alone, under a key of their own", () => {
    const all = statusInput({ index, day: DAY }), own = statusInput({ index, day: DAY, only: "own" });
    expect(all.values()).toEqual(dayProfiles(index, DAY));
    expect(own.values()).toEqual(isolateDay(dayProfiles(index, DAY), "own"));
    expect(own.valuesKey).not.toBe(all.valuesKey);
    expect(statusInput({ index, day: DAY, only: null }).valuesKey).toBe(all.valuesKey);
  });

  it("the gear marks stay off every greyed water (none), as off closed and no-access water", () => {
    const status = isolateDay(dayProfiles(index, DAY), "own");
    const v = { ...NO_VALUES, status, method: { none: [], bait: [], fly: ["m0"] } };
    const shown = JSON.stringify(gearShown("stream", "profile", "fly", v));
    for (const l of status.none) expect(shown).toContain(`"${l}"`);
  });
});
