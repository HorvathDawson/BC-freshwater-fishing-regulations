/** T137 — the TALL cards shot ends at the foot of the Other fish list when the Keep tab has one (prototype tall.mjs). */
import { describe, expect, it } from "vitest";
// @ts-expect-error — a plain .mjs module of the node tools (no declaration file)
import { END_IDS, tallEnd } from "./tall.mjs";

describe("tall shot end rule", () => {
  it("cards: the max bottom of keep-group / keep-plain / keep-otherfish", () => {
    expect(END_IDS).toEqual(["keep-group", "keep-plain", "keep-otherfish"]);
    expect(tallEnd("cards", { "keep-group": [400, 900], "keep-plain": [700], "keep-otherfish": [1250] }, 2000)).toBe(1250);
  });
  it("cards without an Other fish list: the last card", () => {
    expect(tallEnd("cards", { "keep-group": [400, 900], "keep-plain": [], "keep-otherfish": [] }, 2000)).toBe(900);
  });
  it("cards with nothing measured, and keep mode: the Keep tab's foot", () => {
    expect(tallEnd("cards", {}, 2000)).toBe(2000);
    expect(tallEnd("keep", { "keep-otherfish": [1250] }, 2000)).toBe(2000);
  });
});
