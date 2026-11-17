/**
 * (T814) The coho loader as the web renders it (vitest resolves `.web.tsx` first), and what both halves share
 * (`./coho`): two CSS `steps()` keyframes paging the sheet in pixels, no JavaScript per frame, the sheet printed in the
 * palette's coho colours, the ground behind it, and every theme with a sheet.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { CohoLoader } from "./CohoLoader";
import { LoadingState } from "./LoadingState";
import { COHO } from "./coho.generated";
import { COHO_GROUND_PAD, cohoHeight, cohoSheet } from "./coho";
import { DARK, LIGHT, THEMES } from "./theme";

declare const setReduceMotion: (on: boolean) => void;

afterEach(() => { cleanup(); setReduceMotion(false); });

const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};
const by = (c: HTMLElement, id: string) => c.querySelector<HTMLElement>(`[data-testid='${id}']`)!;

describe("<CohoLoader> web (T814)", () => {
  it("announces itself as a progress indicator with the screen's words", () => {
    const { getByLabelText } = render(<CohoLoader palette={LIGHT} label="Loading rivers" />);
    expect(getByLabelText("Loading rivers").getAttribute("role")).toBe("progressbar");
  });

  it("is its width wide and the frame's aspect tall, clipping a sheet of the whole grid", () => {
    const { container } = render(<CohoLoader palette={LIGHT} width={192} />);
    const box = container.querySelector<HTMLElement>("[role='progressbar']")!;
    const cols = by(container, "coho-cols");
    expect([box.style.width, box.style.height, cols.parentElement!.parentElement!.style.overflow,
            cols.style.width, cols.style.height])
      .toEqual(["192px", `${cohoHeight(192)}px`, "hidden", `${192 * COHO.columns}px`, `${cohoHeight(192) * COHO.rows}px`]);
    expect(cohoHeight(192) / 192).toBe(COHO.frame.height / COHO.frame.width);
  });

  it("pages the sheet with two CSS steps keyframes, in pixels: a row a tenth of the loop, a column a frame", () => {
    const { container } = render(<CohoLoader palette={LIGHT} width={192} />);
    const rows = by(container, "coho-rows"), cols = by(container, "coho-cols");
    expect([rows.style.animation, rows.style.getPropertyValue("--coho-rows"),
            cols.style.animation, cols.style.getPropertyValue("--coho-cols")])
      .toEqual([`coho-row ${COHO.loopMs}ms steps(${COHO.rows}) infinite`, `-${cohoHeight(192) * COHO.rows}px`,
                `coho-col ${COHO.loopMs / COHO.rows}ms steps(${COHO.columns}) infinite`, `-${192 * COHO.columns}px`]);
    // one row's columns are one row's time: the two never disagree about the frame
    expect((COHO.loopMs / COHO.rows / COHO.columns) * COHO.frames).toBe(COHO.loopMs);
  });

  it("moves under reduced motion too (owner 2026-11-17: \"we will ignore reduced motion for this\")", () => {
    setReduceMotion(true);
    const { container } = render(<CohoLoader palette={LIGHT} />);
    expect([by(container, "coho-rows").style.animationPlayState, by(container, "coho-cols").style.animationPlayState,
            by(container, "coho-cols").style.animation.includes("steps")]).toEqual(["", "", true]);
  });

  it("draws its ground a little past the frame, in the palette's coho ground", () => {
    for (const p of [LIGHT, DARK]) {
      const { container } = render(<CohoLoader palette={p} />);
      const g = by(container, "coho-ground");
      expect([g.style.backgroundColor, g.style.inset]).toEqual([rgb(p.coho.ground), `-${COHO_GROUND_PAD}px`]);
      cleanup();
    }
  });

  it("every theme has the sheet printed in its own coho ink and red; a palette no theme wears has none", () => {
    for (const p of Object.values(THEMES)) expect(cohoSheet(p)).toBeTruthy();
    const keys = Object.keys(COHO.sheets).sort();
    expect(keys).toEqual(Object.keys(THEMES).sort());
    expect(() => cohoSheet({ ...LIGHT, coho: { ...LIGHT.coho, red: LIGHT.ink } })).toThrow(/no coho sheet/);
  });

  it("LoadingState draws it when asked (coho), its held place the coho's size; the circling fish otherwise", () => {
    const coho = render(<LoadingState palette={LIGHT} size={192} coho shown />);
    expect([!!by(coho.container, "coho-cols"), !!coho.container.querySelector("[data-testid='fish-sprite']")]).toEqual([true, false]);
    cleanup();
    const held = render(<LoadingState palette={LIGHT} size={192} coho shown={false} />);
    expect([by(held.container, "loading-held").style.width, by(held.container, "loading-held").style.height])
      .toEqual(["192px", `${cohoHeight(192)}px`]);
    cleanup();
    const fish = render(<LoadingState palette={LIGHT} size={96} shown />);
    expect([!!fish.container.querySelector("[data-testid='coho-cols']"), !!by(fish.container, "fish-sprite")]).toEqual([false, true]);
  });
});
