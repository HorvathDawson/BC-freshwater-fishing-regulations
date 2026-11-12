/**
 * T670 — what the Keep tab must draw ON the tab, never only in a sheet: under the Possession row the fish its value does
 * not hold for and the water's own possession limits (K1, `possession.also`), and under a species row the fish of the
 * row that must go back (K2, `otherFish[].back`). Mounted over the frozen vedder_1010 Keep page with the two fields set.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { RegsPage } from "@app/core";
import { OtherFish, PossessionRow } from "./KeepBlocks";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const KEEP = (vedder1010 as unknown as RegsPage).keep;
const actions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} } as never;
const texts = (c: HTMLElement, id: string) => [...c.querySelectorAll(`[data-testid='${id}']`)].map((e) => e.textContent);

describe("T670 the lines under a Keep row", () => {
  it("K1: every `also` sentence is drawn under the Possession row; none, no line; no value word, no empty value", () => {
    const also = ["Have no more than 3 walleye in possession. Fish at home don’t count.", "Have no more than 3 northern pike in possession. Fish at home don’t count."];
    const a = render(<PossessionRow possession={{ ...KEEP.possession!, also }} palette={LIGHT} actions={actions} />);
    expect(texts(a.container, "keep-possession-also")).toEqual(also);
    expect(a.container.textContent).toContain(KEEP.possession!.value);
    cleanup();
    const b = render(<PossessionRow possession={{ ...KEEP.possession!, also: [] }} palette={LIGHT} actions={actions} />);
    expect(texts(b.container, "keep-possession-also")).toEqual([]);
    cleanup();
    const c = render(<PossessionRow possession={{ ...KEEP.possession!, value: "", also }} palette={LIGHT} actions={actions} />);
    expect(c.container.textContent).toBe(`${KEEP.possession!.label}${also.join("")}›`);
  });

  it("K2: a row's `back` lines are drawn under its name, each with its status", () => {
    const rows = KEEP.otherFish!.map((o, i) => (i ? o : { ...o, back: [{ status: "release" as const, says: "Release every mountain whitefish.", rule: "e::r" }] }));
    const { container } = render(<OtherFish rows={rows} keys={KEEP.otherFishKey} palette={LIGHT} actions={actions} />);
    expect(texts(container, "otherfish-back:release")).toEqual(["Release every mountain whitefish."]);
    cleanup();
    expect(texts(render(<OtherFish rows={KEEP.otherFish!} keys={KEEP.otherFishKey} palette={LIGHT} actions={actions} />).container, "otherfish-back:release")).toEqual([]);
  });
});
