/**
 * T480 (owner 2026-11-07) — one size style on the Keep group card: every kind's size is ONE chip, what happens outside it
 * plain words beside it ("60 cm (24 in) or longer" · "release any shorter"); the steelhead's size is its DEFINITION
 * ("Counts as a steelhead:" + chip "rainbow trout over 50 cm (20 in)" + ⓘ), not a chip with loose grey words. Mounted
 * over the frozen vedder_1010 Keep group (react-native-web renders the web app from the same component, AGENTS 26).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { RegsPage } from "@app/core";
import { GroupCard } from "./GroupCard";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const G = (vedder1010 as unknown as RegsPage).keep.groups[0]!;
const sp = (s: string | null) => (s ?? "").replace(/ /g, " ");

describe("T480 one size style", () => {
  it("every size is a chip; a rule's release sits beside it; nothing else draws a size", () => {
    const actions = { onSheet() {}, onDay() {}, onTab() {}, onBook() {} };
    render(<GroupCard g={G} palette={LIGHT} actions={actions} />);
    const chips = screen.getAllByTestId("chip:size");
    expect(chips.map((c) => sp(c.textContent))).toEqual(["30 cm (12 in) or longer", "60 cm (24 in) or longer",
      "rainbow trout over 50 cm (20 in)", "50 cm (20 in) or less"]);
    // one look: every chip on the tint
    expect(new Set(chips.map((c) => c.style.backgroundColor)).size).toBe(1);
    expect(screen.getAllByTestId("size-then").map((e) => e.textContent)).toEqual(["release any shorter", "release any shorter"]);
    const def = screen.getByTestId("size-def");
    expect(sp(def.textContent)).toBe("Counts as a steelhead:rainbow trout over 50 cm (20 in)ⓘ");
    // the definition is said once: its old presence sentence is gone, no loose "rainbow trout"
    expect(document.body.textContent).not.toContain("here counts as a steelhead");
    expect(screen.queryByText("rainbow trout")).toBeNull();
  });
});
