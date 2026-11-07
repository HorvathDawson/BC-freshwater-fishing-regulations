/**
 * T481 (owner 2026-11-07) — the card head draws "Fishing more than one water today? ›" under the region's total, a link
 * opening the row's `multi:r<row>` sheet (core t481.test.ts builds the link and the sheet); no link where core gives none.
 * The size header is a plain header now: it no longer sticks on its own (its line is in the one pin bar). Mounted over
 * the frozen vedder_1010 Keep group; react-native-web renders the web app from the same component (AGENTS 26).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { GroupVM, RegsPage, SheetKey } from "@app/core";
import { GroupCard } from "./GroupCard";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const BASE = (vedder1010 as unknown as RegsPage).keep.groups[0]!;
const G: GroupVM = { ...BASE, head: { ...BASE.head, multi: [{ t: "link", s: "Fishing more than one water today? ›", open: "multi:r0" }] } };

function mount(g: GroupVM = G) {
  const opened: SheetKey[] = [];
  const actions = { onSheet: (k: SheetKey) => opened.push(k), onDay() {}, onTab() {}, onBook() {} };
  const { container } = render(<GroupCard g={g} palette={LIGHT} actions={actions} />);
  return { opened, container };
}

describe("T481 the region's total, said once", () => {
  it("the link under it opens the row's multi sheet", () => {
    const { opened } = mount();
    const link = screen.getByRole("link", { name: /Fishing more than one water today\?/ });
    fireEvent.click(link);
    expect(opened).toEqual(["multi:r0"]);
  });
  it("no link where the head has none", () => {
    mount({ ...G, head: { ...G.head, multi: null } });
    expect(screen.queryByTestId("multi-link")).toBeNull();
  });
  it("the size header is not sticky", () => {
    const { container } = mount();
    const heads = [...container.querySelectorAll<HTMLElement>("[data-testid='khead']")];
    expect(heads.length).toBeGreaterThan(0);
    expect(heads.filter((h) => h.style.position === "sticky")).toEqual([]);
  });
});
