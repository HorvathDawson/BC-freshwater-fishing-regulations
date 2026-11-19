/**
 * T481 (owner 2026-11-07) — the card head draws "Fishing more than one water today? ›" under the region's total, a link
 * opening the row's `multi:r<row>` sheet (core t481.test.ts builds the link and the sheet); no link where core gives none.
 * (T810 cutover) Mounted over the group card a real page still draws: the Vedder's Whitefish (its Other fish sheet,
 * testing/otherGroup.ts), whose head carries the link as cut. The old "size header is not sticky" case is retired: no
 * real page draws a tier head (`khead`) since the cutover. react-native-web renders the web app from the same component.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { GroupVM, SheetKey, SheetVM } from "@app/core";
import { SheetBody } from "./SheetBody";
import { GroupCard } from "./GroupCard";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";
import { otherGroup } from "./testing/otherGroup";

afterEach(cleanup);

const G: GroupVM = otherGroup(vedder1010, "other:o1");

function mount(g: GroupVM = G) {
  const opened: SheetKey[] = [];
  const actions = { onSheet: (k: SheetKey) => opened.push(k), onDay() {}, onTab() {}, onBook() {} };
  const { container } = render(<GroupCard g={g} palette={LIGHT} actions={actions} />);
  return { opened, container };
}

describe("T481 the region's total, said once", () => {
  it("(T883) in the fish sheet the link sits with the sheet's links, after How this was decided, and opens the row's multi sheet", () => {
    const opened: SheetKey[] = [];
    const vm = (vedder1010 as unknown as { sheets: Record<string, SheetVM> }).sheets["other:o1"]!;
    const { container } = render(<SheetBody vm={vm} palette={LIGHT} onOpen={(k) => opened.push(k)} onBook={() => {}} />);
    const text = container.textContent!;
    expect(text.indexOf("How this was decided")).toBeLessThan(text.indexOf("Fishing more than one water today?"));
    expect(screen.queryByTestId("multi-link"), "not inside the card any more").toBeNull();
    fireEvent.click(screen.getByText(/Fishing more than one water today\?/));
    expect(opened).toEqual([`multi:r${G.row}`]);
  });
  it("a card that keeps its head draws the link under the region's total", () => {
    const { opened } = mount({ ...G, head: { ...G.head, multi: [{ t: "link", s: "Fishing more than one water today? ›", open: `multi:r${G.row}` }] } });
    fireEvent.click(screen.getByRole("link", { name: /Fishing more than one water today\?/ }));
    expect(opened).toEqual([`multi:r${G.row}`]);
  });
  it("no link where the head has none", () => {
    mount({ ...G, head: { ...G.head, multi: null } });
    expect(screen.queryByTestId("multi-link")).toBeNull();
  });
});
