/**
 * T481 (owner 2026-11-07) — the card head draws "Fishing more than one water today? ›" under the region's total, a link
 * opening the row's `multi:r<row>` sheet (core t481.test.ts builds the link and the sheet); no link where core gives none.
 * (T810 cutover) Mounted over the group card a real page still draws: the Vedder's Whitefish (its Other fish sheet,
 * testing/otherGroup.ts), whose head carries the link as cut. The old "size header is not sticky" case is retired: no
 * real page draws a tier head (`khead`) since the cutover. react-native-web renders the web app from the same component.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { GroupVM, SheetKey } from "@app/core";
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
  it("the link under it opens the row's multi sheet", () => {
    const { opened } = mount();
    const link = screen.getByRole("link", { name: /Fishing more than one water today\?/ });
    fireEvent.click(link);
    expect(opened).toEqual([`multi:r${G.row}`]);
  });
  it("no link where the head has none", () => {
    mount({ ...G, head: { ...G.head, multi: null } });
    expect(screen.queryByTestId("multi-link")).toBeNull();
  });
});
