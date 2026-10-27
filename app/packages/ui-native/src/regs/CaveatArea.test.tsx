/**
 * T208: a rule of one named area of a lake (François Lake r1, "the area at the outlet … described on map 1") is never only
 * behind the in-part fold. The fold's head says the area; when such a rule is in force on the day the fold opens. Mutations:
 * dropping the head line fails the first test; `useState(false)` fails the second.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { CaveatVM } from "@app/core";
import { LIGHT } from "../theme";
import { CaveatBlock } from "./KeepBlocks";

afterEach(cleanup);

const NOOP = { onSheet() {}, onDay() {}, onBook() {}, onTab() {} };
const TAG = "Outlet area only (see map 1)";
const block = (now: boolean): CaveatVM => ({
  kind: "inpart", hot: now, title: now ? "Fishing is different in some spots today" : "Some spots have their own rules", calm: null, later: null,
  items: [{ what: [{ t: "text", s: "No fishing · Nov 15–May 31" }], where: "Where: The area at the outlet of François Lake described on map 1",
    rule: null, fish: null, book: [], area: { tag: TAG, map: [{ t: "text", s: "See map 1 · " }, { t: "book", printed: 51, pdf: 57, bare: true }] } }],
  areas: [TAG], areaNow: now,
});

describe("T208 in-part fold names its areas", () => {
  it("not in force: folded, the head says the area", () => {
    render(<CaveatBlock caveat={block(false)} palette={LIGHT} actions={NOOP} />);
    expect(screen.getByTestId("caveat-areas").textContent).toBe(TAG);
    expect(screen.queryByTestId("keep-caveat-area")).toBeNull();
    fireEvent.click(screen.getByRole("button"));
    expect(screen.getByTestId("keep-caveat-area").textContent).toContain("No fishing · Nov 15–May 31");
  });
  it("in force on the day: opens unfolded, the area rule and its map link shown", () => {
    render(<CaveatBlock caveat={block(true)} palette={LIGHT} actions={NOOP} />);
    const it_ = screen.getByTestId("keep-caveat-area");
    expect(it_.textContent).toContain(TAG + ":");
    expect(it_.textContent).toContain("See map 1 · ");
  });
});
