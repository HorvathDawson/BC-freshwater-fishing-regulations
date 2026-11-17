/**
 * (T759, owner 2026-11-15: "mark the folded row") THE FOLDED SALMON ROW wears a small mark when its card holds a DFO issue
 * banner, so the banner is not missed while the card is folded; no mark without a banner; a closed-all-year row with a banner
 * still folds open to it. The mark is named by the banner's own title (no new words).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { pickSalmon, salmonCard, salmonGlance } from "@app/core";
import { ISSUE_ITEMS, ISSUE_SIDS, issueFeed, issueRelease, ISSUES } from "@app/core/regs/testing";
import { LIGHT } from "../theme";
import { SpeciesRows } from "./KeepBlocks";

afterEach(cleanup);
const ON = new Date(2026, 8, 15);
const TITLE = "A DFO rule to check on this water";
const row = (item: string | null, sid: number) => salmonGlance(salmonCard(pickSalmon(issueRelease(), issueFeed(ISSUES)), { sids: [sid], item }, ON));
const draw = (r: ReturnType<typeof row>) =>
  render(<SpeciesRows rows={[]} keys={[]} palette={LIGHT} actions={{} as never} folds={[{ key: "salmon", row: r, body: () => null }]} />);

describe("the folded Salmon row's issue mark (T759)", () => {
  it("a stretch with a banner: the row carries the mark, named by the banner's title", () => {
    const r = row(ISSUE_ITEMS.campbell, ISSUE_SIDS.campbell);
    expect(r.issue).toBe(true);
    draw(r);
    const mark = screen.getByTestId("glance-issue");
    expect(mark.getAttribute("aria-label")).toBe(TITLE);
    // inside the pressable row, which stays folded: the banner itself is not drawn
    expect(screen.getByTestId("glance:salmon").contains(mark)).toBe(true);
    expect(screen.queryByTestId("salmon-issue")).toBeNull();
  });

  it("a stretch of another river: no mark, and the row is the row it was", () => {
    const r = row(ISSUE_ITEMS.other, ISSUE_SIDS.other);
    expect(r.issue).toBeUndefined();
    draw(r);
    expect(screen.queryByTestId("glance-issue")).toBeNull();
  });
});
