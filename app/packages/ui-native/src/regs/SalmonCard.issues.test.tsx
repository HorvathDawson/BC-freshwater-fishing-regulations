/**
 * (T759) THE ISSUE BANNERS on the Salmon card: one per issue the live feed lists on the stretch's water, DFO's rows
 * verbatim, in key order; none for another water, an undrawn place already said by the caveat, a region the feed did
 * not read, or no feed. The rest of the card (rows, chip, colours) is byte for byte the card without them. Hand-built
 * feed in the T759 ticket's shape (`@app/core/regs/testing` `salmonIssues`).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { pickSalmon, salmonCard, type SalmonFacts } from "@app/core";
import { CAMPBELL_ISSUES, ISSUES, ISSUE_ITEMS, ISSUE_SIDS, issueFeed, issueRelease } from "@app/core/regs/testing";
import { LIGHT } from "../theme";
import { SalmonCard } from "./SalmonCard";

afterEach(cleanup);

const ON = new Date(2026, 8, 15);
const draw = (f: SalmonFacts, sid: number, item: string | null) =>
  render(<SalmonCard card={salmonCard(f, { sids: [sid], item }, ON)} palette={LIGHT} />);
const banners = () => screen.queryAllByTestId("salmon-issue").map((b) => [...b.children].map((c) => c.textContent));
const CLOSE = "Check DFO’s page for where it applies before you fish.";
const TITLE = "A DFO rule to check on this water";
const live = (issues: unknown = ISSUES) => pickSalmon(issueRelease(), issueFeed(issues));

describe("the Salmon card's issue banners (T759)", () => {
  it("one issue: its lead-in, DFO's area and row verbatim, the closing line", () => {
    const one = { ...ISSUES, waters: { "1:campbell-river": { ...ISSUES.waters["1:campbell-river"], list: [CAMPBELL_ISSUES[0]] } } };
    draw(live(one), ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell);
    expect(banners()).toEqual([[TITLE,
      "DFO lists a rule for this water that this app has not placed on the map yet. It may apply here:",
      "from the end of Maple Street downstream to the confluence with the Quinsam River.", "All | Aug 1 to Oct 31 | No fishing", CLOSE]]);
  });

  it("three issues on one river: all shown, in key order, every row verbatim", () => {
    draw(live(), ISSUE_SIDS.campbell2, ISSUE_ITEMS.campbell);
    expect(banners()).toEqual([
      [TITLE, "DFO reworded a closed area on this water. The app has not placed the new wording yet:", "Pierre Ck.",
       "No fishing for salmon — within a 400 m radius of the mouth of Pierre Ck.", CLOSE],
      [TITLE, "DFO reworded a rule for this water. The app applies it where the earlier wording was; that has not been confirmed yet:",
       "from a fishing boundary sign at the  cement block upstream.", "Coho | Jul 1 to Oct 31 | 2 per day", "Chinook | All year | 0", CLOSE],
      [TITLE, "DFO lists a rule for this water that this app has not placed on the map yet. It may apply here:",
       "from the end of Maple Street downstream to the confluence with the Quinsam River.", "All | Aug 1 to Oct 31 | No fishing", CLOSE]]);
  });

  it.each([
    ["a stretch of another river", () => draw(live(), ISSUE_SIDS.other, ISSUE_ITEMS.other)],
    ["an undrawn place the caveat already says", () => draw(live(), ISSUE_SIDS.babine, ISSUE_ITEMS.babine)],
    ["a region the feed did not read (held)", () => draw(live(), ISSUE_SIDS.held, ISSUE_ITEMS.held)],
    ["offline (the release's own file)", () => draw(pickSalmon(issueRelease(), null), ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell)],
  ])("none on %s", (_what, go) => {
    go();
    expect(screen.queryByTestId("salmon-issues")).toBeNull();
  });

  it("the rest of the card is byte for byte the card without issues", () => {
    const html = (f: SalmonFacts) => {
      const { container, unmount } = draw(f, ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell);
      container.querySelector('[data-testid="salmon-issues"]')?.remove();
      const out = container.innerHTML;
      unmount();
      return out;
    };
    const withIssues = live(), without = live({ waters: {}, no_water: [], unread: {} });
    expect(salmonCard(withIssues, { sids: [ISSUE_SIDS.campbell], item: ISSUE_ITEMS.campbell }, ON).issues.length).toBe(3);
    expect(html(withIssues)).toBe(html(without));
  });
});
