/**
 * (T828 follow-up) A NEW DFO FEED ROW ON A MATCHED WATER SAYS ITS WORDS ONCE. The same row is both an unplaced place
 * (`unplaced.json`, T521: the rules line "Location not yet on the map: …") and an `unmatched` issue (T759: the banner,
 * `unmatched:<fingerprint>`). The banner is the attention element and carries the row, so the rules line leaves it out;
 * an unplaced row with no shown banner keeps its rules line.
 */
import { describe, expect, it } from "vitest";
import { salmonCard } from "./salmon";
import { parseDfoUnplaced, pickSalmon } from "./salmonFeed";
import { issueLines, salmonIssueItems } from "./salmonIssues";
import { ISSUE_ITEMS, ISSUE_SIDS, issueFeed, issueRelease } from "./regs/testing/salmonIssues";

const ON = new Date(2026, 8, 15);
// the same row the Campbell's `unmatched:fp2` issue quotes, as the feed's unplaced.json lists it
const same = { fingerprint: "fp2", region: "1", items: [ISSUE_ITEMS.campbell], water: "Campbell River",
               scope: "from the end of Maple Street downstream to the confluence with the Quinsam River.", rows: ["All | Aug 1 to Oct 31 | No fishing"] };
// an unplaced row with no issue behind it
const lone = { fingerprint: "zz9", region: "1", items: [ISSUE_ITEMS.campbell], water: "Campbell River", scope: "above the weir", rows: ["Coho | Sep 1 to Sep 30 | 1 per day"] };

const live = (rows: unknown[]) => pickSalmon(issueRelease(), { ...issueFeed(), unplaced: rows.length, unplacedRows: parseDfoUnplaced(rows) });
const card = (f: ReturnType<typeof live>) => salmonCard(f, { sids: [ISSUE_SIDS.campbell], item: ISSUE_ITEMS.campbell }, ON);
const unplacedLines = (c: ReturnType<typeof card>) => c.waterRules.filter((t) => t.startsWith("Location not yet on the map"));
/** how many times DFO's row (either spelling: cells joined " · " or " | ") is on the card, rules lines and banners together */
const times = (c: ReturnType<typeof card>) =>
  [...c.waterRules, ...c.issues.flatMap(issueLines)].filter((t) => t.includes("All · Aug 1 to Oct 31 · No fishing") || t.includes("All | Aug 1 to Oct 31 | No fishing")).length;

describe("DFO's words once (T828 follow-up)", () => {
  it("a feed row both unplaced and an unmatched issue: the banner has the words, the rules line does not; the stripe holds the water", () => {
    const f = live([same]), c = card(f);
    expect(c.issues.some((b) => b.key === "unmatched:fp2" && b.rows.includes("All | Aug 1 to Oct 31 | No fishing"))).toBe(true);
    expect(unplacedLines(c)).toEqual([]);
    expect(times(c)).toBe(1);
    expect(c.notListed).toBeNull();
    // the "Check rule" stripe (the map's issue items), from the same feed
    expect(salmonIssueItems(f.payload as never).has(ISSUE_ITEMS.campbell)).toBe(true);
  });

  it("an unplaced row with no banner keeps its rules line", () => {
    const c = card(live([same, lone]));
    expect(unplacedLines(c)).toEqual(["Location not yet on the map: above the weir — Coho · Sep 1 to Sep 30 · 1 per day"]);
  });

  it("a banner not shown (its region unread) leaves the rules line: the words still show once", () => {
    const held = { fingerprint: "fph", region: "3", items: [ISSUE_ITEMS.held], water: "Held River", scope: "below the falls.", rows: ["All | All year | No fishing"] };
    const c = salmonCard(live([held]), { sids: [ISSUE_SIDS.held], item: ISSUE_ITEMS.held }, ON);
    expect(c.issues).toEqual([]);
    expect(unplacedLines(c).length).toBe(1);
  });
});
