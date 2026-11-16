/**
 * (T759) THE LIVE FEED'S ISSUES AS BANNERS: on every stretch of the issue's water (its registry item), DFO's row
 * verbatim, sorted by key; never a status. Hand-built feed in the ticket's shape (`regs/testing/salmonIssues.ts`).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { setFailureSink } from "./failure";
import { salmonCard, type SalmonFacts } from "./salmon";
import { parseSalmonFeed, pickSalmon, withSalmonFeed } from "./salmonFeed";
import { issueLines, salmonIssueItems, SALMON_ISSUE_WORDS } from "./salmonIssues";
import { CAMPBELL_ISSUES, ISSUES, ISSUE_ITEMS, ISSUE_SIDS, issueFeed, issueFeedRaw, issueRelease } from "./regs/testing/salmonIssues";

const ON = new Date(2026, 8, 15);
const card = (f: SalmonFacts, sid: number, item: string | null) => salmonCard(f, { sids: [sid], item }, ON);
const live = () => pickSalmon(issueRelease(), issueFeed());

let reports: string[] = [];
let undo: () => void = () => {};
beforeEach(() => { reports = []; undo = setFailureSink((e) => reports.push((e as Error).message)); });
afterEach(() => undo());

describe("the feed's issues (T759)", () => {
  it("says each of a water's issues on every stretch of it, DFO's rows verbatim, in key order", () => {
    for (const sid of [ISSUE_SIDS.campbell, ISSUE_SIDS.campbell2]) {
      const got = card(live(), sid, ISSUE_ITEMS.campbell).issues;
      expect(got.map((b) => b.key)).toEqual(["closed_area_unbound:1:campbell:upper#closed:a1b2c3d4e5f6", "drift:fp1", "unmatched:fp2"]);
      expect(got.map(issueLines)).toEqual([
        ["DFO reworded a closed area on this water. The app has not placed the new wording yet:", "Pierre Ck.",
         "No fishing for salmon — within a 400 m radius of the mouth of Pierre Ck.", "Check DFO’s page for where it applies before you fish."],
        ["DFO reworded a rule for this water. The app applies it where the earlier wording was; that has not been confirmed yet:",
         "from a fishing boundary sign at the  cement block upstream.", "Coho | Jul 1 to Oct 31 | 2 per day", "Chinook | All year | 0",
         "Check DFO’s page for where it applies before you fish."],
        ["DFO lists a rule for this water that this app has not placed on the map yet. It may apply here:",
         "from the end of Maple Street downstream to the confluence with the Quinsam River.", "All | Aug 1 to Oct 31 | No fishing",
         "Check DFO’s page for where it applies before you fish."]]);
      expect(got.every((b) => b.title === "A DFO rule to check on this water" && b.water === "Campbell River")).toBe(true);
    }
    expect(reports).toEqual([]);
  });

  it("never changes a status, a colour or anything else on the card", () => {
    const none = pickSalmon(issueRelease(), issueFeed({ waters: {}, no_water: [], unread: {} }));
    for (const [sid, item] of [[ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell], [ISSUE_SIDS.campbell2, ISSUE_ITEMS.campbell]] as const) {
      const { issues, ...rest } = card(live(), sid, item);
      const { issues: zero, ...plain } = card(none, sid, item);
      expect(zero).toEqual([]);
      expect(JSON.stringify(rest)).toBe(JSON.stringify(plain));
      expect(issues.length).toBeGreaterThan(0);
    }
    expect([...live().payload.chains]).toEqual([...none.payload.chains]);
  });

  it("goes by the stretch's registry item, never its chains: another water's stretch and an unnamed reach have none", () => {
    expect(card(live(), ISSUE_SIDS.other, ISSUE_ITEMS.other).issues).toEqual([]);
    expect(card(live(), ISSUE_SIDS.campbell, null).issues).toEqual([]);
    // the Campbell's item on another water's chain still has them (item → its water's issues)
    expect(card(live(), ISSUE_SIDS.other, ISSUE_ITEMS.campbell).issues.length).toBe(3);
  });

  it("an undrawn place the stretch's chains already list as not drawn is the caveat's, not a banner; one they do not is", () => {
    expect(card(live(), ISSUE_SIDS.babine, ISSUE_ITEMS.babine).issues).toEqual([]);
    const seeded = { ...ISSUES, waters: { ...ISSUES.waters, "6:babine-lake": { ...ISSUES.waters["6:babine-lake"],
      list: [{ ...ISSUES.waters["6:babine-lake"].list[0]!, location_id: "6:babine:seeded-after-the-release" }] } } };
    expect(card(pickSalmon(issueRelease(), issueFeed(seeded)), ISSUE_SIDS.babine, ISSUE_ITEMS.babine).issues.map((b) => b.area))
      .toEqual(["within 400 m of the mouths."]);
  });

  it("a water in a region the feed did not read shows none; read, it shows its own", () => {
    expect(card(live(), ISSUE_SIDS.held, ISSUE_ITEMS.held).issues).toEqual([]);
    const read = issueFeed({ ...ISSUES, unread: {} });
    expect(card(pickSalmon(issueRelease(), read), ISSUE_SIDS.held, ISSUE_ITEMS.held).issues.map((b) => b.key)).toEqual(["unmatched:fph"]);
  });

  it("without a feed — offline, the release's own file, the minimum-shape fallback — none", () => {
    const r = issueRelease();
    expect(card(pickSalmon(r, null), ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell).issues).toEqual([]);
    const shaped = { ...r, payload: { ...r.payload, p: { "1:campbell:lower": { r: "1", piece: 1, bound: 1 } } } } as SalmonFacts;
    const fallback = pickSalmon(shaped, issueFeed());                   // no dfo.pieces: the release's own file
    expect(card(fallback, ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell).issues).toEqual([]);
    expect(card(r, ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell).issues).toEqual([]);
  });

  it("an older feed (no issues) has none; a malformed block refuses the feed; no_water shows on no stretch", () => {
    expect(card(pickSalmon(issueRelease(), parseSalmonFeed(issueFeedRaw(undefined))!), ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell).issues).toEqual([]);
    const bad = [null, [], { waters: {}, no_water: [] }, { ...ISSUES, unread: { "3": 1 } },
                 { ...ISSUES, waters: { x: { r: "1", name: "X", items: "gnis:1", list: [] } } },
                 { ...ISSUES, waters: { x: { r: "1", name: "X", items: [], list: [{ kind: "drift", key: "k", area: "a", rows: "r" }] } } },
                 { ...ISSUES, waters: { x: { r: "1", name: "X", items: [], list: [{ kind: "drift", key: "k", area: "a", rows: [], place: 1 }] } } }];
    expect(bad.map((b) => parseSalmonFeed(issueFeedRaw(b)))).toEqual(bad.map(() => null));
    const ids = Object.values(ISSUE_ITEMS);
    const shown = ids.filter((i) => card(live(), ISSUE_SIDS.other, i).issues.some((b) => b.water === "Nowhere Creek"));
    expect(shown).toEqual([]);
  });

  it("an unknown kind is shown with the generic words and reported once", () => {
    const odd = { ...ISSUES, waters: { "1:campbell-river": { ...ISSUES.waters["1:campbell-river"], list: [
      { kind: "renamed_place", key: "renamed_place:x", area: "the old bridge.", rows: ["All | All year | No fishing"] }, ...CAMPBELL_ISSUES] } } };
    const f = issueFeed(odd);
    const got = card(withSalmonFeed(issueRelease(), f), ISSUE_SIDS.campbell, ISSUE_ITEMS.campbell).issues;
    expect(issueLines(got.find((b) => b.kind === "renamed_place")!)).toEqual([
      "DFO lists a rule for this water that this app has not matched yet:", "the old bridge.", "All | All year | No fishing",
      "Check DFO’s page for where it applies before you fish."]);
    void withSalmonFeed({ ...issueRelease() }, issueFeed(odd)).payload;
    expect(reports).toEqual(['salmon feed: issue kind "renamed_place" has no words in this app; shown with the generic words']);
  });

  it("the words, whole", () => {
    expect(SALMON_ISSUE_WORDS).toEqual({
      title: "A DFO rule to check on this water",
      lead: {
        unmatched: "DFO lists a rule for this water that this app has not placed on the map yet. It may apply here:",
        drift: "DFO reworded a rule for this water. The app applies it where the earlier wording was; that has not been confirmed yet:",
        closed_area_unbound: "DFO reworded a closed area on this water. The app has not placed the new wording yet:",
      },
      generic: "DFO lists a rule for this water that this app has not matched yet:",
      close: "Check DFO’s page for where it applies before you fish.",
    });
  });

  it("names the items the map's issue look would draw (not drawn yet)", () => {
    expect([...salmonIssueItems(live().payload)].sort()).toEqual([ISSUE_ITEMS.campbell]);
    expect([...salmonIssueItems(pickSalmon(issueRelease(), issueFeed({ ...ISSUES, unread: {} })).payload)].sort())
      .toEqual([ISSUE_ITEMS.campbell, ISSUE_ITEMS.held]);
    expect(salmonIssueItems(pickSalmon(issueRelease(), null).payload).size).toBe(0);
  });
});
