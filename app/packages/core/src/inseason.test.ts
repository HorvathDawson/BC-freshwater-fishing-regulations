import { describe, expect, it } from "vitest";
import {
  inSeasonBanner, inSeasonDates, inSeasonNoticeRows, inSeasonOn, inSeasonParts, inSeasonSpan, inSeasonUnseen, joinInSeason,
  parseInSeasonFeed, parseInSeasonLinks, type InSeasonFeed, type InSeasonMarks, type InSeasonRow,
} from "./inseason";

const SRC = "https://www2.gov.bc.ca/fishing-regulations";
const row = (o: Partial<InSeasonRow> & { id: string }): InSeasonRow =>
  ({ region: "4", water: "Kaslo River", effective: "", from: null, to: null, change: "No fishing", ...o });
const feed = (rows: InSeasonRow[], removed: string[] = []): InSeasonFeed =>
  ({ format: "inseason-feed/1", fetchedAt: "2026-10-28T12:00:00Z", source: SRC, rows, removed: removed.map((id) => ({ id })) });

describe("in-season: reading the feed and links", () => {
  it("refuses what is not a whole feed / links", () => {
    expect(parseInSeasonFeed({ format: "dfo-feed/1" })).toBeNull();
    expect(parseInSeasonFeed({ format: "inseason-feed/1", fetchedAt: "x", rows: [{ id: 1 }] })).toBeNull();
    expect(parseInSeasonFeed(feed([row({ id: "is-a" })]))?.rows).toHaveLength(1);
    expect(parseInSeasonLinks({ format: "nope", matches: {} })).toBeNull();
  });
  it("drops a match with an extent it cannot read — never a wider one", () => {
    const l = parseInSeasonLinks({ format: "inseason-links/1", matches: {
      "is-a": { extents: [{ item: "gnis:1", op: "whole" }] },
      "is-b": { extents: [{ item: "gnis:1", op: "between", at: ["A"] }] },           // one cut for a between
      "is-c": { extents: [{ item: "gnis:1", op: "upstream_of", at: ["A"] }, { item: "gnis:2", op: "sideways" }] },
    } })!;
    expect(Object.keys(l.matches)).toEqual(["is-a"]);
  });
});

describe("in-season: when a row is in effect", () => {
  it("override > feed > the one printed window's end, in the from year", () => {
    const r = row({ id: "is-a", from: "2026-07-30", parsed: { kinds: ["closure"], window: { from_month: 7, from_day: 30, to_month: 9, to_day: 15 } } });
    expect(inSeasonSpan(r)).toEqual({ from: "2026-07-30", to: "2026-09-15" });
    expect(inSeasonSpan({ ...r, to: "2026-08-31" })).toEqual({ from: "2026-07-30", to: "2026-08-31" });
    expect(inSeasonSpan(r, { to: "2026-10-01" })).toEqual({ from: "2026-07-30", to: "2026-10-01" });
    // a window over the new year rolls on; a rescind's quoted window is not its end
    expect(inSeasonSpan(row({ id: "x", from: "2026-11-01", parsed: { kinds: ["closure"], window: { from_month: 11, from_day: 1, to_month: 3, to_day: 31 } } })).to)
      .toBe("2027-03-31");
    expect(inSeasonSpan(row({ id: "x", from: "2026-04-01", parsed: { kinds: ["rescind"], window: { from_month: 4, from_day: 1, to_month: 6, to_day: 30 } } })).to)
      .toBeNull();
  });
  it("joins: removed rows never, a future or past row not, matched vs unmatched by the links", () => {
    const f = feed([row({ id: "is-a", from: "2026-01-01" }), row({ id: "is-b", from: "2026-11-01" }),
                    row({ id: "is-c", from: "2025-07-04", to: "2025-08-31" }), row({ id: "is-d" }), row({ id: "is-e" })], ["is-e"]);
    const l = parseInSeasonLinks({ format: "inseason-links/1", matches: { "is-a": { extents: [{ item: "gnis:1", op: "whole" }] } } });
    const j = joinInSeason(f, l, "2026-10-28");
    expect(j.matched.map((c) => c.id)).toEqual(["is-a"]);
    expect(j.unmatched.map((c) => c.id)).toEqual(["is-d"]);
    expect(joinInSeason(f, l, "2026-11-02").unmatched.map((c) => c.id)).toEqual(["is-b", "is-d"]);
  });
});

// A river of four parts, mouth (0) up: 0 ─A─ 1 ─B─ 2 ─C─ 3, and a side branch 4 joining part 1 at D.
const RIVER: InSeasonMarks = { parts: 5, marks: [
  { label: "A", down: 0, up: [1] }, { label: "B", down: 1, up: [2] }, { label: "C", down: 2, up: [3] },
  { label: null, down: 1, up: [4] },
] };

describe("in-season: which parts an extent covers", () => {
  const sorted = (s: Set<number> | null) => (s ? [...s].sort() : s);
  it("whole, upstream / downstream of a cut, between two cuts (either order)", () => {
    expect(sorted(inSeasonParts({ item: "i", op: "whole" }, RIVER))).toEqual([0, 1, 2, 3, 4]);
    expect(sorted(inSeasonParts({ item: "i", op: "upstream_of", at: ["B"] }, RIVER))).toEqual([2, 3]);
    expect(sorted(inSeasonParts({ item: "i", op: "downstream_of", at: ["B"] }, RIVER))).toEqual([0, 1, 4]);
    expect(sorted(inSeasonParts({ item: "i", op: "between", at: ["A", "C"] }, RIVER))).toEqual([1, 2, 4]);
    expect(sorted(inSeasonParts({ item: "i", op: "between", at: ["C", "A"] }, RIVER))).toEqual([1, 2, 4]);
  });
  it("a label that is not a part boundary here is never guessed", () => {
    expect(inSeasonParts({ item: "i", op: "upstream_of", at: ["Hwy 3 bridge"] }, RIVER)).toBeNull();
  });
  it("on a page: its part, the whole water's page, an unresolved cut shown on the whole water as partial", () => {
    const c = (extents: any[]) => ({ id: "is-a", region: "4", water: "W", text: "No fishing", from: null, to: null, source: SRC, extents });
    const up = c([{ item: "i", op: "upstream_of", at: ["B"] }]);
    expect(inSeasonOn([up], "i", 3, RIVER).map((p) => p.partial)).toEqual([false]);
    expect(inSeasonOn([up], "i", 0, RIVER)).toEqual([]);
    expect(inSeasonOn([up], "other", 3, RIVER)).toEqual([]);
    expect(inSeasonOn([up], "i", null, RIVER)).toHaveLength(1);
    const lost = c([{ item: "i", op: "upstream_of", at: ["Nowhere"] }]);
    expect(inSeasonOn([lost], "i", 0, RIVER).map((p) => p.partial)).toEqual([true]);
    expect(inSeasonOn([up], "i", 0, null).map((p) => p.partial)).toEqual([true]);
  });
});

describe("in-season: the words, whole", () => {
  it("dates", () => {
    expect(inSeasonDates("2026-10-15", "2026-11-30")).toBe("Oct 15–Nov 30");
    expect(inSeasonDates("2025-09-16", "2026-01-31")).toBe("Sep 16, 2025–Jan 31, 2026");
    expect(inSeasonDates("2026-04-01", null)).toBe("since Apr 1, 2026");
    expect(inSeasonDates(null, null)).toBeNull();
  });
  it("the banner line, as the owner wrote it", () => {
    const b = inSeasonBanner({ partial: false, change: { id: "is-a", region: "8", water: "Okanagan River", from: "2026-10-15",
      to: "2026-11-30", text: "No fishing July 30 to September 15.", source: SRC, extents: [] } });
    expect(b.line).toBe("In-season change (Oct 15–Nov 30): No fishing July 30 to September 15. — source ›");
    expect([b.url, b.partial, b.note]).toEqual([SRC, null, "Shown as posted; the answers on this page do not include it yet."]);
    expect(inSeasonBanner({ partial: true, change: { id: "x", region: "8", water: "W", from: null, to: null, text: "T", source: SRC, extents: [] } }).line)
      .toBe("In-season change: T — source ›");
  });
  it("the notice rows, and which are unseen", () => {
    const u = [{ id: "is-k", region: "4", water: "Kaslo River", from: "2025-04-04", to: null, text: "Rescinding …", source: SRC, extents: [] },
               { id: "is-p", region: "provincial", water: "All waters", from: null, to: null, text: "X", source: SRC, extents: [] }];
    expect(inSeasonNoticeRows(u).map((r) => `${r.where} | ${r.dates}`))
      .toEqual(["Region 4 · Kaslo River | since Apr 4, 2025", "Provincial · All waters | null"]);
    expect(inSeasonUnseen(u, new Set(["is-k"]))).toEqual(["is-p"]);
  });
});
