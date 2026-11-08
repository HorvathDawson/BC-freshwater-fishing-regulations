import { describe, expect, it } from "vitest";
import {
  INSEASON_FAILURES, inSeasonBanner, inSeasonCover, inSeasonDates, inSeasonNoticeRows, inSeasonSpan, inSeasonSummary, inSeasonUnseen,
  inSeasonWhere, joinInSeason, parseInSeasonFeed, parseInSeasonLinks, type InSeasonChange, type InSeasonFeed, type InSeasonRow,
  type InSeasonRun, type InSeasonSection,
} from "./inseason";
import { readFileSync } from "node:fs";
const PARITY = JSON.parse(readFileSync(new URL("./inseason-cover.parity.json", import.meta.url), "utf-8")) as {
  water: InSeasonSection[]; cases: { name: string; runs: number[][]; lakes: number[]; expect: number[] }[] };

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
  it("refuses the whole links file on any notice it cannot read — never a narrower or wider one (T520)", () => {
    const ok = { format: "inseason-links/2", release: "r-1", words: { a: "A" }, notices: {
      "is-a": { waters: { "gnis:1": { all: true } } },
      "is-b": { to: "2026-12-31", waters: { "gnis:2": { runs: [[7, 0, 100]], ends: [["a", "source"]] } } },
      "is-c": { waters: { "gnis:3": { lakes: [0], ends: [] } } } } };
    expect(Object.keys(parseInSeasonLinks(ok)!.notices)).toEqual(["is-a", "is-b", "is-c"]);
    const bad = (w: unknown) => parseInSeasonLinks({ ...ok, notices: { ...ok.notices, "is-z": { waters: { "gnis:9": w } } } });
    expect(bad({ runs: [[7, 100, 0]], ends: [] })).toBeNull();            // not a range
    expect(bad({ runs: [[7, 0, 100]], ends: [["a", "nowhere"]] })).toBeNull();   // an end token with no words
    expect(bad({ runs: [], ends: [] })).toBeNull();                       // covers nothing
    expect(bad({ all: true, runs: [[7, 0, 1]] })).toBeNull();
    expect(parseInSeasonLinks({ format: "inseason-links/1", release: "r-1", matches: {} })).toBeNull();   // the old format
    expect(parseInSeasonLinks({ format: "inseason-links/2", words: {}, notices: {} })).toBeNull();        // no release
    expect(parseInSeasonLinks({ format: "inseason-links/2", release: "r-1", notices: {} })).toBeNull();   // no words
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
    const l = parseInSeasonLinks({ format: "inseason-links/2", release: "r-1", words: {}, notices: { "is-a": { waters: { "gnis:1": { all: true } } } } });
    const j = joinInSeason(f, l, "2026-10-28");
    expect(j.matched.map((c) => c.id)).toEqual(["is-a"]);
    expect(j.unmatched.map((c) => c.id)).toEqual(["is-d"]);
    expect(joinInSeason(f, l, "2026-11-02").unmatched.map((c) => c.id)).toEqual(["is-b", "is-d"]);
  });
});

// (T520) a water of five sections: stream 7 at 0, 100, 200, 300 (handles 1-4) and one lake piece (handle 9)
const WATER: InSeasonSection[] = [...[1, 2, 3, 4].map((sid): InSeasonSection => ({ sid, line: { blk: 7, from: (sid - 1) * 100 } })),
  { sid: 9, line: null }];
const WORDS = { a: "A", b: "B" };
const ch = (waters: InSeasonChange["waters"], onRelease = true): InSeasonChange =>
  ({ id: "is-a", region: "2", water: "W", text: "No fishing", from: null, to: null, source: SRC, waters, onRelease,
     linksRelease: "r-1", words: WORDS });

describe("inSeasonWhere (T518, T520): the sentence, composed in core from end tokens", () => {
  const W = { hg: "Hell's Gate", sp: "Spuzzum Creek confluence", p: "Pitt Lake inlet" };
  it("from A to B, upstream of A, downstream of B, several joined; the whole main line says nothing", () => {
    expect(inSeasonWhere([["sp", "hg"]], W)).toBe("From Spuzzum Creek confluence to Hell's Gate");
    expect(inSeasonWhere([["p", "source"]], W)).toBe("Upstream of Pitt Lake inlet");
    expect(inSeasonWhere([["mouth", "sp"]], W)).toBe("Downstream of Spuzzum Creek confluence");
    expect(inSeasonWhere([["mouth", "sp"], ["hg", "source"]], W))
      .toBe("Downstream of Spuzzum Creek confluence; Upstream of Hell's Gate");
    expect(inSeasonWhere([["mouth", "source"]], W)).toBeNull();
    expect(inSeasonWhere([], W)).toBeNull();
  });
});

describe("one fixture, two readers (T520): Python covered_by_runs and inSeasonCover agree", () => {
  for (const c of PARITY.cases)
    it(c.name, () => {
      const got = inSeasonCover(ch({ i: { runs: c.runs as unknown as InSeasonRun[], lakes: c.lakes, ends: [] } }), "i",
                                PARITY.water, null);
      expect(got?.sections ?? []).toEqual(c.expect);
    });
});

describe("inSeasonCover (T520): where a change lands on a page", () => {
  const part = { "i": { runs: [[7, 100, 300]] as const, lakes: [0], ends: [["a", "b"]] as const } };
  it("all: every section, no ends — on any release", () => {
    expect(inSeasonCover(ch({ i: { all: true } }), "i", WATER, new Set([1]))).toEqual({ sections: null, where: null, failure: null });
    expect(inSeasonCover(ch({ i: { all: true } }, false), "i", null, null)).toEqual({ sections: null, where: null, failure: null });
  });
  it("some: exactly the sections whose start lies in a range, the lake piece by its place; the ends said", () => {
    expect(inSeasonCover(ch(part), "i", WATER, null)).toEqual({ sections: [2, 3, 9], where: "From A to B", failure: null });
    expect(inSeasonCover(ch(part), "i", WATER, new Set([1, 2]))).toEqual({ sections: [2], where: "From A to B", failure: null });
    // a page it covers whole: no "where" words
    expect(inSeasonCover(ch(part), "i", WATER, new Set([2, 3]))).toEqual({ sections: [2, 3], where: null, failure: null });
  });
  it("none: not on this page, not on another water", () => {
    expect(inSeasonCover(ch(part), "i", WATER, new Set([1, 4]))).toBeNull();
    expect(inSeasonCover(ch(part), "other", WATER, null)).toBeNull();
    expect(inSeasonCover(ch(null), "i", WATER, null)).toBeNull();
  });
  it("off-release, no lines, or landing on nothing: the whole water, its ends, and a failure to report", () => {
    expect(inSeasonCover(ch(part, false), "i", WATER, new Set([1])))
      .toEqual({ sections: null, where: "From A to B", failure: INSEASON_FAILURES.offRelease });
    expect(inSeasonCover(ch(part), "i", null, new Set([1])))
      .toEqual({ sections: null, where: "From A to B", failure: INSEASON_FAILURES.noLines });
    expect(inSeasonCover(ch({ i: { runs: [[8, 0, 50]], ends: [["mouth", "a"]] } }), "i", WATER, new Set([1])))
      .toEqual({ sections: null, where: "Downstream of A", failure: INSEASON_FAILURES.nothing("is-a", "i") });
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
    const b = inSeasonBanner({ id: "is-a", region: "8", water: "Okanagan River", from: "2026-10-15",
      to: "2026-11-30", text: "No fishing July 30 to September 15.", source: SRC, waters: null, onRelease: true, linksRelease: null, words: {} }, { where: null });
    expect(b.line).toBe("In-season change (Oct 15–Nov 30): No fishing July 30 to September 15. — source ›");
    expect([b.url, b.where, b.note]).toEqual([SRC, null, "Shown as posted; the answers on this page do not include it yet."]);
    // (T518) a change on part of the page says where, in its own ends
    const p = inSeasonBanner({ id: "x", region: "8", water: "W", from: null, to: null, text: "T", source: SRC, waters: null, onRelease: true, linksRelease: null, words: {} },
      { where: "From Hell's Gate to Spuzzum Creek confluence" });
    expect([p.where, p.line]).toEqual(["Where: From Hell's Gate to Spuzzum Creek confluence",
      "In-season change: T Where: From Hell's Gate to Spuzzum Creek confluence. — source ›"]);
  });
  it("the notice rows, and which are unseen", () => {
    const u = [{ id: "is-k", region: "4", water: "Kaslo River", from: "2025-04-04", to: null, text: "Rescinding …", source: SRC, waters: null, onRelease: true, linksRelease: null, words: {} },
               { id: "is-p", region: "provincial", water: "All waters", from: null, to: null, text: "X", source: SRC, waters: null, onRelease: true, linksRelease: null, words: {} }];
    expect(inSeasonNoticeRows(u).map((r) => `${r.where} | ${r.dates}`))
      .toEqual(["Region 4 · Kaslo River | since Apr 4, 2025", "Provincial · All waters | null"]);
    expect(inSeasonUnseen(u, new Set(["is-k"]))).toEqual(["is-p"]);
  });
});

describe("in-season: the links' release (AGENTS 6)", () => {
  const links = parseInSeasonLinks({ format: "inseason-links/2", release: "r-1", words: { "lake:1:inlet": "Pitt Lake inlet" },
    notices: { "is-a": { waters: { "gnis:7551": { runs: [[7, 0, 100]], ends: [["lake:1:inlet", "source"]] },
                                   "gnis:9": { all: true } } } } });
  const f = feed([row({ id: "is-a" })]);
  it("on its release the ranges are read; on another, never (a whole water still holds)", () => {
    expect(joinInSeason(f, links, "2026-10-29", "r-1").matched[0]!.onRelease).toBe(true);
    const [c] = joinInSeason(f, links, "2026-10-29", "r-2").matched;
    expect(c!.onRelease).toBe(false);
    expect(inSeasonCover(c!, "gnis:9", WATER, new Set([1]))!.failure).toBeNull();
    expect(inSeasonCover(c!, "gnis:7551", WATER, new Set([1]))!.failure).toBe(INSEASON_FAILURES.offRelease);
  });
});

describe("inSeasonSummary (T472)", () => {
  const b = (from: string | null) => inSeasonBanner({ id: "x", region: "2", water: "Fraser River", from, to: null, text: "t", source: "u", waters: null, onRelease: true, linksRelease: null, words: {} }, { where: null });
  it("none: null; one: its head and see; several: the count", () => {
    expect(inSeasonSummary([])).toBeNull();
    expect(inSeasonSummary([b("2025-05-16")])).toBe("In-season change (since May 16, 2025) · see ›");
    expect(inSeasonSummary([b(null), b(null)])).toBe("2 in-season changes · see ›");
  });
});
