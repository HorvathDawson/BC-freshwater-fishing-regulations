import { describe, expect, it } from "vitest";
import {
  INSEASON_FAILURES, INSEASON_LINKS_FORMAT, inSeasonAbsentWaters, inSeasonBanner, inSeasonCover, inSeasonDates, inSeasonNoticeRows, inSeasonSpan, inSeasonSummary, inSeasonUnseen,
  inSeasonWhere, joinInSeason, parseInSeasonFeed, parseInSeasonLinks, type InSeasonChange, type InSeasonFeed, type InSeasonRow,
  type InSeasonRun, type InSeasonSection,
} from "./inseason";
// imported, never read from disk: core has no platform (AGENTS rule 22, the boundaries gate)
import parity from "./inseason-cover.parity.json" with { type: "json" };
const PARITY = parity as unknown as {
  water: InSeasonSection[]; cases: { name: string; runs: number[][]; lakes: boolean; expect: number[] }[] };
import spanCases from "./inseason-span.parity.json" with { type: "json" };
const SPAN = spanCases as unknown as { cases: { name: string; row: Partial<InSeasonRow> & { id: string };
  effective?: { from?: string; to?: string }; expect: [string | null, string | null]; inForce: Record<string, boolean> }[] };

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
    const ok = { format: INSEASON_LINKS_FORMAT, notices: {
      "is-a": { waters: { "gnis:1": { all: true } } },
      "is-b": { to: "2026-12-31", waters: { "gnis:2": { runs: [[7, 0, 100]], ends: [["a", "source"]] } } },
      "is-c": { waters: { "gnis:3": { lakes: true, ends: [] } } } } };
    expect(Object.keys(parseInSeasonLinks(ok)!.notices)).toEqual(["is-a", "is-b", "is-c"]);
    const bad = (w: unknown) => parseInSeasonLinks({ ...ok, notices: { ...ok.notices, "is-z": { waters: { "gnis:9": w } } } });
    expect(bad({ runs: [[7, 100, 0]], ends: [] })).toBeNull();            // not a range
    expect(bad({ runs: [[7, 0, 100]], ends: [["a", ""]] })).toBeNull();   // an empty end token
    expect(bad({ runs: [], ends: [] })).toBeNull();                       // covers nothing
    expect(bad({ all: true, runs: [[7, 0, 1]] })).toBeNull();
    expect(bad({ lakes: [0], ends: [] })).toBeNull();                     // (T673) /3's lake places: lakes is `true`
    expect(parseInSeasonLinks({ format: "inseason-links/1", release: "r-1", matches: {} })).toBeNull();   // the old format
    expect(parseInSeasonLinks({ format: "inseason-links/2", release: "r-1", words: {}, notices: {} })).toBeNull();   // the words-carrying one
    // (T673) /3 named the one release its ranges were read on: refused whole (the caller reports it, naming the format)
    expect(parseInSeasonLinks({ format: "inseason-links/3", release: "r-1", notices: {} })).toBeNull();
    expect(INSEASON_FAILURES.unreadableLinks("inseason-links/3")).toBe(`in-season matches.json: inseason-links/3, but this app reads ${INSEASON_LINKS_FORMAT} — refused (matched notices fall back to the last links read, else to the list of changes not on the map)`);
    // (T673) no release, no words: an empty file of notices reads
    expect(parseInSeasonLinks({ format: INSEASON_LINKS_FORMAT, notices: {} })).toEqual({ format: INSEASON_LINKS_FORMAT, notices: {} });
  });
});

describe("one case file, two readers (T548): Python span and inSeasonSpan agree", () => {
  for (const c of SPAN.cases)
    it(c.name, () => {
      const r = row({ ...c.row, id: c.row.id });
      expect(inSeasonSpan(r, c.effective)).toEqual({ from: c.expect[0], to: c.expect[1] });
      const links = c.effective ? parseInSeasonLinks({ format: INSEASON_LINKS_FORMAT,
        notices: { [r.id]: { ...c.effective, waters: { "gnis:1": { all: true } } } } }) : null;
      for (const [day, want] of Object.entries(c.inForce)) {
        const j = joinInSeason(feed([r]), links, day);
        expect([day, j.matched.length + j.unmatched.length === 1]).toEqual([day, want]);
      }
    });
});

describe("in-season: when a row is in effect", () => {
  it("joins: removed rows never, a future or past row not, matched vs unmatched by the links", () => {
    const f = feed([row({ id: "is-a", from: "2026-01-01" }), row({ id: "is-b", from: "2026-11-01" }),
                    row({ id: "is-c", from: "2025-07-04", to: "2025-08-31" }), row({ id: "is-d" }), row({ id: "is-e" })], ["is-e"]);
    const l = parseInSeasonLinks({ format: INSEASON_LINKS_FORMAT, notices: { "is-a": { waters: { "gnis:1": { all: true } } } } });
    const j = joinInSeason(f, l, "2026-10-28");
    expect(j.matched.map((c) => c.id)).toEqual(["is-a"]);
    expect(j.unmatched.map((c) => c.id)).toEqual(["is-d"]);
    expect(joinInSeason(f, l, "2026-11-02").unmatched.map((c) => c.id)).toEqual(["is-b", "is-d"]);
  });
});

// (T520) a water of five sections: stream 7 at [0, 100), [100, 200), [200, 300), [300, 400) (handles 1-4) and one lake
// piece (handle 9)
const WATER: InSeasonSection[] = [...[1, 2, 3, 4].map((sid): InSeasonSection =>
  ({ sid, line: { blk: 7, from: (sid - 1) * 100, to: sid * 100 } })), { sid: 9, line: null }];
/** (T517) the release's `display.ends` (what "Where" says, through the one end resolver) */
const TABLE = { a: ["A"], b: ["B"], mouth: ["Mouth"], source: ["Source"] };
const ch = (waters: InSeasonChange["waters"]): InSeasonChange =>
  ({ id: "is-a", region: "2", water: "W", text: "No fishing", from: null, to: null, source: SRC, waters });

describe("inSeasonWhere (T518, T520): the sentence, composed in core from end tokens", () => {
  const W = { hg: ["Hell's Gate"], sp: ["Spuzzum Creek confluence"], p: ["Pitt Lake outlet", "to:Pitt Lake inlet"],
              mouth: ["Mouth"], source: ["Source"] };
  it("from A to B, upstream of A, downstream of B, several joined; the whole main line says nothing", () => {
    expect(inSeasonWhere([["sp", "hg"]], W)).toBe("From Spuzzum Creek confluence to Hell's Gate");
    expect(inSeasonWhere([["p", "source"]], W)).toBe("Upstream of Pitt Lake inlet");
    expect(inSeasonWhere([["mouth", "sp"]], W)).toBe("Downstream of Spuzzum Creek confluence");
    expect(inSeasonWhere([["mouth", "sp"], ["hg", "source"]], W))
      .toBe("Downstream of Spuzzum Creek confluence; Upstream of Hell's Gate");
    expect(inSeasonWhere([["mouth", "source"]], W)).toBeNull();
    expect(inSeasonWhere([], W)).toBeNull();
  });
  it("(T517) a lower end says its `to:` words, an upper end its first; a token the release lacks throws, naming it", () => {
    expect(inSeasonWhere([["mouth", "p"]], W)).toBe("Downstream of Pitt Lake outlet");
    expect(() => inSeasonWhere([["sp", "gone"]], W)).toThrow("ends: token gone is not in this release's display.ends");
  });
});

describe("one fixture, two readers (T520, T673): Python touched and inSeasonCover agree", () => {
  for (const c of PARITY.cases)
    it(c.name, () => {
      const got = inSeasonCover(ch({ i: { runs: c.runs as unknown as InSeasonRun[], ...(c.lakes ? { lakes: true as const } : {}),
                                          ends: [] } }), "i", PARITY.water, null, TABLE);
      expect(got?.sections ?? []).toEqual(c.expect);
    });
});

describe("inSeasonCover (T520): where a change lands on a page", () => {
  const part = { "i": { runs: [[7, 100, 300]] as const, lakes: true as const, ends: [["a", "b"]] as const } };
  it("all: every section, no ends — on any release", () => {
    expect(inSeasonCover(ch({ i: { all: true } }), "i", WATER, new Set([1]), TABLE)).toEqual({ sections: null, where: null, failure: null, whereFailure: null });
    expect(inSeasonCover(ch({ i: { all: true } }), "i", null, null, TABLE)).toEqual({ sections: null, where: null, failure: null, whereFailure: null });
  });
  it("some: exactly the sections a range touches, every lake piece with `lakes`; the ends said", () => {
    expect(inSeasonCover(ch(part), "i", WATER, null, TABLE)).toEqual({ sections: [2, 3, 9], where: "From A to B", failure: null, whereFailure: null });
    expect(inSeasonCover(ch(part), "i", WATER, new Set([1, 2]), TABLE)).toEqual({ sections: [2], where: "From A to B", failure: null, whereFailure: null });
    // a page it covers whole: no "where" words
    expect(inSeasonCover(ch(part), "i", WATER, new Set([2, 3]), TABLE)).toEqual({ sections: [2, 3], where: null, failure: null, whereFailure: null });
  });
  it("none: not on this page, not on another water", () => {
    expect(inSeasonCover(ch(part), "i", WATER, new Set([1, 4]), TABLE)).toBeNull();
    expect(inSeasonCover(ch(part), "other", WATER, null, TABLE)).toBeNull();
    expect(inSeasonCover(ch(null), "i", WATER, null, TABLE)).toBeNull();
  });
  it("no lines, or touching nothing: the whole water, its ends, and a failure to report", () => {
    expect(inSeasonCover(ch(part), "i", null, new Set([1]), TABLE))
      .toEqual({ sections: null, where: "From A to B", failure: INSEASON_FAILURES.noLines, whereFailure: null });
    expect(inSeasonCover(ch({ i: { runs: [[8, 0, 50]], ends: [["mouth", "a"]] } }), "i", WATER, new Set([1]), TABLE))
      .toEqual({ sections: null, where: "Downstream of A", failure: INSEASON_FAILURES.nothing("is-a", "i"), whereFailure: null });
  });
  it("(T517) an end the release's `display.ends` lacks drops ONLY the \"Where\" line: the sections stay, the reason is given", () => {
    expect(inSeasonCover(ch({ i: { runs: [[7, 100, 300]], ends: [["a", "gone"]] } }), "i", WATER, null, TABLE))
      .toEqual({ sections: [2, 3], where: null, failure: null,
                 whereFailure: INSEASON_FAILURES.noWords("is-a", "ends: token gone is not in this release's display.ends") });
  });
  it("(T517) the table could not be read: the notice is placed as ever, with no \"Where\" and the reason", () => {
    expect(inSeasonCover(ch({ i: { runs: [[7, 100, 300]], ends: [["a", "b"]] } }), "i", WATER, null, null))
      .toEqual({ sections: [2, 3], where: null, failure: null, whereFailure: INSEASON_FAILURES.noTable });
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
      to: "2026-11-30", text: "No fishing July 30 to September 15.", source: SRC, waters: null }, { where: null });
    expect(b.line).toBe("In-season change (Oct 15–Nov 30): No fishing July 30 to September 15. — source ›");
    expect([b.url, b.where, b.note]).toEqual([SRC, null, "Shown as posted; the answers on this page do not include it yet."]);
    // (T518) a change on part of the page says where, in its own ends
    const p = inSeasonBanner({ id: "x", region: "8", water: "W", from: null, to: null, text: "T", source: SRC, waters: null },
      { where: "From Hell's Gate to Spuzzum Creek confluence" });
    expect([p.where, p.line]).toEqual(["Where: From Hell's Gate to Spuzzum Creek confluence",
      "In-season change: T Where: From Hell's Gate to Spuzzum Creek confluence. — source ›"]);
  });
  it("the notice rows, and which are unseen", () => {
    const u = [{ id: "is-k", region: "4", water: "Kaslo River", from: "2025-04-04", to: null, text: "Rescinding …", source: SRC, waters: null },
               { id: "is-p", region: "provincial", water: "All waters", from: null, to: null, text: "X", source: SRC, waters: null }];
    expect(inSeasonNoticeRows(u).map((r) => `${r.where} | ${r.dates}`))
      .toEqual(["Region 4 · Kaslo River | since Apr 4, 2025", "Provincial · All waters | null"]);
    expect(inSeasonUnseen(u, new Set(["is-k"]))).toEqual(["is-p"]);
  });
});

describe("in-season: ONE file, any release (T673, owner 2026-11-13)", () => {
  // the same links, read on two releases of one water: R1 (the one the range was resolved on: stream 7 cut at 100, 200,
  // 300 — `WATER`) and R0, an older one whose boundaries sit elsewhere (cut at 50, 150, 250, 350: no boundary at 100 or
  // 300), and a lake piece in each
  const links = parseInSeasonLinks({ format: INSEASON_LINKS_FORMAT,
    notices: { "is-a": { waters: { "gnis:7551": { runs: [[7, 100, 300]], ends: [["a", "b"]] },
                                   "gnis:9": { all: true } } } } })!;
  const R0: InSeasonSection[] = [[21, 0, 50], [22, 50, 150], [23, 150, 250], [24, 250, 350], [25, 350, 400]]
    .map(([sid, from, to]): InSeasonSection => ({ sid: sid!, line: { blk: 7, from: from!, to: to! } }))
    .concat([{ sid: 29, line: null }]);
  const [c] = joinInSeason(feed([row({ id: "is-a" })]), links, "2026-10-29").matched;
  it("on the release it was resolved on: exactly its sections, with its \"Where\"", () => {
    expect(inSeasonCover(c!, "gnis:7551", WATER, null, TABLE))
      .toEqual({ sections: [2, 3], where: "From A to B", failure: null, whereFailure: null });
  });
  it("on an older release whose boundaries moved: every section the stretch touches — the two it reaches only partly into too — and its \"Where\"", () => {
    expect(inSeasonCover(c!, "gnis:7551", R0, null, TABLE))
      .toEqual({ sections: [22, 23, 24], where: "From A to B", failure: null, whereFailure: null });
    // a page holding only a partly-touched section shows it there, never drops it
    expect(inSeasonCover(c!, "gnis:7551", R0, new Set([24, 25]), TABLE))
      .toEqual({ sections: [24], where: "From A to B", failure: null, whereFailure: null });
    expect(inSeasonCover(c!, "gnis:7551", R0, new Set([21, 25, 29]), TABLE)).toBeNull();
  });
  it("a whole water holds on both", () => {
    for (const w of [WATER, R0])
      expect(inSeasonCover(c!, "gnis:9", w, null, TABLE)).toEqual({ sections: null, where: null, failure: null, whereFailure: null });
  });
});

describe("inSeasonSummary (T472)", () => {
  const b = (from: string | null) => inSeasonBanner({ id: "x", region: "2", water: "Fraser River", from, to: null, text: "t", source: "u", waters: null }, { where: null });
  it("none: null; one: its head and see; several: the count", () => {
    expect(inSeasonSummary([])).toBeNull();
    expect(inSeasonSummary([b("2025-05-16")])).toBe("In-season change (since May 16, 2025) · see ›");
    expect(inSeasonSummary([b(null), b(null)])).toBe("2 in-season changes · see ›");
  });
});

describe("in-season: waters this release does not hold (T693)", () => {
  const f = feed([row({ id: "is-a" }), row({ id: "is-b" }), row({ id: "is-c" })]);
  const l = parseInSeasonLinks({ format: INSEASON_LINKS_FORMAT, notices: {
    "is-a": { waters: { "gnis:gone": { all: true } } },
    "is-b": { waters: { "gnis:gone": { all: true }, "gnis:here": { all: true } } },
    "is-c": { waters: { "gnis:here": { all: true } } } } });
  it("every water absent: not matched (the bell takes it); a held one keeps it matched; none absent: as before", () => {
    const j = joinInSeason(f, l, "2026-10-28", new Set(["gnis:gone"]));
    expect([j.matched.map((c) => c.id), j.unmatched.map((c) => [c.id, c.waters])]).toEqual([["is-b", "is-c"], [["is-a", null]]]);
    expect(joinInSeason(f, l, "2026-10-28").matched.map((c) => c.id)).toEqual(["is-a", "is-b", "is-c"]);
    expect(joinInSeason(f, l, "2026-10-28", new Set()).unmatched).toEqual([]);
  });
  it("names the absent waters of a change, sorted", () => {
    const [, b] = joinInSeason(f, l, "2026-10-28").matched;
    expect(inSeasonAbsentWaters(b!, new Set(["gnis:gone", "gnis:x"]))).toEqual(["gnis:gone"]);
  });
});

