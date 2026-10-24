import { describe, expect, it } from "vitest";
import { FIXTURE as fixture } from "./statusIndex.fixture";
import { accessOf } from "./access";
import { dayProfiles, mapStatusOn, NO_ACCESS_PROFILE, profileLabel, statusOfCode, statusOn, UNLISTED_PROFILE,
         waterStatusOn } from "./status";
import { dayOfYear, decodeStatusIndex, StatusIndexError, type StatusIndex } from "./statusIndex";
import { CALENDAR_CASES, weekdayOf } from "./calendar.generated";

/*
 * THE FIXTURE IS THE PIPELINE'S. `statusIndex.fixture.ts` is written by
 * pipeline/tests/test_status_index.py: the bytes are the Python encoder's, the answers the
 * Python reader's, and a test there fails if this file falls behind. So "the app decodes what
 * the pipeline wrote, and reads it the same way" is one assertion across both languages.
 */
const BYTES = Uint8Array.from(atob(fixture.bytes), (c) => c.charCodeAt(0));
const day = (iso: string) => {
  const [y, m, d] = iso.split("-").map(Number);
  return new Date(y!, m! - 1, d!);
};

/** Every row the fixture states, as `[key, date, expected, got]` where they differ. */
function disagreements(ix: StatusIndex): unknown[] {
  return [
    ...fixture.sections.map(([s, d, want]) =>
      [s, d, want, ix.codeOn(s as number, day(d as string))]),
    ...fixture.waters.map(([i, d, want]) =>
      [i, d, want, ix.waterCodeOn(i as string, day(d as string))]),
  ].filter((r) => r[2] !== r[3]);
}

describe("the status index decoder", () => {
  it("reads the pipeline's bytes exactly as the pipeline does", () => {
    const ix = decodeStatusIndex(BYTES, fixture.handles, fixture.reach_digest);
    expect(ix.handles).toBe("147b20dce7d8576c");
    expect(ix.reachDigest).toBe(fixture.reach_digest);
    expect(() => decodeStatusIndex(BYTES, fixture.handles, "0000000000000000"))
      .toThrow(/cut from rule bindings/);
    expect(fixture.sections.length + fixture.waters.length).toBeGreaterThan(100);
    expect(disagreements(ix)).toEqual([]);
    expect(ix.sections).toBe(5);
    expect(ix.waters).toBe(3);
  });

  it("covers every code, a season over New Year and Feb 29 in the fixture", () => {
    const codes = new Set([...fixture.sections, ...fixture.waters].map((r) => r[2]));
    expect([...codes].sort()).toEqual(["base", "closed", "outside", "own"]);
    const ix = decodeStatusIndex(BYTES, null);
    expect(ix.codeOn(3, new Date(2025, 11, 31))).toBe("closed");
    expect(ix.codeOn(3, new Date(2026, 0, 1))).toBe("closed");
    expect(ix.codeOn(3, new Date(2026, 2, 31))).toBe("own");
    expect(ix.codeOn(900, new Date(2024, 1, 29))).toBe("closed");
    expect(ix.codeOn(900, new Date(2025, 2, 1))).toBe("base");
    // The handle as the map reports it: a string is accepted, junk is base, never a throw.
    expect(ix.codeOn("3", new Date(2026, 0, 1))).toBe("closed");
    expect(ix.codeOn("x", new Date(2026, 0, 1))).toBe("base");
    expect(ix.codeOn(-1, new Date(2026, 0, 1))).toBe("base");
  });

  it("reads Feb 29 as Feb 28 (day 59) and puts Mar 1 on day 61 in every year (P06)", () => {
    expect(dayOfYear(new Date(2024, 1, 29))).toBe(59);
    expect(dayOfYear(new Date(2036, 1, 29))).toBe(59);
    expect(dayOfYear(new Date(2025, 2, 1))).toBe(61);
    expect(dayOfYear(new Date(2024, 2, 1))).toBe(61);
    expect(dayOfYear(new Date(2025, 0, 1))).toBe(1);
    expect(dayOfYear(new Date(2025, 11, 31))).toBe(366);
    expect(dayOfYear({ month: 2, day: 28 })).toBe(59);
  });

  it("answers every worked calendar case as the Python spec does (calendar_spec.cases)", () => {
    expect(CALENDAR_CASES.length).toBeGreaterThanOrEqual(7);
    for (const c of CALENDAR_CASES) {
      const [y, m, d] = c.date.split("-").map(Number) as [number, number, number];
      expect({ date: c.date, day: dayOfYear(new Date(y, m - 1, d)), weekday: weekdayOf(y, m, d) })
        .toEqual({ date: c.date, day: c.reading_day, weekday: c.weekday });
    }
    // the Zymoetz case: Fri 2036-02-29 reads Feb 28's day on a Friday, not Feb 28's Thursday
    expect([dayOfYear(new Date(2036, 1, 29)), weekdayOf(2036, 2, 29), weekdayOf(2036, 2, 28)])
      .toEqual([59, "Friday", "Thursday"]);
  });

  it("reads the day on the LOCAL calendar — Dec 31 at night is still Dec 31", () => {
    // A time whose UTC date differs from its local one, in whatever zone the test runs.
    const off = new Date(2025, 11, 31).getTimezoneOffset();
    const at = off > 0 ? new Date(2025, 11, 31, 23, 30) : new Date(2026, 0, 1, 0, 30);
    if (off === 0) return;                    // UTC: there is no such time
    const ix = decodeStatusIndex(BYTES, null);
    expect(dayOfYear(at)).toBe(off > 0 ? 366 : 1);
    // section 3 is closed Dec 1 - Mar 30; on Nov 30 / Apr 1 the other side reads differently
    const edge = off > 0 ? new Date(2025, 10, 30, 23, 30) : new Date(2025, 11, 1, 0, 30);
    expect(ix.codeOn(3, edge)).toBe(off > 0 ? "own" : "closed");
  });

  it("refuses a file built for another atlas, and a damaged one", () => {
    expect(() => decodeStatusIndex(BYTES, "0000000000000000")).toThrow(StatusIndexError);
    expect(() => decodeStatusIndex(BYTES, "0000000000000000")).toThrow(/refused/);
    const bad = BYTES.slice();
    bad[0] = 0;
    expect(() => decodeStatusIndex(bad, null)).toThrow(/not a status index/);
    const v3 = BYTES.slice();
    v3[4] = 3;
    expect(() => decodeStatusIndex(v3, null)).toThrow(/version 3/);
    expect(() => decodeStatusIndex(BYTES.slice(0, BYTES.length - 2), null)).toThrow(StatusIndexError);
    const long = new Uint8Array(BYTES.length + 1);
    long.set(BYTES);
    expect(() => decodeStatusIndex(long, null)).toThrow(/trailing/);
  });

  /*
   * MUTATION. The comparison above must be able to fail: flip one byte at a time through the
   * range tables and the section and water columns, and every flip either is refused by the
   * decoder or changes an answer the fixture pins. A flip nothing notices would be a part of
   * the file this test does not actually read.
   */
  it("notices every one-byte change to the file's tables", () => {
    const unnoticed: number[] = [];
    for (let i = 21; i < BYTES.length; i++) {
      const m = BYTES.slice();
      m[i] = m[i]! ^ 0x01;
      let ix: StatusIndex;
      try { ix = decodeStatusIndex(m, null); } catch { continue; }
      if (!disagreements(ix).length) unnoticed.push(i);
    }
    expect(unnoticed).toEqual([]);
  });
});

describe("one status vocabulary over the index", () => {
  const ix = decodeStatusIndex(BYTES, fixture.handles);
  it("maps the file's codes to the three statuses; tidal and outside are none", () => {
    expect(statusOfCode("closed")).toBe("closed");
    expect(statusOfCode("own")).toBe("own");
    expect(statusOfCode("base")).toBe("base");
    expect(statusOfCode("tidal")).toBeNull();
    expect(statusOfCode("outside")).toBeNull();
    expect(statusOn(ix, 1_958_036, new Date(2025, 5, 1))).toBeNull();     // outside B.C.
  });
  it("answers sections and waters through the same index, and nothing without one", () => {
    expect(statusOn(ix, 5, new Date(2025, 5, 1))).toBe("own");
    expect(statusOn(ix, 6, new Date(2025, 5, 1))).toBe("base");
    expect(waterStatusOn(ix, "gnis:1", new Date(2025, 0, 15))).toBe("closed");
    expect(waterStatusOn(ix, "gnis:1", new Date(2025, 5, 15))).toBe("own");
    expect(waterStatusOn(ix, "gnis:2", new Date(2025, 5, 15))).toBe("base");
    expect(statusOn(null, 5, new Date())).toBeNull();
    expect(waterStatusOn(undefined, "gnis:1", new Date())).toBeNull();
  });
});

/*
 * THE MAP'S TWO HALVES OF THE ANSWER (colour-latency AP): a section's profile label, set once, and the day's label
 * sets, carried in the style. Together they must say exactly what `statusOn` says for every section and day the
 * fixture pins — that is what keeps the map line and the search row one answer (AGENTS 23) now that the map no longer
 * asks `statusOn` per section.
 */
describe("profile labels and the day's label sets", () => {
  const ix = decodeStatusIndex(BYTES, fixture.handles);
  /** The status the label sets give a section: the one set holding its label, `none` read as null. */
  const viaProfiles = (s: number, on: Date): string | null => {
    const sets = dayProfiles(ix, on);
    const label = profileLabel(ix, s);
    const hits = (Object.keys(sets) as (keyof typeof sets)[]).filter((k) => sets[k].includes(label));
    expect(hits.length, `${s} ${label} sits in ${hits.length} sets`).toBe(1);
    return hits[0] === "none" ? null : hits[0]!;
  };

  it("give every fixture section, on every fixture day, exactly the status statusOn gives", () => {
    const wrong = fixture.sections
      .map(([s, d]) => [s, d, statusOn(ix, s as number, day(d as string)),
                        viaProfiles(s as number, day(d as string))])
      .filter((r) => r[2] !== r[3]);
    expect(wrong).toEqual([]);
    // away from the fixture's rows too: a section the file never mentions is base, through the unlisted label
    expect(viaProfiles(1_000_000, new Date(2025, 5, 1))).toBe("base");
  });

  it("labels each listed section by its profile, and an unlisted one as base all year", () => {
    expect([0, 2, 3, 4, 5, 6, 899, 900, 901, 1_958_036, 1_958_037, -1].map((s) => profileLabel(ix, s)))
      // 900 (closed Feb 28-29) is p0; 3 and 4 share p1 (closed Dec 1 - Mar 30); 5 (own all year) p2; outside B.C. p3
      .toEqual(["b", "b", "p1", "p1", "p2", "b", "b", "p0", "b", "p3", "b", "b"]);
    expect(profileLabel(ix, "3")).toBe("p1");          // the handle as the map reports it
    expect(profileLabel(ix, "x")).toBe(UNLISTED_PROFILE);
    expect(ix.profiles).toBe(4);
  });

  it("splits the profiles by day, whole — a season over New Year, Feb 28's closure, outside B.C.", () => {
    expect(dayProfiles(ix, new Date(2026, 0, 1)))
      .toEqual({ closed: ["p1"], own: ["p2"], base: ["b", "p0"], no_access: ["n"], none: ["p3"] });
    expect(dayProfiles(ix, new Date(2025, 6, 15)))
      .toEqual({ closed: [], own: ["p1", "p2"], base: ["b", "p0"], no_access: ["n"], none: ["p3"] });
    expect(dayProfiles(ix, new Date(2024, 1, 29)))
      .toEqual({ closed: ["p0", "p1"], own: ["p2"], base: ["b"], no_access: ["n"], none: ["p3"] });
  });

  it("(T54) a section MOSTLY inside no-access land wears the no-access label every day; the regulation answer does not move", () => {
    const access = accessOf([{ tier: "no_access", name: "Coquitlam Watershed" }, { tier: "private", name: "" }],
                            [3, 5, 6, 7], [0, 0, 1, 0], [1000, 500, 1000, 501]);
    const withAccess: StatusIndex = Object.assign(Object.create(Object.getPrototypeOf(ix)) as StatusIndex, ix, { access });
    // 3 (closed Dec-Mar, wholly inside): the fourth colour; 5 (own, exactly half inside): its own; 6 (private): base
    expect([3, 5, 6, 7, 4].map((s) => profileLabel(withAccess, s))).toEqual(["n", "p2", "b", "n", "p1"]);
    const sets = dayProfiles(withAccess, new Date(2026, 0, 1));
    expect(sets.no_access).toEqual([NO_ACCESS_PROFILE]);
    // what the pushed-data map draws is the same answer; the regulation answer (`statusOn`) is untouched
    expect([3, 5, 6, 7].map((s) => mapStatusOn(withAccess, s, new Date(2026, 0, 1))))
      .toEqual(["no_access", "own", "base", "no_access"]);
    expect([3, 7].map((s) => statusOn(withAccess, s, new Date(2026, 0, 1)))).toEqual(["closed", "base"]);
    // no access table: nothing is no access
    expect(mapStatusOn(ix, 3, new Date(2026, 0, 1))).toBe("closed");
    expect(mapStatusOn(null, 3, new Date())).toBeNull();
  });

  it("refuses a profile the file does not have rather than reading past its tables", () => {
    expect(() => ix.profileCodeOn(4, new Date()))
      .toThrow(new StatusIndexError("status index: no profile 4 (the file has 4)"));
    expect(() => ix.profileCodeOn(-1, new Date())).toThrow(StatusIndexError);
  });
});
