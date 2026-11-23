/**
 * T622 / T605 item 1 -- THE GAUGE PAGE'S SCRIPT CARRIES CORE'S FUNCTIONS, written from their own source (gauges.ts
 * `clientScript` / `CORE_INLINE`). The page ships as one plain script that cannot import, so a parity test runs the SHIPPED
 * text and holds what it says to core's, over the dates and ages where a copy would drift (a DST day, the permanent-time
 * change, midnight, a minute either side of the age lines' edges).
 */
import { runInNewContext } from "node:vm";
import { describe, expect, it } from "vitest";
import { ageWords, bcDayWords, bcIsoDayWords, bcStampWords, BC_TIME_LABEL, figure, freshness, readingAgeWords, STALE_AFTER_MS } from "@app/core";
import { CORE_INLINE, clientScript } from "./gauges";

/** the script with its self-start replaced by an export of what it carries (the rest is the shipped text, unchanged) */
function carried(): Record<string, (...a: never[]) => unknown> & { BC_TIME_LABEL: string } {   // eslint-disable-line
  const src = clientScript();
  const start = /^gaugePage\(document,Date\.now,window\.fetch\.bind\(window\)\)\.catch\([^\n]*\n/m;
  expect(src, "the script calls gaugePage once, at the end").toMatch(start);
  const names = ["figure", "bcDayWords", "bcIsoDayWords", "bcStampWords", "BC_TIME_LABEL", "freshness", "readingAgeWords", "ageWords"];
  const ctx: Record<string, unknown> = { document: {}, window: { fetch: () => { throw new Error("no network in this test"); } }, console };
  runInNewContext(src.replace(start, `globalThis.__carried={${names.join(",")}};\n`), ctx);
  return ctx.__carried as never;
}

const T = (iso: string) => Date.parse(iso);
const INSTANTS = [
  T("2026-01-15T20:00:00Z"),            // PST, before the permanent change
  T("2026-03-08T09:59:00Z"),            // a minute before 02:00 PST on the spring-forward day
  T("2026-03-08T10:00:00Z"),            // the instant B.C. went to UTC-7 for good
  T("2026-07-01T06:59:00Z"),            // B.C. 23:59 on 30 June
  T("2026-07-01T07:00:00Z"),            // B.C. midnight
  T("2026-11-01T08:30:00Z"), T("2026-11-01T09:30:00Z"),   // the old fall-back hour: no change now
  T("2026-12-31T06:59:00Z"), T("2026-12-31T07:00:00Z"),   // New Year's Eve into the day
  T("2027-02-28T19:05:00Z"),
];

describe("the gauge page's script is core's, written in (T622)", () => {
  const c = carried() as Record<string, any> & { BC_TIME_LABEL: string };   // eslint-disable-line @typescript-eslint/no-explicit-any
  it("has no import, and carries every function it was asked to", () => {
    const src = clientScript();
    expect(src).not.toMatch(/^\s*import\b/m);
    for (const [, names] of CORE_INLINE) for (const n of names) expect(src, n).toMatch(new RegExp(`(?:function|const) ${n}\\b`));
    expect(src).toContain(`const STALE_AFTER_MS=${STALE_AFTER_MS};`);
  });
  it("says a figure, a B.C. day, a B.C. stamp and its label exactly as core does", () => {
    for (const v of [0, 0.004, 0.123, 4.567, 22.24, 99.95, 100.4, 1060.4, -3.2]) expect(c.figure(v), String(v)).toBe(figure(v));
    for (const t of INSTANTS) {
      expect(c.bcDayWords(t), String(t)).toBe(bcDayWords(t));
      expect(c.bcStampWords(t), String(t)).toBe(bcStampWords(t));
    }
    for (const d of ["2026-03-08", "2026-11-01", "2026-12-31", "2027-02-28"]) expect(c.bcIsoDayWords(d), d).toBe(bcIsoDayWords(d));
    expect(c.BC_TIME_LABEL).toBe(BC_TIME_LABEL);
  });
  it("words an age and the reading's line exactly as core does: seconds to days, every edge", () => {
    const NOW = T("2026-11-09T18:00:00Z");
    for (const ms of [0, 30_000, 59_999, 60_000, 3_599_000, 3_600_000, 5_400_000, 47 * 3_600_000, 48 * 3_600_000, 72 * 3_600_000, NaN])
      expect(c.ageWords(ms), String(ms)).toBe(ageWords(ms));
    for (const fetched of [NOW - 5_000, NOW - 600_000, NOW - 2 * 3_600_000, NOW - 2 * 3_600_000 - 1, NOW - 40 * 3_600_000, null]) {
      const mine = c.freshness(fetched, NOW, 2 * 3_600_000);
      expect(mine).toEqual(freshness(fetched, NOW, 2 * 3_600_000));
      for (const at of ["2026-11-09 at 10:00 PT", null])
        expect(c.readingAgeWords(mine, at)).toBe(readingAgeWords(freshness(fetched, NOW, 2 * 3_600_000), at));
    }
  });
});
