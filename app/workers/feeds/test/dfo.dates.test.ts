// T225 follow-up (owner 2026-10-28): the robust DFO date reader (src/dfo/dates.ts) over every date cell DFO has
// printed (fixtures/dfo/date-cells.json + the cells of every committed page), the shapes the Python missed, a seeded
// fuzz of rewritings that must read the same, and garbage that must come back `unparsed` (never a throw, never a guess).
// Then what an unreadable cell becomes downstream: a `check` verdict on every day, never a dropped limit.
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { readDates, type DateCell } from "../src/dfo/dates";
import { extract } from "../src/dfo/locations";
import { parseRegion } from "../src/dfo/parse";
import { Interner, locationCalendar, NOT_LISTED } from "../src/dfo/salmon";
import { DATES_UNPARSED, LAST_DAY, parseDateRange, toRules, type DateRange } from "../src/dfo/typed";
import { untangle } from "../src/dfo/untangle";
import { REPO } from "./dfo.scenario";

const FIX = join(REPO, "app/workers/feeds/test/fixtures/dfo");
const corpus: string[] = JSON.parse(readFileSync(join(FIX, "date-cells.json"), "utf-8")).cells;
const pageCells = (() => {
  const out = new Set<string>();
  for (const d of ["pages", "history"])
    for (const f of readdirSync(join(FIX, d))) {
      const m = /region(\d[ab]?)[-_]/.exec(f);
      if (!m || !f.endsWith(".html")) continue;
      try {
        const [, rules] = extract(untangle(parseRegion(readFileSync(join(FIX, d, f), "utf-8").replace(/\r\n?/g, "\n"), m[1]!)));
        for (const r of rules) out.add(r.dates);
      } catch {
        /* a page that does not parse is the page-level hold's business */
      }
    }
  return [...out];
})();
const R = (fm: number, fd: number, tm: number, td: number): DateRange => ({ from_month: fm, from_day: fd, to_month: tm, to_day: td });
const ok = (ranges: DateRange[], openEnded = false): DateCell => ({ kind: "ok", ranges, openEnded });

describe("readDates: the recorded corpus", () => {
  const all = [...new Set([...corpus, ...pageCells])];
  it(`every one of ${all.length} printed cells reads (blank and 'To be determined' aside)`, () => {
    const bad = all.filter((c) => {
      const k = readDates(c).kind;
      return k === "unparsed" || (k === "empty" && c.trim() !== "") || (k === "tbd" && !/determined/i.test(c));
    });
    expect(bad).toEqual([]);
  });
  it("agrees with the Python reader on every cell the Python could read", () => {
    const differ: string[] = [];
    for (const c of all) {
      const py = parseDateRange(c);
      if (!py) continue;
      const got = readDates(c);
      if (got.kind !== "ok" || got.ranges.length !== 1 || JSON.stringify(got.ranges[0]) !== JSON.stringify(py)) differ.push(c);
    }
    expect(differ).toEqual([]);
  });
});

describe("readDates: the shapes the Python missed", () => {
  const cases: [string, DateCell][] = [
    ["Apr 1 until further notice", ok([R(4, 1, 12, 31)], true)],
    ["July 1 until futher notice", ok([R(7, 1, 12, 31)], true)],
    ["May 8 2018 until further notice", ok([R(5, 8, 12, 31)], true)],
    ["Until further notice", ok([R(1, 1, 12, 31)], true)],
    ["from Oct 1 until further notice", ok([R(10, 1, 12, 31)], true)],
    ["Effective October 15, until further notice", ok([R(10, 15, 12, 31)], true)],
    ["Aug 2 To Aug 31", ok([R(8, 2, 8, 31)])],
    ["July 1 until Aug 8", ok([R(7, 1, 8, 8)])],
    ["October 1 to December 31", ok([R(10, 1, 12, 31)])],
    ["October 1 to …", { kind: "unparsed", raw: "October 1 to …" }],
    ["1 Oct to 31 Dec", ok([R(10, 1, 12, 31)])],
    ["Oct 1st to Dec 31st", ok([R(10, 1, 12, 31)])],
    ["Sept. 15 through Oct 30", ok([R(9, 15, 10, 30)])],
    ["Oct 1 to Dec 31, 2025", ok([R(10, 1, 12, 31)])],
    ["June 15 to July 14 2017", ok([R(6, 15, 7, 14)])],
    ["Nov 01- to Dec 31", ok([R(11, 1, 12, 31)])],
    ["Sep 21-Dec 31", ok([R(9, 21, 12, 31)])],
    ["Jan 01–Dec 31", ok([R(1, 1, 12, 31)])],
    ["Oct 1 - 15", ok([R(10, 1, 10, 15)])],
    ["Nov 30 to Jan 15", ok([R(11, 30, 1, 15)])], // across the year end
    ["Sep 15 to Jan 31", ok([R(9, 15, 1, 31)])],
    ["Aprl 1 to Jun 15", ok([R(4, 1, 6, 15)])],
    ["Ocotber 1 to Dec 31", ok([R(10, 1, 12, 31)])],
    ["Setpember 1 to Spet 30", ok([R(9, 1, 9, 30)])],
    ["Jul 1 to Aug 31, Oct 1 to Dec 31", ok([R(7, 1, 8, 31), R(10, 1, 12, 31)])],
    ["Jul 1 to Aug 31 and Oct 1 until further notice", ok([R(7, 1, 8, 31), R(10, 1, 12, 31)], true)],
    ["August", ok([R(8, 1, 8, 31)])],
    ["Aug 15", ok([R(8, 15, 8, 15)])],
    ["Feb 29 to Mar 4", ok([R(2, 29, 3, 4)])],
    ["*Apr 1 to Mar 31*", ok([R(4, 1, 3, 31)])],
    ["To be determined", { kind: "tbd" }],
    ["TBD", { kind: "tbd" }],
    ["", { kind: "empty" }],
    ["   ", { kind: "empty" }],
    ["Jun 31 to Jul 4", { kind: "unparsed", raw: "Jun 31 to Jul 4" }], // no such day: never a guess
    ["Feb 30 to Mar 4", { kind: "unparsed", raw: "Feb 30 to Mar 4" }],
    ["see notice", { kind: "unparsed", raw: "see notice" }],
    ["Weekends in August", { kind: "unparsed", raw: "Weekends in August" }],
    ["Mar to", { kind: "unparsed", raw: "Mar to" }],
    ["Jum 1 to Jul 4", { kind: "unparsed", raw: "Jum 1 to Jul 4" }], // a 3-letter typo is ambiguous: not guessed
    ["13 to 15", { kind: "unparsed", raw: "13 to 15" }],
  ];
  for (const [cell, want] of cases) it(JSON.stringify(cell), () => expect(readDates(cell)).toEqual(want));
});

describe("readDates: seeded fuzz", () => {
  let seed = 20261028;
  const rnd = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;
  const pick = <T,>(xs: T[]) => xs[Math.floor(rnd() * xs.length)]!;
  const FULL = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
  const ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const ord = (d: number) => `${d}${d % 10 === 1 && d !== 11 ? "st" : d % 10 === 2 && d !== 12 ? "nd" : d % 10 === 3 && d !== 13 ? "rd" : "th"}`;
  const writePoint = (m: number, d: number) => {
    const name = pick([FULL[m - 1]!, ABBR[m - 1]!, ABBR[m - 1]! + ".", (m === 9 ? "Sept" : ABBR[m - 1]!), FULL[m - 1]!.toUpperCase(), ABBR[m - 1]!.toLowerCase()]);
    const day = pick([String(d), String(d).padStart(2, "0"), ord(d)]);
    return rnd() < 0.15 ? `${day} ${name}` : `${name}${pick([" ", "  ", " "])}${day}`;
  };

  it("2,000 rewritings of real ranges read to the same range", () => {
    const misses: string[] = [];
    for (let k = 0; k < 2000; k++) {
      const fm = 1 + Math.floor(rnd() * 12);
      const tm = 1 + Math.floor(rnd() * 12);
      const fd = 1 + Math.floor(rnd() * LAST_DAY[fm - 1]!);
      const td = 1 + Math.floor(rnd() * LAST_DAY[tm - 1]!);
      const sep = pick([" to ", " To ", " TO ", "-", " - ", "–", " – ", "—", " through ", " thru ", " until ", "- to ", " till "]);
      let cell = writePoint(fm, fd) + sep + writePoint(tm, td);
      if (rnd() < 0.2) cell += pick([" 2026", ", 2025", " 2019"]);
      if (rnd() < 0.1) cell = `*${cell}*`;
      if (rnd() < 0.1) cell = pick(["Effective ", "From ", "from "]) + cell;
      const got = readDates(cell);
      if (got.kind !== "ok" || JSON.stringify(got.ranges) !== JSON.stringify([R(fm, fd, tm, td)])) misses.push(`${cell} → ${JSON.stringify(got)}`);
    }
    expect(misses.slice(0, 10)).toEqual([]);
  });

  it("2,000 open-ended rewritings keep the start and say open", () => {
    const misses: string[] = [];
    for (let k = 0; k < 2000; k++) {
      const fm = 1 + Math.floor(rnd() * 12);
      const fd = 1 + Math.floor(rnd() * LAST_DAY[fm - 1]!);
      const cell = `${rnd() < 0.2 ? "from " : ""}${writePoint(fm, fd)}${rnd() < 0.2 ? " 2018" : ""} ${pick(["until further notice", "until futher notice", "Until Further Notice", "until further notification", "till further notice"])}`;
      const got = readDates(cell);
      if (got.kind !== "ok" || !got.openEnded || JSON.stringify(got.ranges) !== JSON.stringify([R(fm, fd, 12, 31)])) misses.push(`${cell} → ${JSON.stringify(got)}`);
    }
    expect(misses.slice(0, 10)).toEqual([]);
  });

  it("5,000 garbage cells: never a throw, and anything read is a real calendar range", () => {
    const ALPHA = ["Oct", "1", "to", "-", "Dec", "31", "until", "further", "notice", " ", ",", "and", "32", "0", "Feb", "30", "xyz", "…", "Sept.", "th", "2026", "&", ";", "*"];
    for (let k = 0; k < 5000; k++) {
      let cell = "";
      for (let n = Math.floor(rnd() * 9); n > 0; n--) cell += pick(ALPHA) + (rnd() < 0.6 ? " " : "");
      const got = readDates(cell);
      if (got.kind === "ok")
        for (const r of got.ranges) {
          expect(r.from_day).toBeGreaterThanOrEqual(1);
          expect(r.from_day).toBeLessThanOrEqual(LAST_DAY[r.from_month - 1]!);
          expect(r.to_day).toBeLessThanOrEqual(LAST_DAY[r.to_month - 1]!);
        }
    }
  });
});

describe("downstream: an unreadable cell is `check`, never a dropped limit", () => {
  const cal = (rows: { species: string; dates: string; limits_gear: string }[]) => {
    const rules = rows.flatMap((r, i) => toRules(r, "fp000000", i, true));
    const verdicts = new Interner<unknown>();
    verdicts.add(NOT_LISTED);
    const gears = new Interner<unknown>();
    const c = locationCalendar(rules as never, verdicts, gears, true);
    const on = (k: number, md: number) => {
      // md → day index (0-based, leap year)
      const m = Math.floor(md / 100);
      const d = md % 100;
      const day = LAST_DAY.slice(0, m - 1).reduce((a, b) => a + b, 0) + d - 1;
      const runs = c.sp[k]!;
      let v = -1;
      for (let i = 0; i < runs.length; i += 2) if (runs[i]! <= day) v = runs[i + 1]!;
      return v < 0 ? null : (verdicts.items[v] as { s: string; t: string });
    };
    return { rules, on, gears: gears.items };
  };

  it("the Python made an unreadable row an advisory with no limit; robust keeps the limit, flagged check", () => {
    const py = toRules({ species: "Coho", dates: "1 Oct to 31 Dec", limits_gear: "2 per day" }, "fp", 0);
    expect(py.map((r) => r.type)).toEqual(["advisory"]); // the limit was gone
    const ours = toRules({ species: "Coho", dates: "Oct 1 to Dec 31 (subject to review)", limits_gear: "2 per day" }, "fp", 0, true);
    expect(ours).toMatchObject([{ type: "retention_limit", take: 2, species: ["CO"], check: DATES_UNPARSED, when: { unparsed: ["Oct 1 to Dec 31 (subject to review)"] } }]);
  });

  it("an unreadable row makes its species `check` on every day, even where another row would answer", () => {
    const { on } = cal([
      { species: "Coho", dates: "Jan 1 to Dec 31", limits_gear: "No fishing for salmon" },
      { species: "Coho", dates: "Oct 1 to Dec 31 (subject to review)", limits_gear: "2 per day" },
      { species: "Chinook", dates: "Jan 1 to Dec 31", limits_gear: "No fishing for salmon" },
    ]);
    for (const md of [101, 615, 1015, 1231]) {
      expect(on(1, md)?.s).toBe("check"); // coho
      expect(on(1, md)?.t).toMatch(/^Dates unparsed — check DFO: 2 per day \(as printed: Oct 1 to Dec 31 \(subject to review\)\)/);
      expect(on(0, md)?.s).toBe("closed"); // chinook untouched
    }
  });

  it("'until further notice' runs on past the year end, but any dated row outranks it", () => {
    const { on } = cal([
      { species: "Coho", dates: "Jan 1 to Mar 31", limits_gear: "No fishing for salmon" },
      { species: "Coho", dates: "Apr 1 until further notice", limits_gear: "Non-retention" },
    ]);
    expect(on(1, 215)?.s).toBe("closed"); // the dated Jan–Mar row
    expect(on(1, 401)?.s).toBe("release");
    expect(on(1, 1231)?.s).toBe("release");
    expect(on(1, 401)?.t).toBe("Non-retention (from Apr 1 until further notice)");
    const alone = cal([{ species: "Coho", dates: "Oct 15 until further notice", limits_gear: "Non-retention" }]);
    expect(alone.on(1, 115)?.s).toBe("release"); // next January: still until further notice
  });

  it("a gear rule with an unreadable cell shows on every day, flagged", () => {
    const { gears } = cal([{ species: "Coho", dates: "whenever", limits_gear: "2 per day, single barbless hook" }]);
    expect(gears.flat().some((g) => String(g).startsWith(`${DATES_UNPARSED}: `))).toBe(true);
  });
});
