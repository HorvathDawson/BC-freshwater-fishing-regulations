// T556: THE PYTHON ↔ WORKER PIN for the DFO rule that exists twice. `fixtures/dfo/python-vectors.generated.json` is what the PYTHON
// reads on the saved DFO pages (generated: `python -m pipeline.tools.emit.dfo_vectors`; its `--check` runs in pytest,
// `python -m pipeline.tools.emit --check` and fast-fail): every location's fingerprint, every table locator's rows, typed
// rules and year calendar, the name key, and the shared hand rows of dfo-rows.json. The worker must reproduce all of it:
//   * in its Python-parity mode, everything;
//   * in ROBUST mode (what production runs), everything except the differences NAMED below — a difference not named
//     here fails, and a named one that no longer shows fails too (the list cannot go stale).
// Numbers compare by value (the worker carries a Python float as PyFloat; `dumps` spells it). A missing file fails.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { typePage } from "../src/dfo/live";
import { fingerprint, landmarks, normalize } from "../src/dfo/locations";
import { key } from "../src/dfo/names";
import { dumps } from "../src/dfo/pyjson";
import { Interner, locationCalendar, SPECIES } from "../src/dfo/salmon";
import { toRules, type TypedRule } from "../src/dfo/typed";
import { REG } from "./wording.fixture";

const FIX = join(__dirname, "fixtures/dfo");
interface Calendar { sp: number[][]; g: number[]; verdicts: { s: string; t: string }[]; gears: unknown[] }
interface PyLocator { fingerprint: string; rows: string[]; rules: Record<string, unknown>[]; words?: unknown; calendar: Calendar }
interface Vectors {
  format: string;
  species: string[];
  text: { section: string | null; water: string; specific_area: string; fingerprint: string; normalize: [string, string]; landmarks: string[] }[];
  names: [string, string][];
  rows: Record<string, { rules: Record<string, unknown>[]; calendar: Calendar }>;
  pages: Record<string, { structure: unknown; locations: Record<string, unknown>[]; locators: PyLocator[] }>;
}
const V: Vectors = JSON.parse(readFileSync(join(FIX, "python-vectors.generated.json"), "utf-8"));
const ROWS: { cases: { name: string; rows: [string, string, string][] }[] } = JSON.parse(readFileSync(join(FIX, "dfo-rows.json"), "utf-8"));
const page = (slug: string) => readFileSync(join(FIX, `pages/region${slug}-eng.html`), "utf-8");
/** the worker's value as the JSON it would write (PyFloat → number, undefined dropped) */
const plain = <T>(x: T): T => (x === undefined ? x : JSON.parse(dumps(x)));
const calendarOf = (rules: TypedRule[], robust: boolean): Calendar => {
  const verdicts = new Interner<unknown>(), gears = new Interner<unknown>();
  return plain({ ...locationCalendar(rules as never[], verdicts, gears, robust), verdicts: verdicts.items, gears: gears.items }) as Calendar;
};

/** ROBUST MODE'S NAMED DIFFERENCE (dates.ts; live.ts "ROBUST MODE"): a row dated "<day> until further notice". The Python
 *  reader refuses the cell and types the row with no dates; the worker keeps the start day, marks it open-ended and says
 *  so in the answer's words. The DAYS the answer holds are the same (all year). */
const OPEN_ENDED = / \| [A-Z][a-z]+ \d{1,2} until further notice \| /;
const openEnded = (l: PyLocator): boolean => l.rows.some((r) => OPEN_ENDED.test(r));

describe("the vectors were read", () => {
  it("the file is the generator's, and holds every saved page", () => {
    expect(V.format).toBe("dfo-parity-vectors/1");
    expect(Object.keys(V.pages).sort()).toEqual(["1", "2", "3", "4", "5a", "5b", "6", "7", "8"]);
    expect(Object.values(V.pages).reduce((n, p) => n + p.locators.length, 0)).toBeGreaterThan(200);
    expect(V.names.length).toBeGreaterThan(100);
    expect(V.species).toEqual([...SPECIES]);
    expect(Object.keys(V.rows).sort()).toEqual(ROWS.cases.map((c) => c.name).sort());
  });
});

describe("the location fingerprint and the name key: worker == Python", () => {
  it("fingerprint / normalize / landmarks on every text case (each synonym, the typography folded, what stays apart)", () => {
    expect(V.text.map((c) => ({ ...c, fingerprint: fingerprint(c.section, c.water, c.specific_area),
                                normalize: [normalize(c.water), normalize(c.specific_area)], landmarks: landmarks(`${c.water} ${c.specific_area}`) })))
      .toEqual(V.text);
    // the cases mean something: drift the fingerprint survives (abbreviations, case, typography) is ONE place …
    const fp = (water: string, area: string) => V.text.find((c) => c.water === water && c.specific_area === area)!.fingerprint;
    expect(fp("Fraser River", "Downstream of the Hwy. 1 bridge")).toBe(V.text[2]!.fingerprint);
    expect(fp("Chilliwack R.", V.text[5]!.specific_area)).toBe(fp("Chilliwack River", V.text[6]!.specific_area));
    expect(fp("Stamp R. & Somass R.", V.text[7]!.specific_area)).toBe(fp("Stamp River and Somass River", V.text[8]!.specific_area));
    // … and a real change is another: the highway's number, the section, a count of signs
    expect(new Set([V.text[1]!, V.text[3]!, V.text[4]!].map((c) => c.fingerprint)).size).toBe(3);
    expect(fp("Kitsumkalum River", "Three signs")).not.toBe(fp("Kitsumkalum River", "4 signs"));
    expect(new Set(V.text.map((c) => c.fingerprint)).size).toBe(V.text.length - 3);
  });
  it("names.key on every case and every water the saved pages name", () => {
    expect(V.names.map(([n]) => [n, key(n)])).toEqual(V.names);
  });
});

describe.each(Object.keys(V.pages))("region %s: the saved page", (slug) => {
  const want = V.pages[slug]!;
  for (const robust of [false, true])
    it(`${robust ? "robust (production)" : "Python-parity"} mode: locations, fingerprints, rows, typed rules, calendars`, () => {
      const [typed, locs, structure] = typePage(page(slug), slug, robust, REG);
      expect(plain(structure)).toEqual(want.structure);
      expect(locs.map((l) => ({ fingerprint: l.fingerprint, section: l.section, water: l.water, specific_area: l.specific_area, landmarks: l.landmarks })))
        .toEqual(want.locations);
      expect(typed.locators.map((l) => l.fingerprint)).toEqual(want.locators.map((l) => l.fingerprint));
      typed.locators.forEach((got, i) => {
        const py = want.locators[i]!;
        const at = `region ${slug} ${py.fingerprint} ${JSON.stringify(py.rows)}`;
        expect((got as { error?: string }).error, at).toBeUndefined();
        expect(got.rows, at).toEqual(py.rows);
        expect(plain((got as { words?: unknown }).words), at).toEqual(py.words);
        const rules = plain(got.rules) as unknown as Record<string, unknown>[];
        const cal = calendarOf(got.rules, robust);
        if (!robust || !openEnded(py)) {
          expect(rules, at).toEqual(py.rules);
          expect(cal, at).toEqual(py.calendar);
          return;
        }
        // the named difference, and nothing else: the same rules but for the open-ended dates; the same days, the
        // answer's words saying since when
        expect(rules.some((r) => (r.when as { open_ended?: boolean } | undefined)?.open_ended), at).toBe(true);
        expect(rules.map((r) => ((r.when as { open_ended?: boolean } | undefined)?.open_ended ? (({ when: _w, ...rest }) => rest)(r) : r)), at).toEqual(py.rules);
        expect({ sp: cal.sp, g: cal.g, gears: cal.gears, status: cal.verdicts.map((v) => v.s) }, at)
          .toEqual({ sp: py.calendar.sp, g: py.calendar.g, gears: py.calendar.gears, status: py.calendar.verdicts.map((v) => v.s) });
        expect(cal.verdicts.map((v) => v.t.replace(/ \(from [A-Z][a-z]+ \d{1,2} until further notice\)$/, "")), at).toEqual(py.calendar.verdicts.map((v) => v.t));
        expect(cal.verdicts.map((v) => v.t), at).not.toEqual(py.calendar.verdicts.map((v) => v.t));
      });
    });
});

describe("robust mode's named difference is real and counted", () => {
  it("six locators on the saved pages are dated 'until further notice' (regions 1, 2, 4, 8)", () => {
    const hit = Object.entries(V.pages).flatMap(([slug, p]) => p.locators.filter(openEnded).map((l) => `${slug}:${l.fingerprint.slice(0, 8)}`));
    expect(hit).toEqual(["1:7eab1584", "2:d367a8ea", "4:1d1c79cf", "8:afc9f631", "8:833f8748", "8:b70729bd"]);
  });
});

describe("the shared hand rows (dfo-rows.json): typed rules and calendar == Python", () => {
  for (const robust of [false, true])
    it.each(ROWS.cases.map((c) => [c.name, c] as const))(`${robust ? "robust" : "Python-parity"}: %s`, (name, c) => {
      const rules = c.rows.flatMap(([species, dates, limits_gear], i) => toRules({ species, dates, limits_gear }, "rows", i, robust, REG));
      expect(plain(rules)).toEqual(V.rows[name]!.rules);
      expect(calendarOf(rules, robust)).toEqual(V.rows[name]!.calendar);
    });
});
