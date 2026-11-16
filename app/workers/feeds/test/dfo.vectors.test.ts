// T556: THE PYTHON ↔ WORKER PIN for the DFO rule that exists twice. `fixtures/dfo/python-vectors.generated.json` is what the PYTHON
// reads on the saved DFO pages (generated: `python -m pipeline.tools.emit.dfo_vectors`; its `--check` runs in pytest,
// `python -m pipeline.tools.emit --check` and fast-fail): every location's fingerprint, every table locator's rows, typed
// rules and year calendar, the name key, and the shared hand rows of dfo-rows.json. The worker must reproduce all of it:
//   * in both its Python-parity mode and ROBUST mode (what production runs), everything — (T676, owner 2026-11-12) the
//     one named difference ("<day> until further notice") is gone: the Python reads a date cell with the worker's
//     reader (`dates.read_dates`), so an open-ended row is the same rules and the same words on both sides.
// (T676) Also: the join to the curated entries (entries.ts byFingerprint / reconcile, on the fixture entry files and a
// page-default case), the region header (header.ts parseHeader / digest / resolve, quota.ts headerAnswers /
// headerClosure), and ONE reading of a date cell (dates.ts readDates == dates.read_dates on every cell, no exception).
// Numbers compare by value (the worker carries a Python float as PyFloat; `dumps` spells it). A missing file fails.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { statementLocators } from "../src/dfo/cascade";
import { livePieceRules } from "../src/dfo/closures";
import { readDates } from "../src/dfo/dates";
import { byFingerprint, loadEntryFile, reconcile } from "../src/dfo/entries";
import { digest, parseHeader, resolve } from "../src/dfo/header";
import { build } from "../src/dfo/feed";
import { ROW_HELD_VERDICT, typePage } from "../src/dfo/live";
import { fingerprint, landmarks, normalize } from "../src/dfo/locations";
import { key } from "../src/dfo/names";
import { dumps } from "../src/dfo/pyjson";
import { headerAnswers, headerClosure } from "../src/dfo/quota";
import { Interner, locationCalendar, SPECIES } from "../src/dfo/salmon";
import { toRules, type TypedRule } from "../src/dfo/typed";
import { REG } from "./wording.fixture";
import { runDfo } from "../src/dfo";
import { MemStore, privStore } from "./dfo.scenario";

const FIX = join(__dirname, "fixtures/dfo");
interface Calendar { sp: number[][]; g: number[]; verdicts: { s: string; t: string }[]; gears: unknown[] }
interface PyLocator { fingerprint: string; rows: string[]; rules: Record<string, unknown>[]; words?: unknown; calendar: Calendar }
interface Vectors {
  format: string;
  species: string[];
  text: { section: string | null; water: string; specific_area: string; fingerprint: string; normalize: [string, string]; landmarks: string[] }[];
  names: [string, string][];
  rows: Record<string, { rules: Record<string, unknown>[]; calendar: Calendar; error?: string }>;
  pages: Record<string, { structure: unknown; locations: Record<string, unknown>[]; locators: PyLocator[]; entries: Join; header: Header;
                          /** T757: piece id -> its rules and calendar (closures.live_piece_rules) */
                          pieces: Record<string, { rules: Record<string, unknown>[]; calendar: Calendar }> }>;
  joins: Record<string, Join & { slug: string; entries: unknown }>;
  dates: [string, unknown][];
  /** (T757, T758) the Python live.run over the saved pages (and edited ones, `cases`) */
  live: unknown;
}
/** T676: the curated entry file joined to a page (entries.by_fingerprint → location_id, entries.reconcile) */
interface Join { by_fingerprint: Record<string, string>; reconcile: unknown }
/** T676: the region header (header.parse_header / digest / resolve, deliver.salmon.header_answers, quota.header_closure) */
interface Header { notes: unknown[]; digest: string; scopes: Record<string, string[]>; unresolved: unknown[]; answers: unknown; closure: unknown }
const V: Vectors = JSON.parse(readFileSync(join(FIX, "python-vectors.generated.json"), "utf-8"));
const ROWS: { cases: { name: string; rows: [string, string, string][]; robust?: unknown }[] } = JSON.parse(readFileSync(join(FIX, "dfo-rows.json"), "utf-8"));
const page = (slug: string) => readFileSync(join(FIX, `pages/region${slug}-eng.html`), "utf-8");
const entryText = (slug: string): string | null => {
  try {
    return readFileSync(join(FIX, `entries/region-${slug}.json`), "utf-8");
  } catch {
    return null; // no curated file: the empty entry file, as entries.load makes (the Python side reads the same absence)
  }
};
const joinOf = (ef: ReturnType<typeof loadEntryFile>, locs: Parameters<typeof reconcile>[1], structure: Parameters<typeof reconcile>[2]) => ({
  by_fingerprint: Object.fromEntries([...byFingerprint(ef)].map(([fp, l]) => [fp, l.location_id]).sort(([a], [b]) => (a! < b! ? -1 : a! > b! ? 1 : 0))),
  reconcile: plain(reconcile(ef, locs, structure)),
});
/** the worker's value as the JSON it would write (PyFloat → number, undefined dropped) */
const plain = <T>(x: T): T => (x === undefined ? x : JSON.parse(dumps(x)));
const calendarOf = (rules: TypedRule[]): Calendar => {
  const verdicts = new Interner<unknown>(), gears = new Interner<unknown>();
  return plain({ ...locationCalendar(rules as never[], verdicts, gears), verdicts: verdicts.items, gears: gears.items }) as Calendar;
};

/** (T676) a row dated "<day> until further notice" — once robust mode's named difference, now read the same both sides */
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
        expect(plain(got.rules), at).toEqual(py.rules);
        expect(calendarOf(got.rules), at).toEqual(py.calendar);
      });
    });
  // T676: the join to the curated entries and the region header — one reading, whatever the date mode
  it("the curated entries joined to the page (by_fingerprint, reconcile) and the region header (notes, digest, names, answers, closure)", () => {
    const html = page(slug);
    const [, locs, structure] = typePage(html, slug, true, REG);
    const ef = loadEntryFile(slug, entryText(slug));
    expect(joinOf(ef, locs, structure)).toEqual(want.entries);
    const notes = parseHeader(html, slug);
    expect(plain(notes)).toEqual(want.header.notes);
    expect(digest(notes)).toBe(want.header.digest);
    const [scopes, unresolved] = resolve(notes, ef);
    expect({ scopes, unresolved }).toEqual({ scopes: want.header.scopes, unresolved: want.header.unresolved });
    expect(plain(headerAnswers(notes))).toEqual(want.header.answers);
    expect(plain(headerClosure(notes, slug))).toEqual(want.header.closure);
  });
  // T757: the closed-area pieces the page's rows name (closures.live_piece_rules), each its rules and its year
  it("the closed-area pieces its rows name: ids, rules, calendars", () => {
    const [, locs] = typePage(page(slug), slug, true, REG);
    const got = livePieceRules(loadEntryFile(slug, entryText(slug)), locs);
    expect(Object.fromEntries([...got].map(([lid, rs]) => [lid, { rules: plain(rs), calendar: calendarOf(rs as TypedRule[]) }]))).toEqual(want.pieces);
  });
});

describe("T757: the pieces the saved pages carry are not empty", () => {
  it("17 in all: Thompson 3 (region 3), Babine 12 mouths + the Gullwing line (region 6), the middle Shuswap 1 (region 8)", () => {
    expect(Object.fromEntries(Object.entries(V.pages).map(([s, p]) => [s, Object.keys(p.pieces).length]).filter(([, n]) => n)))
      .toEqual({ "3": 3, "6": 13, "8": 1 });
  });
});

describe("T676: the join's hand cases and the pages' coverage", () => {
  it("a curated page default (the page's sentence, never a table row) is never called dormant", () => {
    const c = V.joins.page_default!;
    const [, locs, structure] = typePage(page(c.slug), c.slug, true, REG);
    const ef = loadEntryFile(c.slug, JSON.stringify(c.entries));
    expect(joinOf(ef, locs, structure)).toEqual({ by_fingerprint: c.by_fingerprint, reconcile: c.reconcile });
    const outcomes = (c.reconcile as { outcomes: { location_id: string | null; status: string }[] }).outcomes;
    expect(outcomes.filter((o) => o.location_id === "8:page-default")).toEqual([]);
  });
  it("the join and header rows are not empty: places bound, header notes read, header names bound to items", () => {
    const statuses = new Set(Object.values(V.pages).flatMap((p) => (p.entries.reconcile as { outcomes: { status: string }[] }).outcomes.map((o) => o.status)));
    expect(statuses.has("ok")).toBe(true);
    expect(Object.values(V.pages).every((p) => Object.keys(p.entries.by_fingerprint).length > 0)).toBe(true);
    expect(Object.values(V.pages).some((p) => p.header.notes.length > 0)).toBe(true);
    expect(Object.values(V.pages).some((p) => Object.keys(p.header.scopes).length > 0)).toBe(true);
  });
});

describe("T676: ONE reading of a DFO date cell — the release's reader (dates.read_dates) == the worker's (readDates)", () => {
  it(`every recorded cell, every named shape, and a seeded fuzz (${V.dates.length} cells)`, () => {
    expect(V.dates.length).toBeGreaterThan(1500);
    const differ = V.dates.filter(([cell, want]) => !eq(readDates(cell), want));
    expect(differ.map(([cell, want]) => ({ cell, python: want, worker: readDates(cell) }))).toEqual([]);
  });
  it("the cases mean something: some read, some open-ended, some lists, some refused", () => {
    const kinds = V.dates.map(([, w]) => w as { kind: string; openEnded?: boolean; ranges?: unknown[] });
    expect(kinds.filter((w) => w.kind === "ok").length).toBeGreaterThan(500);
    expect(kinds.filter((w) => w.kind === "unparsed").length).toBeGreaterThan(300);
    expect(kinds.some((w) => w.openEnded)).toBe(true);
    expect(kinds.some((w) => (w.ranges?.length ?? 0) > 1)).toBe(true);
    expect(kinds.some((w) => w.kind === "tbd") && kinds.some((w) => w.kind === "empty")).toBe(true);
  });
});
const eq = (a: unknown, b: unknown): boolean => {
  try {
    expect(a).toEqual(b);
    return true;
  } catch {
    return false;
  }
};

describe("(T676) the open-ended rows are in the vectors, read the same on both sides", () => {
  it("six locators on the saved pages are dated 'until further notice' (regions 1, 2, 4, 8), each typed open-ended", () => {
    const hit = Object.entries(V.pages).flatMap(([slug, p]) => p.locators.filter(openEnded).map((l) => `${slug}:${l.fingerprint.slice(0, 8)}`));
    expect(hit).toEqual(["1:7eab1584", "2:d367a8ea", "4:1d1c79cf", "8:afc9f631", "8:833f8748", "8:b70729bd"]);
    const typed = Object.values(V.pages).flatMap((p) => p.locators.filter(openEnded));
    expect(typed.every((l) => l.rules.some((r) => (r.when as { open_ended?: boolean } | undefined)?.open_ended))).toBe(true);
    expect(typed.every((l) => l.calendar.verdicts.some((v) => / until further notice\)/.test(v.t)))).toBe(true);
  });
});

describe("the shared hand rows (dfo-rows.json): typed rules and calendar == Python", () => {
  it.each(ROWS.cases.map((c) => [c.name, c] as const))("%s", (name, c) => {
      if (c.robust) {
        // (T679 D6) rows that do not type: both readers refuse them with the same error type; robust, neither stops the
        // page (the worker's locator carries `error` and no rules — live.ts holds the place; the Python's reads check)
        const recs = c.rows.map(([species, dates, limits_gear], order) =>
          ({ fingerprint: "rows".repeat(4), species, dates, limits_gear, fishery_notices: [], source: "", order }));
        const [loc] = build({ region: "2", date_modified: null, preamble: [], rules: recs }, true, REG).locators;
        expect([loc!.rules, loc!.error?.split(":")[0]]).toEqual([[], V.rows[name]!.error]);
        expect([V.rows[name]!.calendar.verdicts.map((v) => v.s), ROW_HELD_VERDICT.s]).toEqual([["check"], "check"]);
        return;
      }
      const rules = c.rows.flatMap(([species, dates, limits_gear], i) => toRules({ species, dates, limits_gear }, "rows", i, REG));
      expect(plain(rules)).toEqual(V.rows[name]!.rules);
      expect(calendarOf(rules)).toEqual(V.rows[name]!.calendar);
    });
});

// (T757, T758) THE LIVE FEED, WHOLE: the worker's runDfo over the saved pages and the fixture entry files == the Python's
// `live.run` over the same (vectors `live`): every location — places, the page-default places (T758: the page's own
// sentence, cascade.ts), the closed-area pieces — in its region, each species' year and the gear lines compared by the
// verdict / gear itself (each feed numbers its tables its own way), and the rows shown in DFO's words. No exception list.
describe("the live feed: the worker's runDfo == the Python's live.run", () => {
  type Feed = { locs: Record<string, { r: string; sp: number[][]; g: number[]; x?: unknown }>; verdicts: unknown[]; gears: unknown[]; unplaced: number };
  const named = (f: Feed) => Object.fromEntries(Object.entries(f.locs).map(([lid, c]) => [lid, {
    r: c.r, sp: c.sp.map((runs) => runs.map((x, k) => (k % 2 ? (x >= 0 ? f.verdicts[x] : null) : x))),
    g: c.g.map((x, k) => (k % 2 ? (x >= 0 ? f.gears[x] : null) : x)), ...(c.x ? { x: c.x } : {}) }]));
  const run = async (pages: Record<string, string>): Promise<Feed> => {
    const feed = new MemStore();
    await runDfo({ fetch: async () => new Response(""), feed, priv: privStore(), now: "2026-10-28T12:00:00Z", regions: Object.keys(pages),
                   entries: async (s) => readFileSync(join(FIX, `entries/region-${s}.json`), "utf-8"), fetcher: async (s) => [pages[s]!, null] });
    return JSON.parse(feed.m.get("index.json")!);
  };
  const L = V.live as Feed & { cases: Record<string, Feed & { slug: string; edit: [string, string] }> };
  const all = () => Object.fromEntries(Object.keys(V.pages).map((s) => [s, page(s)]));
  const DEFAULTS = ["1:default:region", "2:default:areas-13-15-16", "2:default:areas-28-29", "3:default:region", "5a:default:region", "7:default:region", "8:default:region"];

  it("the saved pages: 274 places, every one answered as the Python's", async () => {
    const got = await run(all());
    expect(Object.keys(L.locs).length).toBe(274);
    expect(named(got)).toEqual(named(L));
    expect([got.unplaced, L.unplaced]).toEqual([0, 0]);
  });
  it("the 7 page defaults are there, each closed every day in its page's own sentence; Region 2's two each its own", async () => {
    const got = named(await run(all()));
    expect(Object.keys(got).filter((l) => l.includes(":default:")).sort()).toEqual(DEFAULTS);
    const words = (lid: string) => [...new Set((got[lid]!.sp.flatMap((runs) => runs.filter((_, k) => k % 2)) as { s: string; t: string }[]).map((v) => `${v.s}: ${v.t}`))];
    expect(words("2:default:areas-28-29")).toEqual(["closed: There is no fishing for salmon in waters of Region 2 flowing into Areas 28 and 29 except for the opportunities listed in the table below."]);
    expect(words("2:default:areas-13-15-16")).toEqual(["closed: Unless otherwise stated in the table below, the daily limit for waters in Region 2 flowing into Areas 13, 15, and 16 is zero (0)."]);
    expect(words("3:default:region")).toEqual(["closed: There is no fishing for salmon in Region 3 except for the opportunities listed in the table below."]);
    expect(words("8:default:region")).toEqual(["closed: There is no fishing for salmon in Region 8 except for the opportunities listed in the table below."]);
  });
  it.each(Object.keys(L.cases))("an edited page (%s) reads as the Python's", async (name) => {
    const c = L.cases[name]!;
    const html = page(c.slug);
    expect(html.split(c.edit[0]).length - 1).toBe(1);
    const got = await run({ [c.slug]: html.replace(c.edit[0], c.edit[1]) });
    expect(named(got)).toEqual(named(c));
    expect([got.unplaced, c.unplaced]).toEqual([0, 0]);
  });
  it("sentence_without_a_place: the page's sentence for a scope no place carries is typed, then skipped — never unplaced", () => {
    const c = L.cases.sentence_without_a_place!;
    const [typed] = typePage(page(c.slug).replace(c.edit[0], c.edit[1]), c.slug, true, REG);
    expect(typed.locators.filter((l) => l.fingerprint.startsWith("page:")).map((l) => l.fingerprint)).toEqual(["page:3:region", "page:3:areas-30-31"]);
    expect(Object.keys(c.locs).sort()).toEqual(Object.keys(L.locs).filter((l) => L.locs[l]!.r === "3").sort());
    expect(c.unplaced).toBe(0);
  });
  it("statementLocators on hand texts (the Python's answers: `cascade.statement_locators`, values pasted from .venv/bin/python)", () => {
    const r3 = "There is no fishing for salmon in Region 3 except for the opportunities listed in the table below.";
    const rule = (fp: string, t: string) => ({ fingerprint: fp, rows: [t], rules: [{ rule_id: fp, type: "retention_limit", verbatim: t, species: ["SA"], take: 0, may_target: false }] });
    // whitespace folded; a repeat dropped; a paragraph that is no default sentence ignored
    const messy = "  There  is no fishing\nfor salmon in Region 3 except for the opportunities listed in the table below. ";
    expect(statementLocators("3", [messy])).toEqual([rule("page:3:region", r3)]);
    expect(statementLocators("3", [messy, r3, "Salmon are fun."])).toEqual([rule("page:3:region", r3)]);
    const r2 = "Unless otherwise stated in the table below, the daily limit for waters in Region 2 flowing into Area 14 and 17 is zero (0).";
    expect(statementLocators("2", [r2])).toEqual([rule("page:2:areas-14-17", r2)]);
    expect(statementLocators("3", [])).toEqual([]);
  });
  it("no_default_sentence: the region 3 page without its sentence publishes no default place, the rest of the region as before", () => {
    const c = L.cases.no_default_sentence!;
    expect(Object.keys(c.locs).filter((l) => l.includes(":default:"))).toEqual([]);
    expect(Object.keys(c.locs).sort()).toEqual(Object.keys(L.locs).filter((l) => L.locs[l]!.r === "3" && l !== "3:default:region").sort());
  });
});
