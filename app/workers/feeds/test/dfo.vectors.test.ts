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
import { carry, QUEUE_ACTION, ROW_HELD_VERDICT, typePage } from "../src/dfo/live";
import { fingerprint, landmarks, normalize } from "../src/dfo/locations";
import { key } from "../src/dfo/names";
import { dumps } from "../src/dfo/pyjson";
import { headerAnswers, headerClosure } from "../src/dfo/quota";
import { Interner, locationCalendar, SPECIES } from "../src/dfo/salmon";
import { typedRules, type TypedRule } from "../src/dfo/typed";
import { REG } from "./wording.fixture";
import { runDfo } from "../src/dfo";
import { MemStore, privStore } from "./dfo.scenario";

const FIX = join(__dirname, "fixtures/dfo");
interface Calendar { sp: number[][]; g: number[]; o?: number[]; verdicts: { s: string; t: string }[]; gears: unknown[] }
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
// A saved page as the PYTHON reads it (the emitter's `Path.read_text`: universal newlines, CRLF read as LF). Git holds the
// pages LF ("text" in .gitattributes), but a working tree may hold them CRLF (the main checkout's are, i/lf w/crlf, since
// 2026-10-28; `git status` calls them clean): read raw there, the cases whose edit spans a line break (gone, new_water,
// unmatched_new, unmatched_undrawn) found their anchor 0 times, not once, though both sides read the same page.
const page = (slug: string) => readFileSync(join(FIX, `pages/region${slug}-eng.html`), "utf-8").replace(/\r\n?/g, "\n");
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
    // (T787) region 4's sits in the table DFO commented out: typed (provenance), every rule hidden, it answers nothing
    const hidden = (l: PyLocator) => l.rules.every((r) => r.hidden === true);
    expect(Object.entries(V.pages).flatMap(([slug, p]) => p.locators.filter((l) => openEnded(l) && hidden(l)).map((l) => `${slug}:${l.fingerprint.slice(0, 8)}`)))
      .toEqual(["4:1d1c79cf"]);
    expect(typed.filter(hidden).every((l) => l.calendar.verdicts.length === 0)).toBe(true);
    expect(typed.filter((l) => !hidden(l)).every((l) => l.calendar.verdicts.some((v) => / until further notice\)/.test(v.t)))).toBe(true);
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
      const rules = c.rows.flatMap(([species, dates, limits_gear], i) => typedRules({ species, dates, limits_gear }, "rows", i, REG));
      expect(plain(rules)).toEqual(V.rows[name]!.rules);
      expect(calendarOf(rules)).toEqual(V.rows[name]!.calendar);
    });
});

// (T757, T758, T759) THE LIVE FEED, WHOLE: the worker's runDfo (production mode) over the saved pages and the fixture
// entry files == the Python's `live.run` over the same (vectors `live`), and on every edited case (`live.cases`, run as
// the feed runs: the saved page first, then the edited one): every location in its region — places, page defaults,
// closed-area pieces — each species' year and the gear lines compared by the verdict / gear itself (each feed numbers its
// tables its own way), the rows in DFO's words; the issues (T759), the feed shape `dfo`, each region's `gone`, and the
// notices of the kinds the Python's case pins (key and text, word for word). No exception list.
describe("the live feed: the worker's runDfo == the Python's live.run", () => {
  type Feed = { locs: Record<string, { r: string; sp: number[][]; g: number[]; o?: number[]; x?: unknown; pr?: string[] }>; verdicts: unknown[]; gears: unknown[]; unplaced: number;
                issues: unknown; dfo: unknown; gone: Record<string, unknown[]>; notices: [string, string][] };
  type Case = Feed & { name: string; slug: string; edits: [string, string][]; page?: string; fail?: boolean; first?: string[]; entries?: Record<string, unknown>[] };
  const L = V.live as Feed & { cases: Record<string, Case> };
  const KINDS = ["dfo-new:", "dfo-drift:", "dfo-new_water:", "dfo-issue:", "dfo-gone:", "dfo-held:", "dfo-fetch:"];
  const lines = (f: Pick<Feed, "gears">, runs: number[]) => runs.map((x, k) => (k % 2 ? (x >= 0 ? f.gears[x] : null) : x));
  // (T770) `o`, another fish's closure lines, by the lines themselves too (absent where none, on both sides)
  const named = (f: Pick<Feed, "locs" | "verdicts" | "gears">) => Object.fromEntries(Object.entries(f.locs).map(([lid, c]) => [lid, {
    r: c.r, sp: c.sp.map((runs) => runs.map((x, k) => (k % 2 ? (x >= 0 ? f.verdicts[x] : null) : x))),
    g: lines(f, c.g), ...(c.o ? { o: lines(f, c.o) } : {}), ...(c.x ? { x: c.x } : {}),
    // (T870) each place's DFO rows as printed (`live.rows_by_location` == `rowsByLocation`)
    ...(c.pr ? { pr: c.pr } : {}) }]));
  /** the case's entry-file edits (the emitter's `_edit_entries`, the same ops) */
  const editEntries = (doc: { locations: Record<string, any>[] }, ops: Record<string, any>[]) => { // eslint-disable-line @typescript-eslint/no-explicit-any
    for (const op of ops) {
      if (op.op === "add_place") {
        const src = doc.locations.find((l) => l.location_id === op.from)!;
        const add: Record<string, unknown> = { ...JSON.parse(JSON.stringify(src)), location_id: op.location_id, fingerprints: [op.fingerprint], status: "active",
                                               source_text: { ...src.source_text, specific_area: op.area } };
        if ("duplicate_of" in op) Object.assign(add, { duplicate_of: op.duplicate_of, duplicate_confirmed: op.duplicate_confirmed });
        if (op.no_extents) add.binding = { ...src.binding, extents: [] };
        doc.locations.push(add);
      } else if (op.op === "add_fingerprint") doc.locations.find((l) => l.location_id === op.to)!.fingerprints.push(op.fingerprint);
      else throw new Error(`unknown entry edit ${op.op}`);
    }
    return doc;
  };
  const entriesOf = (c?: Case) => async (s: string) => {
    const text = readFileSync(join(FIX, `entries/region-${s}.json`), "utf-8");
    return c?.entries && s === c.slug ? JSON.stringify(editEntries(JSON.parse(text), c.entries)) : text;
  };
  const view = (ix: any, notices: { key: string; text: string }[]) => ({ // eslint-disable-line @typescript-eslint/no-explicit-any
    locs: named(ix), unplaced: ix.unplaced, issues: ix.issues, dfo: ix.dfo,
    gone: Object.fromEntries(Object.entries<any>(ix.regions).map(([s, r]) => [s, r.gone ?? []])), // eslint-disable-line @typescript-eslint/no-explicit-any
    notices: notices.filter((n) => KINDS.some((k) => n.key.startsWith(k))).map((n) => [n.key, n.text] as [string, string]).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
  });
  // ONE NAMED DIFFERENCE in the words (T549 item 9, on purpose): the worker's action first reads DFO's page on the Mac
  // (`live run … && live queue …`, `QUEUE_ACTION`), since `live queue` reads the Mac's unplaced.json, never the worker's
  const action = (t: string) => t.replace(/after: python -m pipeline\.regs\.dfo_salmon\.live queue --regions (\w+)$/, (_m, s: string) => `after: ${QUEUE_ACTION(s)}`);
  const want = (f: Feed) => ({ locs: named(f), unplaced: f.unplaced, issues: f.issues, dfo: f.dfo, gone: f.gone,
                               notices: f.notices.map(([k, t]) => [k, action(t)] as [string, string]) });

  it("the saved pages: 274 places, the issues, the shape — every one as the Python's", async () => {
    const feed = new MemStore();
    const got = await runDfo({ fetch: async () => new Response(""), feed, priv: privStore(), now: "2026-10-28T12:00:00Z", regions: Object.keys(V.pages),
                               entries: entriesOf(), fetcher: async (s) => [page(s), null] });
    expect(Object.keys(L.locs).length).toBe(274);
    const ix = JSON.parse(feed.m.get("index.json")!);
    expect(view(ix, got.notices)).toEqual(want(L));
    expect(L.unplaced).toBe(0);
    // the order the ticket names (toEqual ignores key order): waters by id in the published bytes, each list by key
    const ids = Object.keys(ix.issues.waters);
    expect(ids.length).toBeGreaterThan(1);
    expect(ids).toEqual([...ids].sort());
    for (const w of Object.values<{ list: { key: string }[] }>(ix.issues.waters)) expect(w.list.map((i) => i.key)).toEqual(w.list.map((i) => i.key).sort());
  });
  it("the cases the vectors hold: every kind of issue, a removal, the regions not read", () => {
    expect(Object.keys(L.cases).sort()).toEqual(["closed_area_unbound", "drift_close_match", "drift_confirmed", "drift_proposed_variant", "exact_rewording", "gone",
      "new_water", "no_default_sentence", "sentence_without_a_place", "unmatched_near_place_taken", "unmatched_new", "unmatched_new_far_reword", "unmatched_undrawn",
      "unread_held", "unread_not_asked", "unread_stale"]);
  });
  it.each(Object.keys(L.cases))("%s: the worker's run == the Python's (locs, issues, dfo, gone, notices)", async (name) => {
    const c = L.cases[name]!;
    let html = c.page ?? page(c.slug);
    for (const [a, b] of c.edits) {
      expect(html.split(a).length - 1).toBe(1);
      html = html.replace(a, b);
    }
    const feed = new MemStore(), priv = privStore();
    const base = { fetch: async () => new Response(""), feed, priv, entries: entriesOf(c) };
    await runDfo({ ...base, now: "2026-10-28T12:00:00Z", regions: c.first ?? [c.slug], fetcher: async (s) => [page(s), null] });
    const got = await runDfo({ ...base, now: "2026-10-28T18:00:00Z", regions: [c.slug],
                               fetcher: async (s) => { if (c.fail && s === c.slug) throw new Error("down"); return [s === c.slug ? html : page(s), null]; } });
    expect(view(JSON.parse(feed.m.get("index.json")!), got.notices)).toEqual(want(c));
  });
  it("what the cases say (the Python's words, read back)", () => {
    const kinds = (c: Case) => Object.values((c.issues as any).waters as Record<string, { list: { kind: string; why?: string }[] }>) // eslint-disable-line @typescript-eslint/no-explicit-any
      .flatMap((w) => w.list.map((i) => `${i.kind}${i.why ? `/${i.why}` : ""}`)).filter((k) => !k.endsWith("/undrawn") || false);
    const C = L.cases;
    expect(kinds(C.unmatched_new!)).toEqual(["unmatched/new"]);
    expect(kinds(C.unmatched_new_far_reword!)).toEqual(["unmatched/new"]);
    expect(kinds(C.unmatched_near_place_taken!)).toEqual(["unmatched/near_place_taken"]);
    expect(kinds(C.drift_close_match!)).toEqual(["drift"]);
    expect(kinds(C.drift_proposed_variant!)).toEqual(["drift"]);
    expect(kinds(C.drift_confirmed!)).toEqual([]);
    expect(kinds(C.exact_rewording!)).toEqual([]);
    expect(kinds(C.gone!)).toEqual([]);
    expect(C.gone!.gone["1"]!.length).toBe(1);
    expect(kinds(C.closed_area_unbound!)).toEqual(["closed_area_unbound"]);
    expect(((C.new_water!.issues as any).no_water as { why: string }[]).map((i) => i.why)).toEqual(["new_water"]); // eslint-disable-line @typescript-eslint/no-explicit-any
    const und = Object.values((C.unmatched_undrawn!.issues as any).waters["1:quatse-river"].list as { why: string }[]); // eslint-disable-line @typescript-eslint/no-explicit-any
    expect(und.map((i) => i.why)).toEqual(["undrawn"]);
    expect([C.unread_held, C.unread_stale, C.unread_not_asked].map((c) => (c!.issues as any).unread)).toEqual([{ "1": "held" }, { "1": "stale" }, { "8": "not asked" }]); // eslint-disable-line @typescript-eslint/no-explicit-any
    expect(L.dfo).toEqual({ pieces: 1 });
  });
  // (T770, T870) a carried place (`live._carry`): its year in words, its closure lines, its rows in DFO's words and as printed
  it("carry: the worker's carried places are the Python's, words for words, `pr` included", () => {
    const C = (V.live as { carry: { prev: Feed; slug: string; locs: Feed["locs"] } }).carry;
    const v = new Interner<unknown>(), g = new Interner<unknown>();
    const got = carry(C.prev, C.slug, v, g);
    expect(named({ locs: got as Feed["locs"], verdicts: v.items, gears: g.items })).toEqual(C.locs);
    expect(Object.values(C.locs).some((l) => l.pr?.length)).toBe(true);
  });
  it("the 7 page defaults are there, each closed every day in its page's own sentence; Region 2's two each its own", () => {
    const got = named(L);
    expect(Object.keys(got).filter((l) => l.includes(":default:")).sort()).toEqual(["1:default:region", "2:default:areas-13-15-16", "2:default:areas-28-29",
      "3:default:region", "5a:default:region", "7:default:region", "8:default:region"]);
    const words = (lid: string) => [...new Set((got[lid]!.sp.flatMap((runs) => runs.filter((_, k) => k % 2)) as { s: string; t: string }[]).map((v) => `${v.s}: ${v.t}`))];
    expect(words("2:default:areas-28-29")).toEqual(["closed: There is no fishing for salmon in waters of Region 2 flowing into Areas 28 and 29 except for the opportunities listed in the table below."]);
    expect(words("2:default:areas-13-15-16")).toEqual(["closed: Unless otherwise stated in the table below, the daily limit for waters in Region 2 flowing into Areas 13, 15, and 16 is zero (0)."]);
  });
  it("sentence_without_a_place: the page's sentence for a scope no place carries is typed, then skipped — never unplaced", () => {
    const c = L.cases.sentence_without_a_place!;
    const [typed] = typePage(page(c.slug).replace(c.edits[0]![0], c.edits[0]![1]), c.slug, true, REG);
    expect(typed.locators.filter((l) => l.fingerprint.startsWith("page:")).map((l) => l.fingerprint)).toEqual(["page:3:region", "page:3:areas-30-31"]);
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
  it("no_default_sentence: the region 3 page without its sentence publishes no default place", () => {
    const c = L.cases.no_default_sentence!;
    expect(Object.keys(c.locs).filter((l) => l.includes(":default:"))).toEqual([]);
    expect(Object.keys(c.locs).sort()).toEqual(Object.keys(L.locs).filter((l) => L.locs[l]!.r === "3" && l !== "3:default:region").sort());
  });
});
