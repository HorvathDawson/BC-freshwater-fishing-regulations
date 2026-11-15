// T757: THE CLOSED-AREA PIECES (T573 entry B) in the worker's live DFO feed == the Python live feed's.
//   * the oracle `fixtures/dfo/closure-oracle.generated.json` (`python -m pipeline.tools.emit.closure_oracle`): per case the
//     worker's `locs`, `verdicts`, `gears` are the Python's, piece for piece (confirmed on today's row, unconfirmed on
//     today's row, confirmed but dropped by DFO today);
//   * the id grammar (`closures.PIECE_ID_RE`) and the inline key's two typing slips, with every expected value written by
//     the PYTHON (the command beside each), never by this port;
//   * runDfo over the saved pages: one loc per piece the pages name, each the Python's calendar (python-vectors `pieces`);
//   * /status.json says the shape: `dfo.pieces === 1` (pipeline/deliver/release/pieces_gate.py `PIECES_SHAPE`).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { inlineKey, livePieceRules, pieceId, pieceKey } from "../src/dfo/closures";
import { loadEntryFile } from "../src/dfo/entries";
import { runDfo } from "../src/dfo";
import { formatG } from "../src/dfo/py";
import { dumps } from "../src/dfo/pyjson";
import { Interner, locationCalendar, NOT_LISTED } from "../src/dfo/salmon";
import worker from "../src/index";
import { MemStore, privStore } from "./dfo.scenario";

const FIX = join(__dirname, "fixtures/dfo");
const plain = <T>(x: T): T => JSON.parse(dumps(x));

interface Case {
  name: string;
  entry_file: unknown;
  rows: { fingerprint: string; specific_area: string; excludes: string[] }[];
  rules: Record<string, unknown[]>;
  expect: { locs: Record<string, unknown>; verdicts: unknown[]; gears: unknown[] };
}
const ORACLE: { cases: Case[] } = JSON.parse(readFileSync(join(FIX, "closure-oracle.generated.json"), "utf-8"));

/** the worker's answer for one oracle case, built as `live.ts` does after the join (and the oracle as `live.run`) */
function feedOf(c: Case) {
  const ef = loadEntryFile("3", JSON.stringify(c.entry_file));
  const got = livePieceRules(ef, c.rows);
  const verdicts = new Interner<unknown>(), gears = new Interner<unknown>();
  verdicts.add(NOT_LISTED); // verdicts[0], as live.run
  const locs: Record<string, unknown> = {};
  for (const lid of [...got.keys()].sort()) locs[lid] = { r: "3", ...locationCalendar(got.get(lid)!, verdicts, gears) };
  return { rules: plain(Object.fromEntries(got)), feed: plain({ locs, verdicts: verdicts.items, gears: gears.items }) };
}

describe("the oracle: the worker's pieces are the Python live feed's", () => {
  it("the file holds the three cases the spec names, each with something to compare", () => {
    expect(ORACLE.cases.map((c) => c.name)).toEqual(["a_confirmed_on_todays_row", "b_unconfirmed_on_todays_row", "c_confirmed_dropped_today"]);
    for (const c of ORACLE.cases) expect(Object.keys(c.expect.locs).length).toBeGreaterThan(0);
  });
  it.each(ORACLE.cases.map((c) => [c.name, c] as const))("%s: locs, verdicts, gears, and the rules they are made of", (_n, c) => {
    const { rules, feed } = feedOf(c);
    expect(rules).toEqual(c.rules);
    expect(feed).toEqual(c.expect);
  });
  it("what the cases say: a confirmed and an unconfirmed piece are both emitted; one DFO dropped today is not", () => {
    const [a, b, cc] = ORACLE.cases.map((c) => Object.keys(feedOf(c).feed.locs));
    const deadman = "3:thompson-river#closed:9fa028589ff1";
    const juniper = "3:thompson-river#closed:cb48ccedb556";
    expect(a).toEqual([deadman, juniper]);
    expect(b).toEqual([deadman, juniper]);
    expect(cc).toEqual([juniper]);
  });
});

describe("two live wordings of one water naming the same item: one piece, the first entry in id order wins", () => {
  // the Python's answer, for the same two rows (`K.live_piece_rules` on an EntryFile of the two locations, rows in this
  // order): {'3:thompson-river#closed:9fa028589ff1': ['No fishing, year-round — Deadman - …']} — entry `…:a#closed`
  // sorts before `…:b#closed`, so row a's year-round words win though row b comes first on the page
  it("row a's rule, not row b's", () => {
    const [a] = ORACLE.cases;
    const deadman = a!.rows[0]!.excludes[0]!;
    const place = (lid: string, fp: string, text: string) => ({ location_id: lid, region: "3", region_number: 3, kind: "water", precedence: 3,
      water: "Thompson River", water_id: "3:thompson-river", fingerprints: [fp], source_text: { specific_area: text, excludes: [deadman] }, status: "active" });
    const ef = loadEntryFile("3", JSON.stringify({ region: "3", region_number: 3, region_name: "x", waters: [],
      locations: [place("3:thompson-river:b", "fp-b", "Closed in the following areas:"), place("3:thompson-river:a", "fp-a", a!.rows[0]!.specific_area)] }));
    const got = livePieceRules(ef, [{ fingerprint: "fp-b", specific_area: "Closed in the following areas:", excludes: [deadman] },
                                    { fingerprint: "fp-a", specific_area: a!.rows[0]!.specific_area, excludes: [deadman] }]);
    expect(Object.fromEntries([...got].map(([k, v]) => [k, v.map((r) => r.verbatim)]))).toEqual({
      "3:thompson-river#closed:9fa028589ff1": [`No fishing, year-round — ${deadman}`],
    });
  });
});

describe("a place with no water_id (a page default, a section row) builds no piece", () => {
  // the Python's answer: the oracle's entry file with the place's water_id set to None -> `K.live_piece_rules` = {}
  it("today's row says closed over named areas, and still nothing is emitted", () => {
    const c = ORACLE.cases[0]!;
    const ef = JSON.parse(JSON.stringify(c.entry_file));
    ef.locations[0].water_id = null;
    expect([...livePieceRules(loadEntryFile("3", JSON.stringify(ef)), c.rows)]).toEqual([]);
  });
});

describe("the piece id grammar (closures.PIECE_ID_RE == the Python's)", () => {
  // acceptance written by the Python: `K.PIECE_ID_RE.match(id)` for each (.venv/bin/python, pipeline.regs.dfo_salmon.closures)
  const split = (id: string): [string, string] => [id.slice(0, id.lastIndexOf("#closed:")), id.slice(id.lastIndexOf("#closed:") + 8)];
  it("accepts every id the oracle carries", () => {
    const ids = [...new Set(ORACLE.cases.flatMap((c) => Object.keys(c.expect.locs)))];
    expect(ids.length).toBe(2);
    for (const id of ids) expect(pieceId(...split(id))).toBe(id);
  });
  it.each([
    ["a space", "3:thompson river#closed:9fa028589ff1"],
    ["upper case in the water", "3:Thompson-river#closed:9fa028589ff1"],
    ["upper-case hex", "3:thompson-river#closed:9FA028589FF1"],
    ["non-ASCII", "3:thompson-rivér#closed:9fa028589ff1"],
    ["a second #", "3:thompson#x-river#closed:9fa028589ff1"],
    ["a second #closed", "3:thompson-river#closed#closed:9fa028589ff1"],
    ["a short key", "3:thompson-river#closed:9fa028589ff"],
  ])("refuses %s (the Python's match is None too)", (_why, id) => {
    expect(() => pieceId(...split(id))).toThrow(/is not a piece id/);
  });
});

describe("the inline key's two typing slips (closures._inline_key)", () => {
  // the stored Shuswap wording; every key below is the Python's:
  //   .venv/bin/python -c "from pipeline.regs.dfo_salmon import closures as K; print(K._inline_key(t), K.piece_key(t))"
  const clean = "closed from the water survey station (located at 50°19.105' N and 118°48.857' W) approximately 200 m down stream of Shuswap Falls Hatchery to WTFB signs (located at 50°18.181' N and 118°49.105' W) one km down stream";
  // (Python's str.replace replaces every one: replaceAll)
  const paren = clean.replaceAll("(located", "( located");
  const m200 = clean.replaceAll("200 m", "200m");
  const both = paren.replaceAll("200 m", "200m");
  it("'( ' -> '(' and '200m' -> '200 m': the four wordings are one piece, the Python's key", () => {
    for (const t of [clean, paren, m200, both]) expect(inlineKey(t)).toBe("200d03a74aaf");
  });
  it("and the slips matter: the plain piece key of each wording is the Python's, and differs", () => {
    expect([clean, paren, m200, both].map(pieceKey)).toEqual(["200d03a74aaf", "dc9e9f9c2520", "ee2d8411e460", "401f5131f7bb"]);
  });
  it("a circle's radius is spelled as Python's f\"{r:g}\" (values from the Python)", () => {
    const py: [number, string][] = [[400, "400"], [0.5, "0.5"], [1500, "1500"], [1e6, "1e+06"], [1234567, "1.23457e+06"], [1234565, "1.23456e+06"],
      [0.0001, "0.0001"], [0.00001, "1e-05"], [2.5e-5, "2.5e-05"], [100.25, "100.25"], [999999.5, "1e+06"], [20000, "20000"]];
    expect(py.map(([x]) => [x, formatG(x)])).toEqual(py);
  });
});

describe("runDfo over the saved pages: one loc per piece the rows name, each the Python's calendar", () => {
  const V = JSON.parse(readFileSync(join(FIX, "python-vectors.generated.json"), "utf-8"));
  const slugs = Object.keys(V.pages);
  it("17 pieces (Thompson 3, Babine 12 mouths + the Gullwing line, middle Shuswap 1), each answered as the Python's", async () => {
    const feed = new MemStore();
    await runDfo({ fetch: async () => new Response(""), feed, priv: privStore(), now: "2026-10-28T12:00:00Z", regions: slugs,
                   entries: async (s) => readFileSync(join(FIX, `entries/region-${s}.json`), "utf-8"),
                   fetcher: async (s) => [readFileSync(join(FIX, `pages/region${s}-eng.html`), "utf-8"), null] });
    const ix = JSON.parse(feed.m.get("index.json")!);
    const want: Record<string, { r: string; calendar: { sp: number[][]; g: number[]; verdicts: unknown[]; gears: unknown[] } }> = {};
    for (const s of slugs) for (const [lid, p] of Object.entries<any>(V.pages[s].pieces)) want[lid] = { r: s, calendar: p.calendar }; // eslint-disable-line @typescript-eslint/no-explicit-any
    const got = Object.keys(ix.locs).filter((l) => l.includes("#closed:")).sort();
    expect(got).toEqual(Object.keys(want).sort());
    expect(got.length).toBe(17);
    expect(got.filter((l) => ix.locs[l].r === "6").length).toBe(13);
    // the same answers, each through its own verdict table (the robust feed numbers verdicts canonically)
    const named = (sp: number[][], vs: unknown[]) => sp.map((runs) => runs.map((x, k) => (k % 2 && x >= 0 ? vs[x] : x)));
    for (const lid of got) {
      expect(ix.locs[lid].r, lid).toBe(want[lid]!.r);
      expect(named(ix.locs[lid].sp, ix.verdicts), lid).toEqual(named(want[lid]!.calendar.sp, want[lid]!.calendar.verdicts));
      expect(ix.locs[lid].g, lid).toEqual(want[lid]!.calendar.g);
    }
    // the spec's pin for region 6 (test_dfo_live.py: 137 places + Babine's 13 closed areas)
    expect(Object.values<{ r: string }>(ix.locs).filter((l) => l.r === "6").length).toBe(150);
    // (the whole feed, every place with its year, is pinned to the Python's in dfo.vectors.test.ts, "the live feed")
  });
});

describe("/status.json says the feed shape (pieces_gate.py PIECES_SHAPE)", () => {
  const bucket = (status: string) => {
    const m = new Map<string, string>([["private/feeds/status.json", status]]);
    return { m, async get(k: string) { return m.has(k) ? { text: async () => m.get(k)! } : null; }, async put(k: string, b: string) { m.set(k, b); }, async delete(k: string) { m.delete(k); } };
  };
  it("dfo.pieces === 1, on a good status and on a 503", async () => {
    const ok = await worker.fetch(new Request("https://w/status.json"), { BUCKET: bucket(JSON.stringify({ format: "feeds-status/1", updated: "", feeds: {} })), RUN_TOKEN: "t" } as never);
    expect(ok.status).toBe(200);
    expect((await ok.json()).dfo).toEqual({ pieces: 1 });
    const bad = await worker.fetch(new Request("https://w/status.json"), { BUCKET: bucket(JSON.stringify({ format: "other" })), RUN_TOKEN: "t" } as never);
    expect(bad.status).toBe(503);
    expect((await bad.json()).dfo).toEqual({ pieces: 1 });
  });
});
