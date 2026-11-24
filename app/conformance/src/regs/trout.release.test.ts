/**
 * (T810 N, S) THE TROUT TABLE'S VIEW MODEL against the shared vectors and the trout fixture release (always run:
 * committed); since S the table is the only trout path (no dev switch). The proof column of the build spec's N row:
 * - (T810 M, the G proof; folded in here by T866) every vector through `loadRegsInputs`: the tap's segment is the
 *   vector's and `inputs.trout` is the vector's frame, value for value (a table decoded whole, or null: Q-D3);
 * - whole strings per vector: the top line with both day words is the vector's `composed.lead_today` / `lead_on`, and
 *   every other drawn string is the frame's own (none added or changed, contract fight list #5);
 * - each hand case of `trout-hand.json` refused through `reportFailure`, no table drawn (the base frame is drawn);
 * - the screen-reader golden over every vector row (`app/fixtures/golden/trout-a11y.json`; `UPDATE_TROUT_A11Y=1` writes
 *   it, through `regs-recut/sync.mjs`, never by hand);
 * - DRAWN EXACTLY ONCE: over every vector cell and every cell of the trout fixture release, each rows-frame row is drawn
 *   once: by the table (`covers`), as a full row of the main block, or as a line of the Other fish list;
 * - a lone other fish beside the table is drawn in full; no fish is named by both caveat (b) and the table;
 * - (S) a cell with no table (`trout` null): no trout block, no trout sheet, every row drawn by the old card.
 */
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  buildModel, buildRegsPage, drawnRows, listRows, makePlace, segmentIndex, setFailureSink, settle, tableCovers, tapDay, troutOf,
  TROUT_NAV, TroutRefused, type RegsInputs, type RegsPage, type RuleIndex, type TroutValue, type TroutVM,
  FISH, FISH_TIPS, SPECIES_SHEET, TROUT_SHEET, TROUT_SOURCES_KEY, troutIdSheet, TroutLooksMissing, WORDS, type SheetBlock, type SheetVM,
  lineSourcesOf, sourcesText,
} from "@app/core";
import { openTroutFixture, TROUT_FIXTURE_ROOT } from "@app/data/release/node";
import { loadRegsInputs, openRegsStore, targetFor, type RegsStore } from "@app/data/regs";
import { ADULT_RESIDENT, REPO } from "./golden";
import { railSetters } from "./tributaryEvidence";

interface Vector { readonly name: string; readonly item: string; readonly day: string; readonly weekday: string; readonly s: number;
                   readonly anchor: { readonly stream: number; readonly m: number } | { readonly lake: string };
                   readonly frame: TroutValue | null; readonly composed: { readonly lead_today: string; readonly lead_on: string } }
interface Hand { readonly rule_count: number; readonly base: TroutValue; readonly cases: readonly { readonly name: string; readonly frame: TroutValue }[] }

const read = (f: string) => JSON.parse(readFileSync(join(TROUT_FIXTURE_ROOT, f), "utf8"));
const V = read("trout-vectors.generated.json") as { readonly cases: readonly Vector[] };
const HAND = read("trout-hand.json") as Hand;
const LINES = read("section-lines.json").lines as readonly (readonly [number, number, number, number])[];
const WEEKDAY = { mon: "Monday", tue: "Tuesday", wed: "Wednesday", thu: "Thursday", fri: "Friday", sat: "Saturday", sun: "Sunday" } as const;
const A11Y = resolve(REPO, "app/fixtures/golden/trout-a11y.json");
const OTHER_DAY = { month: 1, day: 2 };   // a "today" that is no case's day: the lead says "on <day>"

let st: RegsStore;
beforeAll(async () => { st = await openRegsStore(openTroutFixture()); });

let reports: unknown[] = [], undo: () => void = () => {};
/**
 * (T910) This runs the WEB's store code in node, which has no Web Worker: the web's rules decoder says so once per process
 * (`rulesDecode.web.ts`, reportOnce "regs-rules-worker"), in whichever case first reads rules. That is the test host, not
 * the store or the trout view; `tools/rules-worker.test.ts` holds that report (a browser with no worker is still reported).
 * Only that one report, and only where `Worker` is absent, is left out; every other report counts.
 */
const hostHasNoWorker = (e: unknown) => typeof Worker === "undefined" && e instanceof Error
  && /^regs rules: this browser has no Web Worker;/.test(e.message);
beforeEach(() => { reports = []; undo = setFailureSink((e) => { if (!hostHasNoWorker(e)) reports.push(e); }); });
afterEach(() => undo());

/** The case's section, inside its own water (as the vectors' `Fixture.section` finds it): a lake of one part, or the one
 *  section of the water that starts at the case's metre on its line (T866: the checks of the removed "T810 M" block). */
async function sectionOf(c: Vector): Promise<number> {
  const ord = (await st.itemOrd(c.item))!, parts = await st.parts(ord), mine = new Set<number>();
  for (const p of parts) for (const sid of (await st.partLengths(ord, p.partIx)).keys()) mine.add(sid);
  if ("lake" in c.anchor) {
    expect(parts.length, `${c.name}: a lake of one part`).toBe(1);
    return Math.min(...mine);
  }
  const { stream, m } = c.anchor;
  const hit = LINES.filter(([h, b, lo]) => b === stream && lo === m && mine.has(h)).map(([h]) => h);
  expect(hit, `${c.name}: one section of ${c.item} starts at ${m} m on line ${stream}`).toHaveLength(1);
  return hit[0]!;
}
async function inputsOfCase(c: Vector): Promise<RegsInputs> {
  const [mm, dd] = c.day.split("-").map(Number);
  return loadRegsInputs(st, await targetFor(st, c.item, await sectionOf(c)), { month: mm!, day: dd! },
    WEEKDAY[c.weekday as keyof typeof WEEKDAY], 2026);
}
const pageOf = (inp: RegsInputs, today = inp.day): RegsPage => buildRegsPage(inp, { today, who: ADULT_RESIDENT });
/** (T810 S) the same inputs with no trout table in the cell (Q-D3 null): the mutation every switch-off check became */
const noTable = (inp: RegsInputs): RegsInputs => ({ ...inp, trout: null });

/**
 * How many times the page draws each rows-frame row (contract 2.12 "Split"): the table through `covers`, a full row of
 * the main block (a group or a plain row), a line of the Other fish list (`listRows`, the list `other:oN` opens). A row
 * with a parent is said inside its parent's card (page v36 draws no article for it): it counts once when its parent does.
 */
function drawnCount(inp: RegsInputs, page: RegsPage): number[] {
  const card = buildModel(inp, makePlace(inp), settle(inp, makePlace(inp)));
  const n = card.rows.map(() => 0), keep = page.keep;
  const t = keep.trout;
  if (t) for (const i of t.covers) n[i]! += 1;
  for (const g of keep.groups) n[g.row]! += 1;
  for (const p of keep.plainRows) n[p.row]! += 1;
  for (const i of listRows(card, tableCovers(inp)) ?? []) n[i]! += 1;
  const drawn = drawnRows(card);
  card.rows.forEach((r, i) => {
    const p = r.raw.parent;
    if (!drawn.has(i) && p != null && n[i] === 0 && !(t?.covers.includes(i))) n[i] = n[p]!;
  });
  return n;
}
/** Every row is drawn exactly once on an open day with rows; none on a closed day (the banner speaks). */
function expectDrawnOnce(inp: RegsInputs, page: RegsPage, what: string): void {
  const n = drawnCount(inp, page), open = page.keep.strip?.status === "open" && !!inp.frames;
  expect(n.map((x, i) => [i, x]).filter(([, x]) => x !== (open ? 1 : 0)), `${what}: rows drawn other than once`).toEqual([]);
}
/** The fish caveat (b) names that the table holds too (contract 2.12: none). */
function spokenTwice(page: RegsPage): string[] {
  const t = page.keep.trout;
  if (!t || t.kind !== "table") return [];
  const mine = new Set(t.boxes.flatMap((b) => b.rows.map((r) => r.fish)));
  return page.keep.caveats.filter((c) => c.kind === "timed").flatMap((c) => c.items).flatMap((x) => (x.fish && mine.has(x.fish) ? [x.fish] : []));
}
/** Every string a frame holds (its texts and its sheet texts). */
function frameStrings(f: TroutValue): Set<string> {
  const out = new Set<string>();
  const walk = (v: unknown): void => {
    if (typeof v === "string") out.add(v);
    else if (Array.isArray(v)) v.forEach(walk);
    else if (v && typeof v === "object") Object.values(v).forEach(walk);
  };
  walk(f);
  return out;
}
/** Every string the VM draws but the lead (the slot's) and the a11y labels (composed, pinned by the golden). */
function vmStrings(t: TroutVM): string[] {
  const out: string[] = [];
  const walk = (v: unknown, k: string): void => {
    if (k === "a11y" || k === "lead" || k === "kind" || k === "fish" || k === "key" || k === "origin" || k === "tone") return;
    if (typeof v === "string") out.push(v);
    else if (Array.isArray(v)) v.forEach((x) => walk(x, ""));
    else if (v && typeof v === "object") for (const [kk, vv] of Object.entries(v)) walk(vv, kk);
  };
  walk(t, "");
  return out;
}

describe("T810 N: the trout view model over every vector", () => {
  it("the vectors hold tables and null cells both", () => {
    expect(V.cases.filter((c) => c.frame !== null).length).toBeGreaterThan(40);
    expect(V.cases.some((c) => c.frame === null)).toBe(true);
  });

  it.each(V.cases.map((c) => [c.name, c] as const))("%s", async (_n, c) => {
    const inp = await inputsOfCase(c);
    expect(inp.s).toBe(c.s);                                  // the tap's segment is the vector's
    expect(inp.trout).toEqual(c.frame);
    const on = pageOf(inp), off = pageOf(noTable(inp));
    // whole strings: the lead with both day words; every other drawn string is the frame's own
    const t = on.keep.trout;
    if (c.frame === null) expect(t ?? null).toBeNull();
    else {
      expect(t?.kind).toBe("table");
      const vm = t as TroutVM;
      expect([vm.top.lead, (troutOf(inp, inp.day, OTHER_DAY) as TroutVM).top.lead]).toEqual([c.composed.lead_today, c.composed.lead_on]);
      const own = frameStrings(c.frame), nav = new Set<string>(Object.values(TROUT_NAV));
      expect(vmStrings(vm).filter((x) => !own.has(x) && !nav.has(x))).toEqual([]);
      expect(on.keep.order.filter((b) => b === "trout")).toEqual(["trout"]);
    }
    expect(reports).toEqual([]);
    // drawn exactly once; no fish spoken twice; with no table in the cell, no trout block and every row the old card's
    expectDrawnOnce(inp, on, c.name);
    expect(spokenTwice(on)).toEqual([]);
    expect(off.keep.trout ?? null).toBeNull();
    expect(off.keep.order).not.toContain("trout");
    expectDrawnOnce(noTable(inp), off, `${c.name} (no table)`);
  });
});

describe("T810 N: the hand cases (trout-hand.json): each refused through reportFailure, no table drawn", () => {
  const fake = (f: TroutValue, rows: number): RegsInputs => ({
    day: { month: 7, day: 15 }, trout: f, frames: { rows: { rows: Array.from({ length: rows }) } },
    rules: { has: (ix: number) => ix >= 0 && ix < HAND.rule_count } as unknown as RuleIndex,
  }) as unknown as RegsInputs;
  const ROWS = Math.max(...HAND.base.covers) + 1;

  it("the base frame draws", () => {
    const t = troutOf(fake(HAND.base, ROWS), { month: 7, day: 15 }, { month: 7, day: 15 });
    expect([t?.kind, (t as TroutVM).top.lead, reports]).toEqual(["table", "Up to 2 today", []]);
  });
  it("the file holds the contract's twenty-two", () => expect(HAND.cases).toHaveLength(22));
  it.each(HAND.cases.map((c) => [c.name, c] as const))("%s", (_n, c) => {
    const t = troutOf(fake(c.frame, ROWS), { month: 7, day: 15 }, { month: 7, day: 15 });
    expect(t?.kind).toBe("refused");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toBeInstanceOf(TroutRefused);
  });
  it("(G2 Q4) a possession line on a release or closed box: refused, as the pydantic model refuses it", () => {
    const box = HAND.base.table.boxes[0]!;
    for (const kind of ["release", "closed"] as const) {
      reports = [];
      const f = { ...HAND.base, table: { ...HAND.base.table, boxes: [{ ...box, kind, label: null, strip: null, tag: null, bands: [], possession: "In your possession: no more than 2 trout and char in all" }] } };
      expect(troutOf(fake(f, ROWS), { month: 7, day: 15 }, { month: 7, day: 15 })?.kind, kind).toBe("refused");
      expect((reports[0] as TroutRefused).why, kind).toMatch(/possession/);
    }
  });
  it("(trout 5) a split row: the table keeps its hatchery link, the fish sheet shows it; split with an origin or a label is refused", () => {
    const day = { month: 7, day: 15 }, t0 = HAND.base.table, box = t0.boxes[0]!;
    const withRow = (patch: object, link: boolean): TroutValue => ({ ...HAND.base, table: { ...t0, hatch_link: link,
      boxes: [{ ...box, rows: box.rows.map((r, i) => ({ ...r, ...(i === 0 ? patch : {}), sheet: { ...r.sheet, src: [] } })) }, ...t0.boxes.slice(1)] } });
    const ok = troutOf(fake(withRow({ split: true }, true), ROWS), day, day) as TroutVM;
    expect([ok.kind, ok.hatchLink !== null, ok.boxes[0]!.rows[0]!.split, ok.boxes[0]!.rows[1]!.split]).toEqual(["table", true, true, false]);
    // (T1031) the hatchery link is the Identification tab's (`tf:<row>|id`)
    const links = (k: string) => linksOf(troutIdSheet(`tf:${k}|id`, ok, k, FISH)!);
    expect(links(ok.boxes[0]!.rows[0]!.key)).toContain("hatch");
    expect(links(ok.boxes[0]!.rows[1]!.key)).not.toContain("hatch");
    expect(reports).toEqual([]);
    // the table link must follow a split row: hatch_link false beside one is refused
    expect(troutOf(fake(withRow({ split: true }, false), ROWS), day, day)?.kind).toBe("refused");
    for (const patch of [{ origin: "wild", key: `${box.rows[0]!.fish}/wild` }, { label: "HATCHERY ONLY" }]) {
      reports = [];
      expect(troutOf(fake(withRow({ split: true, ...patch }, true), ROWS), day, day)?.kind, JSON.stringify(patch)).toBe("refused");
      expect((reports[0] as TroutRefused).why).toMatch(/merged hatchery and wild row has no origin and no label/);
    }
  });
  it("covers past the rows frame: refused (the reader's check)", () => {
    expect(troutOf(fake(HAND.base, ROWS - 1), { month: 7, day: 15 }, { month: 7, day: 15 })?.kind).toBe("refused");
    expect((reports[0] as TroutRefused).why).toMatch(/covers names row 2 of a rows frame of 2/);
  });
  it("a refused table on a page: reported, the block names the refusal, its rows drawn by nothing else", async () => {
    const c = V.cases.find((x) => x.name === "skeena-sedan-mill-jul15")!;
    const inp = await inputsOfCase(c);
    const bad = { ...inp, trout: { ...inp.trout!, table: { ...inp.trout!.table, hatch_link: !inp.trout!.table.hatch_link } } };
    const page = pageOf(bad);
    expect([page.keep.trout?.kind, page.keep.order.includes("trout"), reports.length]).toEqual(["refused", true, 1]);
    expect(drawnCount(bad, page).filter((x, i) => bad.trout.covers.includes(i) && x !== 1)).toEqual([]);
    expect(page.keep.groups.filter((g) => bad.trout.covers.includes(g.row))).toEqual([]);
  });
});

describe("T810 N: the screen-reader golden over every vector row", () => {
  it("each row's label is its payload texts in payload order, joined by \". \" (golden)", async () => {
    const got: Record<string, Record<string, string>> = {};
    for (const c of V.cases) {
      if (!c.frame) continue;
      const vm = troutOf(await inputsOfCase(c), { month: 1, day: 1 }, { month: 1, day: 1 }) as TroutVM;
      got[c.name] = Object.fromEntries(vm.boxes.flatMap((b) => b.rows.map((r) => [r.key, r.a11y])));
    }
    if (process.env.UPDATE_TROUT_A11Y === "1") writeFileSync(A11Y, JSON.stringify(got, null, 1) + "\n");
    expect(existsSync(A11Y), "the golden is written by UPDATE_TROUT_A11Y=1 (regs-recut/sync.mjs)").toBe(true);
    expect(got).toEqual(JSON.parse(readFileSync(A11Y, "utf8")));
    // (contract 2.8, amended 11-17) no second stop after a text that ends a sentence: the steelhead quiet line is one
    const said = Object.values(got).flatMap((r) => Object.values(r));
    expect(said.filter((l) => l.includes(".."))).toEqual([]);
    expect(said.some((l) => l.includes("Steelhead are known to be in this water. "))).toBe(true);
  });
});

describe("T810 N: every cell of the trout fixture release", () => {
  /** Per water of the fixture (the vectors' waters), each key once, each segment once: the first 2026 day that reads it. */
  async function* cells(): AsyncGenerator<RegsInputs> {
    const seen = new Set<number>();
    for (const item of [...new Set(V.cases.map((c) => c.item))]) {
      const ord = await st.itemOrd(item);
      if (ord === null) continue;
      for (const p of await st.parts(ord)) {
        if (p.akey === null || seen.has(p.akey)) continue;
        seen.add(p.akey);
        const key = await st.akey(p.akey), starts = await st.segStarts(key.seg);
        const moments = key.mom === null ? null : await st.segMoments(key.mom), first = new Map<number, { month: number; day: number }>();
        for (let t = Date.UTC(2026, 0, 1); t < Date.UTC(2027, 0, 1); t += 86_400_000) {
          const d = new Date(t), md = { month: d.getUTCMonth() + 1, day: d.getUTCDate() }, tap = tapDay(md, 2026);
          const s = segmentIndex(starts, tap.day, moments, tap.weekday, false);
          if (!first.has(s)) first.set(s, md);
        }
        for (const md of first.values())
          yield loadRegsInputs(st, { kind: "part", item, partIx: p.partIx }, md, tapDay(md, 2026).weekday, 2026);
      }
    }
  }

  it("each rows-frame row drawn exactly once; no fish spoken twice; a lone other fish drawn in full", async () => {
    let cellsRead = 0, tables = 0, listed = 0;
    for await (const inp of cells()) {
      expect(inp.trout, "a keyed tap carries its trout cell (required from rows 34)").not.toBeUndefined();
      cellsRead++;
      const page = pageOf(inp), what = `${inp.item?.id} part ${inp.partIx} ${inp.day.month}-${inp.day.day}`;
      expectDrawnOnce(inp, page, what);
      expect(spokenTwice(page), what).toEqual([]);
      const t = page.keep.trout;
      if (t?.kind === "table") {
        tables++;
        const others = inp.frames!.rows.rows.length - t.covers.length;
        // owner "Stays as today": fewer than two other fish are full rows, never an Other fish list
        if (others < 2) expect(page.keep.otherFish, what).toBeNull();
        else { expect(page.keep.otherFish?.length, what).toBe(others); listed++; }
        // each list line opens its own row (`other:oN` reads the same list, by the same covers)
        (page.keep.otherFish ?? []).forEach((o, i) => expect(page.sheet(o.open)?.title, `${what} ${o.open}`).toBe(o.name));
      }
    }
    expect(reports).toEqual([]);
    expect({ cells: cellsRead > 300, tables: tables > 300, listed: listed > 20 }).toEqual({ cells: true, tables: true, listed: true });
  }, 300_000);
});

describe("T810 N: caveat (b) leaves a covered fish to the table", () => {
  /** No fixture cell has a trout answer that changes by weekday (A0: none by hours either), so one is PLANTED: the
   *  tapped segment gets a twin starting the same day (Sundays), on which rainbow trout reads as Dolly Varden does. */
  async function planted(): Promise<RegsInputs> {
    const inp = await inputsOfCase(V.cases.find((x) => x.name === "skeena-sedan-mill-jul15")!);
    const y = inp.year!, s = inp.s!, at = <T,>(a: readonly T[], v: T) => [...a.slice(0, s + 1), v, ...a.slice(s + 1)];
    const rows = y.rowsBySeg[s]!, sunday = { ...rows, fish: { ...rows.fish, RB: rows.fish.DV! } };
    const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"] as const;
    const moments = y.segStarts.map((_, j) => ({ weekdays: j === s ? days : [...days, "Sunday" as const], hours: null }));
    return { ...inp, year: { ...y, segStarts: at(y.segStarts, y.segStarts[s]!), cells: at(y.cells, y.cells[s]!),
      moments: at(moments, { weekdays: ["Sunday"], hours: null }), ladderBySeg: at(y.ladderBySeg, y.ladderBySeg[s]!),
      rowsBySeg: at(y.rowsBySeg, sunday), displayBySeg: at(y.displayBySeg, y.displayBySeg[s]!) } };
  }
  const named = (p: RegsPage) => p.keep.caveats.filter((c) => c.kind === "timed").flatMap((c) => c.items).map((x) => x.fish);
  it("the plant speaks: with no table in the cell (nothing covered), caveat (b) names rainbow trout", async () => {
    expect(named(pageOf(noTable(await planted())))).toContain("RB");
  });
  it("with the table, it holds rainbow trout and caveat (b) does not name it", async () => {
    const page = pageOf(await planted());
    expect([page.keep.trout?.kind, named(page).includes("RB"), spokenTwice(page)]).toEqual(["table", false, []]);
  });
});

describe("T810 N: a lone other fish beside the table stays as today (drawn in full)", () => {
  it("one row left out of covers is a full row of the main block, with no Other fish list", async () => {
    // the fixture has no water with trout and exactly one other fish: leave ONE drawn row of the frame out of covers (an
    // other fish where the water has one), the rest in (a valid frame: covers ascending, in range)
    let checked = 0;
    for (const c of V.cases) {
      const inp = await inputsOfCase(c);
      const n = inp.frames?.rows.rows.length ?? 0;
      if (!inp.trout || n < 2) continue;
      const card = buildModel(inp, makePlace(inp), settle(inp, makePlace(inp))), drawn = drawnRows(card);
      const free = card.rows.map((_, i) => i).filter((i) => drawn.has(i) && !card.rows.some((r) => r.raw.parent === i));
      const back = free.find((i) => !inp.trout!.covers.includes(i)) ?? free[0];
      if (back === undefined) continue;
      const lone = { ...inp, trout: { ...inp.trout, covers: card.rows.map((_, i) => i).filter((i) => i !== back) } };
      const page = pageOf(lone), full = [...page.keep.groups.map((g) => g.row), ...page.keep.plainRows.map((p) => p.row)];
      // in full: a group or a plain row, no list; a pooled row with no stored card is a list line, as it is today (T670 K4)
      const loose = !card.rows[back]!.raw.group?.headline && !!card.rows[back]!.pool;
      expect(loose ? [full, page.keep.otherFish?.length] : [full, page.keep.otherFish], c.name).toEqual(loose ? [[], 1] : [[back], null]);
      expectDrawnOnce(lone, page, c.name);
      checked++;
    }
    expect(checked).toBeGreaterThan(5);
    expect(reports).toEqual([]);
  });
});

/**
 * NO RULE LOGIC (contract 2.12, spec N): core's trout.ts imports no rows, group or card type and no word but the
 * navigation words and `dayPhrase`. Each import of the file must be one of these (module → the names it may bring in).
 * (Here, not in core: the test reads the source, and core imports no platform.)
 */
const ALLOWED: Readonly<Record<string, readonly string[]>> = {
  "../answers.generated": ["Band", "Box", "DateSet", "TroutValue"],   // the trout payload's own types (generated)
  "../failure": ["reportFailure"],
  "./inputs": ["RegsInputs"],
  "./model": ["Day", "TroutBandVM", "TroutBoxVM", "TroutRefusedVM", "TroutRowVM", "TroutVM"],
  "./strip": ["dayPhrase"],
  "./words": ["TROUT_NAV"],
};
/** The imports a source holds that the list does not allow ("module: name"); a side-effect, star or dynamic import never. */
function importProblems(src: string): string[] {
  const out: string[] = [];
  for (const m of src.matchAll(/^(?:import|export(?=\s+(?:type\s+)?[{*]))\b([^;]*?)\bfrom\s*["']([^"']+)["']/gm)) {
    const mod = m[2]!, names = /\{([^}]*)\}/.exec(m[1]!)?.[1];
    if (!names) { out.push(`${mod}: ${m[1]!.trim()}`); continue; }
    for (const raw of names.split(",")) {
      const n = raw.replace(/^\s*type\s+/, "").split(/\s+as\s+/)[0]!.trim();
      if (n && !(ALLOWED[mod] ?? []).includes(n)) out.push(`${mod}: ${n}`);
    }
  }
  for (const m of src.matchAll(/^\s*import\s+["']([^"']+)["']/gm)) out.push(`${m[1]}: (side effect)`);
  for (const m of src.matchAll(/\b(?:import|require)\s*\(\s*["']([^"']+)["']/g)) out.push(`${m[1]}: (dynamic)`);
  return out;
}

describe("T810 N: trout.ts imports no rule logic", () => {
  const src = readFileSync(resolve(REPO, "app/packages/core/src/regs/trout.ts"), "utf8");
  it("every import is a payload type, the failure reporter, the day phrase or a navigation word", () => {
    expect(importProblems(src)).toEqual([]);
    expect([...src.matchAll(/^import /gm)].length).toBe(6);   // the check saw every import
  });
  it("mutation: a rows type, a card helper, the word table or a star import is refused", () => {
    for (const planted of [
      `import type { Row } from "../answers.generated";`,
      `import type { HydRow } from "./card";`,
      `import { rowsAt } from "./card";`,
      `import { WORDS } from "./words";`,
      `import { fmtDay } from "./strip";`,
      `import * as G from "./grouphead";`,
      `import "./tree";`,
    ]) expect(importProblems(`${planted}\n${src}`), planted).not.toEqual([]);
  });
});

/**
 * (T810 S, contract 2.12 "Never in the app") After the switch the app's own trout decisions are gone: the hand fish lists,
 * the strip's trout line, the steelhead quiet line, the possession pick and its value regex, and the dev switch. Read
 * from the source, as the imports test is (core imports no platform).
 */
describe("T810 S: the app's trout decisions are gone (one trout path)", () => {
  const CORE = resolve(REPO, "app/packages/core/src");
  const sources = (readdirSync(CORE, { recursive: true }) as string[])
    .filter((f) => /\.tsx?$/.test(f) && !/\.test\.tsx?$/.test(f) && !f.includes("testing/"))
    .map((f) => [f, readFileSync(join(CORE, f), "utf8")] as const);
  const GONE = ["TROUTISH", "TROUT_LINE_FISH", "troutLine", "STEEL_LINE", "quietOf", "possessionRules", "possessionValue",
                "possessionCaps", "troutTable"];
  it("no core source names one of them", () => {
    expect(sources.length).toBeGreaterThan(50);
    expect(sources.flatMap(([f, src]) => GONE.filter((n) => new RegExp(`\\b${n}\\b`).test(src)).map((n) => `${f}: ${n}`))).toEqual([]);
  });
  it("the possession row reads page_possession and picks nothing (no days compared, no roles walked, no sentence cut)", () => {
    const src = readFileSync(join(CORE, "regs/standing.ts"), "utf8");
    const body = src.slice(src.indexOf("export function possessionOf"));
    expect(body).toContain("page_possession");
    for (const pick of ["per_daily", "roles", ".sort(", ".match(", "RegExp", "possession_cap"]) expect(body, pick).not.toContain(pick);
  });
});

// ── (T810 P) the fish sheet ─────────────────────────────────────────────────────────────────────────────────────────────

/** A sheet's blocks between the head `from` and the next head (the section's body). */
function section(vm: SheetVM, from: string): SheetBlock[] {
  const i = vm.blocks.findIndex((b) => b.k === "head" && b.text === from);
  if (i < 0) return [];
  const j = vm.blocks.findIndex((b, k) => k > i && b.k === "head");
  return vm.blocks.slice(i + 1, j < 0 ? undefined : j);
}
const lineOf = (b: SheetBlock): string => ("text" in b && Array.isArray(b.text) ? b.text.map((r) => ("s" in r ? r.s : "")).join("") : "");
/** The rule keys a sheet's Sources block lists, in its order. */
const sourceKeys = (bs: readonly SheetBlock[]): (string | null)[] =>
  bs.flatMap((b) => (b.k === "sources" ? b.groups.flatMap((g) => g.rows.map((r) => r.key)) : []));
const linksOf = (vm: SheetVM): string[] =>
  // (T1031 item 3) the sheet's links: its "How to tell a hatchery fish ›" (a link block) and its Source line's "All sources ›"
  vm.blocks.flatMap((b) => (b.k === "source" || b.k === "link" ? b.line.flatMap((r) => (r.t === "link" && typeof r.open === "string" ? [r.open] : [])) : []));
const ascending = (xs: readonly number[]) => xs.every((x, i) => i === 0 || xs[i - 1]! < x);

describe("T810 P: the trout fish sheet over every vector", () => {
  it("every fish of every vector opens its sheet, built only from the payload, every rule list in Python's order", async () => {
    let rows = 0, unsortedSheet = 0, unsortedTable = 0, twins = 0, rails = 0;
    for (const c of V.cases) {
      if (!c.frame) continue;
      const inp = await inputsOfCase(c), page = pageOf(inp);
      expect(page.keep.trout?.kind, c.name).toBe("table");
      const f = c.frame.table, keyOf = (ix: number) => inp.rules.get(ix).key;
      for (const b of f.boxes) for (const r of b.rows) {
        const at = `${c.name} ${r.key}`, vm = page.sheet(`tf:${r.key}`);
        expect(vm, at).not.toBeNull();
        if (!vm) continue;
        rows++; if (r.key.includes("/")) twins++;
        // the header: the row's name, its label or origin word as on the row, the sheet's head (bold, first)
        expect([vm.key, vm.title, vm.sub, vm.blocks[0]], at).toEqual([`tf:${r.key}`, r.name, r.label ?? b.label ?? r.origin,
          { k: "value", text: [{ t: "text", s: r.sheet.head }] }]);
        // Here, today: `here` in order, bold where bold, then the coming change (DateSet.sheet); nothing else
        const here = section(vm, TROUT_SHEET.HERE).map((x) => [lineOf(x), x.k === "value"]);
        expect(here, at).toEqual([...r.sheet.here.map((l) => [l.t, l.bold]), ...(c.frame.dates?.sheet[r.key] ?? []).map((s) => [s, false])]);
        // (T1031) one species sheet, two tabs: this one (Sources) and the row's Identification, each the other's tab
        const id = page.sheet(`tf:${r.key}|id`)!, tabs = [[SPECIES_SHEET.ID, `tf:${r.key}|id`], [SPECIES_SHEET.SRC, `tf:${r.key}`]];
        expect([vm.tabs?.map((x) => [x.label, x.open, x.on]), id.tabs?.map((x) => [x.label, x.open, x.on]), id.title, id.sub], at)
          .toEqual([tabs.map(([l, o], i) => [l, o, i === 1]), tabs.map(([l, o], i) => [l, o, i === 0]), vm.title, vm.sub]);
        // How to tell it (Identification): the manifest's pictures (Dolly Varden: both), the tips, the hatchery link where the
        // row says so; none of it on the Sources tab
        expect(id.blocks.filter((x) => x.k === "image").length, at).toBe(FISH[r.fish]!.length);
        expect(vm.blocks.some((x) => x.k === "image"), at).toBe(false);
        expect(section(id, WORDS.SHEET_LOOK_FOR).flatMap((x) => (x.k === "list" ? x.items.map((i) => lineOf({ k: "para", text: i.line })) : [])), at).toEqual(FISH_TIPS[r.fish]);
        expect([linksOf(id).includes("hatch"), linksOf(vm).includes("hatch")], at).toEqual([r.origin !== null || r.label !== null || r.split || b.label !== null, false]);
        // the fish's own picture key opens the same sheet on Identification (the first row of that fish)
        const first = f.boxes.flatMap((x) => x.rows).find((x) => x.fish === r.fish)!.key;
        expect(page.sheet(`sp:${r.fish}`)?.tabs?.map((x) => x.open), at).toEqual([`tf:${first}|id`, `tf:${first}`]);
        // (T883) How this was decided where the fish's stored roles cover the row's whole list: every rule of `row.src` once
        // (a step, an other rule or a folded one), the answer the sheet's own head; else Sources: `FishSheet.src` exactly.
        // Then "All sources ›" (the table's every rule)
        const flow = vm.blocks.find((x): x is Extract<SheetBlock, { k: "flow" }> => x.k === "flow")?.flow;
        if (flow) {
          rails++;
          expect(flow.result, at).toBe(`So: ${r.sheet.head}`);
          const all = [...flow.steps, ...flow.other, ...flow.quiet], drawn = all.map((x) => x.rule);
          // (re-review a) a rule twice only once per origin, each saying whose; never one origin's role for both
          const whose = (x: (typeof all)[number]) => /^(Hatchery|Wild) ones/.exec(x.tag ?? "")?.[0] ?? "";
          expect(new Set(all.map((x) => `${x.rule}|${whose(x)}`)).size, at).toBe(all.length);
          expect(all.filter((x) => !whose(x)).every((x) => all.filter((y) => y.rule === x.rule).length === 1), at).toBe(true);
          expect(drawn.every((k) => k !== null && r.src.map(keyOf).includes(k)), at).toBe(true);
        }
        // (T1031 item 4) then where each rule comes from: `FishSheet.src` exactly, by authority (each rule's pages highlighted)
        expect(sourceKeys(section(vm, SPECIES_SHEET.BREAKDOWN)), at).toEqual(r.sheet.src.map(keyOf));
        expect(section(vm, TROUT_SHEET.SOURCES), at).toEqual([]);
        expect(r.sheet.src.length, at).toBeLessThanOrEqual(4);
        expect(linksOf(vm).at(-1), at).toBe(TROUT_SOURCES_KEY);
        if (!ascending(r.sheet.src)) unsortedSheet++;
      }
      const all = page.sheet(TROUT_SOURCES_KEY);
      expect([all?.title, sourceKeys(all?.blocks ?? [])], c.name).toEqual([TROUT_SHEET.SOURCES, f.src.map(keyOf)]);
      // (T810 cutover) its sub line names the water and the day, as a row's sources sheet did (SheetHost.test draws it)
      expect(all?.sub?.startsWith(`${page.header.name} · `), `${c.name}: ${all?.sub}`).toBe(true);
      if (!ascending(f.src)) unsortedTable++;
      // a key the table does not hold opens nothing; a cell with no table opens no trout sheet
      const held = new Set(f.boxes.flatMap((b) => b.rows.map((r) => r.key)));
      const absent = ["CT", "DV", "EB", "GB", "LT", "RB", "ST"].flatMap((x) => [x, `${x}/wild`, `${x}/hatchery`]).find((k) => !held.has(k))!;
      expect(page.sheet(`tf:${absent}`), c.name).toBeNull();
      const off = pageOf(noTable(inp));
      expect([off.sheet(`tf:${f.boxes[0]!.rows[0]!.key}`), off.sheet(TROUT_SOURCES_KEY)], c.name).toEqual([null, null]);
    }
    expect(rows).toBeGreaterThan(100);
    expect(rails, "(T883) rows whose stored roles cover their list draw the rail").toBeGreaterThan(0);
    expect(twins).toBeGreaterThan(0);
    // the order guard bites: Python's order is not ascending somewhere, so an app that sorted would fail above
    expect([unsortedSheet > 0, unsortedTable > 0]).toEqual([true, true]);
    expect(reports).toEqual([]);
  });

  it("(T1031 per-row pages) each row sheet's Source line links the pages of EVERY rule behind its row, each with its rules' highlights", async () => {
    const boxKey = (b: { pdf: number; box: readonly number[] }) => `${b.pdf}:${b.box.join(",")}`;
    let rows = 0, beyond = 0, lois = "";
    for (const c of V.cases) {
      if (!c.frame) continue;
      const inp = await inputsOfCase(c), page = pageOf(inp);
      for (const b of c.frame.table.boxes) for (const r of b.rows) {
        const at = `${c.name} ${r.key}`, vm = page.sheet(`tf:${r.key}`)!, last = vm.blocks.at(-1)!;
        expect(last.k, at).toBe("source");
        if (last.k !== "source") continue;
        rows++;
        const line = lineSourcesOf(last.line, vm.title);
        // the pages: every rule of `TRow.src` (Python's whole list, never the breakdown's four), each once, ascending;
        // each page's highlight boxes are every one its citing rules hold on that PDF page (T215)
        const want = new Map<number, { pdf: number; boxes: Set<string> }>();
        for (const ix of r.src) {
          const f = inp.rules.get(ix).facts!;
          expect(f.book.length + (f.cited ? 1 : 0), `${at}: rule ${ix} cites no page`).toBeGreaterThan(0);
          for (const [printed, pdf] of f.book) {
            const w = want.get(printed) ?? { pdf, boxes: new Set<string>() };
            for (const x of f.book_box ?? []) if (x.pdf === pdf) w.boxes.add(boxKey(x));
            want.set(printed, w);
          }
        }
        const got = line.links.flatMap((l) => (l.k === "book" ? [[l.printed, l.pdf, new Set((l.box ?? []).map(boxKey))] as const] : []));
        expect(got, at).toEqual([...want.entries()].sort(([a], [z]) => a - z).map(([p, w]) => [p, w.pdf, w.boxes]));
        expect(line.links.at(-1), at).toMatchObject({ k: "sheet", label: TROUT_SHEET.ALL_SOURCES, open: TROUT_SOURCES_KEY });
        // the breakdown's pages are all on the line; some rows' lines carry a page the breakdown (cut to 4) does not
        const listed = new Set(r.sheet.src.flatMap((ix) => inp.rules.get(ix).facts!.book.map(([p]) => p)));
        if ([...want.keys()].some((p) => !listed.has(p))) beyond++;
        if (c.name === "lois-jul15" && r.key === "DV") lois = sourcesText(line);
      }
    }
    expect(rows).toBeGreaterThan(300);
    // (the measure, 2026-11-24: 19 of 356 rows) a page only the cut rules cite: a line built from `FishSheet.src` fails here
    expect(beyond).toBeGreaterThan(10);
    // AGENTS 33, the whole line: Lois Lake's Dolly Varden row is Region 2's (p.21) AND the lake's own row's (p.24: its
    // "Hatchery only: wild ones go back", the 5th rule of the row, past the breakdown's four)
    expect(lois).toBe("Source B.C. Synopsis p.21 ↗ p.24 ↗ All sources ›");
    expect(reports).toEqual([]);
  });

  it("every fish code of the vectors has a picture and tips (a missing one is listed here, and reported on the sheet)", () => {
    const codes = new Set(V.cases.flatMap((c) => c.frame?.table.boxes.flatMap((b) => b.rows.map((r) => r.fish)) ?? []));
    expect(codes.size).toBeGreaterThan(4);
    expect([...codes].filter((x) => !FISH[x]?.length).map((x) => `${x}: no picture`)).toEqual([]);
    expect([...codes].filter((x) => !FISH_TIPS[x]?.length).map((x) => `${x}: no tips`)).toEqual([]);
    expect(FISH.DV!.length).toBe(2);   // Dolly Varden and bull trout, both drawn
  });

  it("a fish the manifest lacks is reported, never drawn as a blank section", async () => {
    const c = V.cases.find((x) => x.frame)!, inp = await inputsOfCase(c), t = pageOf(inp).keep.trout as TroutVM;
    const r = t.boxes[0]!.rows[0]!;
    const vm = troutIdSheet(`tf:${r.key}|id`, t, r.key, {});   // (T1031) the pictures are the Identification tab's
    expect(vm!.blocks.some((b) => b.k === "image")).toBe(false);
    expect(reports.map((e) => [(e as TroutLooksMissing).fish, (e as TroutLooksMissing).what])).toEqual([[r.fish, "picture"]]);
  });
});

describe("T892: the trout fold's tributary line (keepOf → KeepVM.troutFrom) over every vector", () => {
  it("set only where every row's rail is set, for every origin, by the same waters' tributary rules; none on Shamrock Creek", async () => {
    const lines: Record<string, string | null> = {}, named: Record<string, string[]> = {};
    for (const c of V.cases) {
      const inp = await inputsOfCase(c), page = pageOf(inp), t = page.keep.trout;
      const rows = (t?.kind === "table" ? t.boxes.flatMap((b) => b.rows) : []).map((r) => railSetters(
        page.sheet(`tf:${r.key}`)?.blocks.find((x): x is Extract<SheetBlock, { k: "flow" }> => x.k === "flow")?.flow ?? null, r.origin));
      const all = rows.length > 0 && rows.every((x) => x.setByDown && x.names.join("|") === rows[0]!.names.join("|"));
      if (page.keep.troutFrom) expect(all, c.name).toBe(true);   // a line: every row's rail set by those waters (the sweep proves the rest)
      lines[c.name] = page.keep.troutFrom ?? null;
      named[c.name] = [...new Set(rows.flatMap((x) => x.mentions))];
    }
    // (review 2-1) Lonzo Creek's only step on Shamrock Creek's rows is the hatchery ones' size rule; the number and the
    // wild release are Region 2's: its rows' sheets name Lonzo Creek, the fold says nothing
    expect([lines["shamrock-jul15"], lines["kilgard-jul15"], named["shamrock-jul15"]]).toEqual([null, null, ["Lonzo Creek"]]);
  });
});
