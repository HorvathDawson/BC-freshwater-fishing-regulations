/**
 * (T810 N) THE TROUT TABLE'S VIEW MODEL against the shared vectors and the trout fixture release (always run: committed),
 * with the dev switch on (`troutTable`). The proof column of the build spec's N row:
 * - whole strings per vector: the top line with both day words is the vector's `composed.lead_today` / `lead_on`, and
 *   every other drawn string is the frame's own (none added or changed, contract fight list #5);
 * - each hand case of `trout-hand.json` refused through `reportFailure`, no table drawn (the base frame is drawn);
 * - the screen-reader golden over every vector row (`app/fixtures/golden/trout-a11y.json`; `UPDATE_TROUT_A11Y=1` writes
 *   it, through `regs-recut/sync.mjs`, never by hand);
 * - DRAWN EXACTLY ONCE: over every vector cell and every cell of the trout fixture release, each rows-frame row is drawn
 *   once: by the table (`covers`), as a full row of the main block, or as a line of the Other fish list;
 * - a lone other fish beside the table is drawn in full; no fish is named by both caveat (b) and the table;
 * - the switch off: the page is today's (no `trout`, the old split).
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  buildModel, buildRegsPage, drawnRows, listRows, makePlace, segmentIndex, setFailureSink, settle, tableCovers, tapDay, troutOf,
  TROUT_NAV, TroutRefused, type RegsInputs, type RegsPage, type RuleIndex, type TroutValue, type TroutVM,
} from "@app/core";
import { openTroutFixture, TROUT_FIXTURE_ROOT } from "@app/data/release/node";
import { loadRegsInputs, openRegsStore, targetFor, type RegsStore } from "@app/data/regs";
import { ADULT_RESIDENT, REPO } from "./golden";

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
beforeEach(() => { reports = []; undo = setFailureSink((e) => reports.push(e)); });
afterEach(() => undo());

/** The case's section, inside its own water (as inputs.release.test.ts finds it). */
async function sectionOf(c: Vector): Promise<number> {
  const ord = (await st.itemOrd(c.item))!, parts = await st.parts(ord), mine = new Set<number>();
  for (const p of parts) for (const sid of (await st.partLengths(ord, p.partIx)).keys()) mine.add(sid);
  if ("lake" in c.anchor) return Math.min(...mine);
  const { stream, m } = c.anchor;
  return LINES.find(([h, b, lo]) => b === stream && lo === m && mine.has(h))![0];
}
async function inputsOfCase(c: Vector): Promise<RegsInputs> {
  const [mm, dd] = c.day.split("-").map(Number);
  return loadRegsInputs(st, await targetFor(st, c.item, await sectionOf(c)), { month: mm!, day: dd! },
    WEEKDAY[c.weekday as keyof typeof WEEKDAY], 2026);
}
const pageOf = (inp: RegsInputs, on: boolean, today = inp.day): RegsPage =>
  buildRegsPage(inp, { today, who: ADULT_RESIDENT, ...(on ? { troutTable: true } : {}) });

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
  for (const i of listRows(card, tableCovers(inp, true)) ?? []) n[i]! += 1;
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

describe("T810 N: the trout view model over every vector (dev switch on)", () => {
  it("the vectors hold tables and null cells both", () => {
    expect(V.cases.filter((c) => c.frame !== null).length).toBeGreaterThan(40);
    expect(V.cases.some((c) => c.frame === null)).toBe(true);
  });

  it.each(V.cases.map((c) => [c.name, c] as const))("%s", async (_n, c) => {
    const inp = await inputsOfCase(c);
    expect(inp.trout).toEqual(c.frame);
    const on = pageOf(inp, true), off = pageOf(inp, false);
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
    // drawn exactly once; no fish spoken twice; the switch off is today's page
    expectDrawnOnce(inp, on, c.name);
    expect(spokenTwice(on)).toEqual([]);
    expect(off.keep.trout).toBeUndefined();
    expect(off.keep.order).not.toContain("trout");
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
  it("the file holds the contract's fourteen", () => expect(HAND.cases).toHaveLength(14));
  it.each(HAND.cases.map((c) => [c.name, c] as const))("%s", (_n, c) => {
    const t = troutOf(fake(c.frame, ROWS), { month: 7, day: 15 }, { month: 7, day: 15 });
    expect(t?.kind).toBe("refused");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toBeInstanceOf(TroutRefused);
  });
  it("covers past the rows frame: refused (the reader's check)", () => {
    expect(troutOf(fake(HAND.base, ROWS - 1), { month: 7, day: 15 }, { month: 7, day: 15 })?.kind).toBe("refused");
    expect((reports[0] as TroutRefused).why).toMatch(/covers names row 2 of a rows frame of 2/);
  });
  it("a refused table on a page: reported, the block names the refusal, its rows drawn by nothing else", async () => {
    const c = V.cases.find((x) => x.name === "skeena-sedan-mill-jul15")!;
    const inp = await inputsOfCase(c);
    const bad = { ...inp, trout: { ...inp.trout!, table: { ...inp.trout!.table, hatch_link: !inp.trout!.table.hatch_link } } };
    const page = pageOf(bad, true);
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

describe("T810 N: every cell of the trout fixture release (dev switch on)", () => {
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
      if (inp.trout === undefined) continue;
      cellsRead++;
      const page = pageOf(inp, true), what = `${inp.item?.id} part ${inp.partIx} ${inp.day.month}-${inp.day.day}`;
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
  it("the plant speaks: with the switch off, caveat (b) names rainbow trout", async () => {
    expect(named(pageOf(await planted(), false))).toContain("RB");
  });
  it("with the switch on, the table holds rainbow trout and caveat (b) does not name it", async () => {
    const page = pageOf(await planted(), true);
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
      const page = pageOf(lone, true), full = [...page.keep.groups.map((g) => g.row), ...page.keep.plainRows.map((p) => p.row)];
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
