/**
 * (T1031 item 3) THE ONE SOURCE LINE on the fixture states' real pages (the parity store: the committed fixture release in
 * CI). Two holds:
 *  1. Each sheet draws AT MOST ONE Source line, and that line is its last block: a link that is not a source (the trout
 *     fish sheet's "How to tell a hatchery fish ›", #69 review) is a `link` block, never a second "Source" line.
 *  2. WHAT IS CITED IS UNCHANGED (owner brief: "Don't change what is cited, only how it is shown"): the synopsis pages of
 *     every block that shows a source — the Gear / Ways to fish / Boats / Reference lines, each closure on the closed
 *     banner, each sheet's source block, the trout table (its drawn rows: no page) — are PINNED below as the base branch drew
 *     them (feat/T991-sources-per-tab at 40b90ba94, before this change: the same collector run there), and the line each
 *     block now draws (`sectionSourcesOf` / `lineSourcesOf` / `troutSourcesOf`) links exactly those pages. One addition
 *     since (T1031, trout per-row pages): each trout row sheet's Source line cites its row's pages (`ROW_PAGES`).
 */
import { describe, expect, it } from "vitest";
import {
  buildRegsPage, lineSourcesOf, sectionSourcesOf, troutSourcesOf, type Line, type RegsPage, type SheetBlock, type SheetVM,
  type SheetKey, type SourceLineVM, type TabSourcesVM,
} from "@app/core";
import { EDGE_FIXTURE_STATES, FIXTURE_STATES, STYLE_FIXTURE_STATES, sheetKeysOf } from "./fixtures";
import { ADULT_RESIDENT, inputsOf, parityGate, requestOf, taps } from "./golden";
import { pin } from "../pins";

const gate = await parityGate();
// a state the re-cut declared gone (taps.json `gone`: its part merged away) has no tap to build from
const STATES = [...new Set<string>([...FIXTURE_STATES, ...STYLE_FIXTURE_STATES, ...EDGE_FIXTURE_STATES])].filter((id) => !(id in (taps().gone ?? {}))).sort();

async function pageOf(id: string): Promise<RegsPage | null> {
  const inp = await inputsOf(id);
  return inp && buildRegsPage(inp, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
}
/** Every sheet the page can open, the trout table's row sheets and every-rule sheet among them. */
function sheetsOf(page: RegsPage): [string, SheetVM][] {
  const t = page.keep.trout;
  const keys = [...new Set([...sheetKeysOf(page), ...(t?.kind === "table" ? ["src:trout", ...t.boxes.flatMap((b) => b.rows.map((r) => `tf:${r.key}`))] : [])])];
  return keys.sort().flatMap((k): [string, SheetVM][] => { const vm = page.sheet(k as SheetKey); return vm ? [[k, vm]] : []; });
}
const linePages = (l: Line): number[] => l.flatMap((r) => (r.t === "book" ? [r.printed] : []));
/** Every page a value's book runs (`{t:"book"}`) name, walked whole. */
const bookRuns = (v: unknown): number[] => (Array.isArray(v) ? v.flatMap(bookRuns)
  : v && typeof v === "object" ? ((v as { t?: unknown; printed?: unknown }).t === "book" && typeof (v as { printed?: unknown }).printed === "number"
    ? [(v as { printed: number }).printed] : Object.values(v).flatMap(bookRuns)) : []);
const nested = (bs: readonly SheetBlock[]): SheetBlock[] => bs.flatMap((b) => [b, ...(b.k === "fold" ? nested(b.body) : [])]);
const drawnPages = (vm: TabSourcesVM): number[] => vm.links.flatMap((l) => (l.k === "book" ? [l.printed] : []));
const uniqSorted = (xs: number[]) => [...new Set(xs)].sort((a, b) => a - b);

/** Each source-showing block of a page and the pages it cites, read off the view model (what the base drew from). */
function citedPerBlock(page: RegsPage): Record<string, number[]> {
  const out: Record<string, number[]> = {};
  const src = (name: string, s: SourceLineVM | null | undefined) => { if (s) out[name] = s.pages.map((p) => p.printed); };
  src("gear", page.gear?.src);
  src("ways", page.methods?.ways?.src);
  src("boats", page.methods?.boats?.src);
  src("reference", page.methods?.reference?.src);
  (page.keep.banner?.chain ?? []).forEach((c, i) => { out[`banner:${i}`] = linePages(c.book); });
  for (const [k, vm] of sheetsOf(page)) {
    // a sheet's source block that cites a page (a link alone, "All sources ›", cites none)
    nested(vm.blocks).forEach((b, i) => { if (b.k === "source" && linePages(b.line).length) out[`sheet:${k}#${i}`] = linePages(b.line); });
    // (#69 review) the trout table cites what it DRAWS: a page link among its rows (it draws none: its rows are
    // Python's words), never its every-rule sheet's pages (p.6 on the Vedder: a quota and a duty the table never says)
    if (k === "src:trout") out["trout"] = uniqSorted(bookRuns(page.keep.trout));
  }
  return out;
}

/** The pages each block's Source line now links, by the composer that draws it. */
function drawnPerBlock(page: RegsPage): Record<string, number[]> {
  const out: Record<string, number[]> = {};
  const src = (name: string, s: SourceLineVM | null | undefined) => { if (s) out[name] = drawnPages(sectionSourcesOf(s, name)); };
  src("gear", page.gear?.src);
  src("ways", page.methods?.ways?.src);
  src("boats", page.methods?.boats?.src);
  src("reference", page.methods?.reference?.src);
  (page.keep.banner?.chain ?? []).forEach((c, i) => { out[`banner:${i}`] = drawnPages(lineSourcesOf(c.book, c.text, `rule:${c.rule}`)); });
  for (const [k, vm] of sheetsOf(page)) {
    nested(vm.blocks).forEach((b, i) => { if (b.k === "source" && linePages(b.line).length) out[`sheet:${k}#${i}`] = drawnPages(lineSourcesOf(b.line, vm.title)); });
    if (k === "src:trout") out["trout"] = drawnPages(troutSourcesOf(page));
  }
  return out;
}

/** The base branch's pages per block (feat/T991-sources-per-tab 40b90ba94, `citedPerBlock` run there over the fixture release). */
const PINNED: Record<string, Record<string, number[]>> = {
  glenn_501: { "gear": [8, 34, 36], "ways": [8], "reference": [8, 10], "sheet:pos:pos#6": [34], "trout": [] },
  glenn_615: { "gear": [8, 34, 36], "ways": [8], "reference": [8, 10], "sheet:pos:pos#6": [34], "trout": [] },
  klwa_1010: { "gear": [8], "ways": [8], "reference": [8, 10], "sheet:pos:pos#6": [34], "trout": [] },
  klwa_1012: { "gear": [8], "ways": [8], "reference": [8, 10], "sheet:pos:pos#6": [34], "trout": [] },
  vedder_1010: { "gear": [8, 21, 22], "ways": [8], "reference": [8, 10], "sheet:pos:pos#6": [21], "trout": [] },
  vedder_515: { "gear": [8, 21, 22], "ways": [22], "reference": [8, 10], "sheet:pos:pos#6": [21], "trout": [] },
  vedder_615: { "banner:0": [22] },
  x15p2_1010: { "gear": [8, 21, 23], "ways": [8], "boats": [23], "reference": [8, 10], "sheet:pos:pos#6": [21], "trout": [] },
  x31p_1010: {  },
  x35p_1010: { "gear": [8], "ways": [8], "reference": [8, 10], "sheet:pos:pos#6": [49], "trout": [] },
};

/**
 * (T1031, trout per-row pages) ADDED on top of the base: each trout row sheet's Source line (`tf:<row>`, was "All sources ›"
 * alone, no page) cites the pages of every rule behind its row (`TRow.src`, uncut), one run per rule in Python's order,
 * repeats as the runs hold them (the line draws each page once): a page beside the zone's where a rule printed there
 * decides the row (glenn DV p.40, vedder RB p.22, x15p2 ST p.23, the Vedder steelhead's p.6 licence duties).
 */
const ROW_PAGES: Record<string, Record<string, number[]>> = {
  klwa_1010: { "sheet:tf:CT#9": [37, 34, 34], "sheet:tf:DV#9": [37, 37, 34], "sheet:tf:EB#8": [37, 34], "sheet:tf:GB#8": [37, 34], "sheet:tf:LT#8": [37, 34], "sheet:tf:RB#9": [37, 34, 34] },
  klwa_1012: { "sheet:tf:CT#9": [37, 34, 34], "sheet:tf:DV#9": [37, 37, 34], "sheet:tf:EB#8": [37, 34], "sheet:tf:GB#8": [37, 34], "sheet:tf:LT#8": [37, 34], "sheet:tf:RB#9": [37, 34, 34] },
  vedder_1010: { "sheet:tf:CT#10": [21, 21, 21, 21, 21, 21], "sheet:tf:DV#11": [21, 21, 21, 21, 21, 21, 21], "sheet:tf:EB#10": [21, 21, 21, 21, 21, 21], "sheet:tf:GB#10": [21, 21, 21, 21, 21, 21], "sheet:tf:LT#11": [21, 21, 21, 21, 21, 21, 21], "sheet:tf:RB#10": [22, 21, 21, 21], "sheet:tf:ST#13": [21, 21, 6, 21, 6, 21, 21, 21, 21] },
  vedder_515: { "sheet:tf:CT#7": [22, 21, 21], "sheet:tf:DV#12": [21, 21, 21, 21, 21, 21, 21], "sheet:tf:EB#11": [21, 21, 21, 21, 21, 21], "sheet:tf:GB#11": [21, 21, 21, 21, 21, 21], "sheet:tf:LT#12": [21, 21, 21, 21, 21, 21, 21], "sheet:tf:RB#7": [22, 21, 21], "sheet:tf:ST#12": [21, 21, 6, 21, 6, 21, 21, 21, 21] },
  x15p2_1010: { "sheet:tf:CT#10": [21, 21, 21, 21, 21, 21], "sheet:tf:DV#11": [21, 21, 21, 21, 21, 21, 21], "sheet:tf:EB#10": [21, 21, 21, 21, 21, 21], "sheet:tf:GB#10": [21, 21, 21, 21, 21, 21], "sheet:tf:LT#11": [21, 21, 21, 21, 21, 21, 21], "sheet:tf:RB#9": [21, 21, 21, 21, 21], "sheet:tf:ST#8": [23, 21] },
  x35p_1010: { "sheet:tf:CT#9": [49, 49, 49], "sheet:tf:DV#10": [49, 49, 49, 49], "sheet:tf:EB#9": [49, 49, 49], "sheet:tf:GB#9": [49, 49, 49], "sheet:tf:LT#12": [50, 50, 49, 49, 50, 50], "sheet:tf:RB#9": [49, 49, 49] },
};
/** (T1135) Glenn Creek's row pages move with the release (0.10.0: the Lardeau's p.38 reached it, a sheet line went): they
 *  are release pins (app/fixtures/golden/release-pins.json, `sourceLine.rowPages.<state>`), re-cut with the fixtures. */
const PINNED_ROWS = ["glenn_501", "glenn_615"];
const rowsOf = (m: Record<string, number[]> | undefined) => Object.fromEntries(Object.entries(m ?? {}).filter(([k]) => k.startsWith("sheet:tf:")));
const EXPECTED = Object.fromEntries(Object.entries(PINNED).map(([id, m]) => [id, { ...m, ...ROW_PAGES[id] }]));

describe.skipIf(gate.skip)("the one Source line on the fixture pages (T1031 item 3)", () => {
  it("each sheet draws at most one Source line, as its last block", async () => {
    const bad: string[] = [];
    for (const id of STATES) {
      const page = await pageOf(id);
      if (!page) continue;
      for (const [k, vm] of sheetsOf(page)) {
        const all = nested(vm.blocks), at = vm.blocks.findIndex((b) => b.k === "source");
        const n = all.filter((b) => b.k === "source").length;
        if (n > 1 || (n === 1 && at !== vm.blocks.length - 1)) bad.push(`${id} ${k}: ${n} source block(s), top-level at ${at} of ${vm.blocks.length}`);
      }
    }
    expect(bad).toEqual([]);
  }, 60_000);

  it("cites per block exactly what the base branch cited, and each block's line links exactly those pages", async () => {
    const cited: Record<string, Record<string, number[]>> = {}, drawn: Record<string, Record<string, number[]>> = {};
    for (const id of STATES) {
      const page = await pageOf(id);
      if (!page) continue;
      cited[id] = citedPerBlock(page);
      drawn[id] = Object.fromEntries(Object.entries(drawnPerBlock(page)).map(([k, v]) => [k, v]));
    }
    if (process.env.PRINT_SOURCE_PAGES === "1") console.log(`PINNED ${JSON.stringify(cited)}`);
    for (const id of PINNED_ROWS) pin(`sourceLine.rowPages.${id}`, rowsOf(cited[id]));
    expect(cited).toEqual({ ...EXPECTED, ...Object.fromEntries(PINNED_ROWS.map((id) => [id, { ...EXPECTED[id], ...rowsOf(cited[id]) }])) });
    // a block's line: its pages once each, ascending (the order the line draws them)
    expect(drawn).toEqual(Object.fromEntries(Object.entries(cited).map(([id, m]) => [id, Object.fromEntries(Object.entries(m).map(([k, v]) => [k, uniqSorted(v)]))])));
  }, 60_000);
});
