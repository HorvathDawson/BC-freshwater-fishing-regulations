/**
 * (T1031 per-row pages) A trout row sheet's Source line over hand-built rules: the pages of every rule of `TRow.src` in
 * one line, each with its own highlight boxes, then "All sources ›"; a cited index the page's rules lack is REPORTED and
 * names no page (W46), never a silent gap.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { setFailureSink } from "../../failure";
import { lineSourcesOf, sourcesText } from "../../tabSources";
import type { RuleIndex } from "../inputs";
import type { SheetKey } from "../sheetKey";
import type { TroutVM } from "../model";
import { TROUT_SHEET } from "../words";
import { TROUT_SOURCES_KEY, TroutRowRuleMissing, troutFishSheet } from "./trout";

const rule = (ix: number, book: [number, number][], box: unknown[] = []) =>
  ({ key: `r::${ix}`, missing: false, kind: "quota", f: {}, export: { verbatim: `words ${ix}` }, facts: { title: `Rule ${ix}`, book, book_box: box } });
const BOX = { pdf: 26, box: [1, 2, 3, 4] };
const RULES: Record<number, unknown> = {
  1: rule(1, [[21, 23]]),
  2: rule(2, [[24, 26]], [BOX]),
  3: rule(3, [[21, 23]]),
};
const STAND_IN = { key: "", missing: true, kind: "quota", f: {}, export: { verbatim: "" }, facts: null };
const INDEX = { get: (ix: number) => RULES[ix] ?? STAND_IN } as unknown as Pick<RuleIndex, "get">;

const table = (src: number[]): TroutVM => ({ boxes: [{ label: null, rows: [{ key: "DV", fish: "DV", name: "Dolly Varden/bull trout", origin: null,
  label: null, src, coming: [], sheet: { head: "Keep", here: [], src: src.slice(0, 1) } }] }] }) as unknown as TroutVM;
const lineOf = (src: number[]) => {
  const vm = troutFishSheet("tf:DV" as SheetKey, table(src), "DV", INDEX)!, last = vm.blocks.at(-1)!;
  if (last.k !== "source") throw new Error("the sheet's last block is not its Source line");
  return lineSourcesOf(last.line, vm.title);
};

let reports: unknown[] = [], undo: () => void = () => {};
beforeEach(() => { reports = []; undo = setFailureSink((e) => reports.push(e)); });
afterEach(() => undo());

describe("a trout row sheet's Source line (T1031 per-row pages)", () => {
  it("names every page of every rule behind the row, once each, ascending, each with its rules' boxes, then All sources", () => {
    const line = lineOf([2, 1, 3]);
    expect(sourcesText(line)).toBe(`Source B.C. Synopsis p.21 ↗ p.24 ↗ ${TROUT_SHEET.ALL_SOURCES}`);
    expect(line.links.map((l) => (l.k === "book" ? [l.printed, l.box ?? []] : l.k === "sheet" ? l.open : null)))
      .toEqual([[21, []], [24, [BOX]], TROUT_SOURCES_KEY]);
    expect(reports).toEqual([]);
  });

  it("reports a cited index the page's rules lack, and names no page for it", () => {
    const line = lineOf([1, 9]);
    expect(sourcesText(line)).toBe(`Source B.C. Synopsis p.21 ↗ ${TROUT_SHEET.ALL_SOURCES}`);
    expect(reports.map((e) => [(e as Error).name, (e as TroutRowRuleMissing).row, (e as TroutRowRuleMissing).ix])).toEqual([["TroutRowRuleMissing", "DV", 9]]);
  });
});
