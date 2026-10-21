/**
 * ONE TAP → `RegsInputs` (T09): everything the Regulations page needs for a tap, read from the store once, so core turns
 * it into a page without another read — the water, its parts and picker, the part's key, the day's segment and its five
 * frames, every segment's ladder / rows / display frames (the year), the display frame of every leap day, the rules seen
 * through the part's via, licensing words, statics, and (a closed-all-year picker group) the sibling parts' closures.
 *
 * Owner rule 2026-10-21: the store holds every answer (verdicts, decided rows, display status and closing). This file
 * only SELECTS them — which key, which segment of the day, which frames — through the pipeline's own tap
 * (`encode.tap` / `tap_key`: T05 `segmentIndex`); it decides nothing.
 *
 * Ports: page_v36.js @ 7fb6809f `makePlace` / `segStarts` / `momentsOf` / `segOf` / `frameIx` / `choicesOf` / `groupOf` /
 * `mergedHtml` (the reads only); `pipeline/deliver/answers/encode.py` `tap` / `tap_key`; `store/decode.py` `unnamed_name`.
 */
import {
  broadOf, ruleIndex, segmentIndex, tapDay, viaOf, yearSegments,
  type AkeyRow, type CellIds, type Day, type DisplayFrame, type LadderFrameWire, type MergedPart, type Moment, type Picker,
  type RegsInputs, type RegsTarget, type RegsYear, type RowsValue, type RuleIndex, type Statics, type Weekday,
} from "@app/core";
import { RegsStoreError } from "./errors";
import type { PartRow, RegsReads, StoreStatics, StoredRule } from "./reads";

/** A tap the store cannot answer: no such water, no such part, an unnamed section the store names no place for. */
export class RegsInputsError extends Error {
  override name = "RegsInputsError";
}

/** The store as this file reads it (a `RegsStore` is one). */
export type InputsStore = RegsReads;

/** The display frame of every leap day 1..366 (index d-1) — the SAME object for days of one segment (T13/T42 read it). */
export function displayByDay(year: Omit<RegsYear, "displayByDay">, calendarYear: number): (DisplayFrame | null)[] {
  return yearSegments(year.segStarts, year.moments, calendarYear).map((s) => year.displayBySeg[s] ?? null);
}

/** The statics as `RegsInputs` carries them: the store's, as read (`pdfPages` the release's printed → PDF page map). */
const staticsOf = (st: StoreStatics): Statics => st;

/** Every distinct id of one frame family over a key's cells, read once each (a key's segments often share frames). */
async function byId<T>(cells: readonly CellIds[], fam: keyof CellIds, read: (id: number) => Promise<T>): Promise<T[]> {
  const ids = [...new Set(cells.map((c) => c[fam]))];
  const got = new Map(await Promise.all(ids.map(async (id) => [id, await read(id)] as const)));
  return cells.map((c) => got.get(c[fam])!);
}

/** The key's year and the tap's cell: steps 5–8 of the read sequence. */
async function keyed(store: InputsStore, key: AkeyRow, day: Day, weekday: Weekday, calendarYear: number) {
  const a = key.akey;
  const [segStarts, moments, cells, ruleset] = await Promise.all([
    store.segStarts(key.seg), key.mom === null ? Promise.resolve(null) : store.segMoments(key.mom), store.cells(a),
    store.ruleset(key.ruleset)]);
  if (cells.length !== key.nseg || segStarts.length !== key.nseg)
    throw new RegsStoreError("damaged", `regs store: akey ${a} has ${cells.length} cells and ${segStarts.length} segment starts, not ${key.nseg}`);
  const s = segmentIndex(segStarts, tapDay(day, calendarYear).day, moments, weekday, false);
  const cell = cells[s]!;
  const [ladderBySeg, rowsBySeg, displayBySeg, gear, licence] = await Promise.all([
    byId<LadderFrameWire>(cells, "ladder", (id) => store.ladder(a, id)),
    byId<RowsValue>(cells, "rows", (id) => store.rows(a, id)),
    byId<DisplayFrame>(cells, "display", (id) => store.display(a, id)),
    store.gear(a, cell.gear), store.licence(a, cell.licence)]);
  const partial = { segStarts, moments: moments as readonly Moment[] | null, cells, ladderBySeg, rowsBySeg, displayBySeg };
  const year: RegsYear = { ...partial, displayByDay: displayByDay(partial, calendarYear) };
  const frames = { ladder: ladderBySeg[s]!, rows: rowsBySeg[s]!, gear, licence, display: displayBySeg[s]! };
  return { s, cell, year, frames, ruleset: { reach: ruleset.reach, trib: ruleset.trib } };
}

/** The tap → `RegsInputs`. Throws `RegsInputsError` for a water or part the store does not hold, a `RegsStoreError` for a
 *  refused / damaged store, a `TapError` for a date that does not exist. */
export async function loadRegsInputs(store: InputsStore, target: RegsTarget, day: Day, weekday: Weekday,
                                     calendarYear: number): Promise<RegsInputs> {
  const [st, stored, lics] = await Promise.all([store.statics(), store.rules(), store.lics()]);
  const statics = staticsOf(st), sp = st.exportSpecies;
  const base = { target, day, weekday, calendarYear, lics, statics };
  const outside = (rules: RuleIndex) =>
    ({ key: null, s: null, cell: null, frames: null, year: null, ruleset: null, rules, mergedParts: null }) as const;

  if (target.kind === "section") {
    const a = await store.sectionAkey(target.handle);
    if (a === 0) return { ...base, item: null, partIx: 0, place: null, ...outside(ruleIndex(stored, new Map(), sp)) };
    const pl = await store.sectionPlace(target.handle);
    if (pl === null) throw new RegsInputsError(`regs: section ${target.handle} has a key (${a}) but no place`);
    const [placeRow, key] = await Promise.all([store.place(pl), store.akey(a)]);
    const name = (placeRow.names as Readonly<Record<string, string>>)[key.kind];
    if (name === undefined) throw new RegsInputsError(`regs: place ${pl} has no name for a ${key.kind}`);
    const k = await keyed(store, key, day, weekday, calendarYear);
    return { ...base, item: null, partIx: 0, place: { label: placeRow.label, name }, key, s: k.s, cell: k.cell,
             frames: k.frames, year: k.year, ruleset: k.ruleset, rules: ruleIndex(stored, viaOf(k.ruleset), sp), mergedParts: null };
  }

  const ord = await store.itemOrd(target.item);
  if (ord === null) throw new RegsInputsError(`regs: no water ${target.item} in the regs store`);
  const [it, parts] = await Promise.all([store.item(ord), store.parts(ord)]);
  const picker = it.picker === null ? null : await store.picker(it.picker);
  const { partIx } = target;
  if (!Number.isInteger(partIx) || partIx < 0 || partIx >= parts.length)
    throw new RegsInputsError(`regs: ${target.item} has ${parts.length} part(s), no part ${partIx}`);
  const item = { id: it.id, ord: it.ord, name: it.name, kind: it.kind, sections: it.sections,
                 parts: parts.map((p) => p.facts), partSections: parts.map((p) => p.sections), picker, steelhead: it.steelhead };
  const part = parts[partIx]!;
  if (part.akey === null) return { ...base, item, partIx, place: null, ...outside(ruleIndex(stored, new Map(), sp)) };

  const key = await store.akey(part.akey);
  const k = await keyed(store, key, day, weekday, calendarYear);
  return { ...base, item, partIx, place: null, key, s: k.s, cell: k.cell, frames: k.frames, year: k.year, ruleset: k.ruleset,
           rules: ruleIndex(stored, viaOf(k.ruleset), sp),
           mergedParts: await mergedParts(store, stored, sp, parts, picker, partIx, day, weekday, calendarYear) };
}

/** page mergedHtml's reads: only when the part's picker group is closed and holds ≥ 2 parts — each part's label, sections
 *  and the first of its day's decided closures (`broadOf` over its display frame, through its own via). */
async function mergedParts(store: InputsStore, stored: ReadonlyMap<number, StoredRule>, sp: StoreStatics["exportSpecies"],
                           parts: readonly PartRow[], picker: Picker | null, partIx: number, day: Day, weekday: Weekday, calendarYear: number): Promise<readonly MergedPart[] | null> {
  const g = picker?.choices.find((c) => c.parts.includes(partIx));
  if (!g?.closed || g.parts.length < 2) return null;
  const reading = tapDay(day, calendarYear).day;
  return Promise.all(g.parts.map(async (i): Promise<MergedPart> => {
    const p = parts[i];
    if (!p) throw new RegsInputsError(`regs: the picker names part ${i}, which the water does not have`);
    const head = { partIx: i, label: p.facts?.label ?? "", sections: p.sections };
    if (p.akey === null) return { ...head, broad: null };
    const k = await store.akey(p.akey);
    const [starts, moms, rs] = await Promise.all([store.segStarts(k.seg), k.mom === null ? null : store.segMoments(k.mom),
      store.ruleset(k.ruleset)]);
    const c = await store.cell(k.akey, segmentIndex(starts, reading, moms, weekday, false));
    const f = await store.display(k.akey, c.display);
    return { ...head, broad: broadOf(f, ruleIndex(stored, viaOf(rs), sp))[0] ?? null };
  }));
}

/**
 * The part a reader's tap opens. A tapped section resolves through water `part_section` (the part that holds it, P09);
 * a section the item's parts do not list, through its key: the first part whose akey is the section's (`section_akey`;
 * two parts sharing a key → the lower part_ix). Else — or with no section — the first part with a key, else part 0.
 */
export async function targetFor(store: InputsStore, item: string, section: number | null): Promise<RegsTarget> {
  const ord = await store.itemOrd(item);
  if (ord === null) throw new RegsInputsError(`regs: no water ${item} in the regs store`);
  const parts = await store.parts(ord);
  const at = (partIx: number): RegsTarget => ({ kind: "part", item, partIx });
  if (section !== null) {
    const named = await store.sectionPart(ord, section);
    if (named !== null) return at(named);
    const a = await store.sectionAkey(section);
    const i = a === 0 ? -1 : parts.findIndex((p) => p.akey === a);
    if (i >= 0) return at(i);
  }
  const k = parts.findIndex((p) => p.akey !== null);
  return at(k >= 0 ? k : 0);
}
