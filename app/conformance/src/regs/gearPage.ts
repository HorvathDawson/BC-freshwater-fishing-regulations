/**
 * T30–T32 parity helpers: the Gear and Methods tabs (core `GearVM` / `MethodsVM`) read the way port/golden.mjs reads the
 * prototype's DOM (`gearTab()`, `methodsTab()`): each field in the golden's own shape, and every drawn line of the tab
 * (golden `gear.lines_` / `methods.lines`: one string per block element, separate elements one space). A projection only
 * reads the VM; it never re-derives a value.
 *
 * The golden's sheet keys embed the state id (`srcs:gl|vedder_1010|hook`, `srcs:bait|vedder_1010`); the app's never do
 * (contracts §6 #1) — T36's `goldenKeyOf` (sheetKey.ts, the one bridge) maps them.
 */
import { GEAR, goldenKeyOf, plain, WORDS, type ChangeVM, type GearVM, type MethodsVM, type SourceLineVM, type YesNoVM } from "@app/core";
import { norm } from "./eq";

const bookText = (p: { printed: number; pdf: number }) => plain([{ t: "book", printed: p.printed, pdf: p.pdf }]);
/** a term run's words: one id "ⓘ", two or more "What these words mean" */
const tipText = (ids: readonly string[]) => (ids.length ? plain([{ t: "term", ids }]) : "");
const j = (...xs: (string | null | undefined)[]) => norm(xs.filter(Boolean).join(" "));

export const srcProj = (s: SourceLineVM | null, id: string) => s && {
  who: s.who, open: goldenKeyOf(s.open, id), pages: s.pages.map((p) => ({ text: bookText(p), pdf: p.pdf })),
};
export const srcLine = (s: SourceLineVM | null): string => (s ? j(s.who, ...s.pages.map(bookText)) : "");

/** golden chg(): {now, chip, text, when, inForce} */
export const chgProj = (c: ChangeVM) => ({
  now: c.inForce, chip: c.chip, text: chgText(c), when: `${c.when}:`, inForce: c.inForce,
});
const chgText = (c: ChangeVM) => j(`${c.when}:`, c.says, c.inForce ? GEAR.IN_FORCE : "");
export const chgLine = (c: ChangeVM): string => j(c.chip, chgText(c));

/** golden yn() */
export const ynProj = (list: YesNoVM) => list.map((c) => ({ col: c.col, head: c.head, items: c.items.map((i) => ({ mark: i.mark, name: i.name, note: i.note })) }));
export const ynLines = (list: YesNoVM): string[] => list.flatMap((c) => [c.head, ...c.items.map((i) => j(i.mark, i.name, i.note))]);

/** The Gear tab in the golden's `gear` shape (lines_ included). */
export function gearProj(v: GearVM, id: string) {
  const terms = (vt: readonly (readonly string[])[]) => vt.filter((t) => t.length).map((t) => t.join(","));
  return {
    closed: v.closed, day: v.day,
    mode: v.mode && { value: v.mode.value, terms: terms([v.mode.terms]), changes: v.mode.changes.map(chgProj), src: srcProj(v.mode.src, id) },
    lines: v.lines.map((l) => ({ k: l.k, label: l.label, values: [...l.values], terms: terms(l.valueTerms), changes: l.changes.map(chgProj), src: srcProj(l.src, id) })),
    bait: v.bait && { label: v.bait.label, notes: v.bait.notes.map(plain), dated: v.bait.dated.map(chgProj), list: ynProj(v.bait.list), src: srcProj(v.bait.src, id) },
    lines_: gearLines(v),
  };
}

/** Every drawn line of the Gear tab (golden `gear.lines_`). */
export function gearLines(v: GearVM): string[] {
  if (v.closed !== null || v.closedMore.length) return [v.closed ?? "", ...v.closedMore].map(norm).filter(Boolean);
  const out: string[] = [WORDS.TAB_GEAR, v.day ?? ""];
  if (v.mode) out.push(j(v.mode.value, tipText(v.mode.terms)), ...v.mode.changes.map(chgLine), srcLine(v.mode.src));
  for (const l of v.lines) {
    const vals = l.values.map((x, i) => j(x, tipText(l.valueTerms[i] ?? [])));
    out.push(j(l.label, vals[0]), ...vals.slice(1), ...l.changes.map(chgLine), srcLine(l.src));
  }
  if (v.bait) out.push(v.bait.label, ...v.bait.notes.map(plain), ...v.bait.dated.map(chgLine), ...ynLines(v.bait.list), srcLine(v.bait.src));
  return out.map(norm).filter(Boolean);
}

/** The Methods tab in the golden's `methods` shape. */
export function methodsProj(v: MethodsVM, id: string) {
  return {
    closed: v.closed,
    ways: v.ways && { label: v.ways.label, list: ynProj(v.ways.list), src: srcProj(v.ways.src, id) },
    boats: v.boats && { label: v.boats.label, items: [...v.boats.items], src: srcProj(v.boats.src, id) },
    reference: v.reference && { label: v.reference.label, title: v.reference.title, sub: v.reference.sub, open: false,
      groups: v.reference.groups.map((g) => ({ head: g.head, items: [...g.items] })), src: srcProj(v.reference.src, id) },
    lines: methodsLines(v),
  };
}

/** Every drawn line of the Methods tab (golden `methods.lines`). */
export function methodsLines(v: MethodsVM): string[] {
  if (v.closed !== null || v.closedMore.length) return [v.closed ?? "", ...v.closedMore].map(norm).filter(Boolean);
  const out: string[] = [];
  if (v.ways || v.boats) out.push(WORDS.TAB_METHODS);
  if (v.ways) out.push(v.ways.label, ...ynLines(v.ways.list), srcLine(v.ways.src));
  if (v.boats) out.push(v.boats.label, ...v.boats.items, srcLine(v.boats.src));
  if (v.reference) {
    const r = v.reference;
    out.push(j(r.label, r.title, r.sub), ...r.groups.flatMap((g) => [g.head, ...g.items]), srcLine(r.src));
  }
  return out.map(norm).filter(Boolean);
}

