/**
 * T36 — the Parts sheet (prototype regs-v11 `sheetHtml` parts branch @ 7fb6809f, with `fullPart` and `statusOf`): one row per
 * picker choice, in the picker's option order (page v36 `renderParts`: grouped under their headings) — its label, its open / closed word on the day, its long label when that says more,
 * and a ✓ on the choice holding the part read. Pressing a row reads that choice's first part.
 *
 * Owner rule 2026-10-21: nothing here decides a regulation. The day word is the choice's STORED display status on the day
 * (`RegsInputs.choicesOnDay`, T09: the same frame that part's own page reads for its header dot and strip), worded as the
 * page's own status is (page.ts: closed → closed, anything else B.C. → open; no key or tidal → the prototype's "unknown",
 * where its card draws no open / closed word). The labels are the store's picker and part facts.
 *
 * Not ported (D1): the prototype's "This prototype has no map of this water, so its parts are listed here." — the app
 * frames every named part on the map; the sheet is the list.
 */
import type { RegsInputs } from "../inputs";
import type { SheetBlock, SheetVM } from "../model";
import type { SheetKey } from "../sheetKey";
import { WORDS } from "../words";

/** The data cut a label with "…" (a match key, never said) — header.ts `uncut`. */
const uncut = (s: string | null | undefined): string => (s && !s.includes("…") ? s : "");
const longOf = (t: string) => t.replace(/ · \d+ sections?$/, "");

type Status = NonNullable<RegsInputs["choicesOnDay"]>[number]["status"];
const wordOf = (s: Status): string => (s === "closed" ? WORDS.PART_CLOSED : s === "base" || s === "own" ? WORDS.PART_OPEN : WORDS.PART_UNKNOWN);

/** page v36 renderParts: the picker's options under their headings (a headed picker), each heading where it first
 *  appears — the order the prototype's snapshot lists a water's parts in. Indices into `choices`. */
export function pickerOrder(picker: { readonly headed: boolean; readonly choices: readonly { readonly heading: string }[] }): number[] {
  const heads = new Map<string, number[]>();
  picker.choices.forEach((c, i) => { const k = picker.headed ? c.heading || "" : ""; (heads.get(k) ?? heads.set(k, []).get(k)!).push(i); });
  return [...heads.values()].flat();
}

export function partsSheet(key: SheetKey, inp: RegsInputs): SheetVM | null {
  const it = inp.item, days = inp.choicesOnDay, choices = it?.picker?.choices ?? [];
  if (!it || !it.picker || !days || choices.length < 2 || days.length !== choices.length) return null;
  const items = pickerOrder(it.picker).map((i) => {
    const c = choices[i]!;
    const part = days[i]!.partIx, long = longOf(c.text);
    const text = uncut(it.parts[part]?.short) || long || it.name;
    return { text, sub: null, status: wordOf(days[i]!.status), long: long && long !== text ? long : null,
             checked: c.parts.includes(inp.partIx), part };
  });
  const blocks: SheetBlock[] = [{ k: "choice", items }];
  return { key, title: WORDS.PARTS_TITLE, sub: null, blocks };
}
