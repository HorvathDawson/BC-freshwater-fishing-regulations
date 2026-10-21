/**
 * T31 — the Gear tab's Bait item: the ban note with its ⓘ, the dated bait-ban changes from the strip ("Next May 1–31:
 * bait ban", or "All year: bait ban · in force today"), one ✓ / ✕ list of the bait elements (each its note small under
 * it) and the item's source line.
 *
 * Owner rule 2026-10-21 (decisions.md): which bait is allowed, why, the carry limit and the exception it is to are the
 * PIPELINE's gear frame (`GearAnswer.bait[]`, `bait_ban`); the ban's dates are the stored strip bit 16 of each book day.
 * This file words and arranges them; it decides nothing (moveAllowances only moves words between two notes).
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): page v36 gearParts' bait block (WHY, the item note), prototype dated (with
 * bits 16), buildGear's bait cell (note2, the UR2 7 / 20h allowance move, the UR4 fix 6 all-year line). `also_allowed`
 * (✓ dead fish … `<small>`) is not shown: the prototype reads only the item's `<span>` (decisions D18 a).
 */
import { closedFoot, changeVM, gearAnswer, sourceLine, type GearCtx, bookBits } from "./gear";
import type { GearAnswer, RegsInputs } from "./inputs";
import { text, type Line, type Run } from "./line";
import type { BaitVM, ChangeVM } from "./model";
import { bookIndex, spanWords } from "./strip";
import { BAIT, ELEM, GEAR } from "./words";
import { yesNo } from "./yesno";
import type { StripState } from "./inputs";

/** One bait element as page v36 draws it: ✓ or ✕, its name, its note (`s`, edited by moveAllowances), its rule. */
export interface BaitItem { readonly ok: boolean; readonly name: string; s: string; readonly rix: number | null }

const elemName = (e: string): string => ELEM[e] ?? e;

/** page v36 gearParts bait: per element, its name and note — the ban it is excepted from, else why, else "allowed" —
 *  and "carry up to N kg". */
export function baitItems(g: GearAnswer): BaitItem[] {
  return g.bait.map((b) => {
    const sub = b.excepted_from ? BAIT.EXCEPTED(elemName(b.excepted_from.element).toLowerCase())
      : b.by ? (b.why ? (BAIT.WHY[b.why] ?? b.why) : "") : BAIT.ALLOWED;
    return { ok: b.ok, name: elemName(b.element), s: [sub, b.carry_kg != null ? BAIT.CARRY(b.carry_kg) : ""].filter(Boolean).join(BAIT.SEP),
             rix: b.by?.[0] ?? null };
  });
}

/** prototype buildGear.note2: a note that only repeats its column ("allowed", "banned", "bait ban") loses that word. */
export function note2(s: string): string {
  return s.replace(/^(allowed|banned|bait ban)(\s*·\s*)?/, "");
}

/** prototype UR2 7 / round 20h: "<x> is still OK" on a banned item moves to the allowed item it names — unless that item
 *  already names the ban it is excepted from, where the phrase is only dropped. */
export function moveAllowances(items: BaitItem[]): void {
  for (const x of items.filter((i) => !i.ok)) {
    const m = /^(.*?)(?:\s*·\s*)?\b(\w+) is still OK$/i.exec(note2(x.s));
    if (!m) continue;
    const to = items.find((y) => y.ok && new RegExp("^" + m[2], "i").test(y.name));
    if (!to) continue;
    x.s = m[1]!;
    if (!/ban excepts it/i.test(to.s)) to.s = [to.s, BAIT.STILL_OK(m[2]!)].filter(Boolean).join(BAIT.SEP);
  }
}

const BAN_BIT = 16;
/** prototype dated(st, 16): each run of the year (stored strip bits) under a bait ban and not closed, shorter than the
 *  year, as a change — its dates, the strip labels of its flags, "Now" while `today365` is in it. */
export function datedBans(bits365: readonly (number | null)[], today365: number, labels: readonly StripState[]): readonly ChangeVM[] {
  const out: ChangeVM[] = [], all = bits365.length;
  let i = 0;
  while (i < all) {
    const f = bits365[i]!;
    let n = 1;
    while (i + n < all && bits365[i + n] === f) n++;
    if (f != null && !(f & 1) && (f & BAN_BIT) && n < all) {
      const words = [4, 8, 16].filter((b) => b & BAN_BIT & f).map((b) => labels.find((x) => x.bit === b)?.label ?? "").join(BAIT.SEP);
      out.push(changeVM(spanWords(i, i + n - 1), words, today365 >= i && today365 < i + n));
    }
    i += n;
  }
  return out;
}

/** prototype UR4 fix 6: a ban every day of the year gets its line — "All year:" + the first banned item's own words. */
export function allYearBan(bits365: readonly (number | null)[], items: readonly BaitItem[]): ChangeVM | null {
  const no = items.find((x) => !x.ok);
  if (!bits365.length || !bits365.every((f) => f != null && (f & BAN_BIT)) || !no || !/ban/i.test(no.s)) return null;
  return changeVM(GEAR.ALL_YEAR, no.s.replace(/^(allowed|banned)\s*·\s*/, "").trim(), true);
}

/** The Bait item, or null on a day the Gear tab draws no items. */
export function buildBait(inp: RegsInputs, ctx: GearCtx): BaitVM | null {
  const g = gearAnswer(inp);
  if (!g || closedFoot(inp, ctx)) return null;
  const items = baitItems(g);
  moveAllowances(items);
  const bits = bookBits(inp), dated = datedBans(bits, bookIndex(inp.day), inp.statics.strip);
  const all = dated.length ? null : allYearBan(bits, items);
  const note: Run[] = [text(BAIT.BAN_NOTE), ...(ctx.glossary.has("bait_ban") ? [{ t: "term", ids: ["bait_ban"] } as const] : [])];
  const notes: Line[] = g.bait_ban ? [note] : [];
  return {
    label: BAIT.LABEL, notes, dated: all ? [all] : dated,
    list: yesNo(items.filter((x) => x.ok).map((x) => [x.name, note2(x.s)] as const), items.filter((x) => !x.ok).map((x) => [x.name, note2(x.s)] as const)),
    src: sourceLine("srcs:bait", items.flatMap((x) => (x.rix == null ? [] : [x.rix])), ctx),
  };
}
