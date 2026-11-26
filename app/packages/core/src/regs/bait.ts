/**
 * T31 — the Gear tab's Bait item: the ban note with its ⓘ, the bait-ban heading (T515: when it runs, from the strip), one ✓ / ✕ list of the bait elements (each its note small under
 * it) and the item's source line.
 *
 * Owner rule 2026-10-21 (decisions.md, "The backend answers; the app only reads"): which bait is allowed, why, the carry limit and the exception it is to are the
 * PIPELINE's gear frame (`GearAnswer.bait[]`, `bait_ban`); the ban's dates are the stored strip bit 16 of each book day.
 * This file words and arranges them; it decides nothing (moveAllowances only moves words between two notes).
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): page v36 gearParts' bait block (WHY, the item note), prototype dated (with
 * bits 16), buildGear's bait cell (note2, the UR2 7 / 20h allowance move, the UR4 fix 6 all-year line).
 * (T797 item 2, owner 2026-11-16 "Show it", reversing decisions D18 a) an element's stored `also_allowed` (dead fin fish
 * when fishing for white sturgeon, p.8 and p.23) is said under it, in its rule's own words (D18 b).
 */
import { closedFoot, changeVM, gearAnswer, sourceLine, type GearCtx, bookBits } from "./gear";
import type { GearAnswer, RegsInputs } from "./inputs";
import { text, type Line, type Run } from "./line";
import { clean } from "./names";
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
    // (T922) roe reads as roe: no "the fish or fish parts ban excepts it" note
    const sub = b.excepted_from && b.element === "roe" ? BAIT.ALLOWED : b.excepted_from ? BAIT.EXCEPTED(elemName(b.excepted_from.element).toLowerCase())
      : b.by ? (b.why ? (BAIT.WHY[b.why] ?? b.why) : "") : BAIT.ALLOWED;
    return { ok: b.ok, name: elemName(b.element), s: [sub, b.carry_kg != null ? BAIT.CARRY(b.carry_kg) : ""].filter(Boolean).join(BAIT.SEP),
             rix: b.by?.[0] ?? null };
  });
}

/** (T797 item 2, D18 b) per bait element, in `g.bait` order: the rules of its stored allowances (`also_allowed`), each
 *  once. The pipeline decides them (`gear.py` bait `also_allowed`); this only reads their rule indices. */
export function allowanceRules(g: GearAnswer): readonly (readonly number[])[] {
  return g.bait.map((b) => [...new Set((b.also_allowed ?? []).map((a) => a.clause[0]))]);
}

/** (T797 item 2) an element's note, then each allowance's rule label (the stored words, as the boats line says its
 *  rules), each label once. */
export function noteWithAllowances(note: string, rixs: readonly number[], rules: GearCtx["rules"]): string[] {
  const said = [...new Set(rixs.filter((ix) => rules.has(ix)).map((ix) => clean(rules.get(ix).export.label)))];
  // (T922) the item's own note splits at its " · " (why, carry limit); each allowance label stays whole
  return [...(note ? note.split(BAIT.SEP) : []), ...labelsOnce(said)].filter(Boolean);
}

/**
 * (T900) rule labels of the same exception said once: two rules worded "<head> — <where>" (white sturgeon's dead fin fish,
 * one rule per entry of the book: "upstream of the CPR Bridge at Mission", "Fraser River; Pitt River, …") print the head
 * once and each rule's own "— <where>" (with any tail it carries) after it, "·" between them. A label without a
 * " — <where>" part, or with a head no other label shares, stands as it is. Every word of every label stays.
 */
export function labelsOnce(labels: readonly string[]): string[] {
  const cut = (l: string): [string, string | null] => { const i = l.indexOf(BAIT.DASH); return i < 0 ? [l, null] : [l.slice(0, i), l.slice(i + BAIT.DASH.length)]; };
  const out: string[] = [], at = new Map<string, number>(), where: string[][] = [];
  for (const l of labels) {
    const [head, w] = cut(l);
    const j = w === null ? undefined : at.get(head);
    if (j !== undefined) { where[j]!.push(w!); continue; }
    if (w !== null) { at.set(head, out.length); where[out.length] = [w]; } else where[out.length] = [];
    out.push(head);
  }
  return out.map((head, i) => (where[i]!.length ? head + BAIT.DASH + where[i]!.join(BAIT.SEP) : head));
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
    if (!/ban excepts it/i.test(to.s) && !/^roe\b/i.test(to.name)) to.s = [to.s, BAIT.STILL_OK(m[2]!)].filter(Boolean).join(BAIT.SEP);
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

/**
 * (T515) the one line over the list — what is banned and when, the status once, the ⓘ: "No natural bait — all year",
 * "No natural bait May 1–31 · in force today", "Bait ban Jan 1–May 15, Sep 1–Dec 31 · not today". "No natural bait" only
 * when every bait item is ✕; any ✓ item (an exception) makes it "Bait ban". No line when nothing is banned by season.
 * (T670 G2, owner 2026-11-12) `fished`: the tab is drawn on a day game fish are closed but salmon may be fished, and
 * the bait rules hold for salmon; the strip's bait mark exists only on game-fish-open days, so "not today" would be
 * wrong beside a day's frame that bans bait: the ban is in force today.
 */
export function banHeading(banToday: boolean, items: readonly BaitItem[], allYear: ChangeVM | null, dated: readonly ChangeVM[], term: boolean,
                          fished = false): Line[] {
  if (!allYear && !dated.length && !banToday) return [];
  const head = items.some((x) => x.ok) ? BAIT.BAN_SOME : BAIT.BAN_ALL;
  const when: Run[] = allYear ? [text(BAIT.WHEN_ALL_YEAR)] : dated.length ? [text(" "), text(dated.map((c) => c.when).join(BAIT.WHEN_JOIN))] : [];
  const status: Run[] = allYear || !dated.length ? [] : [text(dated.some((c) => c.inForce) || (fished && banToday) ? BAIT.ON_TODAY : BAIT.OFF_TODAY)];
  return [[text(head), ...when, ...status, ...(term ? [text(" "), { t: "term", ids: ["bait_ban"] } as const] : [])]];
}

/** The Bait item, or null on a day the Gear tab draws no items. */
export function buildBait(inp: RegsInputs, ctx: GearCtx, anyway = false): BaitVM | null {
  const g = gearAnswer(inp);
  if (!g || (!anyway && closedFoot(inp, ctx))) return null;
  const items = baitItems(g), also = allowanceRules(g);
  moveAllowances(items);
  const bits = bookBits(inp), dated = datedBans(bits, bookIndex(inp.day), inp.statics.strip);
  const all = dated.length ? null : allYearBan(bits, items);
  const row = (ok: boolean) => items.flatMap((x, i) => x.ok !== ok ? []
    : [[x.name, noteWithAllowances(note2(x.s), also[i] ?? [], ctx.rules)] as const]);
  const notes: Line[] = banHeading(g.bait_ban, items, all, dated, ctx.glossary.has("bait_ban"), anyway);
  return {
    label: BAIT.LABEL, notes,
    list: yesNo(row(true), row(false)),
    src: sourceLine("srcs:bait", [...items.flatMap((x) => (x.rix == null ? [] : [x.rix])), ...also.flat()], ctx),
  };
}
