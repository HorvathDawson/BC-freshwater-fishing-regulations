/**
 * T841 — THE CLASSIFIED-WATER BADGE in the water's header ("Class I" / "Class II"), owner 2026-11-17: "if a water is
 * classified it should have a badge in the card header".
 *
 * Words only, nothing decided here. Which classified-water designations hold on the part, and on which day, is the stored
 * licence answer (pipeline `deliver/answers/licence.py` `holds`): `holds.designations` are the ones IN FORCE on the day
 * (`read.designations_in_force`), `holds.considered` every licensing record the part's sections list all year ("All
 * licence sources"), designations among them. Each points to a licensing record (`lx`) whose `fields.classified` is "I"
 * or "II" and whose `period` is the book's period (its dates, or "whenever this water is open", or all year).
 *
 * TWO LOOKS FOR THE OWNER, behind one constant (`CLASS_BADGE_MODE`):
 *   "in_period"  (A, default) the badge only on a day a designation is in force: "Class II".
 *   "year_round" (B) the badge on every day of a part that carries a designation, with its period when the book gives
 *                dates ("Class II · Jul 1–Dec 31"); `inForce` false out of period (the component draws it muted). Read
 *                from the day's own frame (`considered`), so no other day is read and nothing is unioned.
 * TWO CLASSES IN FORCE AT ONCE (no part of the 2026 release has two; the rule still holds): the highest, Class I, is
 * said — the Licence tab names every unit.
 */
import type { RegsInputs } from "./inputs";
import { isHolds, lx, type LicWords } from "./licence";
import { rangeTxt, whenDates, type RuleWhen } from "./time";
import { LIC } from "./words";

/** The look the app draws: "in_period" (A) or "year_round" (B). One constant, so the owner's pick is one line. */
export type ClassBadgeMode = "in_period" | "year_round";
export const CLASS_BADGE_MODE: ClassBadgeMode = "in_period";

export interface ClassBadgeVM {
  /** "I" | "II": the class said */
  readonly cls: "I" | "II";
  /** "Class II" */
  readonly label: string;
  /** (B only) the book's dates of the period ("Jul 1–Dec 31"); null in A, and for a period with no dates (the Elk:
   *  "whenever this water is open") */
  readonly period: string | null;
  /** a designation of this class is in force on the page's day; false only in B, out of period (drawn muted) */
  readonly inForce: boolean;
  /** the glossary term a press opens (`classified_waters`), null when the release's glossary has none */
  readonly term: string | null;
  /** the accessible name: the visible words, "Class II" / "Class II, Jul 1–Dec 31" */
  readonly aria: string;
}

type Cls = "I" | "II";
const classOf = (w: LicWords): Cls | null => w.kind === "designation" && (w.fields.classified === "I" || w.fields.classified === "II")
  ? w.fields.classified : null;
/** the highest class among them: Class I over Class II */
const highest = (cs: readonly Cls[]): Cls | null => cs.includes("I") ? "I" : cs.includes("II") ? "II" : null;
/** a designation's period in the book's dates (its stored `period.dates`), "" for a period with none */
const datesOf = (w: LicWords): string => w.period && (w.period as { kind?: string }).kind === "dates"
  ? whenDates({ dates: w.period.dates } as RuleWhen).map(([a, b]) => rangeTxt(a, b)).join(LIC.SEP) : "";

/** The badge for the page's part on its day; null: no badge (not classified, A out of period, no frame: outside B.C.,
 *  a tidal piece, a hand-built page). */
export function classBadgeOf(inp: RegsInputs, mode: ClassBadgeMode = CLASS_BADGE_MODE): ClassBadgeVM | null {
  const H = inp.frames?.licence?.holds;
  if (!H || !isHolds(H)) return null;
  const rec = (ix: number) => { const w = lx(inp, ix); return { ix, w, c: classOf(w) }; };
  const now = (H.designations ?? []).map(rec).filter((r) => r.c);
  const all = mode === "year_round"
    ? [...now, ...(H.considered ?? []).filter((ix) => !(H.designations ?? []).includes(ix)).map(rec).filter((r) => r.c)]
    : now;
  const nowCls = highest(now.map((r) => r.c!));
  const cls = nowCls ?? highest(all.map((r) => r.c!));
  if (!cls) return null;
  const inForce = nowCls !== null;
  const period = mode === "year_round"
    ? [...new Set((inForce ? now : all).filter((r) => r.c === cls).map((r) => datesOf(r.w)).filter(Boolean))].join(LIC.SEP) || null
    : null;
  const label = LIC.CLASS_BADGE(cls);
  const term = inp.statics.glossary?.terms.some((t) => t.id === LIC.CLASS_TERM) ? LIC.CLASS_TERM : null;
  return { cls, label, period, inForce, term, aria: period ? LIC.CLASS_BADGE_ARIA(label, period) : label };
}
