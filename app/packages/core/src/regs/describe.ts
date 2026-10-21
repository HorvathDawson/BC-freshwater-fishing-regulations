/**
 * A rule as a line of words: the kind is the answers' (`RuleFacts.kind`), the words are the page's.
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): closedGate, covers, bands, bandTxt, describe, cutWords.
 * The possession kinds and a snagged fish say the answers' own sentence (user rulings Q15, Q38), never the page's.
 */
import type { RuleView } from "./inputs";
import { cap, clean, writtenName, type SpeciesBook } from "./names";
import { winTxt } from "./time";
import { WORDS } from "./words";

/** A size band (named RuleBand: core flow.ts owns `Band`): from `a` cm up to `b` cm (Infinity = no top), and how many may be kept (null: no number). */
export interface RuleBand { readonly a: number; readonly b: number; readonly take: number | null }

interface ReadFields {
  readonly species?: readonly string[] | null; readonly species_except?: readonly string[] | null;
  readonly origin?: string | null; readonly water?: string | null; readonly take?: number | null;
  readonly unlimited?: boolean | null; readonly period?: string | null;
}

export const closedGate = (r: RuleView): boolean => r.kind === "gate" && !!r.facts?.closure;
export const covers = (r: RuleView, S: string): boolean => r.species.includes(S) && !r.speciesExcept.includes(S);

/** The answers' bands [[from_cm, to_cm|null, take|null]]. */
export function bands(r: RuleView): RuleBand[] {
  return (r.facts?.bands ?? []).map(([a, b, take]) => ({ a, b: b == null ? Infinity : b, take }));
}

export function bandTxt(a: number, b: number): string {
  return a === 0 ? WORDS.UNDER(b) : b === Infinity ? WORDS.OVER(a) : `${a}–${b}${WORDS.CM}`;
}

export function describe(r: RuleView, sp: SpeciesBook): string {
  const f = r.f as ReadFields;
  const name = writtenName(sp, f);
  const o = f.origin ? f.origin + " " : "";
  const w = f.water ? (f.water === "stream" ? WORDS.FROM_STREAMS : WORDS.FROM_LAKES) : "";
  const t = winTxt(r.wins) + (r.tcond ? ` (${r.tcond})` : "");
  const bs = bands(r);
  switch (r.kind) {
    case "gate":
      return (closedGate(r)
        ? WORDS.NO_FISHING_FOR + o + (name === WORDS.ALL_GAME ? WORDS.ANY_GAME : name)
        : WORDS.RELEASE_EVERY + o + name) + w + t;
    case "pool":
      return (f.unlimited
        ? WORDS.NO_LIMIT_ON + name + w
        : `${WORDS.KEEP}${f.take} ${o}${name}${w}${WORDS.A_DAY}${r.species.length > 1 ? WORDS.SHARED : ""}`)
        + bs.filter((s) => s.take === 0).map((s) => WORDS.NONE + bandTxt(s.a, s.b)).join("") + t;
    case "subcap":
      return `${WORDS.INSIDE}${WORDS.AT_MOST}${f.take} ${o}${writtenName(sp, f, WORDS.OR)}${w}` + t;
    case "sizecap":
      return WORDS.INSIDE + bs.filter((s) => s.take != null)
        .map((s) => (s.take === 0 ? WORDS.NONE_BARE : `${WORDS.AT_MOST}${s.take} `) + bandTxt(s.a, s.b)).join(", ")
        + ` (${name})` + t;
    case "size":
      return `${cap(o + name)}${w}: ` + bs.filter((s) => s.take === 0).map((s) => WORDS.RELEASE_ANY + bandTxt(s.a, s.b)).join(", ") + t;
    case "annual": {
      const c = bs.find((s) => s.take != null && s.take > 0);
      const over = c !== undefined && c.a > 0;
      return `${f.take} ${o}${name}${over ? " " + bandTxt(c.a, c.b) : ""}${WORDS.PER}${f.period === "monthly" ? WORDS.A_MONTH : WORDS.A_YEAR}`
        + (over ? WORDS.SMALLER : "");
    }
    case "possession_cap": case "possession": case "caught":
      return r.facts?.plain ? r.facts.plain.replace(/\.$/, "") : WORDS.NO_WORDS(r.key);
    default:
      return clean(r.export.verbatim);
  }
}

/** A text cut to `n` characters at a word, with an ellipsis. */
export function cutWords(t: string, n: number): string {
  return t.length <= n ? t : t.slice(0, n).replace(/[\s,;:·–-]+\S*$/, "") + "…";
}
