/**
 * The day's context every box above the card reads (T07): page v36's `settle` over a tap's `RegsInputs`.
 *
 * Owner rule 2026-10-21 (decisions.md): every answer here is the PIPELINE's — the ladder verdicts (ladder_frame /
 * ladder_verdict: which rules speak, stand beside, are shown, are lifted and by what, sit in undrawn parts) and the
 * display frame's status and its decided `closing`. This file only decodes them (ladder.ts), selects (by segment, fish,
 * origin "none") and groups them into the lists the page draws; it never decides which rule governs, what is lifted or
 * what closes. The subsets below (timed / side / standing / anglerclosed) are filters on the stored lists by the rule's
 * own stored fields (`when` time condition, `fields.side`, `kind`), for where the page draws them.
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): settle (incl. settle.with_), statusAt, isClosedAt, frameIx. Hidden
 * globals replaced: SEC[name].at[w.k][s] (→ `inp.year.*BySeg[s]`), SEC.display.frames, w._seg / w._nym (→ WeakMaps),
 * PLACE (→ the `RegsPlace` argument).
 */
import type { Day } from "./model";
import type { DisplayFrame, RegsInputs, RuleIndex, RuleView } from "./inputs";
import { ladderAt, type Ladder, type VerdictState } from "./ladder";
import type { RegsPlace } from "./place";

export interface Settled {
  /** The segment read; -1 where the place has no key (outside B.C.). */
  readonly s: number;
  readonly day: Day;
  readonly L: Ladder | null;
  /** The display frame of (akey, s). */
  readonly status: DisplayFrame | null;
  readonly speaks: readonly RuleView[];
  readonly beside: readonly RuleView[];
  readonly shown: readonly RuleView[];
  /** Every not_yet_mapped rule over the key's year (first seen, segment then fish order); `now` = not_yet_mapped today. */
  readonly inpart: readonly { readonly rule: RuleView; readonly now: boolean }[];
  /** rix → the rix that lifted it (the last one over the fish, as page v36 keeps it). */
  readonly lifted: ReadonlyMap<number, number>;
  /** The display frame's decided `closing`, most fish first, then rank. */
  readonly broad: readonly RuleView[];
  readonly timed: readonly RuleView[];
  readonly side: readonly RuleView[];
  readonly standing: readonly RuleView[];
  readonly anglerclosed: readonly RuleView[];
}

/**
 * page v36 `settle` broad: the frame's DECIDED closures (`closing [[rix, fish[]]]`, the pipeline's — tidal frames have
 * none), drawn the one naming the most fish first, then by the part's rank. Ordering only: the set is the frame's.
 * Array.prototype.sort is stable, as V8's is in the page.
 */
export function broadOf(frame: DisplayFrame | null, R: RuleIndex): readonly RuleView[] {
  const closing = frame && frame.status !== "tidal" ? frame.closing : [];
  return closing.map(([ix, fs]) => [R.get(ix), fs.length] as const)
    .sort((a, b) => b[1] - a[1] || a[0].rank - b[0].rank)
    .map(([r]) => r);
}

const words = (inp: RegsInputs) =>
  [inp.statics.ladderReasons as readonly string[], inp.statics.ladderReasonState as readonly string[]] as const;

const MEMO = new WeakMap<RegsInputs, Map<string, Settled>>();
const NYM = new WeakMap<RegsInputs, readonly number[]>();

/** Every not_yet_mapped rix over all of the key's segments, first-seen order (page `w._nym`), once per inputs. */
function undrawn(inp: RegsInputs): readonly number[] {
  let out = NYM.get(inp);
  if (!out) {
    const seen = new Set<number>();
    const [reasons, states] = words(inp);
    for (const f of inp.year?.ladderBySeg ?? []) {
      const L = ladderAt(f, reasons, states);
      if (L) for (const o of L.values()) for (const [ix, v] of o.none) if (v.state === "not_yet_mapped") seen.add(ix);
    }
    NYM.set(inp, out = [...seen]);
  }
  return out;
}

const EMPTY: readonly never[] = Object.freeze([]);

/** The same context for segment `s` on `day` (another moment of the day, T15 timedHtml; another day, T14's chain). */
export function settleAt(inp: RegsInputs, place: RegsPlace, s: number, day: Day): Settled {
  const ck = `${s}|${day.month}-${day.day}`;
  let memo = MEMO.get(inp);
  if (!memo) MEMO.set(inp, memo = new Map());
  const hit = memo.get(ck);
  if (hit) return hit;

  const R = place.R;
  if (!place.key || !inp.year) {
    const none: Settled = { s: -1, day, L: null, status: null, speaks: EMPTY, beside: EMPTY, shown: EMPTY, inpart: EMPTY,
      lifted: new Map(), broad: EMPTY, timed: EMPTY, side: EMPTY, standing: EMPTY, anglerclosed: EMPTY };
    memo.set(ck, none);
    return none;
  }
  const [reasons, states] = words(inp);
  const L = ladderAt(inp.year.ladderBySeg[s] ?? null, reasons, states);
  const st = new Map<number, VerdictState>(), lifted = new Map<number, number>();
  if (L) for (const o of L.values()) for (const [ix, v] of o.none) {
    if (v.state === "lifted") lifted.set(ix, v.by!);
    else if (!st.has(ix) || v.state === "speaks") st.set(ix, v.state);
  }
  const with_ = (state: VerdictState) => [...st].filter(([, v]) => v === state).map(([ix]) => R.get(ix));
  const speaks = with_("speaks"), beside = with_("beside"), shown = with_("shown");
  const nymNow = new Set(with_("not_yet_mapped").map((r) => r.rix));
  const inpart = undrawn(inp).map((ix) => ({ rule: R.get(ix), now: nymNow.has(ix) }));
  const status = inp.year.displayBySeg[s] ?? null;
  const out: Settled = {
    s, day, L, status, speaks, beside, shown, inpart, lifted, broad: broadOf(status, R),
    timed: beside.filter((r) => r.tcond), side: beside.filter((r) => r.f.side),
    standing: shown.filter((r) => r.kind === "standing"), anglerclosed: speaks.filter((r) => r.kind === "anglerclosure"),
  };
  memo.set(ck, out);
  return out;
}

/** The tap's day: `inp.day` at the tap's segment `inp.s`. A place with no key settles to empty lists, never throws. */
export function settle(inp: RegsInputs, place: RegsPlace): Settled {
  return settleAt(inp, place, inp.s ?? -1, inp.day);
}

/** Status of a leap-calendar day 1..366 from the year's display frames (page statusAt): base | own | closed | tidal;
 *  null with no key or no frame. The ticket's `statusOn`, renamed: core/status.ts already exports a `statusOn` (the map's
 *  status index) through the same `@app/core` barrel. */
export function displayStatusOn(inp: RegsInputs, leapDay: number): "base" | "own" | "closed" | "tidal" | null {
  return inp.year?.displayByDay[leapDay - 1]?.status ?? null;
}

/** page isClosedAt over a leap-calendar day. */
export const isClosedOn = (inp: RegsInputs, leapDay: number): boolean => displayStatusOn(inp, leapDay) === "closed";
