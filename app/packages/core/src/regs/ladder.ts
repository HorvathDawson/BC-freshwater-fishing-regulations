/**
 * The ladder frame DECODED (T07): per fish and origin, every rule's state as the PIPELINE decided it (answers `ladder`:
 * `ladder_frame` → `ladder_verdict` rows, read by T04 into `LadderFrameWire`). Nothing here decides a state: the wire's
 * five lists ARE the states (speaks / beside / shown / not_yet_mapped, and `lost` [rix, reason, by] whose state is the
 * static `ladder.reason_state[reason]`); this only turns the lists into a lookup.
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): verdictList, ladderAt. Hidden globals replaced: SEC.ladder.verdicts /
 * frames / reasons / reason_state (→ the wire value + the statics, passed in), A.fish (T04 resolved the fish codes),
 * w._seg['L'+s] (→ the caller holds the frame; settle.ts memoises per inputs).
 * Python cross-reference: `encode.LadderCodec.decode_verdict` (the same states, rule ids instead of rix).
 */
import type { LadderFrameWire, VerdictWire } from "./inputs";

export type VerdictState = "speaks" | "beside" | "shown" | "not_yet_mapped" | "lifted" | "displaced" | "moot";
export interface Verdict {
  readonly state: VerdictState;
  /** The lifting rules when only partly lifted (wire `partly` [[rix, [by…]]]); null otherwise. */
  readonly partly: readonly number[] | null;
  /** `ladder.reasons[r]` for a lost rule. */
  readonly reason?: string;
  /** The rix that beat or lifted it (a lost rule). */
  readonly by?: number;
}
export type Origin = "none" | "hatchery" | "wild";
/** fish code → origin → rix → verdict, in the frame's fish order. */
export type Ladder = ReadonlyMap<string, Readonly<Record<Origin, ReadonlyMap<number, Verdict>>>>;

const LISTED = ["speaks", "beside", "shown", "not_yet_mapped"] as const;

/** page verdictList: one wire verdict → rix → verdict, in the wire's order (the four lists, then `lost`). `partly` marks
 *  an entry already listed and is ignored for one that is not (page :150). */
export function verdictList(v: VerdictWire, reasons: readonly string[], reasonState: readonly string[]): Map<number, Verdict> {
  const [speaks, beside, shown, nym, partly, lost] = v;
  const out = new Map<number, Verdict>();
  [speaks, beside, shown, nym].forEach((l, k) => l.forEach((i) => out.set(i, { state: LISTED[k]!, partly: null })));
  for (const [i, by] of partly) { const x = out.get(i); if (x) out.set(i, { ...x, partly: by }); }
  for (const [i, r, by] of lost) out.set(i, { state: reasonState[r] as VerdictState, partly: null, reason: reasons[r], by });
  return out;
}

/** page ladderAt: a frame → per fish (frame order) → per origin, the `common` verdict overlaid by the fish's own
 *  (`new Map([...base, ...own])`). The store's 2-element row `[fish, v]` (one verdict for all three origins) reaches here
 *  already spread to [fish, v, v, v] by T04's `ladder` read. null frame → null.
 *  (T917) One map for origins that carry the same wire verdict (the spread row), and the overlay made as `base` copied
 *  then the own verdicts set (the insertion order of `new Map([...base, ...own])`). The maps are read-only (`Ladder`), so
 *  sharing them changes no reader. Decoding once per segment is settle.ts's (`ladderOfSeg`, held with the inputs). */
export function ladderAt(frame: LadderFrameWire | null, reasons: readonly string[], reasonState: readonly string[]): Ladder | null {
  if (!frame) return null;
  const base = verdictList(frame.common, reasons, reasonState);
  const at = (v: VerdictWire): ReadonlyMap<number, Verdict> => {
    const m = new Map(base);
    for (const [i, x] of verdictList(v, reasons, reasonState)) m.set(i, x);
    return m;
  };
  const out = new Map<string, Readonly<Record<Origin, ReadonlyMap<number, Verdict>>>>();
  for (const [fish, none, hatchery, wild] of frame.rows) {
    const n = at(none), h = hatchery === none ? n : at(hatchery), w = wild === none ? n : wild === hatchery ? h : at(wild);
    out.set(fish, { none: n, hatchery: h, wild: w });
  }
  return out;
}
