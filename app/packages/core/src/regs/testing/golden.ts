/**
 * The release-backed oracles' shapes (tests.md §1.2), types only: `taps.json` (the Python reference decoder's taps over the
 * release's store). conformance/src/regs/golden.ts reads it; core unit tests over hand fixtures may type literals copied from
 * it. Not exported from the regs index: `@app/core/regs/testing` is a test-only subpath. (T439: golden.json's types are gone
 * with it.)
 */
import type { Moment } from "../../answers.generated";
import type { Weekday } from "../../calendar.generated";


export interface Cell { ladder: number; rows: number; gear: number; licence: number; display: number }

export interface Tap {
  item: string; partIx: number; md: number; date: string /* "2026-05-15" */; weekday: Weekday;
  item_ord: number; akey: number; seg: number; mom: number | null; nseg: number; starts: number[]; moments: Moment[] | null;
  s: number; cell: Cell; displayByDay: number[];
  /** the golden's part index where the release renumbered the part (taps.py PART_MOVED); `partIx` is the store's */
  goldenPartIx?: number;
  /** the part took in sections the release merged into it (taps.py PART_GROWN): its extent words are not the golden's */
  grown?: string;
  /** the part kept its label and rows but the atlas re-cut the sections under it (taps.py PART_RECOUNTED, T67): only
   *  its section count is not the golden's — `sections` is the store's */
  recounted?: { reason: string; sections: number };
  /** (T278) a tidal cut took part of the golden part (taps.py PART_TIDAL): this is its freshwater remainder — same rows,
   *  its extent words are not the golden's; `tidalPartIx` is the store's unkeyed tidal part */
  tidal?: { reason: string; tidalPartIx: number };
}

/** A golden state whose part the release merged away (taps.py PART_GONE): never tapped; the parity excludes it with
 *  `reason` (conformance exclusions.ts PART_GONE_IDS). `partIx` is the golden's; the water is in the store's `nowIn`. */
export interface GoneTap {
  item: string; partIx: number; nowIn: number; reason: string; md: number; date: string; weekday: Weekday;
}

/** (T278) A golden state whose WHOLE part is tidal now (taps.py PART_TIDAL): the tidal piece has no freshwater rules and
 *  is off the map (the store's part `tidalPartIx` is unkeyed), so it is never tapped; the parity excludes it with `reason`
 *  (conformance exclusions.ts PART_TIDAL_IDS). `partIx` is the golden's. */
export interface TidalTap {
  item: string; partIx: number; tidalPartIx: number; reason: string; md: number; date: string; weekday: Weekday;
}

/** A Feb 29 case (P06/D31): the date reads `readingDay` (59) on its own `weekday`, as `encode.tap_key` taps it. */
export interface LeapTap {
  id: string; item: string; partIx: number; date: string; weekday: Weekday; readingDay: number; akey: number;
  starts: number[]; moments: Moment[] | null; s: number; cell: Cell;
}

export interface TapsMeta {
  generator: string; golden: string; version: string; year: number;
  // answers_sha256 pairs the oracles with a release (P20: taps.py reads the release's store); store_digest is null when
  // the release predates `store_meta` (pipeline/deliver/store/release.py)
  store: { format: string; section_handles: string; reach_digest: string; rule_ids_sha256: string; answers_sha256?: string;
           store_digest: string | null;
           layout: string };
}

export interface TapsFile {
  meta: TapsMeta;
  states: Record<string, Tap>;
  unnamed: { handle: number; akey: number; place: number; name: string }[];
  leap: LeapTap[];
  /** golden states with no part in the store (taps.py PART_GONE) — not in `states` */
  gone: Record<string, GoneTap>;
  /** (T278) golden states whose whole part is tidal now (taps.py PART_TIDAL) — not in `states` */
  tidal: Record<string, TidalTap>;
  /** item → why: the waters whose parts the release re-cut (taps.py), so their list of parts is not the golden's */
  recut: Record<string, string>;
}
