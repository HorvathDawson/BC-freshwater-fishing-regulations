/**
 * The golden oracles' shapes (tests.md §1.2), types only: `app/fixtures/golden/golden.json` (page v38 @
 * bdda3586, ≥ b999cfd7 — one space wherever two elements abut) and `taps.json` (the Python reference decoder's taps over
 * the store the goldens were cut from). conformance/src/regs/golden.ts reads them; core unit tests over hand fixtures may
 * type literals copied from them. Not exported from the regs index: `@app/core/regs/testing` is a test-only subpath.
 */
import type { Moment } from "../../answers.generated";
import type { Weekday } from "../../calendar.generated";

/** A golden block a parity suite compares (one `<block>.parity.test.ts` each). */
export type Block = "header" | "keep" | "gear" | "methods" | "licence" | "sheets" | "pins";

export interface GoldenSheet {
  title: string | null; sub: string | null; folds: { summary: string; open: boolean }[];
  pages: { text: string; pdf: number | null }[]; lines: string[]; nested: string[];
}

export interface GoldenState {
  id: string; w: string; d: number; p: number; water: string; waterKind: string; part: string; md: number;
  sub: string; parts: unknown[]; header: unknown; keep: unknown; gear: unknown; methods: unknown;
  licence: Record<`${"adult" | "u16"}|${string}`, unknown>;
  /** golden sheet key → the sheet, or "@" (the shared copy in `GoldenFile.sheets`) */
  sheets: Record<string, GoldenSheet | "@">;
  pins: unknown;
}

export interface GoldenFile {
  meta: { generator: string; ref: string; sha: string; version: string; states: number; sheets: number; profiles: number;
          misses: string[] };
  states: Record<string, GoldenState>;
  sheets: Record<string, GoldenSheet>;
}

export interface Cell { ladder: number; rows: number; gear: number; licence: number; display: number }

export interface Tap {
  item: string; partIx: number; md: number; date: string /* "2026-05-15" */; weekday: Weekday;
  item_ord: number; akey: number; seg: number; mom: number | null; nseg: number; starts: number[]; moments: Moment[] | null;
  s: number; cell: Cell; displayByDay: number[];
  /** the golden's part index where the release renumbered the part (taps.py PART_MOVED); `partIx` is the store's */
  goldenPartIx?: number;
  /** the part took in sections the release merged into it (taps.py PART_GROWN): its extent words are not the golden's */
  grown?: string;
}

/** A golden state whose part the release merged away (taps.py PART_GONE): never tapped; the parity excludes it with
 *  `reason` (conformance exclusions.ts PART_GONE_IDS). `partIx` is the golden's; the water is in the store's `nowIn`. */
export interface GoneTap {
  item: string; partIx: number; nowIn: number; reason: string; md: number; date: string; weekday: Weekday;
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
  /** item → why: the waters whose parts the release re-cut (taps.py), so their list of parts is not the golden's */
  recut: Record<string, string>;
}
