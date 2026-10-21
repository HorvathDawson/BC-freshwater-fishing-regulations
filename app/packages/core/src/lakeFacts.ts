/**
 * LAKE FACTS: the release file `lakes` (format `lakes/1`, `pipeline/deliver/lake_facts.py`) — per lake SECTION, whether
 * it was surveyed (a sheet, or a digitised chart) and the day it was last stocked. They are the values the map's Depth
 * and Stocked lake modes colour by, through the one colouring engine (owner 2026-10-21): nothing about a lake's survey
 * or stocking is in the tiles, so a new release alone recolours them.
 *
 * Layout (little-endian, varints LEB128): b"BCLK", u8 1, 8 bytes of the section_handles digest, asof + 1, count, then
 * per section (sid ascending) the sid delta, u8 survey (0 none, 1 sheet, 2 digitised), last stocked day + 1 (0 never).
 * Days count from 1900-01-01 (FFSBC's roster begins in 1901). A section the file does not list has neither.
 */

/** The style's `survey_state` enum, as the file codes it (1, 2). */
export type SurveyState = "sheet" | "digitised";
const SURVEY: readonly (SurveyState | null)[] = [null, "sheet", "digitised"];

/**
 * How long ago a lake was last stocked, as the style's `stock_recency` enum bands it (the Layers sheet's `STOCK_BANDS`
 * say the same in words): at most `days` days ago.
 */
export const STOCK_RECENCY = [
  { band: "season", days: 120 },
  { band: "year", days: 365 },
  { band: "recent", days: 1095 },
  { band: "decade", days: 3650 },
  { band: "old", days: Infinity },
] as const;
export type StockRecency = typeof STOCK_RECENCY[number]["band"];

export interface LakeFacts {
  /** The section_handles digest the file was built against. */
  readonly handles: string;
  /** The stocking roster's day (days since 1900-01-01), or null when the release had none. */
  readonly asof: number | null;
  /** How many sections the file lists. */
  readonly sections: number;
  survey(sid: number): SurveyState | null;
  /** The last day the section's lake was stocked (days since 1900-01-01), or null: never, or not known. */
  stockedDay(sid: number): number | null;
}

export class LakeFactsError extends Error {}

const EPOCH_UTC = Date.UTC(1900, 0, 1);

/** A CALENDAR date's day number (days since 1900-01-01), read in the date's own local calendar. */
export function dayNumber(d: Date): number {
  return Math.round((Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()) - EPOCH_UTC) / 86_400_000);
}

/** The band a last-stocked day falls in on `today`; null when it was never stocked. A day after today is this season. */
export function stockRecency(stocked: number | null, today: number): StockRecency | null {
  if (stocked === null) return null;
  const ago = Math.max(0, today - stocked);
  return STOCK_RECENCY.find((b) => ago <= b.days)!.band;
}

export function decodeLakeFacts(b: Uint8Array, handles: string): LakeFacts {
  if (b.length < 13 || b[0] !== 0x42 || b[1] !== 0x43 || b[2] !== 0x4c || b[3] !== 0x4b || b[4] !== 1)
    throw new LakeFactsError("lake facts: not a lakes/1 file");
  let digest = "";
  for (let i = 5; i < 13; i++) digest += b[i]!.toString(16).padStart(2, "0");
  if (digest !== handles) throw new LakeFactsError(`lake facts: built for handles ${digest}, not ${handles}`);
  let i = 13;
  const varint = (): number => {
    let n = 0, mul = 1;
    for (;;) {
      if (i >= b.length) throw new LakeFactsError("lake facts: truncated");
      const x = b[i++]!;
      n += (x & 0x7f) * mul;
      if (!(x & 0x80)) return n;
      mul *= 128;
    }
  };
  const asof = varint();
  const n = varint();
  const survey = new Map<number, SurveyState>();
  const stocked = new Map<number, number>();
  let sid = 0;
  for (let k = 0; k < n; k++) {
    sid += varint();
    if (i >= b.length) throw new LakeFactsError("lake facts: truncated");
    const s = SURVEY[b[i++]!];
    if (s === undefined) throw new LakeFactsError(`lake facts: survey code ${b[i - 1]} at section ${sid}`);
    if (s) survey.set(sid, s);
    const d = varint();
    if (d) stocked.set(sid, d - 1);
  }
  if (i !== b.length) throw new LakeFactsError(`lake facts: ${b.length - i} trailing bytes`);
  return {
    handles: digest, asof: asof ? asof - 1 : null, sections: n,
    survey: (s) => survey.get(s) ?? null,
    stockedDay: (s) => stocked.get(s) ?? null,
  };
}
