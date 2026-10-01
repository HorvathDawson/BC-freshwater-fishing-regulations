/**
 * THE STATUS INDEX, decoded — what colour a section or a water is on a day.
 *
 * Built by `python -m pipeline.deliver.status_index` from the bundle, through the reference
 * reader (`read.effective_rules`); its module docstring is the format spec and decides what
 * closed / own / base mean. This file only READS it: it decides nothing, so the map and every
 * list get the pipeline's one answer (AGENTS 23, 53).
 *
 *   "BCSI" · version 1 · 8-byte section_handles digest
 *   nProfiles × (nRuns × varint(length << 3 | code))        the shared range tables, 366 days
 *   nRuns, then columns gap[] count[] profile[]              sections, as runs of handles
 *   nItems, then columns shared[] suffixLen[] bytes profile[]  waters, front-coded item ids
 *
 * Lookup is O(1): the runs are expanded once into one byte (or two) per section handle, and
 * every profile into a 366-day row, so `codeOn` is two array reads.
 *
 * VINTAGE. A section key is an integer handle that means a different river in another atlas
 * (`SectionId` in @app/data). The file carries the digest; `decodeStatusIndex` refuses one
 * that does not match the digest the caller holds, so a mixed set is never coloured.
 */
import type { SectionKey } from "./section";

/** What the file can say about a section on a day. `tidal` and `outside` are not statuses. */
export type StatusCode = "base" | "own" | "closed" | "tidal" | "outside";

/** The file's code numbers, in order — `pipeline.deliver.status_index` BASE..OUTSIDE. */
export const STATUS_CODES: readonly StatusCode[] = ["base", "own", "closed", "tidal", "outside"];

const MAGIC = [0x42, 0x43, 0x53, 0x49]; // "BCSI"
const VERSION = 1;
const DAYS = 366;
/** Days before each month on the catalogue's leap calendar (Feb always has 29). */
const BEFORE = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];

/**
 * A date's day on the catalogue's calendar (`catalogue._day_index`): 1..366, Feb 29 is 60 and
 * Mar 1 is 61 IN EVERY YEAR, so a printed "Mar 1" is the same day whether or not the year is a
 * leap year. Read in the date's LOCAL calendar — the regulations are dated where the water is.
 */
export function dayOfYear(on: Date | { month: number; day: number }): number {
  const [m, d] = on instanceof Date ? [on.getMonth() + 1, on.getDate()] : [on.month, on.day];
  return BEFORE[m - 1]! + d;
}

export interface StatusIndex {
  /** The `section_handles` digest the file was built against (16 hex digits). */
  readonly handles: string;
  /** How many sections and waters are listed (every other one is base). */
  readonly sections: number;
  readonly waters: number;
  /** The code for one section handle — the tiles' feature id — on a day. Absent → base. */
  codeOn(section: SectionKey, on: Date): StatusCode;
  /** The code for one water (`item_id`) on a day — its parts rolled up. Absent → base. */
  waterCodeOn(item: string, on: Date): StatusCode;
}

export class StatusIndexError extends Error {}

/**
 * UTF-8 to a string, by hand: Hermes (the native app's engine) has no `TextDecoder`, and the
 * ids are short (ASCII today — `blk:355991777`, `gnis:8634`).
 */
function utf8(b: Uint8Array): string {
  let out = "";
  for (let i = 0; i < b.length;) {
    const c = b[i++]!;
    const cp = c < 0x80 ? c
      : c < 0xe0 ? ((c & 0x1f) << 6) | (b[i++]! & 0x3f)
      : c < 0xf0 ? ((c & 0x0f) << 12) | ((b[i++]! & 0x3f) << 6) | (b[i++]! & 0x3f)
      : ((c & 0x07) << 18) | ((b[i++]! & 0x3f) << 12) | ((b[i++]! & 0x3f) << 6) | (b[i++]! & 0x3f);
    out += String.fromCodePoint(cp);
  }
  return out;
}

/**
 * Decode the file. `expectHandles` is the digest the tiles and bundle agree on; a file built
 * against any other is refused (throws), never read — its handles name different sections.
 */
export function decodeStatusIndex(bytes: Uint8Array, expectHandles: string | null): StatusIndex {
  let pos = 0;
  const fail = (why: string): never => { throw new StatusIndexError(`status index: ${why}`); };
  const byte = (): number => (pos < bytes.length ? bytes[pos++]! : fail("truncated"));
  const v = (): number => {
    let n = 0, mul = 1, b: number;
    do { b = byte(); n += (b & 0x7f) * mul; mul *= 128; } while (b & 0x80);
    return n;
  };
  for (const m of MAGIC) if (byte() !== m) fail("not a status index");
  const version = byte();
  if (version !== VERSION) fail(`version ${version}, this app reads ${VERSION}`);
  let handles = "";
  for (let i = 0; i < 8; i++) handles += byte().toString(16).padStart(2, "0");
  if (expectHandles !== null && handles !== expectHandles)
    fail(`built for atlas ${handles}, the map is ${expectHandles} — refused`);

  const nProfiles = v();
  if (nProfiles > 65534) fail("too many profiles");
  const days = new Uint8Array(nProfiles * DAYS);
  for (let p = 0; p < nProfiles; p++) {
    let d = 0;
    for (let r = v(); r > 0; r--) {
      const x = v(), code = x & 7, len = Math.floor(x / 8);
      if (code >= STATUS_CODES.length || d + len > DAYS) fail("bad range table");
      days.fill(code, p * DAYS + d, p * DAYS + d + len);
      d += len;
    }
    if (d !== DAYS) fail("a range table does not cover the year");
  }

  const nRuns = v();
  const gaps = new Array<number>(nRuns), counts = new Array<number>(nRuns);
  for (let i = 0; i < nRuns; i++) gaps[i] = v();
  let top = 0, listed = 0;
  for (let i = 0; i < nRuns; i++) {
    counts[i] = v();
    top += gaps[i]! + counts[i]!;
    listed += counts[i]!;
  }
  // profile + 1 per handle; 0 = absent (base)
  const bySection = nProfiles < 255 ? new Uint8Array(top) : new Uint16Array(top);
  let end = 0;
  for (let i = 0; i < nRuns; i++) {
    const p = v();
    if (p >= nProfiles) fail("a section names no profile");
    const start = end + gaps[i]!;
    bySection.fill(p + 1, start, start + counts[i]!);
    end = start + counts[i]!;
  }

  const nItems = v();
  const shared = new Array<number>(nItems), lens = new Array<number>(nItems);
  for (let i = 0; i < nItems; i++) shared[i] = v();
  for (let i = 0; i < nItems; i++) lens[i] = v();
  const ids = new Array<string>(nItems);
  let prev = new Uint8Array(0);
  for (let i = 0; i < nItems; i++) {
    if (shared[i]! > prev.length || pos + lens[i]! > bytes.length) fail("bad water ids");
    const id = new Uint8Array(shared[i]! + lens[i]!);
    id.set(prev.subarray(0, shared[i]!));
    id.set(bytes.subarray(pos, pos + lens[i]!), shared[i]!);
    pos += lens[i]!;
    ids[i] = utf8(id);
    prev = id;
  }
  const byWater = new Map<string, number>();
  for (let i = 0; i < nItems; i++) {
    const p = v();
    if (p >= nProfiles) fail("a water names no profile");
    byWater.set(ids[i]!, p);
  }
  if (pos !== bytes.length) fail("trailing bytes");

  const at = (p: number, on: Date): StatusCode => STATUS_CODES[days[p * DAYS + dayOfYear(on) - 1]!]!;
  return {
    handles,
    sections: listed,
    waters: nItems,
    codeOn(section, on) {
      const s = typeof section === "number" ? section : Number(section);
      const p = Number.isInteger(s) && s >= 0 && s < bySection.length ? bySection[s]! : 0;
      return p === 0 ? "base" : at(p - 1, on);
    },
    waterCodeOn(item, on) {
      const p = byWater.get(item);
      return p === undefined ? "base" : at(p, on);
    },
  };
}
