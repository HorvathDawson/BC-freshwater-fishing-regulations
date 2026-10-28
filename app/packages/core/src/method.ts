/**
 * (T213) BAIT BAN AND FLY FISHING ONLY, decoded — the map's "Bait & fly" colouring, a third choice beside Standard and
 * Salmon.
 *
 * The release file `method` (`method/1`, `pipeline/deliver/method.py`) is a projection of the answers' open-days strips
 * (the bits the water page's strip draws), so the map and the strip cannot disagree. Per section and day of the year it
 * says one class, decided in the pipeline:
 *
 *   none     open, neither bait ban nor fly only           `color.method.none`
 *   bait     bait ban                                       `color.method.bait`
 *   fly      fly fishing only / artificial flies only       `color.method.fly`
 *
 * CLOSED IS NOT IN THE FILE: a closed day reads `none` here. The status index says closed (it is installed first on the
 * Map tab), so the Bait & fly colouring paints a section the day's status closes in `color.method.closed` before it
 * reads this file — one source for "closed". A section the file does not list is `none` every day. Like the status index, every section points at one of a few
 * hundred day-profiles, so the map stores each section's PROFILE label once (feature-state) and carries the day in its
 * style (`methodDayProfiles`, one match over the labels): a day change is one paint property, a toggle a mode change.
 *
 *     "BCMT", u8 1, 8 bytes section_handles,
 *     nProfiles × (nRuns × varint(length << 2 | code)),    366 days each, the status index's leap calendar
 *     nRuns, then gap[], count[], profile[]                 sid runs (absent: none)
 */
import { readingDayOf } from "./statusIndex";

export const METHOD_CLASSES = ["none", "bait", "fly"] as const;
export type MethodClass = typeof METHOD_CLASSES[number];

const DAYS = 366;

export interface MethodFacts {
  readonly handles: string;
  /** How many day-profiles the file holds. */
  readonly profiles: number;
  /** The day-profile a section reads its year from; null for one the file does not list (none every day). */
  profileOf(sid: number): number | null;
  /** One profile's class on a reading day 1..366. */
  profileClassOn(profile: number, day: number): MethodClass;
}

export class MethodError extends Error {}

export function decodeMethod(b: Uint8Array, handles: string): MethodFacts {
  if (b.length < 13 || b[0] !== 0x42 || b[1] !== 0x43 || b[2] !== 0x4d || b[3] !== 0x54 || b[4] !== 1)
    throw new MethodError("method: not a method/1 file");
  let digest = "";
  for (let i = 5; i < 13; i++) digest += b[i]!.toString(16).padStart(2, "0");
  if (digest !== handles) throw new MethodError(`method: built for handles ${digest}, not ${handles}`);
  let i = 13;
  const varint = (): number => {
    let v = 0, mul = 1;
    for (;;) {
      if (i >= b.length) throw new MethodError("method: truncated");
      const x = b[i++]!;
      v += (x & 0x7f) * mul;
      if (!(x & 0x80)) return v;
      mul *= 128;
    }
  };
  const nProfiles = varint();
  const days = new Uint8Array(nProfiles * DAYS);
  for (let p = 0; p < nProfiles; p++) {
    let d = 0;
    for (let r = varint(); r > 0; r--) {
      const x = varint(), len = Math.floor(x / 4), code = x & 3;
      if (code > 2) throw new MethodError(`method: class code ${code}`);
      if (d + len > DAYS) throw new MethodError("method: a profile runs past 366 days");
      days.fill(code, p * DAYS + d, p * DAYS + d + len);
      d += len;
    }
    if (d !== DAYS) throw new MethodError("method: a profile does not cover 366 days");
  }
  const count = varint();
  const starts = new Float64Array(count), ends = new Float64Array(count), prof = new Int32Array(count);
  const gaps = new Float64Array(count);
  for (let k = 0; k < count; k++) gaps[k] = varint();
  let end = 0;
  for (let k = 0; k < count; k++) {
    starts[k] = end + gaps[k]!;
    ends[k] = starts[k]! + varint();
    end = ends[k]!;
  }
  for (let k = 0; k < count; k++) {
    const p = varint();
    if (p >= nProfiles) throw new MethodError("method: a run names no profile");
    prof[k] = p;
  }
  if (i !== b.length) throw new MethodError(`method: ${b.length - i} trailing bytes`);
  return {
    handles: digest, profiles: nProfiles,
    profileOf(sid) {
      let lo = 0, hi = count - 1;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (sid < starts[mid]!) hi = mid - 1;
        else if (sid >= ends[mid]!) lo = mid + 1;
        else return prof[mid]!;
      }
      return null;
    },
    profileClassOn(p, day) { return METHOD_CLASSES[days[p * DAYS + day - 1]!]!; },
  };
}

/** A section's class on a day (`none` for one the file does not list). */
export function methodClassOn(f: MethodFacts, sid: number, on: Date): MethodClass {
  const p = f.profileOf(sid);
  return p === null ? "none" : f.profileClassOn(p, readingDayOf(on));
}

/** The label of a section the file does not list: `none` every day, so it sits in every day's `none` set. */
export const METHOD_UNLISTED = "m";

/** A section's profile label for the Bait & fly colouring: `m<n>`, fixed for the release (strings: see `profileLabel`). */
export function methodLabel(f: MethodFacts, sid: number): string {
  const p = f.profileOf(sid);
  return p === null ? METHOD_UNLISTED : `m${p}`;
}

/** The day's label sets: which profile labels paint which class. Every label is in exactly one set. */
export type MethodDay = Readonly<Record<MethodClass, readonly string[]>>;

export function methodDayProfiles(f: MethodFacts, on: Date): MethodDay {
  const day = readingDayOf(on);
  const out: Record<MethodClass, string[]> = { none: [METHOD_UNLISTED], bait: [], fly: [] };
  for (let p = 0; p < f.profiles; p++) out[f.profileClassOn(p, day)].push(`m${p}`);
  return out;
}

/** What the legend lists: the file's classes, and closed (the status index's, the Standard map's red). */
export type MethodLegendClass = MethodClass | "closed";

/** The legend's words, in the order it lists them (closed last). */
export const METHOD_WORDS = {
  mode: "Bait & fly",
  legend: { fly: "Fly fishing only", bait: "Bait ban", none: "No bait or fly restriction", closed: "Closed" },
  says: {
    fly: "Fly fishing only or artificial flies only — no bait.",
    bait: "Bait ban: no bait of any kind.",
    none: "Bait and lures allowed under the general rules.",
    closed: "Closed: no fishing for any game fish.",
  },
} as const;

export const METHOD_LEGEND: readonly MethodLegendClass[] = ["fly", "bait", "none", "closed"];
