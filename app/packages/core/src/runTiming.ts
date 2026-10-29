/**
 * (T199) RUN TIMING: when each salmon and steelhead run is in the water, and where — the Map tab's Run timing mode.
 *
 * ITS OWN STREAM, NOT THE RELEASE (owner 2026-10-28): the files are the run-timing feed's (`runtiming/` on the data
 * origin, `pipeline/runtiming/feed/`), read lazily the first time the mode is chosen and kept as the last capture. The
 * paint index is bound to ONE atlas (`section_handles`): on tiles of another atlas nothing is painted.
 *
 *   index.json   runtiming-index/1  the current files (content-named) with their decoded bytes and sha256
 *   runs         runtiming-runs/1   every run: species, slot, population / stream, label, peak, window, months and
 *                                   pentads in the water, its source; the sources' credits and terms; the default
 *   paint        runtiming-paint/1  profiles (run x relation sets, with per slot the months a LOCAL run — spawning or
 *                                   surveyed in the stream — and a PASSING run is in the water) and per section its
 *                                   profile and tens of km to the sea
 *
 * NOTHING HERE DECIDES A TIMING: the pipeline bound and dated every run; this module looks a section's runs up for a
 * species and a day and words them. PSF dates are RIVER ENTRY and are said to be.
 */
import { MONTHS } from "./flow";

export const RUNTIMING_FORMAT = { index: "runtiming-index/1", runs: "runtiming-runs/1",
                                  paint: "runtiming-paint/1" } as const;

export interface RunTimingFile { readonly url: string; readonly bytes: number; readonly sha256: string }
export interface RunTimingIndex {
  readonly format: string;
  readonly built_at: string;
  readonly section_handles: string;
  readonly tile_version: string | null;
  readonly sources: Readonly<Record<string, { credit: string; updated: string | null; fetched: string | null }>>;
  readonly files: { readonly runs: RunTimingFile; readonly paint: RunTimingFile; readonly waters?: RunTimingFile };
}

export interface RunRecord {
  readonly id: string;
  readonly src: "psf" | "dfo";
  /** The species ("Chinook", "Steelhead" …) and its colour slot (pink split by brood line) */
  readonly sp: string;
  readonly slot: number;
  /** the Conservation Unit's name (PSF) or the surveyed stream's (DFO) */
  readonly name: string;
  readonly label?: string;
  readonly peak?: string;
  readonly window?: readonly [string, string];
  /** PSF's data-quality word */
  readonly q?: string;
  /** DFO: [years surveyed, first, last] */
  readonly years?: readonly [number, number, number];
  /** 12 bits, January = bit 0 */
  readonly months: number;
  /** 73 bits as hex, pentad 0 (Jan 1–5) = bit 0 */
  readonly pentads: string;
}

export interface RunSource {
  readonly credit: string; readonly title: string; readonly dataset: string; readonly updated: string | null;
  readonly fetched: string | null; readonly terms: string; readonly url: string; readonly licence?: string;
}

export interface RunsDoc {
  readonly format: string;
  readonly slots: readonly string[];
  readonly default: number;
  readonly relations: readonly string[];
  readonly relation_words: Readonly<Record<string, string>>;
  readonly sources: Readonly<Record<string, RunSource>>;
  readonly runs: readonly RunRecord[];
}

export interface PaintProfile { readonly r: readonly number[]; readonly s: number; readonly m: readonly (readonly [number, number, number])[] }
export interface PaintDoc {
  readonly format: string;
  readonly section_handles: string;
  readonly tile_version: string | null;
  readonly runs_sha256: string;
  readonly profiles: readonly PaintProfile[];
  readonly sid: readonly number[];
  readonly p: readonly number[];
  readonly km: readonly number[];
}

const isObj = (x: unknown): x is Record<string, unknown> => typeof x === "object" && x !== null && !Array.isArray(x);
const isFile = (x: unknown): x is RunTimingFile =>
  isObj(x) && typeof x.url === "string" && typeof x.bytes === "number" && typeof x.sha256 === "string";

/** The index, or null when it is not one this app reads (another format, a file missing). */
export function parseRunTimingIndex(j: unknown): RunTimingIndex | null {
  if (!isObj(j) || j.format !== RUNTIMING_FORMAT.index || typeof j.section_handles !== "string") return null;
  const f = j.files;
  if (!isObj(f) || !isFile(f.runs) || !isFile(f.paint)) return null;
  return j as unknown as RunTimingIndex;
}

/** The runs and the paint index of one index, held to each other: refused (throws) when they are not a pair. */
export function checkRunTiming(runs: unknown, paint: unknown, index: RunTimingIndex): { runs: RunsDoc; paint: PaintDoc } {
  if (!isObj(runs) || runs.format !== RUNTIMING_FORMAT.runs || !Array.isArray(runs.runs) || !Array.isArray(runs.slots))
    throw new Error("run timing: not a runtiming-runs/1 file");
  if (!isObj(paint) || paint.format !== RUNTIMING_FORMAT.paint || !Array.isArray(paint.sid))
    throw new Error("run timing: not a runtiming-paint/1 file");
  const p = paint as unknown as PaintDoc;
  if (p.runs_sha256 !== index.files.runs.sha256) throw new Error("run timing: the paint index is not of these runs");
  if (p.section_handles !== index.section_handles) throw new Error("run timing: the paint index names another atlas");
  if (p.sid.length !== p.p.length || p.sid.length !== p.km.length) throw new Error("run timing: paint columns differ");
  return { runs: runs as unknown as RunsDoc, paint: p };
}

/** What a section shows for a species on a day: a local run in the water (spawning, or surveyed in the stream), a run
 *  passing through, or runs recorded here that are not in the water that month. No run recorded: no state. */
export type RunState = "spawn" | "pass" | "off";
/** The style's `run_state` enum, every member coloured. */
export const RUN_STATES: readonly RunState[] = ["spawn", "pass", "off"];
const STATE_CODE: readonly (RunState | undefined)[] = [undefined, "spawn", "pass", "off"];

/**
 * THE RUN TIMING AS THE MAP AND THE SHEET READ IT: the two files, a section's profile by binary search (no join), and
 * per species slot and month each profile's state, computed once per (slot, month) and kept.
 */
export class RunTimingFacts {
  readonly handles: string;
  readonly runs: readonly RunRecord[];
  readonly slots: readonly string[];
  readonly defaultSlot: number;
  readonly sources: Readonly<Record<string, RunSource>>;
  readonly relationWords: Readonly<Record<string, string>>;
  readonly relations: readonly string[];
  readonly builtAt: string;
  private readonly sids: Int32Array;
  private readonly prof: Int32Array;
  private readonly km: Uint16Array;
  private readonly profiles: readonly PaintProfile[];
  private readonly states = new Map<string, Uint8Array>();

  constructor(index: RunTimingIndex, runs: RunsDoc, paint: PaintDoc) {
    this.handles = index.section_handles;
    this.builtAt = index.built_at;
    this.runs = runs.runs;
    this.slots = runs.slots;
    this.defaultSlot = runs.default;
    this.sources = runs.sources;
    this.relationWords = runs.relation_words;
    this.relations = runs.relations;
    this.profiles = paint.profiles;
    const n = paint.sid.length;
    this.sids = new Int32Array(n);
    this.prof = new Int32Array(paint.p);
    this.km = new Uint16Array(paint.km);
    let s = 0;
    for (let i = 0; i < n; i++) { s += paint.sid[i]!; this.sids[i] = s; }
  }

  /** How many sections carry a run (a record for More / the sheet, not a colour). */
  get sections(): number { return this.sids.length; }

  /** The row of a section in the paint index, or -1. */
  private row(sid: number): number {
    let lo = 0, hi = this.sids.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1, v = this.sids[mid]!;
      if (v === sid) return mid;
      if (v < sid) lo = mid + 1; else hi = mid - 1;
    }
    return -1;
  }

  /** Does any run touch the section? */
  has(sid: number): boolean { return this.row(sid) >= 0; }

  /** A section's state for a species slot and a month (0 = January); undefined: no run of it recorded here. */
  state(sid: number, slot: number, month: number): RunState | undefined {
    const r = this.row(sid);
    if (r < 0) return undefined;
    const key = `${slot}:${month}`;
    let t = this.states.get(key);
    if (!t) {
      t = new Uint8Array(this.profiles.length);
      const bit = 1 << month;
      this.profiles.forEach((p, i) => {
        const m = p.m.find((x) => x[0] === slot);
        if (!m) return;
        t![i] = m[1] & bit ? 1 : m[2] & bit ? 2 : 3;
      });
      this.states.set(key, t);
    }
    return STATE_CODE[t[this.prof[r]!]!];
  }

  /** The runs on a section and how each is here ("spawning" / "passage" / "observed"), in the file's order. */
  runsAt(sid: number): readonly { run: RunRecord; relation: string }[] {
    const r = this.row(sid);
    if (r < 0) return [];
    return this.profiles[this.prof[r]!]!.r.map((x) => ({ run: this.runs[Math.floor(x / 3)]!,
                                                           relation: this.relations[x % 3]! }));
  }

  /** River km to the sea, rounded to tens, where a PSF run is here; else null. */
  kmToSea(sid: number): number | null {
    const r = this.row(sid);
    const k = r < 0 ? 0 : this.km[r]!;
    return k > 0 ? (k - 1) * 10 : null;
  }
}

// ---- species and days ----------------------------------------------------------------------------------------------

/** A species as the reader picks it: pink's two brood lines are one choice, the year shown picks the line. */
export interface RunSpecies { readonly name: string; readonly slots: readonly number[] }

/** The species row, steelhead (the default slot) first, then the file's order; pink's brood lines folded into one. */
export function runSpecies(f: Pick<RunTimingFacts, "slots" | "defaultSlot">): readonly RunSpecies[] {
  const byName = new Map<string, number[]>();
  f.slots.forEach((s, i) => {
    const name = s.replace(/ \((even|odd)\)$/, "");
    (byName.get(name) ?? byName.set(name, []).get(name)!).push(i);
  });
  const all = [...byName].map(([name, slots]) => ({ name, slots }));
  const first = all.findIndex((s) => s.slots.includes(f.defaultSlot));
  return first < 0 ? all : [all[first]!, ...all.filter((_, i) => i !== first)];
}

/** The slot a species paints in a year: pink by its brood line (even years / odd years), every other its own. */
export function slotFor(f: Pick<RunTimingFacts, "slots">, species: RunSpecies, year: number): number {
  if (species.slots.length === 1) return species.slots[0]!;
  const want = year % 2 === 0 ? "(even)" : "(odd)";
  return species.slots.find((s) => f.slots[s]!.endsWith(want)) ?? species.slots[0]!;
}

/** The pentad of a day on the common year (Feb 29 reads as Feb 28): 0..72. */
export function pentadOf(day: Date): number {
  const start = new Date(day.getFullYear(), 0, 1).getTime();
  let d = Math.round((new Date(day.getFullYear(), day.getMonth(), day.getDate()).getTime() - start) / 86_400_000);
  const leap = new Date(day.getFullYear(), 1, 29).getMonth() === 1;
  if (leap && d >= 59) d -= 1;
  return Math.min(72, Math.floor(d / 5));
}

/** Is a run in the water in a day's pentad (its own bits, finer than the map's month)? */
export function inWaterOn(run: Pick<RunRecord, "pentads">, day: Date): boolean {
  const p = pentadOf(day);
  const hex = run.pentads;
  const digit = parseInt(hex[hex.length - 1 - (p >> 2)] ?? "0", 16);
  return ((digit >> (p & 3)) & 1) === 1;
}

// ---- words ---------------------------------------------------------------------------------------------------------

export const RUN_WORDS = {
  mode: "Run timing",
  speciesLabel: "Species",
  loading: "Loading…",
  legend: { spawn: "In the water: spawning or surveyed here", pass: "Passing through", off: "A run here, not this month",
            none: "No run recorded" } satisfies Record<RunState | "none", string>,
  legendTitle: (species: string, month: number) => `${species} runs in ${MONTHS[month] ?? ""}`.trim(),
  sheetTitle: "Fish runs here",
  none: (species: string) => `No ${species.toLowerCase()} run is recorded on this water. That is not the same as no fish.`,
  inWater: "In the water now",
  notInWater: "Not in the water now",
  riverEntry: "PSF dates are when fish enter fresh water.",
  km: (n: number) => `This water is about ${n} km from the sea, so they reach it later.`,
  surveyed: (y: readonly [number, number, number]) => `DFO surveys: ${y[0]} years, ${y[1]}–${y[2]}`,
  stale: "Run timing was made for another map version — it is not drawn.",
  failed: "Run timing could not be loaded.",
  openWater: "Open this water",
  credit: (sources: readonly string[]) => `Run timing: ${sources.join(" ")}`,
} as const;

/** A run's title line: "Steelhead — summer" / "Chinook". */
export function runTitle(r: Pick<RunRecord, "sp" | "label">): string {
  return r.label ? `${r.sp} — ${r.label}` : r.sp;
}

/** A run's dates line, in its source's terms: PSF river entry, DFO in the stream. */
export function runDates(r: Pick<RunRecord, "src" | "window" | "peak">): string | null {
  const w = r.window ? `${r.window[0]} – ${r.window[1]}` : null;
  const pk = r.peak ? `peak ${r.peak}` : null;
  if (!w && !pk) return null;
  const lead = r.src === "psf" ? "Enters fresh water" : "In the stream";
  return `${lead} ${[w, pk].filter(Boolean).join(", ")}`;
}

/** One run on the tap sheet, worded. */
export interface RunRowVM {
  readonly key: string;
  readonly title: string;
  readonly population: string;
  readonly relation: string;
  readonly dates: string | null;
  readonly now: boolean;
  readonly nowWords: string;
  readonly survey: string | null;
  readonly source: string;
}

export interface RunSheetVM {
  readonly title: string;
  readonly rows: readonly RunRowVM[];
  /** the species chosen is not among them: said, never "no fish" */
  readonly noneOfSpecies: string | null;
  readonly riverEntry: string | null;
  readonly km: string | null;
  readonly credit: string;
}

/** The sheet for a tapped section: the chosen species' runs first, then the others; on the day shown. */
export function runSheet(f: RunTimingFacts, sid: number, species: RunSpecies, day: Date): RunSheetVM {
  const here = f.runsAt(sid);
  const mine = (slot: number) => species.slots.includes(slot);
  const ordered = [...here.filter((h) => mine(h.run.slot)), ...here.filter((h) => !mine(h.run.slot))];
  const rows = ordered.map(({ run, relation }) => {
    const now = inWaterOn(run, day);
    return { key: `${run.id}:${relation}`, title: runTitle(run), population: run.name,
             relation: f.relationWords[relation] ?? relation, dates: runDates(run), now,
             nowWords: now ? RUN_WORDS.inWater : RUN_WORDS.notInWater,
             survey: run.years ? RUN_WORDS.surveyed(run.years) : null,
             source: f.sources[run.src]?.credit ?? run.src };
  });
  const psf = here.some((h) => h.run.src === "psf");
  const km = f.kmToSea(sid);
  const used = [...new Set(here.map((h) => h.run.src))].sort();
  return {
    title: RUN_WORDS.sheetTitle, rows,
    noneOfSpecies: here.some((h) => mine(h.run.slot)) ? null : RUN_WORDS.none(species.name),
    riverEntry: psf ? RUN_WORDS.riverEntry : null,
    km: psf && km !== null && km > 0 ? RUN_WORDS.km(km) : null,
    credit: runCredit(f, used.length ? used : Object.keys(f.sources).sort()),
  };
}

/** The legend's credit (the map's foot, short): whose data, by name; the sheet carries the terms in full. */
export function runCreditShort(f: Pick<RunTimingFacts, "sources">): string {
  return RUN_WORDS.credit([Object.keys(f.sources).sort().map((k) => f.sources[k]!.credit).join(" · ")]);
}

/** The credit line every surface showing a timing carries: each source's credit and its terms. */
export function runCredit(f: Pick<RunTimingFacts, "sources">, which: readonly string[]): string {
  return RUN_WORDS.credit(which.filter((k) => f.sources[k]).map((k) => `${f.sources[k]!.credit} — ${f.sources[k]!.terms}`));
}
