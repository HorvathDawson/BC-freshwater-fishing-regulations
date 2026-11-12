/**
 * (T199) RUN TIMING: when each salmon and steelhead run is in the water, and where. (T261, owner 2026-10-29) Its own
 * bottom tab, Runs: a date slider by pentad, several species at once, the watershed groups zoomed out and the streams
 * zoomed in, and DFO's spawner counts in the tap sheet ("No counts" where there are none — never invented).
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
 *                                   profile and tens of km to the sea; (T261) per watershed group the pentads a
 *                                   local and a passing run of each slot is in the water (the zoomed-out field)
 *
 * NOTHING HERE DECIDES A TIMING: the pipeline bound and dated every run; this module looks a section's runs up for a
 * species and a day and words them. PSF dates are RIVER ENTRY and are said to be.
 */
import { MONTH_LAST_DAY } from "./calendar.generated";
import { FEED_FORMATS } from "./feedFormats.generated";
import { monthAbbr, PENTADS } from "./flow";

// (T556) generated from the Python writer's table (pipeline/common/feed_formats.py): never a literal here
export const RUNTIMING_FORMAT = { index: FEED_FORMATS.runtimingIndex, runs: FEED_FORMATS.runtimingRuns,
                                  paint: FEED_FORMATS.runtimingPaint } as const;

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
  /** (T261) DFO: the spawners counted, [year, count], the latest 12 counted years ascending; absent: no counts (every
   *  PSF run — its permission covers the timing, not abundance) */
  readonly n?: readonly (readonly [number, number])[];
  /** (T261, owner 2026-10-29) DFO: the ADULT spawners counted (NATURAL_ADULT_SPAWNERS), same shape, a separate series —
   *  never summed or merged with `n`; it leads the sheet only where `n` is missing */
  readonly na?: readonly (readonly [number, number])[];
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
  /** (T261) which source's runs carry counts, and the words for them */
  readonly counts?: { readonly dfo?: {
    readonly field: string; readonly words: string; readonly years: number;
    readonly adult?: { readonly field: string; readonly words: string } } };
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
  /** (T261) THE ZOOMED-OUT FIELD: per watershed group (the tiles' `basin_id`), per slot present [slot, local pentads,
   *  passing pentads] (73-bit hex, as a run's) — the OR of the runs bound to its sections */
  readonly basins?: { readonly ids: readonly string[]; readonly m: readonly (readonly (readonly [number, string, string])[])[] };
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

/** What a section shows for the species chosen on a day: a local run in the water (spawning, or surveyed in the stream),
 *  a run passing through, or runs recorded here that are not in the water then. No run recorded: no state. */
export type RunState = "spawn" | "pass" | "off";
/** The style's `run_state` enum, every member coloured. */
export const RUN_STATES: readonly RunState[] = ["spawn", "pass", "off"];
const STATE_CODE: readonly (RunState | undefined)[] = [undefined, "spawn", "pass", "off"];

/** A run's 73 pentad bits (hex, pentad 0 = bit 0) as one byte per pentad. */
function pentadBits(hex: string): Uint8Array {
  const out = new Uint8Array(PENTADS);
  for (let p = 0; p < PENTADS; p++) {
    const digit = parseInt(hex[hex.length - 1 - (p >> 2)] ?? "0", 16);
    out[p] = (digit >> (p & 3)) & 1;
  }
  return out;
}

/**
 * THE RUN TIMING AS THE MAP AND THE SHEET READ IT: the two files, a section's profile by binary search (no join), and
 * per species set and PENTAD (T261: the date slider scrubs by five days) each profile's state, computed once per
 * (slots, pentad) from the runs' own pentads and kept; and the zoomed-out field's state per watershed group.
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
  /** (T261) whose counts the runs carry (`n`), and in what words; null: none */
  readonly countsWords: string | null;
  private readonly sids: Int32Array;
  private readonly prof: Int32Array;
  private readonly km: Uint16Array;
  private readonly profiles: readonly PaintProfile[];
  private readonly basinDoc: PaintDoc["basins"] | null;
  private readonly bits: (Uint8Array | undefined)[];
  private readonly states = new Map<string, Uint8Array>();
  private readonly basinStates = new Map<string, Readonly<Record<string, RunState>>>();

  constructor(index: RunTimingIndex, runs: RunsDoc, paint: PaintDoc) {
    this.handles = index.section_handles;
    this.builtAt = index.built_at;
    this.runs = runs.runs;
    this.slots = runs.slots;
    this.defaultSlot = runs.default;
    this.sources = runs.sources;
    this.relationWords = runs.relation_words;
    this.relations = runs.relations;
    this.countsWords = runs.counts?.dfo?.words ?? null;
    this.profiles = paint.profiles;
    this.basinDoc = paint.basins ?? null;
    this.bits = new Array(runs.runs.length);
    const n = paint.sid.length;
    this.sids = new Int32Array(n);
    this.prof = new Int32Array(paint.p);
    this.km = new Uint16Array(paint.km);
    let s = 0;
    for (let i = 0; i < n; i++) { s += paint.sid[i]!; this.sids[i] = s; }
  }

  /** How many sections carry a run (a record for More / the sheet, not a colour). */
  get sections(): number { return this.sids.length; }

  /** Does the feed carry the zoomed-out field (the watershed groups' runs)? */
  get hasField(): boolean { return this.basinDoc !== null; }

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

  /** A run's pentads in the water, one byte each (decoded once). */
  inWater(run: number): Uint8Array {
    return this.bits[run] ??= pentadBits(this.runs[run]!.pentads);
  }

  /** Does any run touch the section? */
  has(sid: number): boolean { return this.row(sid) >= 0; }

  /**
   * A section's state for the chosen species slots on a pentad (0..72); undefined: none of them recorded here. Spawn
   * (a local run in the water) wins over passing, passing over a run recorded but not in the water then.
   */
  state(sid: number, slots: readonly number[], pentad: number): RunState | undefined {
    const r = this.row(sid);
    if (r < 0) return undefined;
    return STATE_CODE[this.table(slots, pentad)[this.prof[r]!]!];
  }

  /** Per profile, the state code (0 none, 1 spawn, 2 pass, 3 off) for the slots and pentad — built once, kept. */
  private table(slots: readonly number[], pentad: number): Uint8Array {
    const key = `${[...slots].sort((a, b) => a - b).join(",")}:${pentad}`;
    let t = this.states.get(key);
    if (t) return t;
    if (this.states.size > 160) this.states.clear();      // a long scrub through every species set: start again
    t = new Uint8Array(this.profiles.length);
    const passage = this.relations.indexOf("passage");
    this.profiles.forEach((pr, i) => {
      let code = 0;
      for (const x of pr.r) {
        const ri = Math.floor(x / 3);
        if (!slots.includes(this.runs[ri]!.slot)) continue;
        if (!this.inWater(ri)[pentad]) { code ||= 3; continue; }
        if (x % 3 !== passage) { code = 1; break; }
        if (code !== 1) code = 2;
      }
      t![i] = code;
    });
    this.states.set(key, t);
    return t;
  }

  /**
   * THE ZOOMED-OUT FIELD: each watershed group's state for the slots and pentad, by its id (the tiles' `basin_id`) —
   * the same order of answers as a section's. Groups with none of these runs are absent (the field's `missing`).
   */
  basins(slots: readonly number[], pentad: number): Readonly<Record<string, RunState>> {
    const b = this.basinDoc;
    if (!b) return {};
    const key = `${[...slots].sort((a, c) => a - c).join(",")}:${pentad}`;
    const kept = this.basinStates.get(key);
    if (kept) return kept;
    if (this.basinStates.size > 160) this.basinStates.clear();
    const out: Record<string, RunState> = {};
    const bit = (hex: string) => {
      const digit = parseInt(hex[hex.length - 1 - (pentad >> 2)] ?? "0", 16);
      return ((digit >> (pentad & 3)) & 1) === 1;
    };
    b.ids.forEach((id, i) => {
      let st: RunState | undefined;
      for (const [slot, local, pass] of b.m[i] ?? []) {
        if (!slots.includes(slot)) continue;
        if (bit(local)) { st = "spawn"; break; }
        if (bit(pass)) st = "pass";
        else st ??= "off";
      }
      if (st) out[id] = st;
    });
    this.basinStates.set(key, out);
    return out;
  }

  /**
   * (T261) THE SEASON AT A GLANCE, the slider's backdrop: per pentad, the share of the watershed groups these species
   * run in where a local run is in the water (`spawn`) and where only a passing one is (`pass`). Empty without the field.
   */
  season(slots: readonly number[]): readonly { readonly spawn: number; readonly pass: number }[] {
    const b = this.basinDoc;
    if (!b) return [];
    const groups = b.m.filter((rows) => rows.some(([slot]) => slots.includes(slot))).length;
    if (!groups) return Array.from({ length: PENTADS }, () => ({ spawn: 0, pass: 0 }));
    return Array.from({ length: PENTADS }, (_, p) => {
      let spawn = 0, pass = 0;
      for (const st of Object.values(this.basins(slots, p))) { if (st === "spawn") spawn++; else if (st === "pass") pass++; }
      return { spawn: spawn / groups, pass: pass / groups };
    });
  }

  /** The runs on a section and how each is here ("spawning" / "passage" / "observed"), in the file's order. */
  runsAt(sid: number): readonly { run: RunRecord; index: number; relation: string }[] {
    const r = this.row(sid);
    if (r < 0) return [];
    return this.profiles[this.prof[r]!]!.r.map((x) => ({ run: this.runs[Math.floor(x / 3)]!, index: Math.floor(x / 3),
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

/** The slots a set of chosen species paints in a year. */
export function slotsFor(f: Pick<RunTimingFacts, "slots">, chosen: readonly RunSpecies[], year: number): number[] {
  return chosen.map((s) => slotFor(f, s, year));
}

/**
 * (T261) THE PICTURE PER SPECIES: the fish art the regulations already draw (`FISH` codes, apps/mobile's
 * `FISH_IMAGES`), so the species chips show the fish, not a word alone. Ocean phase first in every set.
 */
export const RUN_SPECIES_CODE: Readonly<Record<string, string>> =
  { Chinook: "CH", Chum: "CM", Coho: "CO", Pink: "PK", Sockeye: "SK", Steelhead: "ST" };

/** The species kept between visits (`openRunSpeciesStore`): the names, as JSON. */
export function saveRunSpecies(names: readonly string[]): string { return JSON.stringify([...names]); }

/** The kept choice against the species the feed offers: unknown names dropped; none left (or nothing kept): the
 *  default — the feed's default species (steelhead, Q144). */
export function restoreRunSpecies(text: string | null, offered: readonly RunSpecies[]): string[] {
  let names: unknown = null;
  try { names = text ? JSON.parse(text) : null; } catch { names = null; }
  const ok = Array.isArray(names) ? names.filter((n): n is string => offered.some((s) => s.name === n)) : [];
  return ok.length ? [...new Set(ok)] : offered[0] ? [offered[0].name] : [];
}

/** The pentad of a day on the common year (Feb 29 reads as Feb 28): 0..72. */
export function pentadOf(day: Date): number {
  const start = new Date(day.getFullYear(), 0, 1).getTime();
  let d = Math.round((new Date(day.getFullYear(), day.getMonth(), day.getDate()).getTime() - start) / 86_400_000);
  const leap = new Date(day.getFullYear(), 1, 29).getMonth() === 1;
  if (leap && d >= 59) d -= 1;
  return Math.min(72, Math.floor(d / 5));
}

// a common year's month lengths, from the calendar spec (gate 4: one calendar spec; February 28 here)
const MONTH_DAYS = MONTH_LAST_DAY.map((d, m) => (m === 1 ? 28 : d));
const SHORT = MONTH_DAYS.map((_, m) => monthAbbr(m));

/** [month 0..11, day of month] of a day of the common year (0..364). */
function commonDay(d: number): readonly [number, number] {
  let m = 0;
  while (m < 11 && d >= MONTH_DAYS[m]!) { d -= MONTH_DAYS[m]!; m++; }
  return [m, d + 1];
}

/** A pentad's five days, as the slider and the sheet say them: "Aug 11–15", "Feb 28–Mar 4", "Dec 26–31". */
export function pentadLabel(p: number): string {
  const q = Math.max(0, Math.min(72, Math.round(p)));
  const [m0, d0] = commonDay(q * 5);
  const [m1, d1] = commonDay(q === 72 ? 364 : q * 5 + 4);
  return m0 === m1 ? `${SHORT[m0]} ${d0}–${d1}` : `${SHORT[m0]} ${d0}–${SHORT[m1]} ${d1}`;
}

/** The first day of a pentad in a year, as a date (the sheet's and the map's day while the slider holds it). */
export function pentadDay(p: number, year: number): Date {
  const [m, d] = commonDay(Math.max(0, Math.min(72, Math.round(p))) * 5);
  return new Date(year, m, d);
}

/** The month (0..11) a pentad starts in: the slider's month ticks. */
export function pentadMonth(p: number): number { return commonDay(Math.max(0, Math.min(72, p)) * 5)[0]; }

/** The first pentad of each month: where the slider's month ticks sit. */
export const MONTH_PENTADS: readonly number[] = SHORT.map((_, m) => {
  for (let p = 0; p < PENTADS; p++) if (pentadMonth(p) === m) return p;
  return 72;
});

/**
 * THE DATE SLIDER'S KEYS (web): the arrows step a pentad, Page Up / Down a month's six, Home / End the year's ends.
 * The pentad the key moves to, or null for a key the slider leaves alone.
 */
export function sliderKey(key: string, p: number): number | null {
  const to = key === "ArrowRight" || key === "ArrowUp" ? p + 1 : key === "ArrowLeft" || key === "ArrowDown" ? p - 1
    : key === "PageUp" ? p + 6 : key === "PageDown" ? p - 6 : key === "Home" ? 0 : key === "End" ? 72 : null;
  return to === null ? null : Math.max(0, Math.min(72, to));
}

/** Is a run in the water in a day's pentad (its own bits, finer than a month)? */
export function inWaterOn(run: Pick<RunRecord, "pentads">, day: Date): boolean {
  return inWaterAt(run, pentadOf(day));
}

/** Is a run in the water in a pentad (0..72)? */
export function inWaterAt(run: Pick<RunRecord, "pentads">, p: number): boolean {
  const hex = run.pentads;
  const digit = parseInt(hex[hex.length - 1 - (p >> 2)] ?? "0", 16);
  return ((digit >> (p & 3)) & 1) === 1;
}

// ---- words ---------------------------------------------------------------------------------------------------------

/** "Steelhead", "Steelhead and coho", "Steelhead, coho and chum", "4 species". */
export function speciesList(names: readonly string[]): string {
  if (names.length === 0) return "No species";
  if (names.length > 3) return `${names.length} species`;
  const n = names.map((x, i) => (i === 0 ? x : x.toLowerCase()));
  return n.length === 1 ? n[0]! : `${n.slice(0, -1).join(", ")} and ${n[n.length - 1]}`;
}

/** A count as the sheet says it: "60", "4,312", "1.2 million". */
export function countWords(n: number): string {
  if (n >= 1_000_000) return `${(Math.round(n / 100_000) / 10).toString()} million`;
  return Math.round(n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export const RUN_WORDS = {
  tab: "Runs",
  mode: "Run timing",
  speciesLabel: "Species",
  /** (T367) the species row in the Runs bar: every species again; a chip's spoken name (a long press shows it alone) */
  allSpecies: "All",
  allSpeciesSaid: "Show every species",
  chipSaid: (name: string, on: boolean) => `${name}, ${on ? "shown" : "hidden"}. Long-press to show only ${name}`,
  loading: "Loading run timing…",
  legend: { spawn: "In the water: spawning or surveyed here", pass: "Passing through", off: "A run here, not in the water then",
            none: "No run recorded" } satisfies Record<RunState | "none", string>,
  legendTitle: (species: readonly string[]) => `${speciesList(species)} runs`,
  /** below the field's handover: each watershed group by its runs */
  field: "Each watershed group by its runs. Zoom in for the streams.",
  slider: { label: "Day of the year", today: "Today", play: "Play through the year", pause: "Pause",
            value: (p: number, today: boolean) => (today ? `${pentadLabel(p)} (today)` : pentadLabel(p)),
            /** (T377) a peak marker on the histogram: when most watershed groups have that run in the water */
            peak: (name: string | null, p: number) => `${name ? `${name} peak` : "Peak"}: most groups in the water ${pentadLabel(p)}` },
  /** the run-size key: the sheet's bars are on this scale */
  scale: { title: "Spawners counted (DFO; total, else adults) — tap a stream", ticks: ["10", "100", "1k", "10k", "100k"] as const,
           none: "No counts: shown as such, never guessed" },
  sheetTitle: "Fish runs here",
  none: (species: readonly string[]) =>
    `No ${speciesList(species).toLowerCase()} run is recorded on this water. That is not the same as no fish.`,
  inWater: (p: number) => `In the water ${pentadLabel(p)}`,
  notInWater: (p: number) => `Not in the water ${pentadLabel(p)}`,
  riverEntry: "PSF dates are when fish enter fresh water.",
  km: (n: number) => `This water is about ${n} km from the sea, so they reach it later.`,
  surveyed: (y: readonly [number, number, number]) => `DFO surveys: ${y[0]} years, ${y[1]}–${y[2]}`,
  noCounts: "No counts",
  /** the two series' labels: DFO's total and its adult-only estimate, side by side, never added */
  series: { total: "Total spawners", adult: "Adult spawners" },
  latest: (year: number, n: number) => `Latest count ${countWords(n)} (${year})`,
  trend: { up: "Recent counts are higher than the years before",
           down: "Recent counts are lower than the years before",
           flat: "Recent counts are about the same as the years before" },
  countsNote: "Counts are DFO's spawner estimates for the surveyed stream: total spawners and adult spawners are "
    + "separate estimates, shown apart. PSF runs carry no counts here.",
  captured: (built: string) => `Offline: the run timing saved on this device (built ${built.slice(0, 10)}).`,
  stale: "Run timing is out of date for this map, so it isn't drawn.",
  failed: "Run timing could not be loaded, so it isn't drawn.",
  openWater: "Open this water",
  more: (n: number) => (n === 1 ? "1 run of another species here" : `${n} runs of other species here`),
  fewer: "Hide the other species",
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

/** One year's count as a bar: its height on the log scale the legend keys (10 → 0.2 … 100k → 1). */
export interface CountBar { readonly year: number; readonly n: number; readonly h: number; readonly label: string }

/** One labelled series of counts, worded: the bars, the latest, the trend. */
export interface CountSeriesVM {
  readonly label: string;
  readonly bars: readonly CountBar[];
  readonly latest: string;
  readonly trend: string | null;
}

/** A run's counts: its series, the headline one first (total spawners; adult spawners when there is no total) — or null
 *  (the sheet then says "No counts"). Two series stay two: they are different estimates and are never combined. */
export interface RunCountsVM { readonly series: readonly CountSeriesVM[] }

/** Where a count sits on the run-size scale: log10, 10 → 0.2, 100k and over → 1; zero is no bar at all. */
export function countHeight(n: number): number {
  return n <= 0 ? 0 : Math.max(0.06, Math.min(1, Math.log10(n) / 5));
}

const median = (xs: readonly number[]) => {
  const s = [...xs].sort((a, b) => a - b), m = s.length >> 1;
  return s.length % 2 ? s[m]! : (s[m - 1]! + s[m]!) / 2;
};

/**
 * THE RUN'S SIZE, from DFO's counts only (never a PSF number, never guessed): every counted year as a bar, the latest,
 * and with six counted years or more a trend — the median of the last three against the median of up to six before.
 */
export function runCounts(r: Pick<RunRecord, "n"> & { readonly na?: RunRecord["na"] }): RunCountsVM | null {
  const one = (n: RunRecord["n"], label: string): CountSeriesVM | null => {
    if (!n || n.length === 0) return null;
    const bars = n.map(([year, c]) => ({ year, n: c, h: countHeight(c), label: `${year}: ${countWords(c)}` }));
    const [ly, lc] = n[n.length - 1]!;
    let trend: string | null = null;
    if (n.length >= 6) {
      const recent = median(n.slice(-3).map((x) => x[1]));
      const before = median(n.slice(-9, -3).map((x) => x[1]));
      trend = before === 0 ? (recent === 0 ? RUN_WORDS.trend.flat : RUN_WORDS.trend.up)
        : recent / before >= 1.5 ? RUN_WORDS.trend.up : recent / before <= 2 / 3 ? RUN_WORDS.trend.down : RUN_WORDS.trend.flat;
    }
    return { label, bars, latest: RUN_WORDS.latest(ly, lc), trend };
  };
  const series = [one(r.n, RUN_WORDS.series.total), one(r.na, RUN_WORDS.series.adult)].filter((x): x is CountSeriesVM => x !== null);
  return series.length ? { series } : null;
}

/** One run on the tap sheet, worded. */
export interface RunRowVM {
  readonly key: string;
  readonly title: string;
  /** the species' fish art code (`RUN_SPECIES_CODE`) */
  readonly code: string | null;
  readonly population: string;
  readonly relation: string;
  /** how the map draws this run here: spawn (solid) or pass (dashed) — the year chart's colour */
  readonly kind: "spawn" | "pass";
  readonly dates: string | null;
  readonly now: boolean;
  readonly nowWords: string;
  /** the year chart: 73 pentads, in the water or not */
  readonly year: readonly boolean[];
  readonly survey: string | null;
  readonly counts: RunCountsVM | null;
  readonly source: string;
}

export interface RunSheetVM {
  readonly title: string;
  readonly rows: readonly RunRowVM[];
  /** how many of `rows` (the first) are the chosen species'; the rest fold behind `moreWords` */
  readonly mine: number;
  readonly moreWords: string | null;
  /** the species chosen are not among them: said, never "no fish" */
  readonly noneOfSpecies: string | null;
  /** the pentad shown (the year chart's marker) */
  readonly pentad: number;
  readonly riverEntry: string | null;
  readonly km: string | null;
  /** said once when any row has counts or a DFO row has none */
  readonly countsNote: string | null;
  readonly credit: string;
}

/** The sheet for a tapped section: the chosen species' runs first, then the others; on the pentad shown. */
export function runSheet(f: RunTimingFacts, sid: number, chosen: readonly RunSpecies[], pentad: number): RunSheetVM {
  const here = f.runsAt(sid);
  const names = chosen.map((s) => s.name);
  const mine = (r: RunRecord) => names.includes(r.sp);
  const ordered = [...here.filter((h) => mine(h.run)), ...here.filter((h) => !mine(h.run))];
  const rows = ordered.map(({ run, index, relation }) => {
    const bits = f.inWater(index);
    const now = bits[pentad] === 1;
    return { key: `${run.id}:${relation}`, title: runTitle(run), code: RUN_SPECIES_CODE[run.sp] ?? null,
             population: run.name, relation: f.relationWords[relation] ?? relation,
             kind: relation === "passage" ? "pass" as const : "spawn" as const, dates: runDates(run), now,
             nowWords: now ? RUN_WORDS.inWater(pentad) : RUN_WORDS.notInWater(pentad),
             year: Array.from(bits, (b) => b === 1),
             survey: run.years ? RUN_WORDS.surveyed(run.years) : null, counts: runCounts(run),
             source: f.sources[run.src]?.credit ?? run.src };
  });
  const psf = here.some((h) => h.run.src === "psf");
  const km = f.kmToSea(sid);
  const used = [...new Set(here.map((h) => h.run.src))].sort();
  return {
    title: RUN_WORDS.sheetTitle, rows, pentad,
    mine: here.filter((h) => mine(h.run)).length,
    moreWords: rows.length > here.filter((h) => mine(h.run)).length
      ? RUN_WORDS.more(rows.length - here.filter((h) => mine(h.run)).length) : null,
    noneOfSpecies: here.some((h) => mine(h.run)) ? null : RUN_WORDS.none(names),
    riverEntry: psf ? RUN_WORDS.riverEntry : null,
    km: psf && km !== null && km > 0 ? RUN_WORDS.km(km) : null,
    countsNote: rows.length ? RUN_WORDS.countsNote : null,
    credit: runCredit(f, used.length ? used : Object.keys(f.sources).sort()),
  };
}

/** The credit line every surface showing a timing carries: each source's credit and its terms. */
export function runCredit(f: Pick<RunTimingFacts, "sources">, which: readonly string[]): string {
  return RUN_WORDS.credit(which.filter((k) => f.sources[k]).map((k) => `${f.sources[k]!.credit} — ${f.sources[k]!.terms}`));
}

/**
 * (T377) THE SEASON'S PEAK, for a marker on the Runs histogram: the pentad where the most watershed groups have the run in
 * the water (spawning plus passing), the first of a tie. Null for a season with no run at all — no marker, never a
 * marker at January.
 */
export function seasonPeak(season: readonly { readonly spawn: number; readonly pass: number }[]): number | null {
  let at: number | null = null, best = 0;
  season.forEach((s, i) => { const v = s.spawn + s.pass; if (v > best) { best = v; at = i; } });
  return at;
}
