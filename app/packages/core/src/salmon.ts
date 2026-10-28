/**
 * THE DFO SALMON (T211, option A + the Standard / Salmon map toggle): the release file `salmon` (`salmon/1`,
 * `pipeline/deliver/salmon.py`) read, and every word the Salmon card and the Salmon map legend say.
 *
 * NOTHING HERE READS A RULE. The pipeline precomputed, per CHAIN of DFO locations (narrowest first — Region 6 is a
 * cascade) and per day of the year, each species' verdict, the gear lines and the map status. This module only looks
 * them up for a day, groups the species that answer alike, and words it.
 *
 * CLOSED UNLESS DFO OPENS IT (owner 2026-10-27): chain 0 is the UNLISTED water — every species closed, every day — and
 * a section the file places in no run reads chain 0. Explicit in the file (`payload.default`), never an absence.
 *
 * Layout (`salmon/2`, little-endian, varints LEB128), SPLIT PER BUNDLE REGION (owner 2026-10-28: a water page's card
 * fetches its own region's slice, the Salmon map all of them):
 *   salmon       b"BCSA", u8 2, 8 bytes of the section_handles digest, u32 JSON length, the UTF-8 JSON payload
 *   salmon-<r>   b"BCSR", u8 1, 8 bytes of the digest, u8 region length + region, then a run count and per run: sid gap
 *                (from the previous run's end), count, chain — the sections of bundle region <r> (`x`: in none)
 * A section in no LOADED slice reads chain 0, so a reader asks for the slices of the sections it reads (the card: its
 * page's regions; the map: every one).
 *
 * THE REGION HEADER (owner 2026-10-28): each DFO region page's header — daylight only, the facility closure, the
 * aggregate limits, sizes, what an "adult chinook" is — as typed notes on `regions[slug].notes`, DFO's words whole; one
 * naming waters holds only on their items (`scopes`). Which notes a page shows is `salmonNotes`, held to the pipeline's
 * `deliver.salmon.notes_for` by the golden.
 */
import { DAYS_BEFORE_MONTH, MONTHS } from "./calendar.generated";
import { METHOD_WORDS, type MethodClass } from "./method";
import type { GearVM, MethodsVM } from "./regs/model";
import { BOOK_DAYS, fmtDay } from "./regs/strip";
import { WORDS } from "./regs/words";

export const SALMON_SPECIES = ["CH", "CO", "SK", "PK", "CM"] as const;
export type SalmonSpecies = typeof SALMON_SPECIES[number];
// one string, split: a species' name is not a colour, whatever CSS calls it (tools/check-colours.mjs reads "pink" as one)
const NAMES = "chinook coho sockeye pink chum".split(" ");
const SPECIES_NAME = Object.fromEntries(SALMON_SPECIES.map((s, k) => [s, NAMES[k]!])) as
  Readonly<Record<SalmonSpecies, string>>;

/** A species' verdict for a day, as the pipeline decided it. */
export type SalmonVerdictStatus = "open" | "release" | "closed" | "check";
/** What the Salmon colouring paints a section on a day (the style's `salmon_status` enum, every member coloured). */
export const SALMON_MAP_STATUSES = ["open", "release", "closed", "check"] as const;
export type SalmonMapStatus = typeof SALMON_MAP_STATUSES[number];
/** The file's map codes, in its order (0 "none" is a chain with no verdict at all — never built). */
const MAP_CODES: readonly (SalmonMapStatus | "none")[] = ["none", "open", "release", "closed", "check"];

interface Verdict { readonly s: SalmonVerdictStatus; readonly t: string; readonly n?: string; readonly d?: 1 }
interface Chain {
  readonly r: readonly string[]; readonly w: string; readonly l: readonly string[];
  /** per species (SALMON_SPECIES order) runs [start, verdict index, …] */
  readonly sp: readonly (readonly number[])[];
  readonly g: readonly number[]; readonly m: readonly number[];
  /** PARTIAL COVERAGE: DFO places on this section's water not drawn yet (their location ids). While one has rules the
   *  map paints `check` and the card names no status of its own — we cannot tell which stretch they cover. */
  readonly u?: readonly string[];
}
/** One header note (`pipeline/regs/dfo_salmon/header.py`): kind, DFO's words, the values derived from them. */
export interface SalmonNote {
  readonly id: string; readonly k: string; readonly t: string;
  readonly sp?: readonly string[]; readonly cm?: number | readonly number[]; readonly n?: number;
  /** the waters it holds on (scope keys into `scopes[region]`); `x` the waters it excepts; `on` its day windows */
  readonly w?: readonly { readonly n: string; readonly ws: boolean; readonly key: string }[];
  readonly x?: readonly { readonly n: string; readonly ws: boolean; readonly key: string }[];
  readonly xp?: readonly string[];
  readonly on?: readonly (readonly number[])[];
}
interface Region { readonly name: string; readonly asof: string | null; readonly url: string; readonly notes?: readonly SalmonNote[] }
interface Unmapped { readonly items: readonly string[]; readonly r: string; readonly w: string; readonly chain: number; readonly d?: number }
interface Payload {
  readonly format: string; readonly credit: string; readonly default: number;
  readonly regions: Readonly<Record<string, Region>>;
  readonly verdicts: readonly Verdict[]; readonly gears: readonly (readonly string[])[];
  readonly chains: readonly Chain[]; readonly unmapped: readonly Unmapped[];
  /** region → scope key → the items a header note's water stands for */
  readonly scopes?: Readonly<Record<string, Readonly<Record<string, readonly string[]>>>>;
  /** the bundle regions the file has a slice for */
  readonly shards?: readonly string[];
}

export interface SalmonFacts {
  readonly handles: string;
  /** How many chains (chain 0 the unlisted water). */
  readonly chains: number;
  /** The section's chain: the unlisted chain (closed) where the file places no DFO location. */
  chainOf(sid: number): number;
  readonly payload: Payload;
  /** the slices read (bundle regions); a section of another region reads chain 0 here */
  readonly loaded?: readonly string[];
}

export class SalmonError extends Error {}

const digestOf = (b: Uint8Array) => { let d = ""; for (let i = 5; i < 13; i++) d += b[i]!.toString(16).padStart(2, "0"); return d; };

/** The head (`salmon`): its handles and payload. */
export function decodeSalmonHead(b: Uint8Array, handles: string): Payload {
  if (b.length < 17 || b[0] !== 0x42 || b[1] !== 0x43 || b[2] !== 0x53 || b[3] !== 0x41 || b[4] !== 2)
    throw new SalmonError("salmon: not a salmon/2 file");
  const digest = digestOf(b);
  if (digest !== handles) throw new SalmonError(`salmon: built for handles ${digest}, not ${handles}`);
  const n = b[13]! | (b[14]! << 8) | (b[15]! << 16) | (b[16]! * 0x1000000);
  if (17 + n !== b.length) throw new SalmonError(17 + n > b.length ? "salmon: truncated" : `salmon: ${b.length - 17 - n} trailing bytes`);
  return JSON.parse(new TextDecoder().decode(b.subarray(17, 17 + n))) as Payload;
}

/** One region's slice (`salmon-<r>`): its runs. */
export interface SalmonShard {
  readonly region: string;
  readonly starts: Float64Array; readonly ends: Float64Array; readonly chain: Int32Array;
}

export function decodeSalmonShard(b: Uint8Array, handles: string): SalmonShard {
  if (b.length < 15 || b[0] !== 0x42 || b[1] !== 0x43 || b[2] !== 0x53 || b[3] !== 0x52 || b[4] !== 1)
    throw new SalmonError("salmon: not a salmon-shard/1 file");
  const digest = digestOf(b);
  if (digest !== handles) throw new SalmonError(`salmon: slice built for handles ${digest}, not ${handles}`);
  const k = b[13]!;
  const region = new TextDecoder().decode(b.subarray(14, 14 + k));
  let i = 14 + k;
  const varint = (): number => {
    let v = 0, mul = 1;
    for (;;) {
      if (i >= b.length) throw new SalmonError("salmon: truncated");
      const x = b[i++]!;
      v += (x & 0x7f) * mul;
      if (!(x & 0x80)) return v;
      mul *= 128;
    }
  };
  const count = varint();
  const starts = new Float64Array(count), ends = new Float64Array(count), chain = new Int32Array(count);
  let end = 0;
  for (let r = 0; r < count; r++) {
    const s0 = end + varint(), c = varint();
    starts[r] = s0; ends[r] = s0 + c; chain[r] = varint();
    end = s0 + c;
  }
  if (i !== b.length) throw new SalmonError(`salmon: ${b.length - i} trailing bytes`);
  return { region, starts, ends, chain };
}

/** The head with the slices read: a section's chain is the first slice holding it, else chain 0 (closed). */
export function salmonFacts(payload: Payload, handles: string, shards: readonly SalmonShard[]): SalmonFacts {
  const dflt = payload.default ?? 0;
  return {
    handles, chains: payload.chains.length, payload, loaded: shards.map((s) => s.region),
    chainOf(sid) {
      for (const { starts, ends, chain } of shards) {
        let lo = 0, hi = starts.length - 1;
        while (lo <= hi) {
          const mid = (lo + hi) >> 1;
          if (sid < starts[mid]!) hi = mid - 1;
          else if (sid >= ends[mid]!) lo = mid + 1;
          else return chain[mid]!;
        }
      }
      return dflt;
    },
  };
}

/** The head and slices in one go (tests; a reader that has every file). */
export function decodeSalmon(head: Uint8Array, handles: string, shards: readonly Uint8Array[] = []): SalmonFacts {
  return salmonFacts(decodeSalmonHead(head, handles), handles, shards.map((b) => decodeSalmonShard(b, handles)));
}

// ── the day ──────────────────────────────────────────────────────────────────────────────────────────────────────────


/** The file's day index of a LOCAL calendar date: Jan 1 = 0 … Dec 31 = 365 (Feb 29 is 59 every year; DFO's dates have
 *  no year, so Mar 1 is 60 in every year). */
export function salmonDayIndex(d: Date): number {
  return DAYS_BEFORE_MONTH[d.getMonth()]! + d.getDate() - 1;
}

/** The value of runs [start0, v0, start1, v1, …] on day `day`. */
function at(runs: readonly number[], day: number): number {
  let v = runs[1] ?? -1;
  for (let k = 0; k < runs.length; k += 2) {
    if (runs[k]! > day) break;
    v = runs[k + 1]!;
  }
  return v;
}

/** (T227) A region header's closure of all salmon ("There is no fishing for salmon in Region 4 … until further notice"). */
function regionClosedNote(P: Payload, r: string): SalmonNote | undefined {
  return (P.regions[r]?.notes ?? []).find((n) => n.k === "closed" && !n.w?.length);
}

/** (T227, owner 2026-10-28) A chain that is only its regions' own rows (no water of its own, no undrawn place) in regions
 *  whose header closes salmon: the header's closure is its answer every day. */
function regionClosedChain(P: Payload, chain: number): boolean {
  const c = P.chains[chain];
  return !!c && !!c.l.length && !c.u?.length && c.l.every((l) => l.split(":")[1] === "region")
    && !!c.r.length && c.r.every((r) => !!regionClosedNote(P, r));
}

/** What the Salmon colouring paints a chain on a day. */
export function salmonChainStatus(f: SalmonFacts, chain: number, day: number): SalmonMapStatus | null {
  const c = f.payload.chains[chain];
  if (!c) return null;
  if (regionClosedChain(f.payload, chain)) return "closed";      // (T227) the card's region-wide closure, on the map too
  const s = MAP_CODES[at(c.m, day)];
  return s && s !== "none" ? s : null;
}

/** A section's profile label for the Salmon colouring: its chain, fixed for the release (`s<chain>`). */
export const salmonLabel = (f: SalmonFacts, sid: number): string => `s${f.chainOf(sid)}`;

/** The day's label sets for the Salmon colouring: which chain labels paint which status (`none`: no answer). */
export type SalmonDay = Readonly<Record<SalmonMapStatus | "none", readonly string[]>>;

export function salmonDayProfiles(f: SalmonFacts, on: Date): SalmonDay {
  const day = salmonDayIndex(on);
  const out: Record<SalmonMapStatus | "none", string[]> = { open: [], release: [], closed: [], check: [], none: [] };
  for (let c = 0; c < f.chains; c++) out[salmonChainStatus(f, c, day) ?? "none"].push(`s${c}`);
  return out;
}

// ── the region header ────────────────────────────────────────────────────────────────────────────────────────────

const inWindow = (day: number, on: readonly (readonly number[])[] | undefined) =>
  !on || !on.length || on.some(([a, b]) => (a! <= b! ? a! <= day && day <= b! : day >= a! || day <= b!));

/**
 * THE HEADER NOTES A PAGE SHOWS on day `day` (the pipeline's `deliver.salmon.notes_for`, pinned by the golden): `region`
 * — the notes of the page's DFO regions that name no water (one excepting this water is left out; an excepted PART of a
 * water is no exception, it shows verbatim); `water` — a note naming waters, only on their items, in its windows.
 */
export function salmonNotes(P: Payload, regions: readonly string[], item: string | null, day: number):
  { readonly region: readonly SalmonNote[]; readonly water: readonly SalmonNote[] } {
  const region: SalmonNote[] = [], water: SalmonNote[] = [];
  for (const slug of [...new Set(regions)].sort()) {
    const sc = P.scopes?.[slug] ?? {};
    const on = (ws: SalmonNote["w"]) => item !== null && !!ws?.some((w) => (sc[w.key] ?? []).includes(item));
    for (const n of P.regions[slug]?.notes ?? []) {
      if (n.w?.length) { if (on(n.w) && inWindow(day, n.on)) water.push(n); }
      else if (!on(n.x)) region.push(n);
    }
  }
  return { region, water };
}

/** A size line for an open row's species, from the header (a water's own "adult" length beats the region's). */
function sizesOf(species: readonly SalmonSpecies[], notes: readonly SalmonNote[]): string | null {
  const parts: string[] = [];
  for (const sp of species) {
    const of = (k: string) => notes.filter((n) => n.k === k && n.sp?.includes(sp));
    const min = of("min_size")[0]?.cm;
    const adults = of("adult");
    const adult = (adults.find((n) => n.w?.length) ?? adults[0])?.cm;
    const jack = of("jack")[0]?.cm;
    const name = SPECIES_NAME[sp];
    const bits = [typeof min === "number" ? `${name} ${min} cm or more to keep` : null,
                  typeof adult === "number" ? `adult ${name} over ${adult} cm` : null,
                  Array.isArray(jack) ? `jack ${name} ${jack[0]}–${jack[1]} cm` : null].filter((x): x is string => !!x);
    if (bits.length) parts.push(bits.join(", "));
  }
  return parts.length ? `${SALMON_WORDS.sizes}: ${parts.join("; ")}` : null;
}

// ── the words ────────────────────────────────────────────────────────────────────────────────────────────────────────

export const SALMON_WORDS = {
  title: "Salmon",
  badge: "DFO",
  status: { open: "Open", release: "Release only", closed: "Closed", check: "Check DFO", varies: "Varies" } as const,
  headline: { open: "Open today", release: "Release only today", closed: "Closed today", check: "Check DFO today",
              varies: "Varies along this water" } as const,
  all: "Salmon",
  allClosed: "Salmon — closed",
  gear: "Gear",
  varies: "Differs along this water. Tap a stretch on the map for its own answer.",
  unmapped: "Location not yet mapped",
  unmappedLead: "DFO also lists this water at a place not drawn on the map yet:",
  agency: "Fisheries and Oceans Canada",
  notListedLine: "DFO does not list this water for salmon. It is closed to salmon fishing.",
  noteTwoAuthorities: "Salmon are set by DFO, not the provincial regulations above. Both apply.",
  legendTitle: "Salmon, from DFO",
  toggle: {
    standard: "Standard", salmon: "Salmon",
    label: "Colour the regulations by",
    /** (T182) the web reads the salmon file on the first press of Salmon: the choice says so while it loads */
    loading: "Loading…",
  },
  credit: (credit: string) => `Source: ${credit}. Check DFO's page before you fish.`,
  asOf: (iso: string | null) => (iso ? `DFO page dated ${dateWords(iso)}` : "DFO page date not stated"),
  legend: { open: "Salmon may be kept", release: "Salmon: release only", closed: "Closed to salmon",
            check: "Check DFO (not mapped yet, or season not set)" } satisfies Record<SalmonMapStatus, string>,
  /** PARTIAL COVERAGE (owner 2026-10-28): the water has DFO places not drawn yet */
  partial: "Salmon rules on parts of this water aren't mapped yet — check DFO",
  /** (owner 2026-10-28) the region page's header, compact */
  regionRules: "Region rules (DFO)",
  regionRulesOf: (r: string, name: string) => `Region ${r.toUpperCase()} ${name}`.trim(),
  waterRules: "On this water (DFO)",
  /** a header size line on an open row, derived from DFO's header (min size, what an adult is) */
  sizes: "Sizes (DFO)",
  /** (owner 2026-10-28: "bait and fly rules also apply to salmon") the provincial gear mark on the page that day */
  gearLead: "B.C. rules, also for salmon",
  gearParts: "on parts of this water",
  partialLink: "Open DFO's salmon page",
  /** (T227, owner 2026-10-28) the season bars at the top of the Keep tab: the provincial bar's name beside the salmon one */
  barSalmon: "Salmon (DFO)",
  barGeneral: "Game fish (B.C. rules)",
  barLegend: { open: "✓ open", release: "release only", unknown: "see Salmon below", closed: "✕ closed" },
  /** (T227) the Gear tab's heads for DFO's gear conditions */
  gearDfoLead: "For salmon (DFO)",
  gearSalmonOnly: "Gear for salmon fishing: the B.C. gear rules below, and DFO's conditions",
  barDay: { open: "open", release: "release only", check: "check DFO", closed: "closed", varies: "differs along this part" } satisfies Record<SalmonBarStatus, string>,
} as const;

function dateWords(iso: string): string {
  const [y, m, d] = iso.split("-").map(Number);
  return y && m && d ? `${MONTHS[m - 1]} ${d}, ${y}` : iso;
}

const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

export interface SalmonRowVM {
  readonly key: string;
  /** The species this row answers for (SALMON_SPECIES order); the "Salmon — closed" row names all five. */
  readonly species: readonly SalmonSpecies[];
  /** "Coho", "Sockeye · chum", or "Salmon" when every species is closed. */
  readonly label: string;
  readonly status: SalmonVerdictStatus | "varies";
  readonly statusLabel: string;
  /** DFO's own words for the row (each verdict's text, notice included), or the "differs" line; then a header note
   *  naming this water and the row's species (e.g. the Squamish aggregate coho limit) on an open row. */
  readonly notes: readonly string[];
  /** (owner 2026-10-28) the header's sizes for the row's species on an open / release-only row, else null */
  readonly sizes?: string | null;
  /** (T227, owner 2026-10-28) the region header's notes about this row's species (what an "adult chinook" is, an annual
   *  limit …), DFO's words, beside the species — not repeated in the region block. A size the `sizes` line already says
   *  is left out. Only on a row that may be fished (open, release, check, varies). */
  readonly speciesNotes?: readonly string[];
}

export interface SalmonBlockVM {
  readonly rows: readonly SalmonRowVM[];
  /** Every species closed: the block is the one "Salmon — closed" row (owner 2026-10-27). */
  readonly allClosed: boolean;
  readonly gear: readonly string[];
}

export interface SalmonCardVM extends SalmonBlockVM {
  readonly title: string;
  readonly badge: string;
  readonly headline: string;
  readonly headlineStatus: SalmonVerdictStatus | "varies";
  /** The unlisted water's one line, else null. */
  readonly notListed: string | null;
  /** DFO places on this water not drawn yet: their words, the tag, and DFO's rules for them. */
  readonly unmapped: readonly { readonly label: string; readonly tag: string; readonly block: SalmonBlockVM }[];
  /** DFO places on this water are not drawn yet: no status of ours is confident — the line and DFO's page, else null. */
  readonly partial: { readonly text: string; readonly link: string; readonly url: string } | null;
  /** "Fisheries and Oceans Canada · Region 6 Skeena". */
  readonly source: string;
  /** "DFO page dated Aug 28, 2026" (the newest of the regions named). */
  readonly asOf: string;
  readonly credit: string;
  readonly note: string;
  /** (owner 2026-10-28) the page's DFO regions' header rules, compact: one group per region, DFO's words */
  readonly regionRules: readonly { readonly label: string; readonly notes: readonly string[] }[];
  /** header notes naming this water that no row took (DFO's words), e.g. an "adult chinook" length here */
  readonly waterRules: readonly string[];
  /** (owner 2026-10-28) the provincial bait ban / fly only on the page that day — it applies to salmon too — else null */
  readonly gearProvincial: string | null;
  /** (T227, owner 2026-10-28) the page's salmon reduce to ONE closed-all-year statement (a region-wide closure, an
   *  unlisted water, places closed every day): the at-a-glance row says "closed" and opens nothing */
  readonly closedOnly: boolean;
  /** (T227) DFO's year for the page, one bar per DFO place (see `SalmonYearVM`); empty when DFO has no salmon rules here */
  readonly year: SalmonYearVM;
}

/** (T227) A bar's day: the map's four, or `varies` — the part's sections differ that day. */
export type SalmonBarStatus = SalmonMapStatus | "varies";

/** (T227) One salmon season bar: the year's map status per book day (Jan 1 … Dec 31, no Feb 29), as runs. `label`: the
 *  DFO place it is for (null when it is the whole page's one answer); `unmapped`: a DFO place not drawn yet. */
export interface SalmonBarVM {
  readonly label: string | null;
  readonly unmapped: boolean;
  /** the selected day's status on this place, and its words ("release only on Oct 28" is the strip's) */
  readonly today: SalmonBarStatus;
  readonly todayWord: string;
  readonly runs: readonly { readonly status: SalmonBarStatus; readonly days: number }[];
}
/** (T227) A DFO place on the water not drawn yet: DFO's words for it and its own day. */
export interface SalmonUnplacedVM { readonly label: string; readonly today: SalmonBarStatus; readonly todayWord: string }
export interface SalmonYearVM {
  /** ONE bar for the viewed part / section (none where DFO has no salmon rules here) */
  readonly bars: readonly SalmonBarVM[];
  /** the DFO places on the water not drawn yet that have rules (the bar reads "check DFO" on a day one answers otherwise) */
  readonly unplaced: readonly SalmonUnplacedVM[];
  /** the page reads the partial-coverage rule: today's word is "check DFO", whatever a bar shows */
  readonly partial: boolean;
}

/** The provincial gear classes of a page's sections on the day (`methodClassOn`), for the card's gear line. */
export type SalmonGearInput = readonly MethodClass[];

/** The species rows for a set of chains on a day: species alike grouped, every-species-closed collapsed. */
export function salmonBlock(f: SalmonFacts, chains: readonly number[], day: number): SalmonBlockVM {
  const P = f.payload;
  const per = SALMON_SPECIES.map((_, k) => [...new Set(chains.map((c) => at(P.chains[c]!.sp[k]!, day)))]);
  const verdictOf = (ix: number): Verdict => P.verdicts[ix] ?? P.verdicts[0]!;
  const statusOf = (ixs: readonly number[]): SalmonVerdictStatus | "varies" => {
    const ss = [...new Set(ixs.map((ix) => verdictOf(ix).s))];
    return ss.length === 1 ? ss[0]! : "varies";
  };
  const gear = [...new Set(chains.flatMap((c) => {
    const g = at(P.chains[c]!.g, day);
    return g >= 0 ? P.gears[g] ?? [] : [];
  }))];
  const notesOf = (ixs: readonly number[]) => [...new Set(ixs.map((ix) => {
    const v = verdictOf(ix);
    return v.n && !v.t.includes(v.n) ? `${v.t} · ${v.n}` : v.t;
  }))];
  if (per.every((ixs) => statusOf(ixs) === "closed")) {
    const all = [...new Set(per.flat())];
    return { allClosed: true, gear,
             rows: [{ key: "all", species: SALMON_SPECIES, label: SALMON_WORDS.all, status: "closed", statusLabel: SALMON_WORDS.status.closed,
                      notes: notesOf(all) }] };
  }
  const groups = new Map<string, { sp: SalmonSpecies[]; ixs: number[] }>();
  SALMON_SPECIES.forEach((sp, k) => {
    const st = statusOf(per[k]!);
    const key = st === "varies" ? "varies" : per[k]!.join(",");
    const g = groups.get(key) ?? { sp: [], ixs: per[k]! };
    g.sp.push(sp);
    groups.set(key, g);
  });
  const rows = [...groups.entries()].map(([key, g]) => {
    const st = statusOf(g.ixs);
    return { key, species: g.sp, label: cap(g.sp.map((s) => SPECIES_NAME[s]).join(" · ")), status: st,
             statusLabel: SALMON_WORDS.status[st],
             notes: st === "varies" ? [SALMON_WORDS.varies] : notesOf(g.ixs) };
  });
  return { allClosed: false, gear, rows };
}

/**
 * THE SALMON CARD for a page: the chains of the page's sections (the tapped one, else its part's), the DFO places on
 * its water not drawn yet (by the water's item id), on day `on`. A page with no section in the file reads the unlisted
 * chain: "Salmon — closed".
 */
export function salmonCard(f: SalmonFacts, page: { sids: readonly number[]; item: string | null; gear?: SalmonGearInput },
                           on: Date): SalmonCardVM {
  const P = f.payload, day = salmonDayIndex(on);
  const chains = [...new Set(page.sids.map((s) => f.chainOf(s)))].sort((a, b) => a - b);
  if (!chains.length) chains.push(P.default ?? 0);
  const unmapped = page.item === null ? [] : P.unmapped.filter((u) => u.items.includes(page.item!));
  // PARTIAL COVERAGE: a DFO place with rules (not dormant) on this water is not drawn — the whole water says "check DFO"
  const active = new Set(P.unmapped.filter((u) => !u.d).flatMap((u) => P.chains[u.chain]?.l ?? []));
  const partialRegions = [...new Set([
    ...chains.flatMap((c) => (P.chains[c]!.u ?? []).some((l) => active.has(l)) ? P.chains[c]!.r : []),
    ...unmapped.filter((u) => !u.d).map((u) => u.r)])].sort();
  const def = P.default ?? 0;
  // (T227, owner 2026-10-28) A REGION-WIDE CLOSURE: the page's DFO answer is only its region's own rows (no water of its
  // own, no undrawn place) and the region's header says "There is no fishing for salmon in Region 4 … until further
  // notice" — the header's closure is the answer, every day: one closed row in its words, no bar, no partial line.
  const regionClosed = !unmapped.some((u) => !u.d) && chains.some((c) => c !== def)
    && chains.every((c) => c === def || regionClosedChain(P, c));
  const closedWords = regionClosed
    ? [...new Set(chains.flatMap((c) => (c === def ? [] : P.chains[c]!.r).map((r) => regionClosedNote(P, r)!)))] : [];
  const isPartial = !regionClosed && partialRegions.length > 0;
  const raw: SalmonBlockVM = regionClosed
    ? { allClosed: true, gear: [], rows: [{ key: "all", species: SALMON_SPECIES, label: SALMON_WORDS.all, status: "closed",
                                             statusLabel: SALMON_WORDS.status.closed, notes: closedWords.map((n) => n.t) }] }
    : salmonBlock(f, chains, day);
  // (T227, owner 2026-10-28) PARTIAL COVERAGE, PER SPECIES AND DAY: a row keeps its answer only where every DFO place on
  // the water not drawn yet says the same for each of its species that day (then it holds wherever the place lies);
  // else it reads "check DFO". The undrawn places: the water's own (`unmapped`) and those its chains name (`u`).
  const undrawn = isPartial ? undrawnChains(f, chains, unmapped) : [];
  const agrees = (r: SalmonRowVM) => r.status !== "varies" && undrawn.every((uc) =>
    r.species.every((sp) => speciesStatusOn(f, uc, SALMON_SPECIES.indexOf(sp), day) === r.status));
  const block: SalmonBlockVM = !isPartial ? raw : (() => {
    const rows = raw.rows.map((r) => (agrees(r) ? r : { ...r, status: "check" as const, statusLabel: SALMON_WORDS.status.check }));
    return { ...raw, rows, allClosed: raw.allClosed && rows.every((r) => r.status === "closed") };
  })();
  const listed = chains.some((c) => c !== (P.default ?? 0));
  const regionsUsed = [...new Set([...chains.flatMap((c) => P.chains[c]!.r), ...unmapped.map((u) => u.r)])].sort();
  const named = regionsUsed.length ? regionsUsed : Object.keys(P.regions).sort();
  const dates = named.map((r) => P.regions[r]?.asof).filter((x): x is string => !!x).sort();
  // THE REGION HEADER: the page's DFO regions' rules; a note naming this water goes on the row of its species when that
  // row may be fished, else in the water's own lines; an open row says the sizes the header sets for its species
  const hn = salmonNotes(P, regionsUsed, page.item, day);
  const placed = new Set<string>();
  // (T227, owner 2026-10-28) a region note about a species (what an "adult chinook" is, an annual chinook limit) sits
  // with that species' row, the way steelhead's sits with steelhead; the region block keeps only what no row took
  const speciesWise = hn.region.filter((n) => n.sp?.length);
  const placedRegion = new Set<string>(closedWords.map((n) => n.id));
  const rows = block.rows.map((r) => {
    if (block.allClosed || r.status === "closed") return r;
    const ofRow = (n: SalmonNote) => !!n.sp?.some((x) => r.species.includes(x as SalmonSpecies));
    const regionMine = speciesWise.filter(ofRow);
    regionMine.forEach((n) => placedRegion.add(n.id));
    if (r.status !== "open" && r.status !== "release")
      return regionMine.length ? { ...r, speciesNotes: regionMine.map((n) => n.t) } : r;
    const mine = hn.water.filter((n) => n.k !== "adult" && ofRow(n));
    mine.forEach((n) => placed.add(n.id));
    const sizes = sizesOf(r.species, [...hn.water, ...hn.region]);
    // the sizes line already says the minimum, the adult and the jack lengths: those notes are not repeated
    const said = sizes ? SIZE_KINDS : new Set<string>();
    const speciesNotes = regionMine.filter((n) => !said.has(n.k)).map((n) => n.t);
    return { ...r, notes: [...r.notes, ...mine.map((n) => n.t)], sizes, ...(speciesNotes.length ? { speciesNotes } : {}) };
  });
  const regionRules = regionsUsed.map((r) => ({
    label: SALMON_WORDS.regionRulesOf(r, P.regions[r]?.name ?? ""),
    notes: hn.region.filter((n) => n.id.startsWith(`${r}:`) && !placedRegion.has(n.id)).map((n) => n.t),
  })).filter((g) => g.notes.length);
  const gear = page.gear ?? [];
  const marks = (["fly", "bait"] as const).filter((m) => gear.includes(m));
  const gearProvincial = block.allClosed || !marks.length ? null
    : `${SALMON_WORDS.gearLead}${gear.some((g) => g === "none") ? ` (${SALMON_WORDS.gearParts})` : ""}: `
      + marks.map((m) => METHOD_WORDS.says[m]).join(" ");
  const sts = rows.map((r) => r.status);
  const headlineStatus: SalmonVerdictStatus | "varies" =
    sts.includes("open") ? "open" : sts.includes("release") ? "release" : sts.includes("varies") ? "varies"
      : sts.includes("check") ? "check" : "closed";
  return {
    ...block, rows, regionRules, gearProvincial,
    waterRules: hn.water.filter((n) => !placed.has(n.id)).map((n) => n.t),
    title: SALMON_WORDS.title, badge: SALMON_WORDS.badge,
    headline: SALMON_WORDS.headline[headlineStatus], headlineStatus,
    notListed: listed || unmapped.length ? null : SALMON_WORDS.notListedLine,
    unmapped: unmapped.map((u) => ({ label: u.w, tag: SALMON_WORDS.unmapped, block: salmonBlock(f, [u.chain], day) })),
    partial: isPartial ? { text: SALMON_WORDS.partial, link: SALMON_WORDS.partialLink,
                           url: P.regions[partialRegions[0]!]?.url ?? "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/index-eng.html" } : null,
    source: [SALMON_WORDS.agency,
             ...regionsUsed.map((r) => `Region ${r.toUpperCase()} ${P.regions[r]?.name ?? ""}`.trim())].join(" · "),
    asOf: SALMON_WORDS.asOf(dates[dates.length - 1] ?? null),
    credit: SALMON_WORDS.credit(P.credit),
    note: SALMON_WORDS.noteTwoAuthorities,
    year: regionClosed ? { bars: [], unplaced: [], partial: false } : salmonYear(f, chains, undrawn, unmapped, isPartial, day),
    // one closed-all-year statement: the region's closure, or every place of the page closed every day, nothing undrawn
    closedOnly: regionClosed || (!unmapped.some((u) => !u.d && speaks(f, u.chain))
      && chains.every((c) => runsOf((i) => salmonStatusFromSpecies(f, c, i)).every((r) => r.status === "closed"))),
  };
}

/** The header notes a `sizes` line words (`sizesOf`). */
const SIZE_KINDS: ReadonlySet<string> = new Set(["min_size", "adult", "jack"]);

/** The book's day i (0 = Jan 1 … 364 = Dec 31, no Feb 29) as the file's day (Feb 29 is 59). */
const fileDay = (i: number) => (i < 59 ? i : i + 1);
const BOOK_YEAR_DAYS = 365;

/**
 * What a chain's day paints, from its five species' verdicts — the pipeline's `deliver.salmon.map_status` (open if any
 * may be kept, else release if any may be fished, else check, else closed). Read from the species, not the chain's map
 * runs: those say "check" every day on a water with a DFO place not drawn (partial coverage), and a bar names its place.
 * Pinned to the pipeline's map runs on every chain without partial coverage (`salmon.test.ts`).
 */
export function salmonStatusFromSpecies(f: SalmonFacts, chain: number, day: number): SalmonMapStatus {
  const P = f.payload, c = P.chains[chain];
  if (!c) return "closed";
  const ss = c.sp.map((runs) => (P.verdicts[at(runs, day)] ?? P.verdicts[0]!).s);
  for (const st of ["open", "release", "check"] as const) if (ss.includes(st)) return st;
  return "closed";
}

/** A chain DFO actually speaks for: some species, some day, has a verdict other than "not opened by DFO". */
function speaks(f: SalmonFacts, chain: number): boolean {
  const P = f.payload, c = P.chains[chain];
  return !!c && c.sp.some((runs) => runs.some((v, k) => k % 2 === 1 && !P.verdicts[v]?.d));
}

/** A bar's runs over the book's year from a status per FILE day. */
function runsOf(statusOn: (fileDayIx: number) => SalmonBarStatus): SalmonBarVM["runs"] {
  const out: { status: SalmonBarStatus; days: number }[] = [];
  for (let i = 0; i < BOOK_YEAR_DAYS; i++) {
    const st = statusOn(fileDay(i));
    const last = out[out.length - 1];
    if (last && last.status === st) last.days++; else out.push({ status: st, days: 1 });
  }
  return out;
}

/**
 * (T227) THE PAGE'S SALMON YEAR: ONE bar for the viewed part / section. A day no DFO rule speaks for is CLOSED (the
 * pipeline's `NOT_LISTED`: "closed unless DFO opens it"; the region headers set limits and sizes, never an opening). Per
 * day: the status the page's sections share, `varies` where they differ. PARTIAL COVERAGE (owner 2026-10-28): a DFO place
 * on the water not drawn yet may lie on this part, so a day it answers otherwise is "check DFO" — never ours; the places
 * are listed under the bar with their own day. Nothing DFO speaks for here (the unlisted water, "not opened"): no bar.
 */
function salmonYear(f: SalmonFacts, chains: readonly number[], undrawn: readonly number[], unmapped: readonly Unmapped[],
                    partial: boolean, day: number): SalmonYearVM {
  const P = f.payload;
  const speakingUndrawn = undrawn.filter((c) => speaks(f, c));
  if (!chains.some((c) => speaks(f, c)) && !speakingUndrawn.length) return { bars: [], unplaced: [], partial };
  // the page's own places: one status a day, `varies` where its sections differ
  const own = (i: number): SalmonBarStatus => {
    const ss = new Set(chains.map((c) => salmonStatusFromSpecies(f, c, i)));
    return ss.size === 1 ? [...ss][0]! : "varies";
  };
  // an undrawn place may lie on this part: a day it answers otherwise is "check DFO", never our answer
  const at1 = (i: number): SalmonBarStatus => {
    const st = own(i);
    return speakingUndrawn.every((c) => salmonStatusFromSpecies(f, c, i) === st) ? st : "check";
  };
  const st = at1(day);
  const seen = new Set<string>();
  const unplaced: SalmonUnplacedVM[] = [];
  for (const c of speakingUndrawn) {
    const label = unmapped.find((u) => u.chain === c)?.w ?? P.chains[c]?.w ?? "";
    if (seen.has(label)) continue;
    seen.add(label);
    const t = salmonStatusFromSpecies(f, c, day);
    unplaced.push({ label, today: t, todayWord: SALMON_WORDS.barDay[t] });
  }
  return { bars: [{ label: null, unmapped: false, today: st, todayWord: SALMON_WORDS.barDay[st], runs: runsOf(at1) }], unplaced, partial };
}

/** The chains of the DFO places on the page's water not drawn yet (not dormant): the item's own, and those the page's
 *  chains name as undrawn (`u`). */
function undrawnChains(f: SalmonFacts, chains: readonly number[], unmapped: readonly Unmapped[]): number[] {
  const P = f.payload;
  const named = new Set(chains.flatMap((c) => P.chains[c]?.u ?? []));
  const out = new Set<number>();
  for (const u of unmapped) if (!u.d) out.add(u.chain);
  for (const u of P.unmapped) if (!u.d && (P.chains[u.chain]?.l ?? []).some((l) => named.has(l))) out.add(u.chain);
  return [...out].sort((a, b) => a - b);
}

/** One species' verdict status on a chain's day. */
function speciesStatusOn(f: SalmonFacts, chain: number, k: number, day: number): SalmonVerdictStatus {
  const P = f.payload, runs = P.chains[chain]?.sp[k];
  return runs ? (P.verdicts[at(runs, day)] ?? P.verdicts[0]!).s : "closed";
}

/** (T228) A rule line as the key compares it: case and spacing never make two regulations differ. */
const normRule = (t: string) => t.toLowerCase().replace(/\s+/g, " ").trim();

/**
 * (T227 / T228, the map's section merging) A section's SALMON KEY: equal for two sections exactly when DFO's REGULATIONS
 * read the same on both, every day of the year — per species the verdict AND its rule content (the limit, sizes,
 * hatchery-marked or wild, the gear and method conditions and the windows: the rule lines `t` and notices `n` DFO
 * prints, normalised for case and spacing), the gear lines, the map status (partial coverage included). Resolved to
 * CONTENT, never to the file's indexes, so "coho 2 a day" and "coho 4 a day, hatchery-marked only" differ though both
 * are open, and a live feed laid over the release (`withLiveSalmon`) compares the same way. Compare it beside the
 * provincial key; never show it.
 */
export function salmonTimelineKey(f: SalmonFacts, sid: number): string {
  const P = f.payload, c = P.chains[f.chainOf(sid)];
  if (!c) return "";
  const runs = <T>(r: readonly number[], of: (i: number) => T) => r.map((x, k) => (k % 2 ? of(x) : x));
  const verdict = (i: number) => {
    const v = P.verdicts[i];
    return v ? [v.s, normRule(v.t), v.n ? normRule(v.n) : null] : null;
  };
  const gear = (i: number) => (i < 0 ? null : (P.gears[i] ?? []).map(normRule));
  return JSON.stringify([c.sp.map((r) => runs(r, verdict)), runs(c.g, gear), c.m]);
}

/** (T227) How a salmon bar day is drawn: the game-fish bar's own fills (open, release stripes, closed hatch), and
 *  `unknown` — a plain neutral fill — for a day we cannot answer here (DFO's "to be determined", a DFO place not drawn
 *  yet that answers otherwise, sections that differ); the Salmon row's details say why. */
export type SalmonDrawn = "open" | "release" | "closed" | "unknown";
const DRAWN: Readonly<Record<SalmonBarStatus, SalmonDrawn>> = { open: "open", release: "release", closed: "closed", check: "unknown", varies: "unknown" };

/** (T227) The salmon year bar, drawn exactly as the game-fish bar is: its name, the day's word and next change, ONE bar
 *  for the viewed part, the months and the legend. Every breakdown (species, DFO places, places not drawn yet) is in
 *  the Salmon row's details. */
export interface SalmonStripVM {
  readonly title: string;
  /** the selected day: its status, mark ("✓ " / "✕" / none) and word ("Release only", "Check DFO" …) */
  readonly status: SalmonBarStatus;
  readonly mark: string;
  readonly word: string;
  /** the provincial bar's "today" / "on Oct 28", said after the word the same way */
  readonly when: string | null;
  /** the next change, as the game-fish bar says it ("Opens Nov 16" / "Closed from Nov 1"), else null */
  readonly soon: { readonly kind: "soonOpen" | "soon"; readonly text: string } | null;
  readonly runs: readonly { readonly drawn: SalmonDrawn; readonly days: number }[];
  /** the provincial bar's name, drawn above it when the salmon bar is shown */
  readonly generalTitle: string;
  readonly legend: readonly { readonly drawn: SalmonDrawn; readonly label: string }[];
}

const SOON_DAYS = 21;
const bookDayWords = (i: number) => fmtDay(BOOK_DAYS[i]!);   // book day i (no Feb 29) → "Nov 16"

/**
 * (T227, owner 2026-10-28: "exactly like the game-fish bar") The salmon bar under the provincial year bar, or null: none
 * where DFO has no salmon rules for the page (never an all-closed bar for a water without salmon), and none where the
 * salmon bar is closed all year and so is the provincial bar (the two say the same thing). `general`: the provincial
 * year bar (null where the page draws none); `todayAt` its marker (book day / 365).
 */
export function salmonStripOf(card: SalmonCardVM, general: {
  readonly runs: readonly { readonly bits: number | null }[]; readonly line: readonly { readonly kind: string; readonly text: string }[];
  readonly todayAt?: number } | null,
): SalmonStripVM | null {
  const bar = card.year.bars[0];
  if (!bar) return null;
  const generalAllClosed = !!general && general.runs.every((r) => r.bits !== null && (r.bits & 1) === 1);
  if (generalAllClosed && bar.runs.every((r) => r.status === "closed")) return null;
  const runs: { drawn: SalmonDrawn; days: number }[] = [];
  for (const r of bar.runs) {
    const dr = DRAWN[r.status], last = runs[runs.length - 1];
    if (last && last.drawn === dr) last.days += r.days; else runs.push({ drawn: dr, days: r.days });
  }
  const days = runs.flatMap((r) => Array.from({ length: r.days }, () => r.drawn));
  const i = Math.round((general?.todayAt ?? 0) * BOOK_YEAR_DAYS);
  let soon: SalmonStripVM["soon"] = null;
  if (bar.today === "closed") {
    for (let k = 1; k < BOOK_YEAR_DAYS; k++) {
      const dr = days[(i + k) % BOOK_YEAR_DAYS];
      if (dr === "open" || dr === "release") { soon = { kind: "soonOpen", text: WORDS.OPENS(bookDayWords((i + k) % BOOK_YEAR_DAYS)) }; break; }
    }
  } else if (bar.today === "open" || bar.today === "release") {
    for (let k = 1; k <= SOON_DAYS; k++)
      if (days[(i + k) % BOOK_YEAR_DAYS] === "closed") { soon = { kind: "soon", text: WORDS.CLOSED_FROM(bookDayWords((i + k) % BOOK_YEAR_DAYS)) }; break; }
  }
  const present = new Set(runs.map((r) => r.drawn));
  return {
    title: SALMON_WORDS.barSalmon, status: bar.today,
    mark: bar.today === "open" || bar.today === "release" ? "✓ " : bar.today === "closed" ? "✕" : "",
    word: SALMON_WORDS.status[bar.today],
    when: general?.line.find((l) => l.kind === "when")?.text ?? null,
    soon, runs,
    generalTitle: SALMON_WORDS.barGeneral,
    legend: (["open", "release", "unknown", "closed"] as const).filter((d) => present.has(d))
      .map((d) => ({ drawn: d, label: SALMON_WORDS.barLegend[d] })),
  };
}

/** The Salmon legend's credit line: whose rules, as of when, and the unlisted rule — one whole sentence each. */
export function salmonLegendSource(f: SalmonFacts | null): string {
  const dates = f ? Object.values(f.payload.regions).map((r) => r.asof).filter((x): x is string => !!x).sort() : [];
  const newest = dates[dates.length - 1];
  return `Salmon rules from ${SALMON_WORDS.agency} (DFO)${newest ? `, pages dated up to ${dateWords(newest)}` : ""}. `
    + "Waters DFO does not list are closed to salmon.";
}

/** The Salmon legend's rows, in the style's order. */
export function salmonLegend(): readonly { readonly status: SalmonMapStatus; readonly label: string }[] {
  return SALMON_MAP_STATUSES.map((s) => ({ status: s, label: SALMON_WORDS.legend[s] }));
}

/** (T227) What the Gear and Methods tabs draw: each tab's VM (null: the tab is not shown) and DFO's gear conditions. */
export interface GearTabsVM {
  readonly gear: GearVM | null;
  readonly methods: MethodsVM | null;
  readonly salmonGear: { readonly lead: string; readonly lines: readonly string[] } | null;
}

const EMPTY_GEAR: GearVM = { closed: null, closedMore: [], day: null, mode: null, lines: [], bait: null };

/**
 * (T227, owner 2026-10-28) THE GEAR AND METHODS TABS STATE NO CLOSURE. They show gear and method rules only, and only
 * when the viewed part may be fished that day in some way:
 *  - the game-fish (B.C.) rules are open: the page's tabs, with DFO's gear conditions where salmon may be fished too;
 *  - only salmon (DFO: open, release only, or "check DFO") may be: the day's book gear rules as if open (`gearIfFished`
 *    — they apply to salmon fishing) and DFO's gear conditions;
 *  - neither: no Gear and no Methods tab.
 * Tidal and outside-B.C. pages keep their own tabs (they say whose rules hold, not a closure).
 */
export function gearTabsFor(page: { readonly status: string; readonly gear: GearVM | null; readonly methods: MethodsVM | null },
                            anyway: { readonly gear: GearVM | null; readonly methods: MethodsVM | null } | null,
                            strip: SalmonStripVM | null, card: SalmonCardVM | null): GearTabsVM {
  if (page.status === "tidal" || page.status === "outside") return { gear: page.gear, methods: page.methods, salmonGear: null };
  const salmonOpen = !!strip && strip.status !== "closed";
  const dfo = card ? [...new Set([...card.gear, ...card.unmapped.flatMap((u) => u.block.gear)])] : [];
  const salmonGear = salmonOpen && dfo.length ? { lead: SALMON_WORDS.gearDfoLead, lines: dfo } : null;
  const gameOpen = !!page.gear && page.gear.closed === null && !page.gear.closedMore.length;
  if (gameOpen) return { gear: page.gear, methods: page.methods, salmonGear };
  if (salmonOpen) return { gear: anyway?.gear ?? EMPTY_GEAR, methods: anyway?.methods ?? null,
                           salmonGear: { lead: SALMON_WORDS.gearSalmonOnly, lines: dfo } };
  return { gear: null, methods: null, salmonGear: null };
}
