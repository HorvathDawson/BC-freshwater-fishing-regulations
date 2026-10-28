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

/** What the Salmon colouring paints a chain on a day. */
export function salmonChainStatus(f: SalmonFacts, chain: number, day: number): SalmonMapStatus | null {
  const c = f.payload.chains[chain];
  if (!c) return null;
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
  const isPartial = partialRegions.length > 0;
  const raw = salmonBlock(f, chains, day);
  const block: SalmonBlockVM = !isPartial ? raw : {
    ...raw, allClosed: false,
    rows: raw.rows.map((r) => ({ ...r, status: "check" as const, statusLabel: SALMON_WORDS.status.check })),
  };
  const listed = chains.some((c) => c !== (P.default ?? 0));
  const regionsUsed = [...new Set([...chains.flatMap((c) => P.chains[c]!.r), ...unmapped.map((u) => u.r)])].sort();
  const named = regionsUsed.length ? regionsUsed : Object.keys(P.regions).sort();
  const dates = named.map((r) => P.regions[r]?.asof).filter((x): x is string => !!x).sort();
  // THE REGION HEADER: the page's DFO regions' rules; a note naming this water goes on the row of its species when that
  // row may be fished, else in the water's own lines; an open row says the sizes the header sets for its species
  const hn = salmonNotes(P, regionsUsed, page.item, day);
  const placed = new Set<string>();
  const rows = block.rows.map((r) => {
    if (block.allClosed || (r.status !== "open" && r.status !== "release")) return r;
    const mine = hn.water.filter((n) => n.k !== "adult" && n.sp?.some((x) => r.species.includes(x as SalmonSpecies)));
    mine.forEach((n) => placed.add(n.id));
    return { ...r, notes: [...r.notes, ...mine.map((n) => n.t)], sizes: sizesOf(r.species, [...hn.water, ...hn.region]) };
  });
  const regionRules = regionsUsed.map((r) => ({
    label: SALMON_WORDS.regionRulesOf(r, P.regions[r]?.name ?? ""),
    notes: hn.region.filter((n) => n.id.startsWith(`${r}:`)).map((n) => n.t),
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
