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
 * Layout (little-endian, varints LEB128): b"BCSA", u8 1, 8 bytes of the section_handles digest, u32 JSON length, the
 * UTF-8 JSON payload, then a run count and per run: sid gap (from the previous run's end), count, chain.
 */
import { DAYS_BEFORE_MONTH, MONTHS } from "./calendar.generated";

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
}
interface Region { readonly name: string; readonly asof: string | null; readonly url: string }
interface Unmapped { readonly items: readonly string[]; readonly r: string; readonly w: string; readonly chain: number }
interface Payload {
  readonly format: string; readonly credit: string; readonly default: number;
  readonly regions: Readonly<Record<string, Region>>;
  readonly verdicts: readonly Verdict[]; readonly gears: readonly (readonly string[])[];
  readonly chains: readonly Chain[]; readonly unmapped: readonly Unmapped[];
}

export interface SalmonFacts {
  readonly handles: string;
  /** How many chains (chain 0 the unlisted water). */
  readonly chains: number;
  /** The section's chain: the unlisted chain (closed) where the file places no DFO location. */
  chainOf(sid: number): number;
  readonly payload: Payload;
}

export class SalmonError extends Error {}

export function decodeSalmon(b: Uint8Array, handles: string): SalmonFacts {
  if (b.length < 17 || b[0] !== 0x42 || b[1] !== 0x43 || b[2] !== 0x53 || b[3] !== 0x41 || b[4] !== 1)
    throw new SalmonError("salmon: not a salmon/1 file");
  let digest = "";
  for (let i = 5; i < 13; i++) digest += b[i]!.toString(16).padStart(2, "0");
  if (digest !== handles) throw new SalmonError(`salmon: built for handles ${digest}, not ${handles}`);
  const n = b[13]! | (b[14]! << 8) | (b[15]! << 16) | (b[16]! * 0x1000000);
  if (17 + n > b.length) throw new SalmonError("salmon: truncated");
  const payload = JSON.parse(new TextDecoder().decode(b.subarray(17, 17 + n))) as Payload;
  let i = 17 + n;
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
  for (let k = 0; k < count; k++) {
    const s = end + varint(), c = varint();
    starts[k] = s; ends[k] = s + c; chain[k] = varint();
    end = s + c;
  }
  if (i !== b.length) throw new SalmonError(`salmon: ${b.length - i} trailing bytes`);
  const dflt = payload.default ?? 0;
  return {
    handles: digest, chains: payload.chains.length, payload,
    chainOf(sid) {
      let lo = 0, hi = count - 1;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (sid < starts[mid]!) hi = mid - 1;
        else if (sid >= ends[mid]!) lo = mid + 1;
        else return chain[mid]!;
      }
      return dflt;
    },
  };
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
  },
  credit: (credit: string) => `Source: ${credit}. Check DFO's page before you fish.`,
  asOf: (iso: string | null) => (iso ? `DFO page dated ${dateWords(iso)}` : "DFO page date not stated"),
  legend: { open: "Salmon may be kept", release: "Salmon: release only", closed: "Closed to salmon",
            check: "Check DFO (season not set)" } satisfies Record<SalmonMapStatus, string>,
} as const;

function dateWords(iso: string): string {
  const [y, m, d] = iso.split("-").map(Number);
  return y && m && d ? `${MONTHS[m - 1]} ${d}, ${y}` : iso;
}

const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

export interface SalmonRowVM {
  readonly key: string;
  /** "Coho", "Sockeye · chum", or "Salmon" when every species is closed. */
  readonly label: string;
  readonly status: SalmonVerdictStatus | "varies";
  readonly statusLabel: string;
  /** DFO's own words for the row (each verdict's text, notice included), or the "differs" line. */
  readonly notes: readonly string[];
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
  /** "Fisheries and Oceans Canada · Region 6 Skeena". */
  readonly source: string;
  /** "DFO page dated Aug 28, 2026" (the newest of the regions named). */
  readonly asOf: string;
  readonly credit: string;
  readonly note: string;
}

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
             rows: [{ key: "all", label: SALMON_WORDS.all, status: "closed", statusLabel: SALMON_WORDS.status.closed,
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
    return { key, label: cap(g.sp.map((s) => SPECIES_NAME[s]).join(" · ")), status: st,
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
export function salmonCard(f: SalmonFacts, page: { sids: readonly number[]; item: string | null }, on: Date): SalmonCardVM {
  const P = f.payload, day = salmonDayIndex(on);
  const chains = [...new Set(page.sids.map((s) => f.chainOf(s)))].sort((a, b) => a - b);
  if (!chains.length) chains.push(P.default ?? 0);
  const block = salmonBlock(f, chains, day);
  const unmapped = page.item === null ? [] : P.unmapped.filter((u) => u.items.includes(page.item!));
  const listed = chains.some((c) => c !== (P.default ?? 0));
  const regionsUsed = [...new Set([...chains.flatMap((c) => P.chains[c]!.r), ...unmapped.map((u) => u.r)])].sort();
  const named = regionsUsed.length ? regionsUsed : Object.keys(P.regions).sort();
  const dates = named.map((r) => P.regions[r]?.asof).filter((x): x is string => !!x).sort();
  const sts = block.rows.map((r) => r.status);
  const headlineStatus: SalmonVerdictStatus | "varies" =
    sts.includes("open") ? "open" : sts.includes("release") ? "release" : sts.includes("varies") ? "varies"
      : sts.includes("check") ? "check" : "closed";
  return {
    ...block,
    title: SALMON_WORDS.title, badge: SALMON_WORDS.badge,
    headline: SALMON_WORDS.headline[headlineStatus], headlineStatus,
    notListed: listed || unmapped.length ? null : SALMON_WORDS.notListedLine,
    unmapped: unmapped.map((u) => ({ label: u.w, tag: SALMON_WORDS.unmapped, block: salmonBlock(f, [u.chain], day) })),
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
