/**
 * THE DFO SALMON (T211, option A + the Standard / Salmon map toggle): the release file `salmon` (`salmon/4`,
 * `pipeline/deliver/salmon.py`) read, and every word the Salmon card and the Salmon map legend say.
 *
 * NOTHING HERE READS A RULE. The pipeline precomputed, per CHAIN of DFO locations (narrowest first — Region 6 is a
 * cascade) and per day of the year, each species' verdict, the gear lines and the map status. This module only looks
 * them up for a day, groups the species that answer alike, and words it.
 *
 * CLOSED UNLESS DFO OPENS IT (owner 2026-10-27): chain 0 is the UNLISTED water — every species closed, every day — and
 * a section the file places in no run reads chain 0. Explicit in the file (`payload.default`), never an absence.
 *
 * Layout (`salmon/4`; T332: quotas as game-fish quota trees; T356: a header closure, the scoped aggregate limits and the
 * header sizes are in the file — an older file is refused), little-endian, varints LEB128,
 * SPLIT PER BUNDLE REGION (owner 2026-10-28: a water page's card
 * fetches its own region's slice, the Salmon map all of them):
 *   salmon       b"BCSA", u8 4, 8 bytes of the section_handles digest, u32 JSON length, the UTF-8 JSON payload
 *   salmon-<r>   b"BCSR", u8 1, 8 bytes of the digest, u8 region length + region, then a run count and per run: sid gap
 *                (from the previous run's end), count, chain — the sections of bundle region <r> (`x`: in none)
 * A section in no LOADED slice reads chain 0, so a reader asks for the slices of the sections it reads (the card: its
 * page's regions; the map: every one).
 *
 * THE REGION HEADER (owner 2026-10-28): each DFO region page's header — daylight only, the facility closure, the
 * aggregate limits, sizes, what an "adult chinook" is — as typed notes on `regions[slug].notes`, DFO's words whole; one
 * naming waters holds only on their items (`scopes`). Which notes a page shows is `salmonNotes`, held to the pipeline's
 * `deliver.salmon.notes_for` by the golden.
 *
 * (T356) RESOLVED IN PYTHON, LOOKED UP HERE: a region header's closure is the verdict's `c` and the chain's `c` (never
 * read from a location id); the scoped aggregate limits are `regions[r].shared`; the header sizes `regions[r].sizes`
 * (ordered, the first that holds wins). The file is validated on decode (`checkPayload`): a bad index is an error, never
 * a quiet "closed".
 */
import { DAYS_BEFORE_MONTH, MONTHS } from "./calendar.generated";
import { METHOD_WORDS, type MethodClass } from "./method";
import type { CaveatItemVM, CaveatVM, GearVM, MethodsVM } from "./regs/model";
import { inPartBlock } from "./regs/caveats";
import { BOOK_DAYS, fmtDay, legendText } from "./regs/strip";
import { WORDS } from "./regs/words";
import { kindWords, tierWords } from "./regs/tree";
import { text, type Line } from "./regs/line";
import type { TreeKind, TreeTier } from "./answers.generated";
import { runNow, runSliced, YIELD_EVERY, type Steps } from "./sliced";
import { salmonIssueBanners, type SalmonIssues, type SalmonIssueVM } from "./salmonIssues";

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

/** (T320, T332) A species' numbers on an open day (`pipeline/regs/dfo_salmon/quota.py`): `tree` A NODE OF THE GAME-FISH
 *  QUOTA TREE — the trout card's own `TreeTier` (the count tier "4 a day", its kinds — "hatchery-marked coho" folded into
 *  the tier's line, "unmarked (wild) coho" with its own number — and its size tiers "only 2 over 50 cm" / "none over 35 cm" /
 *  a monthly one, `period`), drawn by the trout presenter (`quotaLine`); `u` not one reading — the app shows DFO's words
 *  (`t`) and no number. */
export interface SalmonQuota { readonly tree?: TreeTier; readonly u?: 1 }
/** (T320, T332) THE SALMON BUCKET: the region header's combined limit — `tree` the quota tree's TOTAL tier ("Up to 4
 *  salmon a day in total"; the header's annual / monthly limits its tiers with a `period`), DFO's words in `t` / `tp`;
 *  `possession` the total DFO prints (owner 2026-10-31: no per-species possession). */
export interface SalmonBucket {
  readonly tree: TreeTier; readonly t: string;
  readonly possession?: number; readonly tp?: string;
  readonly ids?: readonly string[];
}
/** (T349, T356) a header's aggregate limit over named waters (Squamish hatchery coho): the quota tree's aggregate tier,
 *  `total` with `whose` its scope; `id` the note it was read from, `w` the note's water keys. The region's own, with or
 *  without a bucket. */
export interface SalmonShared { readonly tree: TreeTier; readonly t: string; readonly id: string | null; readonly w: readonly string[] }
/** (T356, T358) one length as the catalogue's `LengthBand` */
interface SalmonBand { readonly min_cm?: number; readonly max_cm?: number }
/** (T356) the header sizes of species alike (`pipeline/regs/dfo_salmon/quota.sizes`): the minimum to keep, what an adult
 *  and a jack are. Ordered; the first entry of a species that holds (`w` its waters' items, `on` its windows) wins. */
export interface SalmonSizes {
  readonly sp: readonly string[];
  readonly min?: SalmonBand; readonly adult?: SalmonBand; readonly jack?: SalmonBand;
  readonly w?: readonly string[]; readonly on?: readonly (readonly number[])[];
}
/** `c` (T356): the region header note whose closure this verdict is. `f` (T796 A6): a closure answered by DFO's "Finfish
 *  closure" — ALL fishing is closed that day, trout included (`finfishToday`, the page closes) */
interface Verdict { readonly s: SalmonVerdictStatus; readonly t: string; readonly n?: string; readonly d?: 1; readonly tree?: TreeTier; readonly u?: 1; readonly c?: string; readonly f?: 1 }
interface Chain {
  readonly r: readonly string[]; readonly w: string; readonly l: readonly string[];
  /** (T356) the region header notes that close this chain every day (and its undrawn places): its card is the closure */
  readonly c?: readonly string[];
  /** per species (SALMON_SPECIES order) runs [start, verdict index, …] */
  readonly sp: readonly (readonly number[])[];
  readonly g: readonly number[]; readonly m: readonly number[];
  /** (T770, owner 2026-11-16 19:09: "As a closure line (Recommended)") ANOTHER FISH'S CLOSURE: runs [start, index into
   *  `gears`, …] (-1 none) of the lines closing another fish that day, in Python's words ("Eulachon closed all year: No
   *  fishing for eulachon", `deliver.salmon.closures_on`) — the card's closure line, never a gear line. Absent on a chain
   *  with none. Built like `g`: under a live feed composed like it (`composeChain`) */
  readonly o?: readonly number[];
  /** PARTIAL COVERAGE: DFO places on this section's water not drawn yet (their location ids). (T372, the trout way) the
   *  map and the card keep the drawn answer; the card lists these places in its "in part" caveat. */
  readonly u?: readonly string[];
  /** (T372) on an undrawn place's own chain: runs [start, 1|0] — the days DFO speaks for it (the pipeline's
   *  `speaks_runs`), the caveat's "in force today" */
  readonly n?: readonly number[];
  /** (T668 D2/D3) on a section in TWO regions: each region's places (indices into `l`). Each group cascades on its own
   *  and the most strict answer per fish speaks — resolved in Python; the app composes it only under a live feed
   *  (`composeChain`, held to `live.compose` by `salmonStraddle.golden.ts`) */
  readonly s?: readonly (readonly number[])[];
  /** (T668 fix 4) per species, runs [start, who, …] of the place that answers: an index into `l`, -1 none (not listed),
   *  -2 both regions' words together (`pipeline/deliver/salmon_who.py`). ABSENT in a release before fix 4 (e.g.
   *  r-303b4007a70a): the card then names no place on its rows — the card as it was, by design, not a fallback */
  readonly a?: readonly (readonly number[])[];
  /** (T668 Z4a) on a two-region chain: the region slugs whose salmon hours line holds (the tighter one, or both when
   *  they cannot be compared) — resolved in Python. Absent: every region of the chain, as before */
  readonly h?: readonly string[];
}
/** (T668 fix 4, owner 2026-11-11) a DFO place a row can come from, as the scrape holds it: its region slug, its water's
 *  name (absent on a region / section / area default) and its DFO section as the page prints it ("B(ii)"; absent where
 *  the page has no lettered sections) */
export interface SalmonPlace {
  readonly r: string; readonly water?: string; readonly sec?: string;
  /** (T573/T757) a closed area inside a DFO place, its own location (`deliver.salmon.build`); `bound`: it covers a section.
   *  The app knows a piece by this marker only, never by its id. Absent on an ordinary place and in an older release. */
  readonly piece?: 1; readonly bound?: 1;
  /** (T754, owner 2026-11-22: "Other bank: the water's other rules") a placed DFO place worded for ONE BANK of its river:
   *  the bank. It stands beside its sections' places (in their chains' `u`, never `l`), so the page answers by the water's
   *  other rules and the card names the bank (`salmonCard` `side`). Absent on any other place and in an older release. */
  readonly side?: SalmonBank;
}
/** (T754) the bank words, the rule book's `side` words */
export const SALMON_BANKS = ["north", "south", "east", "west"] as const;
export type SalmonBank = typeof SALMON_BANKS[number];
/** (T796 A5, owner 2026-11-16) THE RULE BOOK'S FULL CLOSURE AS A PLACE (`book:<entry_id>::<rule_id>`, `~2` … when one rule
 *  closes different days on different sections): first in the chains of the stretches the book closes to all fishing,
 *  so salmon read closed there too (book p.77). A release-side place (`static`): DFO's feed never carries it, so under a
 *  live feed its year is the release's own (`Payload.static`, `withSalmonFeed`). `r` its region (none for a provincial
 *  row), `water` the book row's name, `book` the rule and its printed pages — what the card names the row by. */
export interface SalmonBookPlace {
  readonly r?: string; readonly water?: string; readonly static: 1;
  readonly book: { readonly rule: string; readonly pages: readonly number[] };
}
/** (T796 A5) a static place's year: per species runs of a RELEASE verdict index (-1: says nothing that day), gear runs —
 *  the live feed's place shape (`live.location_calendar`) */
export interface SalmonStaticYear { readonly sp: readonly (readonly number[])[]; readonly g: readonly number[] }
const isBook = (p: SalmonPlace | SalmonBookPlace): p is SalmonBookPlace => (p as SalmonBookPlace).static === 1;
/** (T549) why the release does not show a curated DFO place (`deliver.salmon.OFF_KIND`) */
export const SALMON_OFF_KINDS = ["tidal", "unplaced", "no_section"] as const;
export type SalmonOffKind = typeof SALMON_OFF_KINDS[number];
/** (T549) a curated DFO place in no chain of the release: why (words + `k`), its region, label and its water's items */
export interface SalmonOffPlace { readonly why: string; readonly k: SalmonOffKind; readonly r: string; readonly w: string; readonly items: readonly string[] }
/** One header note (`pipeline/regs/dfo_salmon/header.py`): kind, DFO's words, the values derived from them. */
export interface SalmonNote {
  readonly id: string; readonly k: string; readonly t: string;
  readonly sp?: readonly string[]; readonly lengths?: readonly SalmonBand[]; readonly n?: number;
  /** the waters it holds on (scope keys into `scopes[region]`); `x` the waters it excepts; `on` its day windows */
  readonly w?: readonly { readonly n: string; readonly ws: boolean; readonly key: string }[];
  readonly x?: readonly { readonly n: string; readonly ws: boolean; readonly key: string }[];
  readonly xp?: readonly string[];
  readonly on?: readonly (readonly number[])[];
  /** (T320) an `aggregate` note's "hatchery-marked" */
  readonly hatchery?: boolean;
  /** (T343) where the note is said instead of as words (`deliver.salmon.header_answers`): the bucket, the sizes line,
   *  the aggregate limit, the closure — or `drop`, never shown */
  readonly fold?: "bucket" | "sizes" | "shared" | "closure" | "hours" | "drop";
}
/** (T343) the page's salmon fishing hours (a `daylight` note), the pipeline's words in `says` */
export interface SalmonHours { readonly says: string; readonly id: string | null }
interface Region { readonly name: string; readonly asof: string | null; readonly url: string; readonly notes?: readonly SalmonNote[];
                  readonly bucket?: SalmonBucket; readonly hours?: SalmonHours; readonly shared: readonly SalmonShared[]; readonly sizes: readonly SalmonSizes[] }
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
  /** (T321) location id → its rows the card shows in DFO's words (`pipeline/regs/dfo_salmon/wording.py`) */
  readonly x?: Readonly<Record<string, readonly SalmonWords[]>>;
  /** (T870) location id → its DFO table rows AS PRINTED ("Coho | Sep 15 to Jan 31 | 1 per day, hatchery marked only"; a
   *  page default's sentence), the rows in force (`deliver.salmon.printed_rows`; the live feed's `locs[lid].pr`): what the
   *  card's Source (DFO) lists as the rows behind its answers. Absent: a release / feed before T870 — the fold then lists
   *  no rows, as it did. */
  readonly pr?: Readonly<Record<string, readonly string[]>>;
  /** (T812) location id -> a closed-area CIRCLE the Salmon map draws (`deliver.salmon.closed_area_circles`): drawing
   *  only, never a section's colour. Absent: a release before T812, or no circle placed. */
  readonly ca?: Readonly<Record<string, SalmonCircle>>;
  /** (T521) the live feed's places no curated location knows (`feeds/dfo/unplaced.json`), each with the app items of
   *  the water it names: shown on that water's page, never painted. Absent in a release's own file. */
  readonly feedUnplaced?: readonly SalmonUnplacedPlace[];
  /** (T759) the live feed's issues (`feeds/dfo/index.json` `issues`): banners on their waters' stretches, never a status.
   *  Absent in a release's own file. */
  readonly feedIssues?: SalmonIssues;
  /** (T668 fix 4) every place of a chain, by location id: what a row is named by. Absent before fix 4. */
  readonly p?: Readonly<Record<string, SalmonPlace | SalmonBookPlace>>;
  /** (T796 A5) each static place's own year (`SalmonBookPlace`), by location id: composed into its chains under a live
   *  feed. Absent: no static place. */
  readonly static?: Readonly<Record<string, SalmonStaticYear>>;
  /** (T549) the curated places the release does not show, with why. Absent: none, or a release before T549. */
  readonly off?: Readonly<Record<string, SalmonOffPlace>>;
  /** (T549, the live feed's, `withSalmonFeed`) an `off` place's year composed from the feed, by location id */
  readonly offFeed?: Readonly<Record<string, Pick<Chain, "sp" | "g" | "m">>>;
  /** (T549, the live feed's) per region slug, how many feed places this release neither shows nor states (`off`) */
  readonly feedOnly?: Readonly<Record<string, number>>;
  /** (T549 item 5, the live feed's) places whose row the feed could not read this run: their last answers stand */
  readonly held?: readonly string[];
}
/** (T812, owner 2026-11-16: "we want the circles to just be circles on the map not separate sections of the lake") ONE
 *  CLOSED AREA DRAWN AS A CIRCLE: `c` its centre [lon, lat], `radius` metres, `g` its ring at true ground radius as the
 *  pipeline computed it (closed: the first point last; the app draws it as given), `chain` the piece's OWN chain (its
 *  days and colour), `r` its region slug, `w` its card label, `items` its water's items. */
export interface SalmonCircle {
  readonly c: readonly [number, number]; readonly radius: number; readonly g: readonly (readonly [number, number])[];
  readonly chain: number; readonly r: string; readonly w: string; readonly items: readonly string[];
}
/** (T521) One DFO place the live feed lists that no curated location knows: its water's app items, the place and its
 *  rows in DFO's own words (`rows`: the page's cells, " | " between them). */
export interface SalmonUnplacedPlace {
  readonly fingerprint: string; readonly region: string; readonly items: readonly string[]; readonly water: string; readonly scope: string;
  readonly rows: readonly string[];
}
/** (T321) A DFO row the app shows in DFO's words: `new` a clause DFO never printed before and `unread` a number no reading
 *  accounts for (both: no number is shown — the row is unparsed), `note` a known clause whose meaning is DFO's words.
 *  `sp` / `d` / `t` the species, dates and limits cells as printed. */
export interface SalmonWords { readonly k: "new" | "unread" | "note"; readonly sp: string; readonly d: string; readonly t: string }

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
  if (b.length < 17 || b[0] !== 0x42 || b[1] !== 0x43 || b[2] !== 0x53 || b[3] !== 0x41 || b[4] !== 4)
    throw new SalmonError("salmon: not a salmon/4 file");
  const digest = digestOf(b);
  if (digest !== handles) throw new SalmonError(`salmon: built for handles ${digest}, not ${handles}`);
  const n = b[13]! | (b[14]! << 8) | (b[15]! << 16) | (b[16]! * 0x1000000);
  if (17 + n !== b.length) throw new SalmonError(17 + n > b.length ? "salmon: truncated" : `salmon: ${b.length - 17 - n} trailing bytes`);
  return checkPayload(JSON.parse(new TextDecoder().decode(b.subarray(17, 17 + n))) as Payload);
}

/** (T356) THE FILE IS CHECKED ONCE, ON DECODE (like `parseSalmonFeed`): every run's verdict / gear / map index in range,
 *  every chain five species, `default` and every unmapped place a real chain, every region its `shared` and `sizes`, every
 *  closure id a note of the chain's regions. A bad file is an error here — never a quiet "closed" later. */
function checkPayload(P: Payload): Payload {
  const bad = (why: string): never => { throw new SalmonError(`salmon: ${why}`); };
  if (P.format !== "salmon/4") bad(`payload format ${String(P.format)}`);
  if (!Array.isArray(P.verdicts) || !P.verdicts.length || !Array.isArray(P.gears) || !Array.isArray(P.chains)) bad("no verdicts, gears or chains");
  if (!Number.isInteger(P.default) || !P.chains[P.default]) bad(`default ${String(P.default)} is no chain`);
  const runs = (r: readonly number[], n: number, what: string, none: boolean) => {
    if (!Array.isArray(r) || !r.length || r.length % 2) bad(`${what}: runs ${JSON.stringify(r)}`);
    for (let k = 1; k < r.length; k += 2)
      if (!Number.isInteger(r[k]) || r[k]! >= n || r[k]! < (none ? -1 : 0)) bad(`${what}: index ${r[k]} of ${n}`);
  };
  for (const [slug, r] of Object.entries(P.regions))
    if (!Array.isArray(r.shared) || !Array.isArray(r.sizes)) bad(`region ${slug} has no shared / sizes`);
  P.chains.forEach((c, i) => {
    if (c.sp?.length !== 5) bad(`chain ${i}: ${c.sp?.length} species`);
    c.sp.forEach((r, k) => runs(r, P.verdicts.length, `chain ${i} species ${k}`, false));
    runs(c.g, P.gears.length, `chain ${i} gear`, true);
    if (c.o !== undefined) runs(c.o, P.gears.length, `chain ${i} closure lines`, true);
    runs(c.m, MAP_CODES.length, `chain ${i} map`, false);
    for (const id of c.c ?? []) if (!closureNote(P, c, id)) bad(`chain ${i}: closure ${id} is no note of its regions`);
    if (c.n !== undefined) runs(c.n, 2, `chain ${i} speaks`, false);
    // (T668) `s` groups and `a` places index `l`; every place `a` can name is in `p`
    if (c.s !== undefined && (!Array.isArray(c.s) || c.s.flat().some((x) => !Number.isInteger(x) || x < 0 || x >= c.l.length)))
      bad(`chain ${i}: groups ${JSON.stringify(c.s)}`);
    if (c.h !== undefined && (!Array.isArray(c.h) || c.h.some((r) => !c.r.includes(r)))) bad(`chain ${i}: hours regions ${JSON.stringify(c.h)}`);
    if (c.a !== undefined) {
      if (c.a.length !== 5) bad(`chain ${i}: ${c.a.length} species of answering places`);
      c.a.forEach((r, k) => {
        if (!Array.isArray(r) || !r.length || r.length % 2) bad(`chain ${i} answering ${k}: runs ${JSON.stringify(r)}`);
        for (let j = 1; j < r.length; j += 2)
          if (!Number.isInteger(r[j]) || r[j]! < -2 || r[j]! >= c.l.length) bad(`chain ${i} answering ${k}: place ${r[j]} of ${c.l.length}`);
      });
      for (const l of c.l) if (!P.p?.[l]) bad(`chain ${i}: place ${l} is not in p`);
    }
  });
  for (const u of P.unmapped) if (!P.chains[u.chain]) bad(`unmapped ${u.w}: chain ${u.chain}`);
  // (T796 A5) a static place is in `p` marked so, with its year in `static` — or a live feed could not keep it
  for (const [lid, y] of Object.entries(P.static ?? {})) {
    const pl = P.p?.[lid];
    if (!pl || !isBook(pl) || !pl.book || typeof pl.book.rule !== "string" || !Array.isArray(pl.book.pages))
      bad(`static ${lid}: not a book place in p`);
    if (!Array.isArray(y.sp) || y.sp.length !== 5) bad(`static ${lid}: ${y.sp?.length} species`);
    y.sp.forEach((r, k) => runs(r, P.verdicts.length, `static ${lid} species ${k}`, true));
    runs(y.g, P.gears.length, `static ${lid} gear`, true);
  }
  for (const [lid, pl] of Object.entries(P.p ?? {})) if (isBook(pl) && !P.static?.[lid]) bad(`static place ${lid} has no year in static`);
  // (T754) a one-bank place names a bank of the four
  for (const [lid, pl] of Object.entries(P.p ?? {}))
    if (!isBook(pl) && pl.side !== undefined && !SALMON_BANKS.includes(pl.side)) bad(`place ${lid}: bank ${String(pl.side)}`);
  for (const [lid, o] of Object.entries(P.off ?? {}))
    if (!o || !SALMON_OFF_KINDS.includes(o.k) || typeof o.w !== "string" || typeof o.r !== "string" || !Array.isArray(o.items))
      bad(`off ${lid}: not {why, k, r, w, items}`);
  // (T812) a circle: a real chain, a closed ring of [lon, lat] pairs, a radius
  for (const [lid, a] of Object.entries(P.ca ?? {})) {
    const pair = (q: unknown) => Array.isArray(q) && q.length === 2 && q.every((x) => typeof x === "number" && Number.isFinite(x));
    if (!a || !P.chains[a.chain] || !pair(a.c) || !(a.radius > 0) || !Array.isArray(a.g) || a.g.length < 4 || !a.g.every(pair)
        || a.g[0]![0] !== a.g[a.g.length - 1]![0] || a.g[0]![1] !== a.g[a.g.length - 1]![1] || typeof a.w !== "string")
      bad(`ca ${lid}: not {c, radius, g (a closed ring), chain, r, w, items}`);
  }
  // (T870) a place's printed rows are strings, never an empty list
  for (const [lid, rows] of Object.entries(P.pr ?? {}))
    if (!Array.isArray(rows) || !rows.length || rows.some((r) => typeof r !== "string" || !r)) bad(`pr ${lid}: not a list of rows`);
  return P;
}

/** (T356) the region header note `id` of one of the chain's regions (its closure's words) */
const closureNote = (P: Payload, c: Chain, id: string): SalmonNote | undefined =>
  c.r.flatMap((r) => P.regions[r]?.notes ?? []).find((n) => n.id === id);

/**
 * One region's slice (`salmon-<r>`): its runs. (T360) Held at 8 bytes a run — the first section (u32), the run's length
 * and its chain (u16 each) — not 20 (two f64 and an i32): ~790k runs, 15.8 MB → 6.3 MB. The file is unchanged; a run
 * longer or a chain higher than 16 bits is refused on decode, never wrapped.
 */
export interface SalmonShard {
  readonly region: string;
  readonly starts: Uint32Array; readonly lens: Uint16Array; readonly chain: Uint16Array;
}

export function decodeSalmonShard(b: Uint8Array, handles: string): SalmonShard {
  return runNow(salmonShardSteps(b, handles));
}

/** (T287) `decodeSalmonShard` in slices (`sliced.ts`): ~35 ms in one piece on a phone. Same answer. */
export function decodeSalmonShardAsync(b: Uint8Array, handles: string): Promise<SalmonShard> {
  return runSliced(salmonShardSteps(b, handles));
}

/** The decoder as steps (`runNow` / `runSliced`). */
export function* salmonShardSteps(b: Uint8Array, handles: string): Steps<SalmonShard> {
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
  const starts = new Uint32Array(count), lens = new Uint16Array(count), chain = new Uint16Array(count);
  let end = 0;
  for (let r = 0; r < count; r++) {
    if ((r & (YIELD_EVERY - 1)) === YIELD_EVERY - 1) yield;
    const s0 = end + varint(), c = varint(), ch = varint();
    if (s0 + c > 0xffffffff || c > 0xffff || ch > 0xffff) throw new SalmonError(`salmon: run ${r} (${s0}+${c}, chain ${ch}) over 32/16 bits`);
    starts[r] = s0; lens[r] = c; chain[r] = ch;
    end = s0 + c;
  }
  if (i !== b.length) throw new SalmonError(`salmon: ${b.length - i} trailing bytes`);
  return { region, starts, lens, chain };
}

/** The head with the slices read: a section's chain is the first slice holding it, else chain 0 (closed). */
export function salmonFacts(payload: Payload, handles: string, shards: readonly SalmonShard[]): SalmonFacts {
  const dflt = payload.default;
  // (T549 item 11) a slice naming a chain the head does not hold is refused here, once — never read as closed later
  for (const s of shards) for (let r = 0; r < s.chain.length; r++)
    if (s.chain[r]! >= payload.chains.length)
      throw new SalmonError(`salmon: slice ${s.region} run ${r} names chain ${s.chain[r]}, the head holds ${payload.chains.length}`);
  return {
    handles, chains: payload.chains.length, payload, loaded: shards.map((s) => s.region),
    chainOf(sid) {
      for (const { starts, lens, chain } of shards) {
        let lo = 0, hi = starts.length - 1;
        while (lo <= hi) {
          const mid = (lo + hi) >> 1;
          if (sid < starts[mid]!) hi = mid - 1;
          else if (sid >= starts[mid]! + lens[mid]!) lo = mid + 1;
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

/**
 * (T356) THE HEADER'S CLOSURE EVERY DAY, the pipeline's `deliver.salmon.closed_by_header`: the region header notes whose
 * verdicts answer every species on every day, else []. The live feed's `c` (`composeChain`) and the card both ask it.
 * (T796 X1) `book`: per species, runs of the answering place (a chain's `a`) and which places are the rule book's — the
 * days a book place answers are left out (they are the book's, not DFO's); no day left is no header closure.
 */
export function closedByHeader(sp: readonly (readonly number[])[], V: readonly Verdict[],
                               book?: { readonly a: readonly (readonly number[])[]; readonly is: (place: number) => boolean }): string[] {
  const ids = new Set<string>();
  const starts = (runs: readonly number[]) => runs.filter((_, k) => k % 2 === 0);
  for (let k = 0; k < sp.length; k++) {
    const who = book?.a[k];
    for (const day of new Set([...starts(sp[k]!), ...(who ? starts(who) : [])])) {
      if (who && book!.is(at(who, day))) continue;
      const v = at(sp[k]!, day), c = v >= 0 ? V[v]!.c : undefined;
      if (!c) return [];
      ids.add(c);
    }
  }
  return [...ids].sort();
}

/** What the Salmon colouring paints a chain on a day. */
export function salmonChainStatus(f: SalmonFacts, chain: number, day: number): SalmonMapStatus | null {
  const c = f.payload.chains[chain];
  // (T549 item 11) a missing chain is said (SalmonError), as everywhere else; `null` is only a chain with no verdict
  if (!c) throw new SalmonError(`salmon: chain ${chain} is not in the salmon file`);
  const s = MAP_CODES[at(c.m, day)];
  return s && s !== "none" ? s : null;
}

/** (T812) ONE CIRCLE AS THE SALMON MAP DRAWS IT ON A DAY: its id, ring and status. */
export interface SalmonCircleDay { readonly id: string; readonly ring: SalmonCircle["g"]; readonly status: SalmonMapStatus }

/**
 * (T812, owner 2026-11-16: "Follow the day (Recommended)") THE DAY'S CLOSED-AREA CIRCLES: each circle (`ca`) on the days
 * its own rule speaks — some species' verdict is the piece's own, not "not opened by DFO" (`d`) — in its own chain's
 * map status. Read from the facts as handed (the release's, or under a live feed `withSalmonFeed`'s composition, which
 * keeps only the circles today's page still names). A day it says nothing it is not drawn: the lake's answer stands.
 */
export function salmonCirclesOn(f: SalmonFacts, day: number): SalmonCircleDay[] {
  const P = f.payload, out: SalmonCircleDay[] = [];
  for (const [id, a] of Object.entries(P.ca ?? {}).sort(([x], [y]) => (x < y ? -1 : x > y ? 1 : 0))) {
    const c = P.chains[a.chain]!;
    if (!c.sp.some((runs) => !P.verdicts[at(runs, day)]?.d)) continue;
    out.push({ id, ring: a.g, status: salmonDayStatus(f, a.chain, day) });
  }
  return out;
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

/**
 * (T796 A6, owner 2026-11-16: "Closes the whole page") THE DAY'S FINFISH CLOSURES ON THE MAP: the chain label (`s<chain>`,
 * `salmonLabel`) of every chain with any species verdict carrying `f` that day — DFO's "Finfish closure", all fishing
 * closed, trout included. The Standard map paints these closed ahead of their status profile (map `statusInput`). Read
 * from the same per-species verdicts and day index as the card's `finfishToday` (the release's, or the live feed's
 * composition); [] when none.
 */
export function finfishDayLabels(f: SalmonFacts, on: Date): string[] {
  const P = f.payload, day = salmonDayIndex(on), out: string[] = [];
  for (let c = 0; c < f.chains; c++) {
    const ch = P.chains[c];
    // (T549 item 11) a missing chain is said, as `salmonChainStatus` says it
    if (!ch) throw new SalmonError(`salmon: chain ${c} is not in the salmon file`);
    if (ch.sp.some((runs) => P.verdicts[at(runs, day)]?.f)) out.push(`s${c}`);
  }
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

/** (T356) A species' header sizes on a page: the first entry of `regions[r].sizes` that holds (a water's own entries
 *  before every region's, as the pipeline ordered them — `quota.sizes`), else none. */
function sizeEntry(P: Payload, regions: readonly string[], item: string | null, day: number, sp: string): SalmonSizes | undefined {
  const rs = [...new Set(regions)].sort();
  const holds = (r: string, e: SalmonSizes) => !!e.w?.length && item !== null && inWindow(day, e.on)
    && e.w.some((k) => (P.scopes?.[r]?.[k] ?? []).includes(item));
  for (const r of rs) for (const e of P.regions[r]?.sizes ?? []) if (e.sp.includes(sp) && holds(r, e)) return e;
  for (const r of rs) for (const e of P.regions[r]?.sizes ?? []) if (e.sp.includes(sp) && !e.w?.length) return e;
  return undefined;
}

/** A size line for an open row's species: the header sizes the pipeline resolved (`sizeEntry`), words only. */
function sizesOf(P: Payload, regions: readonly string[], item: string | null, day: number,
                 species: readonly SalmonSpecies[]): string | null {
  const parts: string[] = [];
  for (const sp of species) {
    const e = sizeEntry(P, regions, item, day, sp);
    if (!e) continue;
    const name = SPECIES_NAME[sp];
    const bits = [e.min?.min_cm !== undefined ? `${name} ${e.min.min_cm} cm or more to keep` : null,
                  e.adult?.min_cm !== undefined ? `adult ${name} over ${e.adult.min_cm} cm` : null,
                  e.jack?.min_cm !== undefined && e.jack.max_cm !== undefined ? `jack ${name} ${e.jack.min_cm}–${e.jack.max_cm} cm` : null]
      .filter((x): x is string => !!x);
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
  /** (T671 D5, owner 2026-11-12) the page reads closed while a DFO place on this water not drawn yet is open today: the
   *  page's answer stays, the headline and the in-part caveat say the part is open and point to DFO for where */
  closedOpenInPart: "Closed today · open in part",
  openInPartCalm: "DFO opens salmon in a part of this water the map can’t draw yet: check DFO for where. Everywhere else, follow this page.",
  /** (T671 D5 extension, owner 2026-11-12) the same where the undrawn place is only catch and release today, or reads
   *  "check" (a row nobody could read); more than one kind on one page-day: open, then catch and release, then check */
  closedReleaseInPart: "Closed today · catch and release in part",
  releaseInPartCalm: "DFO allows catch and release of salmon in a part of this water the map can’t draw yet: check DFO for where. Everywhere else, follow this page.",
  closedCheckInPart: "Closed today · check DFO for part",
  checkInPartCalm: "DFO’s salmon rules for a part of this water the map can’t draw yet could not be read here: check DFO for where and what. Everywhere else, follow this page.",
  /** (T882, owner 2026-11-18 ~21:18: "To be determined I think we could change to actually say to be determined") a
   *  "check" whose every row is DFO's own "To be determined" (`isTbd`) says so, never "Check DFO": the row, the headline,
   *  the folded row and the bar's day, the year bar's key, an undrawn place's in-part line. Its colour stays the check
   *  colour (a row nobody could read keeps "Check DFO") */
  tbd: {
    status: "To be determined",
    headline: "To be determined by DFO",
    day: "to be determined",
    key: "To be determined",
    closedInPart: "Closed today · to be determined in part",
    inPartCalm: "DFO has not set its salmon rules yet (“To be determined”) for a part of this water the map can’t draw yet: check DFO for where. Everywhere else, follow this page.",
  },
  /** (T754, owner 2026-11-22) a DFO place for one bank of the river: its caveat item's place line ("West bank only") and
   *  the caveat's calm line (the other bank reads this page) */
  bankOnly: (side: SalmonBank) => `${side[0]!.toUpperCase()}${side.slice(1)} bank only`,
  otherBankCalm: "On the other bank, this page’s salmon answer applies.",
  /** (T372) the caveat's water noun (only a boat rule's calm line says it; salmon has none) */
  waterNoun: "water",
  agency: "Fisheries and Oceans Canada",
  notListedLine: "DFO does not list this water for salmon. It is closed to salmon fishing.",
  /** (T521) a rule the live feed lists at a DFO place no curated location knows: DFO's own row words after the colon */
  unplacedRule: (dfoWords: string) => `Location not yet on the map: ${dfoWords}`,
  /** (T668 fix 4, owner 2026-11-11) where a row's answer comes from, as DFO's page names the place: "Region 6 · Skeena
   *  River · B(ii)" — the region, the water (none on a region or section default), the page's lettered section as
   *  printed (none where the page has no sections). (LF) A page default's scope "region" (Region 4's and 5B's page
   *  defaults in a release cut before `salmon_who.places_named` dropped it, still on phones) is no section: "Region 5B",
   *  never "Region 5B · region" */
  placeFrom: (region: string, water: string | undefined, sec: string | undefined) =>
    [`Region ${region.toUpperCase()}`, water, sec === "region" ? undefined : sec].filter(Boolean).join(" · "),
  /** (T796 A5, owner 2026-11-16) where a row's answer is the rule book's full closure of the stretch (book p.77: a water
   *  closed to fishing is closed for salmon too): "Rule book p.9 · Skeena River" — the book's printed page(s), its row */
  placeFromBook: (pages: readonly number[], water: string | undefined) =>
    [pages.length ? `Rule book p.${pages.join(", ")}` : "Rule book", water].filter(Boolean).join(" · "),
  /** (T796, owner 2026-11-16 15:19: "we need to make sure it says where the closure is from though") a book closure
   *  in force every day: its words carry no dates of their own, so the card says so */
  bookAllYear: (words: string) => `${words} (all year)`,
  /** (T796) whose answer the card's head names: DFO's, the rule book's, or both on one page-day */
  badgeBook: "Rule book",
  badgeBoth: "Rule book · DFO",
  /** (T796) the source fold's title where the rule book closes some of the page's salmon today */
  sourceFoldBook: "Sources (rule book and DFO)",
  /** (T796) the source fold's note there, in place of `noteTwoAuthorities` (rule book p.77) */
  noteBook: "Where the rule book closes a water to all fishing, salmon are closed there too (rule book p.77). "
    + "Elsewhere salmon are set by DFO, not the provincial regulations above. Both apply.",
  /** (T796) the salmon bar's name where some of its days are the rule book's closure, or all of them */
  barSalmonBoth: "Salmon (DFO and rule book)",
  barSalmonBook: "Salmon (rule book)",
  /** (T796 A6, owner 2026-11-16: "Closes the whole page") DFO's "Finfish closure" closes ALL fishing on the stretch
   *  that day, trout included: the page's closed banner, DFO's words after the colon */
  finfishClosed: (dfo: readonly string[]) =>
    `DFO closes this water to all fishing today (a finfish closure), trout and other fish included: ${dfo.join("; ")}`,
  /** (T668) a straddler's answer that is both regions' words together */
  bothRegions: (regions: readonly string[]) => `${regions.map((r) => `Region ${r.toUpperCase()}`).join(" and ")} together`,
  /** between two places on one row */
  fromSep: "; ",
  /** (T549) a curated DFO place on this water the release does not show (`off`), with the feed's answer today if any */
  offPlace: (k: SalmonOffKind, w: string, today: string | null) =>
    `DFO also lists ${w} (${({ tidal: "in tidal water, not on this map", unplaced: "not on the map yet",
                                no_section: "not placed on this map" } satisfies Record<SalmonOffKind, string>)[k]})`
    + `${today ? `: ${today}` : ""}. Check DFO's page.`,
  /** (T549) feed places of a region this release neither shows nor states */
  feedOnly: (n: number, region: string) =>
    `DFO lists ${n} place${n === 1 ? "" : "s"} in Region ${region.toUpperCase()} that this app's data does not hold yet. Check DFO's page.`,
  /** (T549 item 5) a place whose DFO row the feed could not read: its last answer stands */
  heldRow: "DFO changed a row here that the app could not read, so its earlier answer is shown. Check DFO's page.",
  noteTwoAuthorities: "Salmon are set by DFO, not the provincial regulations above. Both apply.",
  /** (T482, owner 2026-11-07: "the source of the regs should be collapsible") the source block's fold, closed by default */
  sourceFold: "Source (DFO)",
  /** (T870, owner 2026-11-18: "It should have all the rows that apply in source") the fold's list of DFO's rows and header
   *  notes behind the card's answers: its head, then one group per DFO place (`placeFrom`) with its rows as printed, and one
   *  per region page header with the notes the card's lines (hours, the day total, the annual limit, the sizes, a water
   *  system's limit) were read from */
  sourcesHead: "DFO's rows behind these answers",
  sourceHeader: (region: string, name: string) => `Region ${region.toUpperCase()} ${name}`.trim() + " · page header",
  /** (T870 item 2, owner 2026-11-18: "the bait ban here is confusing idk where it is from") each gear line says whose it
   *  is: DFO's row "— DFO"; where DFO's plain bait ban and the B.C. rules' bait ban both hold that day, the ban is said
   *  ONCE with both ("Bait ban: no bait of any kind — DFO (all year) and B.C. rules"; "… and B.C. rules on parts of this
   *  water" where the B.C. ban holds on part of the page only) and the B.C. line leaves it out */
  gearDfo: (line: string) => `${line} — DFO`,
  gearBaitBoth: (when: string | null, parts: boolean) =>
    `${METHOD_WORDS.says.bait.replace(/\.$/, "")} — DFO${when ? ` (${when})` : ""} and B.C. rules${parts ? ` ${SALMON_WORDS.gearParts}` : ""}`,
  /** (T869, owner 2026-11-18, wording A: "10 adult chinook a year from all fresh waters" and, where adult differs, "(here
   *  an adult is over 62 cm, Jul 1–Dec 31)") the annual chinook limit's line where an adult is another length on this
   *  water than the limit's own (DFO's scoped `adult` sizes, each with its window) */
  adultHere: (parts: readonly string[]) => `(here an adult is ${parts.join("; ")})`,
  adultOver: (cm: number, when: string | null) => `over ${cm} cm${when ? `, ${when}` : ""}`,
  adultRest: "the rest of the year",
  legendTitle: "Salmon, from DFO",
  toggle: {
    standard: "Trout & more", salmon: "Salmon",
    label: "Colour the regulations by",
    /** (T425, owner 2026-11-05) the map rail's two buttons: a fish and one word each, the rest said aloud */
    rail: { standard: "Trout", salmon: "Salmon" },
    said: { standard: "Colour by trout and other species' rules", salmon: "Colour by salmon openings" },
    hint: "Changes the colours and the key on the map",
    /** the fish art each button wears (`FISH` codes: rainbow trout, chinook) */
    art: { standard: "RB", salmon: "CH" },
    /** (T182) the web reads the salmon file on the first press of Salmon: the choice says so while it loads */
    loading: "Loading…",
  },
  credit: (credit: string) => `Source: ${credit}. Check DFO's page before you fish.`,
  asOf: (iso: string | null) => (iso ? `DFO page dated ${dateWords(iso)}` : "DFO page date not stated"),
  legend: { open: "Salmon may be kept", release: "Salmon: release only", closed: "Closed to salmon",
            /** (T882, proposal) the map's check colour is DFO's "To be determined" (the only one in the data, 2026-11-18)
             *  or a DFO row the app could not read; a place not drawn yet never paints it (T679 D2) */
            check: "To be determined by DFO, or a DFO row the app could not read" } satisfies Record<SalmonMapStatus, string>,
  /** PARTIAL COVERAGE (owner 2026-10-28): the water has DFO places not drawn yet */
  /** (owner 2026-10-28) the region page's header, compact */
  regionRules: "Region rules (DFO)",
  /** (T309) a region rule's kind, said as its chip; a kind not listed reads "Rule" */
  ruleKinds: { closed: "Closed", daylight: "Daylight", daily: "Daily limit", possession: "Possession", aggregate: "Combined limit",
               min_size: "Size", adult: "Adult", jack: "Jack", bait_ban: "Bait", measure: "Measuring", facility: "Facility",
               other: "Rule" } as Readonly<Record<string, string>>,
  /** (T320) the bucket's words beside its tree; T332: a species' numbers are the tree's own words (`says`, the store's,
   *  like every trout tier's), drawn by the trout presenter \u2014 none are composed here */
  quota: {
    possession: (n: number) => `${n} in possession`,
    /** between two of a quota's lines in one cell */
    sep: " \u00b7 ",
  },
  /** (T320) a species whose DFO row is not one reading: no number, DFO's words under the table */
  quotaUnread: "Limit: see DFO's words below",
  /** (T321, owner 2026-10-31) a water whose DFO rows include one the app cannot read (new wording) */
  wordingChanged: "DFO changed the wording here — read their text",
  /** (T321) rows whose DFO words matter as printed */
  wordingNotes: "DFO's words for this water",
  wordingAllYear: "All year",
  /** (T318) the species table, the DFO words under it, and the places folded */
  tableSpecies: "Species today",
  tableDfoSays: "DFO says",
  placesHead: "Other places on this water (DFO)",
  placeClosed: "Closed to salmon fishing",
  placeShow: "Show rules",
  placeHide: "Hide rules",
  /** the link mark after a tappable line ("Show rules ›", a place with pictures) */
  more: "›",
  ruleFullWording: "Full wording",
  ruleHideWording: "Hide wording",
  regionRulesOf: (r: string, name: string) => `Region ${r.toUpperCase()} ${name}`.trim(),
  waterRules: "On this water (DFO)",
  /** a header size line on an open row, derived from DFO's header (min size, what an adult is) */
  sizes: "Sizes (DFO)",
  /** (owner 2026-10-28: "bait and fly rules also apply to salmon") the provincial gear mark on the page that day */
  gearLead: "B.C. rules, also for salmon",
  gearParts: "on parts of this water",
  /** (T227, owner 2026-10-28) the season bars at the top of the Keep tab: the provincial bar's name beside the salmon one */
  barSalmon: "Salmon (DFO)",
  barGeneral: "Game fish (B.C. rules)",
  barLegend: { open: "✓ Open", release: "Release only", unknown: "See Salmon below", closed: "✕ Closed" },
  /** (T227) the Gear tab's heads for DFO's gear conditions */
  gearDfoLead: "For salmon (DFO)",
  gearSalmonOnly: "Gear for salmon fishing: the B.C. gear rules below, and DFO's conditions",
  barDay: { open: "open", release: "release only", check: "check DFO", closed: "closed", varies: "differs along this part" } satisfies Record<SalmonBarStatus, string>,
} as const;

/**
 * (T882) DFO'S OWN "TO BE DETERMINED": a "check" verdict whose every row is DFO's "To be determined" cell — the pipeline's
 * `typed.py` TBD advisory, its words as printed then its window ("To be determined (Apr 1–Mar 31)", "To be determined (as
 * printed: To be determined)"), rows joined by "; " (`deliver.salmon._text`). A row nobody could read (dates or wording:
 * `DATES_UNPARSED` / `ROW_UNREAD` lead its words) is not one, nor a verdict mixing the two.
 */
const TBD_ROW = /^to be determined\b/i;
function isTbd(v: Verdict | undefined): boolean {
  return v?.s === "check" && v.t.split("; ").every((b) => TBD_ROW.test(b.trim()));
}

/** (T882) a species' "To be determined" on a chain's day, said only where its answer is DFO's row as read today: a HELD
 *  place (`P.held`: DFO changed its row and the app could not read it, the earlier answer carried) keeps "Check DFO" and
 *  the held line. The answering place is the chain's `a`; with none named (a release before T668 fix 4, or both regions'
 *  words together), any held place of the chain holds it. */
function tbdOn(P: Payload, chain: number, k: number, day: number): boolean {
  const ch = P.chains[chain]!;
  if (!isTbd(P.verdicts[at(ch.sp[k]!, day)])) return false;
  if (!P.held?.length) return true;
  const who = ch.a ? at(ch.a[k]!, day) : -1;
  return who >= 0 ? !P.held.includes(ch.l[who]!) : !ch.l.some((l) => P.held!.includes(l));
}

/** (T882) a chain's day reads "check" only by DFO's "To be determined": every species that reads "check" is one (`tbdOn`) */
function checkIsTbd(P: Payload, chain: number, day: number): boolean {
  const ks = SALMON_SPECIES.map((_, k) => k).filter((k) => P.verdicts[at(P.chains[chain]!.sp[k]!, day)]?.s === "check");
  return ks.length > 0 && ks.every((k) => tbdOn(P, chain, k, day));
}

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
  /** (T393, owner 2026-11-05) each species' own sizes line, on EVERY row (closed too): the size rules live on the species
   *  row, not as loose region lines */
  readonly sizesBy?: Readonly<Partial<Record<SalmonSpecies, string>>>;
  /** (T227, owner 2026-10-28) the region header's notes about this row's species (what an "adult chinook" is, an annual
   *  limit …), DFO's words, beside the species — not repeated in the region block. A size the `sizes` line already says
   *  is left out. Only on a row that may be fished (open, release, check, varies). */
  readonly speciesNotes?: readonly string[];
  /** (T320) per species of the row, its numbers when every verdict it has here says the same, else null */
  readonly quota?: Readonly<Partial<Record<SalmonSpecies, SalmonQuota | null>>>;
  /** (T502, owner 2026-11-08: "it just repeats the real table") the row's `notes` the table's rows already say: DFO's
   *  "not opened" words on a closed row, a verdict whose numbers the quota line reads (no notice, nothing unread) */
  readonly repeats?: readonly string[];
  /** (T502) the dates a repeated verdict's words end in ("Sep 1–Dec 31"), kept on the species' quota line */
  readonly window?: string | null;
  /** (T668 fix 4) where the row's answer comes from that day ("Region 6 · Skeena River · B(ii)"; places joined by
   *  `fromSep`), null when no place answers it — or the release names no places (before fix 4: the card as it was) */
  readonly from?: string | null;
  /** (T668 fix 4) the same, each species on its own (the table's rows) */
  readonly fromBy?: Readonly<Partial<Record<SalmonSpecies, string>>>;
}

/** (T309) One region rule as a row: its kind chip, the species it is about (chips), one line, and DFO's whole wording
 *  (`full`, null when the line already is all of it). Nothing is dropped. */
export interface SalmonRuleVM {
  readonly id: string;
  readonly kind: string;
  readonly kindLabel: string;
  readonly species: readonly string[];
  readonly line: string;
  readonly full: string | null;
  /** DFO's words, whole, for the Copy action */
  readonly text: string;
}

/** The first sentence of DFO's words (up to ~110 chars) as the row's line; null `full` when it is all of them. */
export function ruleLine(t: string): { line: string; full: string | null } {
  const m = /^.{20,110}?[.;:](?=\s|$)/s.exec(t.trim());
  const line = m ? m[0].replace(/[;:]$/, "").trim() : t.trim().length > 110 ? `${t.trim().slice(0, 107).replace(/\s+\S*$/, "")}…` : t.trim();
  return { line, full: line === t.trim() ? null : t.trim() };
}

export interface SalmonBlockVM {
  readonly rows: readonly SalmonRowVM[];
  /** Every species closed: the block is the one "Salmon — closed" row (owner 2026-10-27). */
  readonly allClosed: boolean;
  readonly gear: readonly string[];
  /** (T770, owner 2026-11-16 19:09) another fish's closure on the chains that day (their `o`), Python's words: "Eulachon
   *  closed all year: No fishing for eulachon" — a closure line beside the species rows, never in `gear`; [] none */
  readonly closures: readonly string[];
}

export interface SalmonCardVM extends SalmonBlockVM {
  readonly title: string;
  readonly badge: string;
  readonly headline: string;
  readonly headlineStatus: SalmonVerdictStatus | "varies";
  /** The unlisted water's one line, else null. */
  readonly notListed: string | null;
  /** (T372, owner 2026-11-03: the trout way) DFO places on this water not drawn yet: the card keeps the drawn answer and
   *  lists them in the trout card's "in part" caveat (core `inPartBlock`) — those DFO speaks for today first — else null */
  readonly caveat: CaveatVM | null;
  /** (T851) the DFO regions this card speaks for (region ids, sorted) */
  readonly regions: readonly string[];
  /** (T754, owner 2026-11-22) DFO places on the page's stretch worded for ONE BANK of the river (`p[lid].side`): beside
   *  the page's answer, never in it — the trout card's "on one side of the channel only" caveat (kind `side`), each item
   *  the bank, the place in DFO's words and its rules. Absent / null: none (a hand-built card, an older release). */
  readonly side?: CaveatVM | null;
  /** "Fisheries and Oceans Canada · Region 6 Skeena". */
  readonly source: string;
  /** (T796, owner 2026-11-16 15:19) the rule book's closures answering the page's salmon today, each named: [] none */
  readonly book: readonly SalmonBookVM[];
  /** (T796) the source fold's title: "Source (DFO)", or "Sources (rule book and DFO)" when `book` is not empty */
  readonly sourceTitle: string;
  /** "DFO page dated Aug 28, 2026" (the newest of the regions named). */
  readonly asOf: string;
  readonly credit: string;
  readonly note: string;
  /** (owner 2026-10-28) the page's DFO regions' header rules, compact: one group per region, DFO's words */
  readonly regionRules: readonly { readonly label: string; readonly notes: readonly string[]; readonly rules: readonly SalmonRuleVM[] }[];
  /** header notes naming this water that no row took (DFO's words), e.g. an "adult chinook" length here */
  readonly waterRules: readonly string[];
  /** (owner 2026-10-28) the provincial bait ban / fly only on the page that day — it applies to salmon too — else null.
   *  (T870 item 2) a bait ban DFO's line already says with it (`gearLines`) is left out here: said once */
  readonly gearProvincial: string | null;
  /** (T870 item 2) DFO's gear lines (`gear`) as the card says them, each naming whose it is ("Bait ban (all year) — DFO"),
   *  a bait ban both DFO and the B.C. rules set said once with both. Absent on a hand-built card: `gear` as printed */
  readonly gearLines?: readonly string[];
  /** (T870, owner 2026-11-18) the DFO rows and header notes behind the card's answers, for the Source (DFO) fold: one
   *  group per DFO place of the page's chains (its rows as printed, `pr`) and per region page header (the notes its lines
   *  were read from). [] when the file carries none (a release before T870, a closed region) */
  readonly sources?: readonly SalmonSourceVM[];
  /** (T869) where an adult chinook is another length here than the annual limit's own: "(here an adult is over 62 cm,
   *  Sep 1–Dec 31)", said after the bucket's annual chinook line (`salmonTable`); null where it is the same */
  readonly adultHere?: string | null;
  /** (T343) the page's salmon fishing hours (DFO's daylight rule, e.g. "Salmon fishing in daylight only: one hour before
   *  sunrise to one hour after sunset"), null when the page prints none or every salmon is closed */
  readonly hours?: string | null;
  /** (T227, owner 2026-10-28) the page's salmon reduce to ONE closed-all-year statement (a region-wide closure, an
   *  unlisted water, places closed every day): the at-a-glance row says "closed" and opens nothing */
  readonly closedOnly: boolean;
  /** (T227) DFO's year for the page, one bar per DFO place (see `SalmonYearVM`); empty when DFO has no salmon rules here */
  readonly year: SalmonYearVM;
  /** (T320) the salmon bucket of the page's DFO region(s) — null when none prints one, or two print different ones */
  readonly bucket?: SalmonBucket | null;
  /** (T320, owner 2026-10-31) a header's combined limit on a water system this page is in (Squamish), while that
   *  species is open here — else none */
  readonly waterBuckets?: readonly SalmonBucketVM[];
  /** (T321, owner 2026-10-31) the page's DFO rows in DFO's words: `changed` — the wording changed (a clause never seen
   *  before, or a number not read): no number is shown for them, read DFO's text; `notes` — rows whose words matter as
   *  printed ("in the aggregate", "see exception below"). Null when there are none. */
  readonly dfoWords?: SalmonWordsVM | null;
  /** (T759) the live feed's issues on this water, one banner each quoting DFO's row; [] when none. Never a status. */
  readonly issues: readonly SalmonIssueVM[];
  /** (T796 A6) DFO's words of a "Finfish closure" answering on the page's drawn chains that day (a verdict's `f`): ALL
   *  fishing is closed, trout included (`closeForFinfish` closes the page); [] none */
  readonly finfish: readonly string[];
}
/** (T870) one group of the Source (DFO) fold's rows: where from ("Region 2 · Mamquam River", "Region 2 Lower Mainland ·
 *  page header") and DFO's words as printed */
export interface SalmonSourceVM { readonly from: string; readonly lines: readonly string[] }
/** (T796) one rule-book closure answering the page's salmon today: where it is from ("Rule book p.9 · National Parks
 *  and Ecological Reserves", `placeFromBook`) and its words — the book's page, its own line and its dates ("Closed to all
 *  fishing by the rule book (p.9): Fishing is prohibited in Ecological Reserves in B.C. (all year)") */
export interface SalmonBookVM { readonly from: string; readonly words: string }
/** (T321) one DFO row in DFO's words: its species and dates cells (dates "All year" when blank) and the limits cell */
export interface SalmonWordsRowVM { readonly species: string; readonly dates: string; readonly words: string }
export interface SalmonWordsVM {
  readonly changed: readonly SalmonWordsRowVM[];
  readonly changedHead: string;
  readonly notes: readonly SalmonWordsRowVM[];
  readonly notesHead: string;
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
  /** (T882) today's "check" is DFO's "To be determined" on every chain of the bar (absent otherwise) */
  readonly todayTbd?: true;
  /** (T882) every "check" day of the year is DFO's "To be determined" and no day differs along the part: the bar's key
   *  for its uncertain days says "To be determined" (absent otherwise) */
  readonly keyTbd?: true;
}
export interface SalmonYearVM {
  /** ONE bar for the viewed part / section, always (T820: a page closed every day draws the full closed bar) */
  readonly bars: readonly SalmonBarVM[];
  /** (T796) whose answers the bar's days are: DFO's, the rule book's closure, or both over the year */
  readonly by: SalmonSourceBy;
}
/** (T796) whose answer: DFO's (any DFO place, "not opened by DFO", a release naming no places), the rule book's, or both */
export type SalmonSourceBy = "dfo" | "book" | "both";

/** The provincial gear classes of a page's sections on the day (`methodClassOn`), for the card's gear line. */
export type SalmonGearInput = readonly MethodClass[];

/** (T321) the DFO rows of these locations the card shows in DFO's words, once each; null when none */
function wordsOf(P: Payload, lids: readonly string[]): SalmonWordsVM | null {
  const seen = new Set<string>();
  const changed: SalmonWordsRowVM[] = [], notes: SalmonWordsRowVM[] = [];
  for (const l of new Set(lids)) for (const w of P.x?.[l] ?? []) {
    const k = JSON.stringify([w.k === "note", w.sp, w.d, w.t]);
    if (seen.has(k)) continue;
    seen.add(k);
    (w.k === "note" ? notes : changed).push({ species: w.sp, dates: w.d || SALMON_WORDS.wordingAllYear, words: w.t });
  }
  return changed.length || notes.length
    ? { changed, changedHead: SALMON_WORDS.wordingChanged, notes, notesHead: SALMON_WORDS.wordingNotes } : null;
}

/** (T770) another fish's closure lines on the chains that day (their `o`), each said once, the chains' order */
const closuresOf = (P: Payload, chains: readonly number[], day: number): string[] => [...new Set(chains.flatMap((c) => {
  const o = P.chains[c]!.o;
  const x = o ? at(o, day) : -1;
  return x >= 0 ? P.gears[x] ?? [] : [];
}))];

/** The species rows for a set of chains on a day: species alike grouped, every-species-closed collapsed. */
export function salmonBlock(f: SalmonFacts, chains: readonly number[], day: number): SalmonBlockVM {
  const P = f.payload;
  const per = SALMON_SPECIES.map((_, k) => [...new Set(chains.map((c) => at(P.chains[c]!.sp[k]!, day)))]);
  const verdictOf = (ix: number): Verdict => P.verdicts[ix]!;     // every index checked on decode (checkPayload)
  const statusOf = (ixs: readonly number[]): SalmonVerdictStatus | "varies" => {
    const ss = [...new Set(ixs.map((ix) => verdictOf(ix).s))];
    return ss.length === 1 ? ss[0]! : "varies";
  };
  const gear = [...new Set(chains.flatMap((c) => {
    const g = at(P.chains[c]!.g, day);
    return g >= 0 ? P.gears[g] ?? [] : [];
  }))];
  const closures = closuresOf(P, chains, day);
  // (T767 item 4) the feed's short text may leave the condition to the tree ("2 per day (Nov 7–30)", "hatchery-marked
  // only" in `tree.says` alone): where the row draws no quota line (`treeWords`: its verdicts' trees differ), each open
  // verdict says its tree's words beside DFO's, so no condition is said nowhere
  const treeWordsOf = (v: Verdict): string | null => {
    const runs = v.s === "open" ? quotaLine(v) : null;
    const w = runs ? lineText(runs) : "";
    return w && !v.t.toLowerCase().includes(w.toLowerCase()) ? w : null;
  };
  const notesOf = (ixs: readonly number[], treeWords = false) => [...new Set(ixs.map((ix) => {
    const v = verdictOf(ix);
    const w = treeWords ? treeWordsOf(v) : null;
    const t = w ? `${v.t} · ${w}` : v.t;
    return v.n && !v.t.includes(v.n) ? `${t} · ${v.n}` : t;
  }))];
  // (T502) the notes the rows already say: a closed row's "not opened by DFO" (`d`, the pipeline's NOT_LISTED) and an
  // open row's verdict words whose numbers its quota line reads (`read`: every species' quota a tree, nothing unread),
  // neither carrying a notice; the dates such words end in stay, on the quota line
  const repeatsOf = (ixs: readonly number[], read: boolean): { repeats: string[]; window: string | null } => {
    const vs = [...new Set(ixs)].map(verdictOf);
    const rep = vs.filter((v) => !v.n && (v.s === "closed" ? !!v.d : v.s === "open" && read && !!v.tree && !v.u));
    if (rep.length !== vs.length) return { repeats: [], window: null };
    const ws = [...new Set(rep.map((v) => (v.s === "open" ? /\(([^()]+)\)\s*\.?$/.exec(v.t)?.[1] ?? "" : "")))];
    return { repeats: rep.map((v) => v.t), window: ws.length === 1 && ws[0] ? ws[0] : null };
  };
  if (per.every((ixs) => statusOf(ixs) === "closed")) {
    const all = [...new Set(per.flat())];
    return { allClosed: true, gear, closures,
             rows: [{ key: "all", species: SALMON_SPECIES, label: SALMON_WORDS.all, status: "closed", statusLabel: SALMON_WORDS.status.closed,
                      notes: notesOf(all), ...repeatsOf(all, false) }] };
  }
  const groups = new Map<string, { sp: SalmonSpecies[]; ixs: number[] }>();
  SALMON_SPECIES.forEach((sp, k) => {
    const st = statusOf(per[k]!);
    const key = st === "varies" ? "varies" : per[k]!.join(",");
    const g = groups.get(key) ?? { sp: [], ixs: per[k]! };
    g.sp.push(sp);
    groups.set(key, g);
  });
  // (T320) a species' numbers: one reading where every verdict it has on these chains carries the same, else null
  const quotaOf = (k: number): SalmonQuota | null => {
    const qs = [...new Set(per[k]!.map((ix) => {
      const v = verdictOf(ix);
      return JSON.stringify(v.tree || v.u ? { ...(v.tree ? { tree: v.tree } : {}), ...(v.u ? { u: v.u } : {}) } : null);
    }))];
    return qs.length === 1 ? (JSON.parse(qs[0]!) as SalmonQuota | null) : null;
  };
  const rows = [...groups.entries()].map(([key, g]) => {
    const st = statusOf(g.ixs);
    const quota = Object.fromEntries(g.sp.map((sp) => [sp, quotaOf(SALMON_SPECIES.indexOf(sp))]));
    return { key, species: g.sp, label: cap(g.sp.map((s) => SPECIES_NAME[s]).join(" · ")), status: st,
             statusLabel: st === "check" && chains.every((c) => g.sp.every((sp) => tbdOn(P, c, SALMON_SPECIES.indexOf(sp), day)))
               ? SALMON_WORDS.tbd.status : SALMON_WORDS.status[st],
             notes: st === "varies" ? [SALMON_WORDS.varies] : notesOf(g.ixs, g.sp.some((sp) => quota[sp] === null)), quota,
             ...(st === "varies" ? {} : repeatsOf(g.ixs, g.sp.every((sp) => !!quota[sp]?.tree && !quota[sp]?.u))) };
  });
  return { allClosed: false, gear, closures, rows };
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
  if (!chains.length) chains.push(P.default);
  // (T668 fix 1) the water's undrawn places that the page's chains name (`u`): the pipeline keeps an undrawn place to
  // its own region's sections, so Prince George no longer lists Mission. (A release before T668 named every region's
  // undrawn places in `u`, so it reads as it did.)
  const undrawnNamed = new Set(chains.flatMap((c) => P.chains[c]!.u ?? []));
  const unmapped = page.item === null ? []
    : P.unmapped.filter((u) => u.items.includes(page.item!) && (P.chains[u.chain]?.l ?? []).some((l) => undrawnNamed.has(l)));
  // (T549) the curated places on this water the release does not show: a text row each, never painted
  const offs = page.item === null ? [] : Object.entries(P.off ?? {}).filter(([, o]) => o.items.includes(page.item!));
  // (T521) the feed's DFO places on this water that no curated location knows: in the in-part caveat, never painted
  const unplaced = page.item === null ? [] : (P.feedUnplaced ?? []).filter((u) => u.items.includes(page.item!));
  // (T759) the feed's issues on this water: every stretch of it (its item), never its chains
  const banners = salmonIssueBanners(P, page.item, undrawnNamed);
  const def = P.default;
  // (T227, owner 2026-10-28) A REGION-WIDE CLOSURE: the region's header says "There is no fishing for salmon in Region 4
  // … until further notice" — the header's closure is the answer, every day: one closed row in its words, no partial
  // line; (T820, owner 2026-11-16) and the full closed year bar. (T356) The pipeline says which chains it closes (`c`, `quota.header_closure`): looked up, not inferred.
  // (T796 X1) where the rule book's closure stands first on such a chain, the header's closure on every other day
  // (`headerClosure`): still the region's one statement, the book's closure a note on it
  const shut = (c: number) => headerClosure(P, c).length > 0;
  const regionClosed = unmapped.every((u) => u.d || shut(u.chain)) && chains.some((c) => c !== def)
    && chains.every((c) => c === def || shut(c));
  const closedWords = regionClosed ? [...new Set(chains.flatMap((c) => (c === def ? [] : headerClosure(P, c).map((id) => {
    const n = closureNote(P, P.chains[c]!, id);
    if (!n) throw new SalmonError(`salmon: chain ${c}: closure ${id} is no note of its regions`);
    return n;
  }))))] : [];
  // (T796, owner 2026-11-16 15:19: "we need to make sure it says where the closure is from") the rule book's closures
  // answering today, named; a row's note that is one of them says its dates ("all year" where its words carry none)
  const book = bookToday(f, chains, day);
  const block: SalmonBlockVM = regionClosed
    // (T770) another fish's closure stands under the region's salmon closure too: a different fish, its own line
    ? { allClosed: true, gear: [], closures: closuresOf(P, chains, day), rows: [{ key: "all", species: SALMON_SPECIES, label: SALMON_WORDS.all, status: "closed",
                                             statusLabel: SALMON_WORDS.status.closed,
                                             notes: [...closedWords.map((n) => n.t), ...book.map((b) => b.words)] }] }
    : salmonBlock(f, chains, day);
  // (T372, owner 2026-11-03: the trout way) PARTIAL COVERAGE: the rows keep the drawn answer; the DFO places on the
  // water not drawn yet — the water's own (`unmapped`) and those its chains name (`u`) — are the "in part" caveat
  // (T754) a placed one-bank place rides its own sections' `u` too: beside the page's answer, its own caveat (`side`)
  const beside = regionClosed ? [] : undrawnChains(f, chains, unmapped);
  const undrawn = beside.filter((c) => !bankOf(P, c)), banks = beside.filter((c) => !!bankOf(P, c));
  const listed = chains.some((c) => c !== P.default);
  const regionsUsed = [...new Set([...chains.flatMap((c) => P.chains[c]!.r), ...unmapped.map((u) => u.r)])].sort();
  const named = regionsUsed.length ? regionsUsed : Object.keys(P.regions).sort();
  const dates = named.map((r) => P.regions[r]?.asof).filter((x): x is string => !!x).sort();
  // THE REGION HEADER: the page's DFO regions' rules; a note naming this water goes on the row of its species when that
  // row may be fished, else in the water's own lines; an open row says the sizes the header sets for its species
  const hn = salmonNotes(P, regionsUsed, page.item, day);
  const placed = new Set<string>();
  const buckets = [...new Map(regionsUsed.flatMap((r) => (P.regions[r]?.bucket ? [P.regions[r]!.bucket!] : []))
    // (T671 D3) one bucket only where the regions print the SAME tree: Region 8's adds a monthly chinook limit
    .map((b) => [JSON.stringify([b.tree, b.possession ?? null]), b])).values()];
  // (T343) a note the pipeline folded is said by its structure, not as words: a sizes note where a sizes line says its
  // species (`sized`), a bucket note where the one bucket is; an aggregate, a closure or a dropped note never; (T482) a
  // daylight note where its region ships the hours (the card's `hours` line), never where it does not
  const asWords = (n: SalmonNote, sized: ReadonlySet<string>) =>
    !n.fold || (n.fold === "hours" && !P.regions[n.id.split(":")[0]!]?.hours) || (n.fold === "sizes" && !n.sp?.some((x) => sized.has(x)))
      || (n.fold === "bucket" && buckets.length !== 1);
  const none: ReadonlySet<string> = new Set();
  // (T227, owner 2026-10-28) a region note about a species (what an "adult chinook" is, an annual chinook limit) sits
  // with that species' row, the way steelhead's sits with steelhead; the region block keeps only what no row took
  const speciesWise = hn.region.filter((n) => n.sp?.length);
  const placedRegion = new Set<string>(closedWords.map((n) => n.id));
  // (T393, owner 2026-11-05: "should live on the species row itself?" — yes) every species' sizes, closed rows too
  const sizesByOf = (r: SalmonRowVM) => {
    const by = Object.fromEntries(r.species.flatMap((sp) => {
      const line = sizesOf(P, regionsUsed, page.item, day, [sp]);
      return line ? [[sp, line]] : [];
    })) as Partial<Record<SalmonSpecies, string>>;
    return Object.keys(by).length ? { sizesBy: by } : {};
  };
  // (T668 fix 4) where each row comes from: the place that answers each of its species on the page's chains
  const fromOf = (species: readonly SalmonSpecies[]): string | null => {
    const parts = new Set<string>();
    for (const c of chains) {
      const ch = P.chains[c]!;
      if (!ch.l.length) continue;                          // no place: nothing answers (not listed)
      if (!ch.a) return null;                              // a release before fix 4 names no place: the card as it was
      for (const sp of species) {
        const v = at(ch.a[SALMON_SPECIES.indexOf(sp)]!, day);
        if (v === -2) parts.add(SALMON_WORDS.bothRegions([...new Set(ch.l.flatMap((l) => { const p = P.p![l]!; return isBook(p) ? [] : [p.r]; }))].sort()));
        else if (v >= 0) {
          const p = P.p![ch.l[v]!]!;
          // (T796 A5) the rule book's closure is named by the book's page, not a DFO region
          parts.add(isBook(p) ? SALMON_WORDS.placeFromBook(p.book.pages, p.water) : SALMON_WORDS.placeFrom(p.r, p.water, p.sec));
        }
      }
    }
    return parts.size ? [...parts].join(SALMON_WORDS.fromSep) : null;
  };
  const bookWords = new Map(book.map((b) => [b.t, b.words]));
  // (T796 X1) a region closed by its page: DFO's, and the book's too on a day its closure answers
  const todayBy = regionClosed ? (book.length ? "both" : "dfo") : sourceBy(P, chains, (a) => [at(a, day)]);
  const rows = block.rows.map((row0) => {
    const row = bookWords.size ? { ...row0, notes: row0.notes.map((n) => bookWords.get(n) ?? n) } : row0;
    const from = regionClosed ? null : fromOf(row.species);
    const fromBy = Object.fromEntries(regionClosed ? [] : row.species.flatMap((sp) => {
      const x = fromOf([sp]);
      return x ? [[sp, x]] : [];
    })) as Partial<Record<SalmonSpecies, string>>;
    const r = { ...row, ...sizesByOf(row), ...(from ? { from, fromBy } : {}) };
    // (T482, owner 2026-11-07: "combine it into the individual salmon species") every species closed: its sizes still
    // sit on its row (the "Salmon — closed" row's, per species), so no size note is left loose in the region block
    if (block.allClosed) return r;
    if (r.status === "closed") return r;
    const ofRow = (n: SalmonNote) => !!n.sp?.some((x) => r.species.includes(x as SalmonSpecies));
    const regionMine = speciesWise.filter(ofRow);
    regionMine.forEach((n) => placedRegion.add(n.id));
    if (r.status !== "open" && r.status !== "release") {
      const words = regionMine.filter((n) => asWords(n, none)).map((n) => n.t);
      return words.length ? { ...r, speciesNotes: words } : r;
    }
    const mine = hn.water.filter((n) => n.k !== "adult" && ofRow(n));
    mine.forEach((n) => placed.add(n.id));
    const sizes = sizesOf(P, regionsUsed, page.item, day, r.species);
    // the sizes line already says the minimum, the adult and the jack lengths: those notes are not repeated
    const sized: ReadonlySet<string> = sizes ? new Set(r.species) : none;
    const speciesNotes = regionMine.filter((n) => asWords(n, sized)).map((n) => n.t);
    return { ...r, notes: [...r.notes, ...mine.filter((n) => asWords(n, sized)).map((n) => n.t)], sizes,
             ...(speciesNotes.length ? { speciesNotes } : {}) };
  });
  const sizesShown: ReadonlySet<string> = new Set(rows.flatMap((r) => [...(r.sizes ? r.species : []), ...Object.keys(r.sizesBy ?? {})]));
  const regionRules = regionsUsed.map((r) => {
    const mine = hn.region.filter((n) => n.id.startsWith(`${r}:`) && !placedRegion.has(n.id) && asWords(n, sizesShown));
    return {
      label: SALMON_WORDS.regionRulesOf(r, P.regions[r]?.name ?? ""),
      notes: mine.map((n) => n.t),
      rules: mine.map((n): SalmonRuleVM => ({
        id: n.id, kind: n.k, kindLabel: SALMON_WORDS.ruleKinds[n.k] ?? SALMON_WORDS.ruleKinds.other!,
        species: (n.sp ?? []).filter((x): x is SalmonSpecies => (SALMON_SPECIES as readonly string[]).includes(x)).map((x) => cap(SPECIES_NAME[x])),
        ...ruleLine(n.t), text: n.t })),
    };
  }).filter((g) => g.notes.length);
  const gear = page.gear ?? [];
  const marks0 = (["fly", "bait"] as const).filter((m) => gear.includes(m));
  // (T870 item 2) whose each gear line is; DFO's plain bait ban and the B.C. rules' bait ban on one day: said once, both
  // named, and the B.C. line leaves it out (only where the B.C. line would show: not on a closed card)
  const baitBoth = !block.allClosed && marks0.includes("bait") ? block.gear.findIndex((g) => DFO_BAIT.test(g)) : -1;
  const gearLines = block.gear.map((g, i) => (i === baitBoth ? SALMON_WORDS.gearBaitBoth(DFO_BAIT.exec(g)![1] ?? null, gear.some((x) => x === "none")) : SALMON_WORDS.gearDfo(g)));
  const marks = baitBoth >= 0 ? marks0.filter((m) => m !== "bait") : marks0;
  const gearProvincial = block.allClosed || !marks.length ? null
    : `${SALMON_WORDS.gearLead}${gear.some((g) => g === "none") ? ` (${SALMON_WORDS.gearParts})` : ""}: `
      + marks.map((m) => METHOD_WORDS.says[m]).join(" ");
  const sts = rows.map((r) => r.status);
  const headlineStatus: SalmonVerdictStatus | "varies" =
    sts.includes("open") ? "open" : sts.includes("release") ? "release" : sts.includes("varies") ? "varies"
      : sts.includes("check") ? "check" : "closed";
  // (T671 D5, owner 2026-11-12) the page reads closed and a place on this water not drawn yet is OPEN today (the
  // pipeline's verdicts for that place, looked up): the page's answer stays, the headline and caveat say "open in part".
  // (D5 extension, owner 2026-11-12) only catch and release there: "catch and release in part"; "check" there: "check
  // DFO for part". More than one kind on the page-day: open, then catch and release, then check.
  const partKind = headlineStatus !== "closed" ? null
    : (["open", "release", "check"] as const).find((k) => undrawn.some((c) => salmonDayStatus(f, c, day) === k)) ?? null;
  // (T882) "check" there only by DFO's "To be determined": "to be determined in part"
  const partTbd = partKind === "check" && undrawn.every((c) => salmonDayStatus(f, c, day) !== "check" || checkIsTbd(P, c, day));
  const part = partKind === "open" ? { headline: SALMON_WORDS.closedOpenInPart, calm: SALMON_WORDS.openInPartCalm }
    : partKind === "release" ? { headline: SALMON_WORDS.closedReleaseInPart, calm: SALMON_WORDS.releaseInPartCalm }
      : partTbd ? { headline: SALMON_WORDS.tbd.closedInPart, calm: SALMON_WORDS.tbd.inPartCalm }
        : partKind === "check" ? { headline: SALMON_WORDS.closedCheckInPart, calm: SALMON_WORDS.checkInPartCalm } : null;
  // (T882) the page's "check" rows are all DFO's "To be determined": the headline says so (its colour stays the check one)
  const headTbd = headlineStatus === "check" && rows.every((r) => r.status !== "check" || r.statusLabel === SALMON_WORDS.tbd.status);
  const inPart = inPartBlock(undrawnEntries(f, undrawn, unmapped, day), SALMON_WORDS.waterNoun);
  const side = sideBlock(f, banks, unmapped, day);
  const hours = block.allClosed ? null
    // (T668 Z4a) a two-region chain's `h`: the regions whose hours line holds there (the tighter, or both)
    : [...new Set([...new Set([...chains.flatMap((c) => P.chains[c]!.h ?? P.chains[c]!.r), ...unmapped.map((u) => u.r)])].sort()
        .map((r) => P.regions[r]?.hours?.says).filter((x): x is string => !!x))].join(" · ") || null;
  const waterBuckets = hn.water.filter((n) => n.k === "aggregate" && typeof n.n === "number"
      && rows.some((r) => r.status === "open" && !!n.sp?.some((x) => r.species.includes(x as SalmonSpecies))));
  // (T869) the bucket's annual chinook limit, and where an adult is another length here than the limit's own
  const annual = buckets.length === 1 ? buckets[0]!.tree.children.find((c): c is TreeTier => c.node === "tier" && c.period === "annual"
    && !!c.species?.includes("CH") && typeof c.over_cm === "number") : undefined;
  const adultHere = annual ? adultHereOf(P, regionsUsed, page.item, annual.over_cm!) : null;
  // (T870, owner 2026-11-18) the DFO rows and header notes behind the card's answers: none where the region's header
  // closes it (its one statement is the row); the header notes only where a line of the card reads them
  // the bucket's notes as `bucketOf` shows them: the day total while a species may be kept, a period limit while its
  // species may be
  const kept = new Set<string>(rows.filter((r) => r.status === "open").flatMap((r) => r.species));
  const bucketShown = (n: SalmonNote) => buckets.length === 1 && kept.size > 0 && (!n.sp?.length || n.sp.some((x) => kept.has(x)));
  const sharedShown = new Set(waterBuckets.map((n) => n.id));
  const behind = (n: SalmonNote): boolean =>
    n.fold === "hours" ? !!hours && !!P.regions[n.id.split(":")[0]!]?.hours
      : n.fold === "bucket" ? bucketShown(n)
        : n.fold === "sizes" ? !!n.sp?.some((x) => sizesShown.has(x))
          : n.fold === "shared" ? sharedShown.has(n.id) : false;
  const sources: SalmonSourceVM[] = regionClosed ? [] : [
    ...placeSources(P, chains),
    ...regionsUsed.map((r) => ({ from: SALMON_WORDS.sourceHeader(r, P.regions[r]?.name ?? ""),
                                lines: [...new Set([...hn.region, ...hn.water].filter((n) => n.id.startsWith(`${r}:`) && behind(n)).map((n) => n.t))] }))
      .filter((g) => g.lines.length)];
  return {
    ...block, rows, regionRules, gearProvincial, gearLines, sources, adultHere,
    hours,
    bucket: buckets.length === 1 ? buckets[0]! : null,
    waterBuckets: waterBuckets
      .map((n) => {
        // (T349, T356) the store's aggregate tier over the note's waters: the region's `shared` (`quota.shared_buckets`)
        const sh = regionsUsed.flatMap((r) => P.regions[r]?.shared ?? []).find((x) => x.id === n.id);
        if (!sh) throw new SalmonError(`salmon: the aggregate note ${n.id} has no shared limit in its region`);
        const runs = tierWords(sh.tree);
        return { line: lineText(runs), runs, words: [n.t] };
      }),
    // (T521, owner 2026-11-09) with the water's other DFO rules: the feed's rules at a place on this water no curated
    // location knows, marked "location not yet on the map"
    waterRules: [...hn.water.filter((n) => !placed.has(n.id) && asWords(n, sizesShown)).map((n) => n.t),
                 // (T828) a row the banner below already quotes (same fingerprint, an `unmatched` issue shown) is said once, there
                 ...unplacedLines(unplaced.filter((u) => !banners.some((b) => b.key === `unmatched:${u.fingerprint}`))),
                 // (T549) places the release does not show; feed places it neither shows nor states; held rows
                 ...offs.map(([lid, o]) => SALMON_WORDS.offPlace(o.k, o.w, P.offFeed?.[lid] ? offToday(f, lid, o, P.offFeed[lid]!, day) : null)),
                 ...regionsUsed.filter((r) => P.feedOnly?.[r]).map((r) => SALMON_WORDS.feedOnly(P.feedOnly![r]!, r)),
                 ...(chains.some((c) => P.chains[c]!.l.some((l) => P.held?.includes(l))) ? [SALMON_WORDS.heldRow] : [])],
    title: SALMON_WORDS.title,
    badge: todayBy === "book" ? SALMON_WORDS.badgeBook : todayBy === "both" ? SALMON_WORDS.badgeBoth : SALMON_WORDS.badge,
    headline: part ? part.headline : headTbd ? SALMON_WORDS.tbd.headline : SALMON_WORDS.headline[headlineStatus], headlineStatus,
    notListed: listed || unmapped.length || unplaced.length || offs.length ? null : SALMON_WORDS.notListedLine,
    caveat: part && inPart ? { ...inPart, title: part.headline, calm: part.calm } : inPart,
    side,
    dfoWords: regionClosed ? null : wordsOf(P, [...chains.flatMap((c) => [...P.chains[c]!.l, ...(P.chains[c]!.u ?? [])]),
                                                  ...unmapped.filter((u) => !u.d).flatMap((u) => P.chains[u.chain]?.l ?? [])]),
    // (T851) the DFO regions this card speaks for: the feed's read time for the card is theirs alone (`salmonReadAt`)
    regions: named,
    source: [SALMON_WORDS.agency,
             ...regionsUsed.map((r) => `Region ${r.toUpperCase()} ${P.regions[r]?.name ?? ""}`.trim())].join(" · "),
    book: book.map(({ from, words }) => ({ from, words })),
    sourceTitle: book.length ? SALMON_WORDS.sourceFoldBook : SALMON_WORDS.sourceFold,
    asOf: SALMON_WORDS.asOf(dates[dates.length - 1] ?? null),
    credit: SALMON_WORDS.credit(P.credit),
    note: book.length ? SALMON_WORDS.noteBook : SALMON_WORDS.noteTwoAuthorities,
    // (T820, owner 2026-11-16 21:37: "the calendar bar doesnt have a full red line for salmon when closed but instead it
    // is empty with no color on the calendar bar") the region-wide closure keeps its one statement AND draws its year:
    // every day closed, the full closed bar (T227's and X1's "no bar" overruled)
    year: salmonYear(f, chains, day),
    finfish: finfishToday(f, chains, day),
    // (T759) the feed's issues on this water: every stretch of it (its item), never its chains; a place its chains
    // already list as not drawn is the in-part caveat's
    issues: banners,
    // one closed-all-year statement: the region's closure, or every place of the page closed every day, nothing undrawn
    closedOnly: regionClosed || (!beside.some((c) => speaks(f, c))
      && chains.every((c) => runsOf((i) => salmonDayStatus(f, c, i)).every((r) => r.status === "closed"))),
  };
}

/** (T870 item 2) a DFO gear line that is a plain bait ban, its dates (or "all year", `deliver.salmon.gear_text`) */
const DFO_BAIT = /^Bait ban(?: \(([^()]+)\))?$/i;

/**
 * (T870, owner 2026-11-18: "It should have all the rows that apply in source") each DFO place of the page's chains with
 * its rows as printed (`pr`), narrowest first, named as DFO's page names it (`placeFrom`); places named alike are one
 * group, a row said once. The rule book's places are the fold's `book` lines, not here.
 */
function placeSources(P: Payload, chains: readonly number[]): SalmonSourceVM[] {
  const out = new Map<string, string[]>();
  for (const c of chains) {
    const ch = P.chains[c]!;
    for (const lid of ch.l) {
      const rows = P.pr?.[lid];
      const p = P.p?.[lid];
      if (!rows?.length || (p && isBook(p))) continue;
      // (W46) a place with rows is named as DFO names it: one missing from `p` is a bad file, never named another way
      if (!p) throw new SalmonError(`salmon: place ${lid} has rows (pr) and is not in p`);
      const from = SALMON_WORDS.placeFrom(p.r, p.water, p.sec);
      const got = out.get(from) ?? [];
      for (const r of rows) if (!got.includes(r)) got.push(r);
      out.set(from, got);
    }
  }
  return [...out].map(([from, lines]) => ({ from, lines }));
}

/**
 * (T869, owner 2026-11-18, wording A) WHERE AN ADULT CHINOOK IS ANOTHER LENGTH HERE than the annual limit's own (`over`,
 * DFO's "over 50 cm"): the water's scoped `adult` sizes (`regions[r].sizes` with `w` holding this item — the pipeline's
 * `quota.sizes`, looked up as `sizeEntry` does, every window), each "over 62 cm, Sep 1–Dec 31"; where those windows leave
 * days, the region's own adult length "the rest of the year" when it differs too; a water with no scoped length, the
 * region's own (Region 6: over 65 cm). "(here an adult is …)", or null where an adult here is the limit's length.
 */
function adultHereOf(P: Payload, regions: readonly string[], item: string | null, over: number): string | null {
  const rs = [...new Set(regions)].sort();
  const all = rs.flatMap((r) => (P.regions[r]?.sizes ?? []).filter((e) => e.sp.includes("CH") && e.adult?.min_cm !== undefined)
    .map((e) => ({ r, e })));
  const own = item === null ? [] : all.filter(({ r, e }) => !!e.w?.length && e.w.some((k) => (P.scopes?.[r]?.[k] ?? []).includes(item)));
  const region = all.find(({ e }) => !e.w?.length)?.e;
  const window = (on: readonly (readonly number[])[]) =>
    on.map(([a, b]) => `${bookDayWords(a! < 59 ? a! : a! - 1)}–${bookDayWords(b! < 59 ? b! : b! - 1)}`).join(", ");
  const parts = own.filter(({ e }) => e.adult!.min_cm !== over)
    .map(({ e }) => SALMON_WORDS.adultOver(e.adult!.min_cm!, e.on?.length ? window(e.on) : null));
  const rest = own.every(({ e }) => !!e.on?.length) && region && region.adult!.min_cm !== over
    ? [own.length ? `${SALMON_WORDS.adultOver(region.adult!.min_cm!, null)} ${SALMON_WORDS.adultRest}` : SALMON_WORDS.adultOver(region.adult!.min_cm!, null)]
    : [];
  const said = [...new Set([...parts, ...rest])];
  return said.length ? SALMON_WORDS.adultHere(said) : null;
}

/**
 * (T796 A6, owner 2026-11-16: "Closes the whole page (Recommended)") DFO's words of every verdict on these chains that
 * day carrying `f` — a "Finfish closure": all fishing closed, trout included. Read from the same per-species verdicts
 * the card reads (the release's, or the live feed's composition); [] when none.
 */
function finfishToday(f: SalmonFacts, chains: readonly number[], day: number): string[] {
  const P = f.payload;
  return [...new Set(chains.flatMap((c) => P.chains[c]!.sp.flatMap((runs) => {
    const v = P.verdicts[at(runs, day)]!;
    return v.f ? [v.n && !v.t.includes(v.n) ? `${v.t} · ${v.n}` : v.t] : [];
  })))];
}

/** (T796) a chain's `a` value names a rule-book place (not DFO's, nobody, or both regions' words together) */
const isBookAt = (P: Payload, ch: Chain, v: number): boolean => v >= 0 && isBook(P.p![ch.l[v]!]!);

/**
 * (T227, T356) THE REGION HEADER NOTES THAT CLOSE A CHAIN, for the card's region-closed shape: the chain's `c` (the
 * pipeline's, or the live feed's `composeChain`), looked up. (T796 X1) `c` means "every day is the header's", so where the
 * rule book's closure stands first on a chain both drop it (`deliver.salmon._book_chain`, `composeChain` over the static
 * places): the header still closes it every day the book does not — `closedByHeader` over the days DFO's places answer
 * (`a`) — and, as `c` asks, every undrawn place the chain names is closed by the header too. Offline and under a feed
 * alike: both carry the book days in `a`. A chain the book answers every day has no header day: the book's own card.
 */
function headerClosure(P: Payload, c: number): readonly string[] {
  const ch = P.chains[c]!;
  if (ch.c?.length || !ch.a || !ch.l.some((l) => { const p = P.p?.[l]; return !!p && isBook(p); })) return ch.c ?? [];
  const ids = closedByHeader(ch.sp, P.verdicts, { a: ch.a, is: (v) => isBookAt(P, ch, v) });
  const undrawnShut = P.unmapped.every((u) => u.d || !P.chains[u.chain]!.l.some((l) => ch.u?.includes(l)) || !!P.chains[u.chain]!.c?.length);
  return undrawnShut ? ids : [];
}

/**
 * (T796, owner 2026-11-16 15:19) whose answers these chains' salmon are on the days asked (`who`: the `a` values of a
 * species' runs to read): the rule book's, DFO's (a DFO place, "not opened by DFO", a release naming no places), or both.
 */
function sourceBy(P: Payload, chains: readonly number[], who: (a: readonly number[]) => readonly number[]): SalmonSourceBy {
  let book = false, dfo = false;
  for (const c of chains) {
    const ch = P.chains[c]!;
    for (let k = 0; k < SALMON_SPECIES.length; k++) {
      if (!ch.a) { dfo = true; continue; }
      for (const v of who(ch.a[k]!)) if (isBookAt(P, ch, v)) book = true; else dfo = true;
    }
  }
  return book ? (dfo ? "both" : "book") : "dfo";
}

/**
 * (T796, owner 2026-11-16 15:19: "we need to make sure it says where the closure is from") the rule book's closures
 * answering these chains' salmon on a day: each named by the book (`placeFromBook`: its printed pages, its row) and in
 * its words (`t`, the verdict's: "Closed to all fishing by the rule book (p.N): <the book's line> (<dates>)"). A closure
 * in force every day has no dates in its words: `words` says "all year".
 * (T796 W2) "every day" is the CLOSURE'S OWN year (`payload.static`, the release's: kept as it is under a live feed),
 * never the days it answers the chain: a dated closure sorted first on the same chain takes some days from an all-year
 * one, which then lost its dates there (Statlu Creek, p.26, beside the Chehalis's May closure, p.22).
 */
function bookToday(f: SalmonFacts, chains: readonly number[], day: number): (SalmonBookVM & { readonly t: string })[] {
  const P = f.payload, out = new Map<string, SalmonBookVM & { readonly t: string }>();
  for (const c of chains) {
    const ch = P.chains[c]!;
    SALMON_SPECIES.forEach((_, k) => {
      const i = ch.a ? at(ch.a[k]!, day) : -1;
      if (!isBookAt(P, ch, i)) return;
      const lid = ch.l[i]!, p = P.p![lid]! as SalmonBookPlace;
      const year = P.static?.[lid]?.sp[k];
      if (!year) throw new SalmonError(`salmon: book place ${lid} has no year in static`);
      const t = P.verdicts[at(ch.sp[k]!, day)]!.t;
      const words = year.every((x, j) => j % 2 === 0 || x >= 0) ? SALMON_WORDS.bookAllYear(t) : t;
      out.set(words, { t, from: SALMON_WORDS.placeFromBook(p.book.pages, p.water), words });
    });
  }
  return [...out.values()];
}

/** The book's day i (0 = Jan 1 … 364 = Dec 31, no Feb 29) as the file's day (Feb 29 is 59). */
const fileDay = (i: number) => (i < 59 ? i : i + 1);
const BOOK_YEAR_DAYS = 365;

/**
 * What a chain's day is, for the card's bar, headline and in-part caveat: THE MAP'S STATUS (`salmonChainStatus`, the
 * chain's `m` runs — the pipeline's `deliver.salmon.map_status` over the day's five species verdicts, or the live
 * feed's `composeChain`). (T679 D2, owner 2026-11-12) on a water with a DFO place not drawn yet (partial coverage) the
 * map and the bar BOTH give the species verdict: one source, read here, never re-derived from the species.
 * A chain whose map has no answer that day is an error (every chain has five species; the unlisted water is closed).
 */
export function salmonDayStatus(f: SalmonFacts, chain: number, day: number): SalmonMapStatus {
  // (T549 item 11) one answer for a missing chain, as `salmonTimelineKey`: said, never a quiet "closed"
  const s = salmonChainStatus(f, chain, day);
  if (!s) throw new SalmonError(`salmon: chain ${chain} has no map status on day ${day}`);
  return s;
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
 * day: the status the page's sections share, `varies` where they differ. (T372, the trout way) a DFO place on the water
 * not drawn yet never changes the bar — it is the card's caveat. (T820, owner 2026-11-16 21:37: "the calendar bar doesnt
 * have a full red line for salmon when closed but instead it is empty with no color on the calendar bar") ALWAYS one bar:
 * nothing DFO speaks for here (the unlisted water, "not opened") and the region-wide closure are closed every day, so
 * the full closed bar — never no bar.
 */
function salmonYear(f: SalmonFacts, chains: readonly number[], day: number): SalmonYearVM {
  // (T796) whose days the bar draws: the rule book's closure on some (or all) of them names the bar so
  const by = sourceBy(f.payload, chains, (a) => a.filter((_, j) => j % 2 === 1));
  // the page's own places: one status a day, `varies` where its sections differ
  const own = (i: number): SalmonBarStatus => {
    const ss = new Set(chains.map((c) => salmonDayStatus(f, c, i)));
    return ss.size === 1 ? [...ss][0]! : "varies";
  };
  const st = own(day);
  // (T882) a "check" day said by DFO's "To be determined" on every chain of the part
  const tbdOn = (i: number) => chains.every((c) => salmonDayStatus(f, c, i) !== "check" || checkIsTbd(f.payload, c, i));
  const todayTbd = st === "check" && tbdOn(day);
  let keyTbd = false;
  for (let i = 0; i < BOOK_YEAR_DAYS; i++) {
    const s = own(fileDay(i));
    if (s === "varies" || (s === "check" && !tbdOn(fileDay(i)))) { keyTbd = false; break; }
    if (s === "check") keyTbd = true;
  }
  return { bars: [{ label: null, unmapped: false, today: st, todayWord: todayTbd ? SALMON_WORDS.tbd.day : SALMON_WORDS.barDay[st],
                    runs: runsOf(own), ...(todayTbd ? { todayTbd: true as const } : {}), ...(keyTbd ? { keyTbd: true as const } : {}) }], by };
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

/**
 * (T372) The undrawn places as the in-part caveat's entries (core `inPartBlock`, the trout card's): one per place (by its
 * words), `where` the place, `what` its day as the folded place line said it, `detail` DFO's words for it (limits,
 * sizes, notes, gear). In force: the pipeline's `n` — the days DFO speaks for the place.
 */
function undrawnEntries(f: SalmonFacts, undrawn: readonly number[], unmapped: readonly Unmapped[], day: number,
                        where: (c: number, label: string) => string = (_c, label) => WORDS.WHERE(label)) {
  const P = f.payload, seen = new Set<string>();
  return undrawn.flatMap((c) => {
    const ch = P.chains[c]!, label = unmapped.find((u) => u.chain === c)?.w ?? ch.w;
    if (seen.has(label)) return [];
    seen.add(label);
    if (!ch.n) throw new SalmonError(`salmon: the undrawn place ${label} has no in-force days (n)`);
    const b = salmonBlock(f, [c], day);
    const what = blockWhat(b);
    const detail = [...tableRows(b.rows).flatMap((r) => [...(r.quota ? [`${r.name}: ${r.quota}`] : []), ...(r.limit ? [r.limit] : [])]),
                    ...tableSays(b.rows, b.allClosed).flatMap((x) => x.lines.map((l) => (x.label ? `${x.label}: ${l}` : l))),
                    ...b.closures, ...b.gear];
    const item: CaveatItemVM = { what: [text(what)], where: where(c, label), rule: null, fish: null, book: [],
                                 ...(detail.length ? { detail } : {}) };
    return [{ item, now: at(ch.n, day) === 1 }];
  });
}

/** (T754) the bank of a placed one-bank place's own chain (`p[lid].side`, its one place), else undefined */
function bankOf(P: Payload, c: number): SalmonBank | undefined {
  const l = P.chains[c]?.l;
  const pl = l?.length === 1 ? P.p?.[l[0]!] : undefined;
  return pl && !isBook(pl) ? pl.side : undefined;
}

/**
 * (T754, owner 2026-11-22: "Other bank: the water's other rules"; AGENTS rule 49) the one-bank places on the page's
 * stretch as the trout card's side caveat (kind `side`, `WORDS.SIDE_TITLE`): each item "West bank only · <DFO's words>",
 * its day and its DFO rules; those DFO speaks for today first, the rest under "Other dates". Null when there are none.
 */
function sideBlock(f: SalmonFacts, banks: readonly number[], unmapped: readonly Unmapped[], day: number): CaveatVM | null {
  const u = undrawnEntries(f, banks, unmapped, day,
                           (c, label) => `${SALMON_WORDS.bankOnly(bankOf(f.payload, c)!)} · ${label}`);
  if (!u.length) return null;
  const now = u.filter((x) => x.now), later = u.filter((x) => !x.now);
  return { kind: "side", hot: now.length > 0, title: WORDS.SIDE_TITLE, calm: SALMON_WORDS.otherBankCalm,
           items: (now.length ? now : later).map((x) => x.item),
           later: now.length && later.length ? later.map((x) => x.item) : null };
}

/** A place's day in one line, as the folded place line says it: "Chinook open · coho · sockeye · pink · chum closed". */
const blockWhat = (b: SalmonBlockVM): string =>
  b.allClosed ? SALMON_WORDS.placeClosed : b.rows.map((r) => `${r.label} ${r.statusLabel.toLowerCase()}`).join(" · ");

/** (T549) an `off` place's day from the live feed's composition of it (`withSalmonFeed` `offFeed`) */
function offToday(f: SalmonFacts, lid: string, o: SalmonOffPlace, cal: Pick<Chain, "sp" | "g" | "m">, day: number): string {
  const one: SalmonFacts = { handles: f.handles, chains: 1, chainOf: () => 0,
                             payload: { ...f.payload, chains: [{ r: [o.r], w: o.w, l: [lid], ...cal }] } };
  return blockWhat(salmonBlock(one, [0], day));
}

/**
 * (T521, owner 2026-11-09: "still will show on front end tho so it isnt missing before it is fixed and matched") the
 * feed's rules at places on this water that no curated location knows, one line per DFO row in DFO's own words:
 * "Location not yet on the map: below the counting fence — All · Aug 1 until further notice · No fishing for salmon".
 * Never painted, never a part; a water no binding knows is the bell's (`dfoNoticeRows`).
 */
function unplacedLines(unplaced: readonly SalmonUnplacedPlace[]): string[] {
  return unplaced.flatMap((u) => u.rows.map((row) => SALMON_WORDS.unplacedRule(
    [u.scope, dfoRow(row)].filter(Boolean).join(" — "))));
}

/** (T521) a DFO row as the page's cells, " · " between them */
export const dfoRow = (row: string): string => row.split(" | ").map((c) => c.trim()).filter(Boolean).join(" · ");

/** (T228) A rule line as the key compares it: case and spacing never make two regulations differ. */
const normRule = (t: string) => t.toLowerCase().replace(/\s+/g, " ").trim();

/**
 * (T227 / T228, the map's section merging) A section's SALMON KEY: equal for two sections exactly when DFO's REGULATIONS
 * read the same on both, every day of the year — per species the verdict AND its rule content (the limit, sizes,
 * hatchery-marked or wild, the gear and method conditions and the windows: the rule lines `t` and notices `n` DFO
 * prints, normalised for case and spacing, and (T767) the quota tree's words, which can carry a condition `t` leaves out),
 * the gear lines, the map status (partial coverage included). Resolved to
 * CONTENT, never to the file's indexes, so "coho 2 a day" and "coho 4 a day, hatchery-marked only" differ though both
 * are open, and a live feed laid over the release (`withLiveSalmon`) compares the same way. Compare it beside the
 * provincial key; never show it.
 */
export function salmonTimelineKey(f: SalmonFacts, sid: number): string {
  const k = f.chainOf(sid), P = f.payload, c = P.chains[k];
  // (T517) a chain the payload lacks would read alike to another and join pieces it never compared: said, never ""
  if (!c) throw new SalmonError(`salmon: section ${sid} is on chain ${k}, which the salmon file does not hold`);
  const runs = <T>(r: readonly number[], of: (i: number) => T) => r.map((x, k) => (k % 2 ? of(x) : x));
  const verdict = (i: number) => {
    const v = P.verdicts[i];
    if (!v) return null;
    // (T767 item 4) and the tree's words: the feed's short text can leave the condition to the tree ("hatchery-marked
    // only" in `tree.says` alone), so words alike with trees apart are different regulations (the words, never node ids)
    const tree = quotaLine(v);
    return [v.s, normRule(v.t), v.n ? normRule(v.n) : null, tree ? normRule(lineText(tree)) : null];
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
  /** (T879) whose rules the bar is: DFO's, the rule book's, or both — the block's source tag */
  readonly by: SalmonSourceBy;
  /** the selected day: its status, mark ("✓ " / "✕" / none) and word ("Release only", "Check DFO", "To be determined" …) */
  readonly status: SalmonBarStatus;
  readonly mark: string;
  readonly word: string;
  /** the provincial bar's "today" / "on Oct 28", said after the word the same way */
  readonly when: string | null;
  /** the next change, as the game-fish bar says it ("Opens Nov 16" / "Closed from Nov 1"), else null */
  readonly soon: { readonly kind: "soonOpen" | "soon"; readonly text: string } | null;
  /** (T934) `gear`: the B.C. gear bits the run's days carry (fly only 4, artificial flies 8, bait ban 16; 0 none), on days
   *  salmon may be fished only (open / release): a closed day is the closed stripe */
  readonly runs: readonly { readonly drawn: SalmonDrawn; readonly days: number; readonly gear: number }[];
  /** the provincial bar's name, drawn above it when the salmon bar is shown */
  readonly generalTitle: string;
  /** (T934) a gear entry carries its B.C. bit (`bit`) and the game-fish key's own words ("Bait ban") */
  readonly legend: readonly { readonly drawn: SalmonDrawn; readonly label: string; readonly bit?: number }[];
}

/** (T934, owner 2026-11-20 "Bait ban needs to apply to salmon [too]. So would fly fish only") the B.C. gear bits of the
 *  provincial strip that hold for salmon fishing too: fly fishing only 4, artificial flies only 8, bait ban 16 */
export const SALMON_GEAR_BITS = 4 | 8 | 16;

const SOON_DAYS = 21;
const bookDayWords = (i: number) => fmtDay(BOOK_DAYS[i]!);   // book day i (no Feb 29) → "Nov 16"

/**
 * (T227, owner 2026-10-28: "exactly like the game-fish bar") The salmon bar beside the provincial year bar, or null where
 * the card has none. (T820, owner 2026-11-16 21:37) every card has one now: the unlisted water and the region-wide
 * closure draw the full closed bar, never the bare word. (owner 2026-11-13, Capilano River: "when salmon are closed like that is should
 * say the calendar widget like game fish instead maybe?") A water DFO speaks for whose salmon are closed all year draws
 * its bar too — closed, today marked, the legend — even beside a provincial bar closed all year (it was dropped there
 * as "the two say the same", which left a bare "Closed"). `general`: the provincial year bar (null where the page draws
 * none); `todayAt` its marker (book day / 365).
 */
export function salmonStripOf(card: SalmonCardVM, general: {
  readonly runs: readonly { readonly bits: number | null; readonly days?: number }[];
  readonly line: readonly { readonly kind: string; readonly text: string }[];
  readonly todayAt?: number;
  /** (T934) the provincial key, whose gear entries' words the salmon key repeats */
  readonly legend?: readonly { readonly bit: number; readonly mark: "✓" | "✕" | null; readonly label: string }[] } | null,
): SalmonStripVM | null {
  const bar = card.year.bars[0];
  if (!bar) return null;
  const days = bar.runs.flatMap((r) => Array.from({ length: r.days }, () => DRAWN[r.status]));
  // (T934) the provincial strip's gear bits of the same book days; a run with no day count carries none
  const bits = general?.runs.every((r) => r.days !== undefined)
    ? general.runs.flatMap((r) => Array.from({ length: r.days! }, () => (r.bits ?? 0) & SALMON_GEAR_BITS)) : [];
  if (bits.length && bits.length !== days.length)
    throw new Error(`salmonStripOf: the provincial strip has ${bits.length} days, the salmon bar ${days.length}`);
  const runs: { drawn: SalmonDrawn; days: number; gear: number }[] = [];
  days.forEach((dr, i) => {
    // a release day draws the release stripes, which the fly marks would sit under (as on the game-fish bar): the bait band only
    const gear = dr === "open" ? bits[i] ?? 0 : dr === "release" ? (bits[i] ?? 0) & 16 : 0, last = runs[runs.length - 1];
    if (last && last.drawn === dr && last.gear === gear) last.days++; else runs.push({ drawn: dr, days: 1, gear });
  });
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
    // (T796) a bar some of whose days are the rule book's closure is not named DFO's alone
    title: ({ dfo: SALMON_WORDS.barSalmon, both: SALMON_WORDS.barSalmonBoth, book: SALMON_WORDS.barSalmonBook } satisfies
      Record<SalmonSourceBy, string>)[card.year.by],
    by: card.year.by,
    status: bar.today,
    mark: bar.today === "open" || bar.today === "release" ? "✓ " : bar.today === "closed" ? "✕" : "",
    word: bar.today === "check" && bar.todayTbd ? SALMON_WORDS.tbd.status : SALMON_WORDS.status[bar.today],
    when: general?.line.find((l) => l.kind === "when")?.text ?? null,
    soon, runs,
    generalTitle: SALMON_WORDS.barGeneral,
    legend: [
      ...(["open", "release", "unknown", "closed"] as const).filter((d) => present.has(d))
        .map((d) => ({ drawn: d, label: d === "unknown" && bar.keyTbd ? SALMON_WORDS.tbd.key : SALMON_WORDS.barLegend[d] })),
      // (T934) each B.C. gear mark the bar draws, in the game-fish key's words, after the salmon statuses
      ...[4, 8, 16].filter((b) => runs.some((r) => r.gear & b)).map((b) => {
        const l = general?.legend?.find((x) => x.bit === b);
        if (!l) throw new Error(`salmonStripOf: the provincial key has no entry for gear bit ${b}`);
        return { drawn: "open" as const, bit: b, label: legendText(l) };
      }),
    ],
  };
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

const EMPTY_GEAR: GearVM = { closed: null, closedMore: [], day: null, mode: null, lines: [], bait: null, src: null };

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
  // (T372, the trout way) the drawn answer's gear; an undrawn place's is its caveat item's detail
  const dfo = card ? [...new Set(card.gear)] : [];
  const salmonGear = salmonOpen && dfo.length ? { lead: SALMON_WORDS.gearDfoLead, lines: dfo } : null;
  const gameOpen = !!page.gear && page.gear.closed === null && !page.gear.closedMore.length;
  if (gameOpen) return { gear: page.gear, methods: page.methods, salmonGear };
  if (salmonOpen) return { gear: anyway?.gear ?? EMPTY_GEAR, methods: anyway?.methods ?? null,
                           salmonGear: { lead: SALMON_WORDS.gearSalmonOnly, lines: dfo } };
  return { gear: null, methods: null, salmonGear: null };
}


// ── (T318) THE SALMON TABLE ──────────────────────────────────────────────────────────────────────────────────────────

/** One species of the table: today's status and its sizes line (DFO's header), null when the header sets none. */
export interface SalmonTableRowVM {
  readonly code: SalmonSpecies;
  readonly name: string;
  readonly status: SalmonVerdictStatus | "varies";
  readonly statusLabel: string;
  readonly limit: string | null;
  /** (T320) the species' numbers today, words: "4 a day · only 2 over 50 cm" (null: none to say, or `quotaUnread`) —
   *  `quotaRuns`' words */
  readonly quota?: string | null;
  /** (T332) the same line as the trout presenter draws a quota tree (`quotaLine`: tierWords / kindWords, numbers bold) */
  readonly quotaRuns?: Line | null;
  /** (T320) DFO's row could not be read as one number: its words are under the table, no number is shown */
  readonly quotaUnread?: boolean;
  /** (T502) the dates of the verdict the quota line reads ("Sep 1–Dec 31"), under it */
  readonly window?: string;
  /** (T668 fix 4, owner 2026-11-11) where today's answer comes from, as DFO's page names the place: "Region 6 · Skeena
   *  River · B(ii)" (absent: no place answers, or the release names none) */
  readonly from?: string;
}
/** (T320) The bucket line over the species rows: "Up to 4 salmon a day in total · 8 in possession", DFO's words;
 *  (T332) `runs` the line as the trout presenter draws the bucket's total tier. */
export interface SalmonBucketVM { readonly line: string; readonly words: readonly string[]; readonly runs?: Line;
  /** (T671 D3) the bucket's other limits, one line each, in DFO's words (the tree's child tiers: the chinook annual
   *  limit of 10 over 50 cm, Region 8's monthly 4) — those whose species may be kept today */
  readonly tiers?: readonly { readonly line: string; readonly runs: Line }[] }
/** DFO's own words for a group of species, said ONCE (a row grouping species alike), under the table. */
export interface SalmonSaysVM { readonly label: string | null; readonly lines: readonly string[] }
export interface SalmonTableVM {
  /** (T320) the bucket total over the rows (shown when a species may be kept), else null */
  readonly bucket?: SalmonBucketVM | null;
  /** (T320) a water system's own combined limit (Squamish), while that species is open here */
  readonly waterBuckets?: readonly SalmonBucketVM[];
  /** one row per salmon species, SALMON_SPECIES order */
  readonly species: readonly SalmonTableRowVM[];
  readonly says: readonly SalmonSaysVM[];
}

/** A Line's words, as drawn (no space invented, the store's U+00A0 kept). */
export const lineText = (l: Line): string => l.map((r) => ("s" in r ? r.s : "")).join("");

/** Every node of a quota tree, the tier first (pre-order): `pipeline/regs/dfo_salmon/quota.walk`. */
function* walkTree(t: TreeTier): Generator<TreeTier | TreeKind> {
  yield t;
  for (const c of t.children) if (c.node === "tier") yield* walkTree(c); else yield c;
}

/**
 * (T332) A species' quota tree drawn by THE TROUT PRESENTER: each tier by `tierWords`, each kind the tier does not
 * already say (not `folded`) by `kindWords` — the same functions the trout card's quota tree is drawn with — one line,
 * "4 a day · only 2 over 50 cm". Null when there is nothing to say (or `u`: DFO's words stand instead). Decides
 * nothing: every count and word is the store's (`tree`). No per-species possession (owner 2026-10-31).
 */
export function quotaLine(q: SalmonQuota | null | undefined): Line | null {
  if (!q || q.u || !q.tree) return null;
  const parts = [...walkTree(q.tree)].filter((x) => x.node === "tier" || !x.folded)
    .map((x) => (x.node === "tier" ? tierWords(x) : kindWords(x.says, x.n)));
  return parts.flatMap((p, i) => (i ? [text(SALMON_WORDS.quota.sep), ...p] : [...p]));
}
const tableRows = (rows: readonly SalmonRowVM[]): SalmonTableRowVM[] =>
  SALMON_SPECIES.flatMap((code) => {
    const r = rows.find((x) => x.species.includes(code));
    if (!r) return [];
    const q = r.status === "open" ? r.quota?.[code] : undefined;
    const runs = quotaLine(q);
    return [{ code, name: cap(SPECIES_NAME[code]), status: r.status, statusLabel: r.statusLabel,
              limit: r.sizesBy?.[code] ?? (r.species.length === 1 ? r.sizes ?? null : null),
              quota: runs ? lineText(runs) : null, quotaRuns: runs, ...(q?.u ? { quotaUnread: true } : {}),
              // (T502) the dates DFO's repeated words gave, now that those words are not said under the table
              ...(runs && r.window ? { window: r.window } : {}),
              ...(r.fromBy?.[code] ? { from: r.fromBy[code] } : {}) }];
  });
/** (T320) the bucket line, when a species of the table may be kept today: its total tier by the trout presenter */
const bucketOf = (b: SalmonBucket | null | undefined, rows: readonly SalmonRowVM[], adultHere: string | null = null): SalmonBucketVM | null => {
  if (!b || !rows.some((r) => r.status === "open")) return null;
  const runs: Line = [...tierWords(b.tree),
                      ...(b.possession ? [text(SALMON_WORDS.quota.sep + SALMON_WORDS.quota.possession(b.possession))] : [])];
  // (T671 D3) the tree's child tiers (period limits) were drawn nowhere: each is a line under the total while a species
  // it names may be kept today (one naming none: whenever the bucket shows). The words are the store's (`says`).
  const kept = new Set<string>(rows.filter((r) => r.status === "open").flatMap((r) => r.species));
  const tiers = b.tree.children.filter((c): c is TreeTier => c.node === "tier")
    .filter((c) => !c.species?.length || c.species.some((x) => kept.has(x)))
    // (T869) the annual chinook limit says where an adult is another length here (`adultHere`)
    .map((c) => {
      const w = tierWords(c);
      const r = adultHere && c.period === "annual" && c.species?.includes("CH") ? [...w, text(` ${adultHere}`)] : w;
      return { line: lineText(r), runs: r };
    });
  return { line: lineText(runs), runs, words: [b.t, ...(b.tp ? [b.tp] : [])], ...(tiers.length ? { tiers } : {}) };
};
const tableSays = (rows: readonly SalmonRowVM[], allClosed: boolean): SalmonSaysVM[] =>
  rows.map((r) => ({ label: allClosed || rows.length === 1 ? null : r.label, lines: [...r.notes.filter((n) => !r.repeats?.includes(n)), ...(r.species.length > 1 && r.sizes && !r.sizesBy ? [r.sizes] : []), ...(r.speciesNotes ?? [])] }))
    .filter((x) => x.lines.length);

/**
 * (T318, owner 2026-10-31) THE CARD AS A TABLE: one row per species with today's status and sizes, DFO's words for the
 * rows said once under it (never repeated per picture). (T372) The DFO places on the water not drawn yet are the card's
 * caveat (`caveat`), not the table's. The same facts as `salmonCard`, re-shaped: `salmonTableTexts` lists every string
 * the rows hold, and a test pins that all of them are still here.
 */
export function salmonTable(card: SalmonCardVM): SalmonTableVM {
  const b = card.bucket ?? null;
  return {
    bucket: card.allClosed ? null : bucketOf(b, card.rows, card.adultHere ?? null),
    waterBuckets: card.allClosed ? [] : card.waterBuckets ?? [],
    species: tableRows(card.rows),
    says: tableSays(card.rows, card.allClosed),
  };
}

/** Every DFO string a card holds on a water's rows (rows' words, sizes, species notes), (T770) another fish's closure
 *  lines and its gear; the undrawn places' are the caveat's (`caveat.items[].detail`). */
export function salmonRowTexts(card: SalmonCardVM): string[] {
  const of = (rows: readonly SalmonRowVM[]) => rows.flatMap((r) => [...r.notes, ...(r.sizes ? [r.sizes] : []), ...(r.speciesNotes ?? [])]);
  return [...of(card.rows), ...card.closures, ...card.gear];
}

/** Every string the table can show (its rows' limits, the words under it). */
export function salmonTableTexts(t: SalmonTableVM, gear: readonly string[] = []): string[] {
  const species = (rs: readonly SalmonTableRowVM[]) => rs.flatMap((r) => [...(r.limit ? [r.limit] : []), ...(r.quota ? [r.quota] : []),
                                                                           ...(r.window ? [r.window] : [])]);
  const says = (xs: readonly SalmonSaysVM[]) => xs.flatMap((x) => x.lines);
  return [...(t.bucket ? [t.bucket.line, ...(t.bucket.tiers ?? []).map((x) => x.line)] : []), ...(t.waterBuckets ?? []).map((w) => w.line), ...species(t.species), ...says(t.says), ...gear];
}
