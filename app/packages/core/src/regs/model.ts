/**
 * The Regulations page view model — `RegsPage` and everything below it (contracts.md §2). Components draw these and
 * never compute: every finished sentence is here, pinned by a core test against the goldens
 * (app/design/regs-v11/port/golden/golden.json, regs v38 @ 7fb6809f).
 *
 * Later tickets add fields where the contract marks them (T28 `GroupVM.pinLine`), and the functions declared beside
 * their types (T33 `licenceBlockLines`, T36 `sheetOf`/`sheetLines`).
 */
import type { Line, RuleKey } from "./line";
import type { SheetKey } from "./sheetKey";
import type { RegsInputs } from "./inputs";
import type { Weekday } from "../calendar.generated";

export type { Weekday } from "../calendar.generated";
/** The spec's weekday names, Monday first (THE CALENDAR SPEC). */
export { WEEKDAYS } from "../calendar.generated";
/** The calendar day; the year only names its weekday. */
export interface Day { readonly month: number; readonly day: number }
export interface Who { readonly age: "adult" | "u16"; readonly residency: "resident" | "non_resident" | "non_resident_alien" }
export type RegsTarget = { kind: "part"; item: string; partIx: number } | { kind: "section"; handle: number };
export type Tab = "keep" | "gear" | "methods" | "licence";

export interface RegsPage {
  readonly version: "regs-v38";             // the port target: golden meta.version @ 7fb6809f
  readonly target: RegsTarget;
  readonly day: Day; readonly weekday: Weekday; readonly today: boolean; readonly who: Who;
  readonly status: "open" | "closed" | "tidal" | "outside" | "unknown";  // header dot + strip word; NEVER from the status index
  readonly header: HeaderVM;
  readonly keep: KeepVM;
  readonly gear: GearVM;
  readonly methods: MethodsVM;
  readonly licence: LicenceVM | null;      // null: outside B.C. (no licence section; tab hidden)
  readonly sheet: (key: SheetKey) => SheetVM | null;   // lazy; pure over the same inputs
}

/** The whole page for one tap (T21). `pid`: test-harness page id only — sheet keys never embed it (contracts §6 #1). */
export function buildRegsPage(_inp: RegsInputs, _opts: { today: Day; who: Who; tot?: "a" | "b"; pid?: string }): RegsPage {
  throw new Error("buildRegsPage: T21");
}

// ── 2.1 Header (prototype hdr, placeRow, placeGo, shortLabel, hereLabel, fitLabel) ──────────────────────────────────
export interface HeaderVM {
  readonly name: string;                    // item.name (or place.names[kind] for an unnamed section)
  readonly dayWord: string;                 // "Today" when day==today, else weekday short (WD3)       golden header.day
  readonly date: string;                    // golden header.date
  readonly dateAria: string;                // golden header.dateAria
  readonly dot: "open" | "closed" | "faint";// status colour of the pill dot                         golden header.dot
  readonly place: { full: string; short: string } | null;  // two candidates; the component shows full if ≤2 lines else short (fitLabel)
  readonly action: { kind: "map" | "parts"; label: "Map" | "Parts"; aria: string } | null;   // golden header.action
  readonly tabs: readonly { tab: Tab; label: string }[];   // golden header.tabs
}

// ── 2.2 Keep tab (prototype buildKeep) ─────────────────────────────────────────────────────────────────────────────
export interface KeepVM {
  readonly order: readonly KeepBlock[];     // the draw order, = golden keep.order
  readonly strip: StripVM | null;
  readonly inherited: InheritedVM | null;
  readonly banner: BannerVM | null;         // only on a closed day
  readonly groups: readonly GroupVM[];      // every article.row.ledger.g28 (a row with group.headline)
  readonly plainRows: readonly PlainRowVM[];// a row page v36 draws without a card
  readonly otherFish: readonly OtherFishRowVM[] | null;
  readonly caveats: readonly CaveatVM[];    // top caveat blocks, in draw order (T15b)
  readonly possession: PossessionVM | null; // (T15)
  readonly whereNot: WhereNotVM | null;
  readonly outside: string | null;          // words.OUTSIDE_BC (page v36 renderCard foot) (T42)
}
export type KeepBlock = "strip" | "inherited" | "banner" | { caveat: number } | { group: number } | { plain: number } | "otherFish" | "possession" | "whereNot"
  | "outside";
export interface PossessionVM { readonly label: string; readonly value: string; readonly says: string; readonly rule: RuleKey; readonly term: "possession_quota"; readonly open: SheetKey }

/** page v36 waterStrip0 + prototype flagBar / flagLegend / snap runsOf */
export interface StripVM {
  readonly status: "open" | "closed";        // golden keep.strip.statusClass (tidal/outside → StripVM null)
  readonly line: readonly { kind: "status" | "when" | "soon" | "soonOpen"; text: string }[];
  readonly runs: readonly { bits: number | null; days: number }[];   // per-day display_frame.strip over the VIEWED year, run-length (golden bar[].flex)
  readonly todayAt: number;                 // bookIndex(day)/365 → the marker
  readonly months: readonly string[];       // words.MONTH1
  readonly legend: readonly { bit: number; mark: "✓" | "✕" | null; label: string }[];   // golden text = (mark ? mark + " " : "") + label
}

/** page v36 inheritedHtml */
export interface InheritedVM { readonly label: string; readonly lines: readonly { says: Line; caution: string | null }[] }

/** page v36 closersAt / closedBanner; prototype dedupe */
export interface BannerVM {
  readonly kind: "closed" | "tidal";
  readonly big: readonly string[];          // headline phrases left after "say each sentence once" (often empty)
  readonly paras: readonly string[];
  readonly chain: readonly { text: string; scope: string; rule: RuleKey; book: Line }[];  // .closechain li
  readonly reopen: { text: string; day: Day } | null;  // "See the rules from then ›" → sets the day
  readonly merged: { label: string; items: readonly { text: string; level: string | null; rule: RuleKey | null; book: Line }[] } | null;   // page v36 mergedHtml
}

/** prototype per-article block; page v36 cardBlock / treeBlock / rowTitle / scopeBadge */
export interface GroupVM {
  readonly row: number;                     // index into rows frame rows[]
  readonly title: string;                   // page v36 rowTitle(row)
  readonly scope: string | null;            // head kicker, null when moved to the share (moveTag)
  readonly head: GroupHeadVM;
  readonly origin: OriginVM | null;         // the .ghd.gsub block
  readonly tree: readonly TreeNodeVM[] | null;   // root's children when header.tier === tree[0].id, else tree[]
  readonly notKept: readonly NotKeptVM[];
  readonly worked: { label: string; says: string | null; lines: readonly { at: "here" | "elsewhere"; q: string; a: string }[] } | null;   // q carries ":" for at=here
  readonly quiet: Line | null;              // page v36 noRulesLine (STEEL_LINE) + ⓘ steelhead
  readonly more: { text: string; open: SheetKey };   // "See every regulation line ›" → src:r<i>
}
export interface GroupHeadVM {
  readonly form: "a" | "b" | null;          // a REGION total over a stream share; null otherwise
  readonly rootTier: string | null;         // header.tier id
  readonly lead: Line | null;               // form a: headline words with its n big
  readonly leadN: number | null;            // the share lead's number; golden head.leadN is its string form
  readonly words: Line;                     // the root tier's words, its n big
  readonly ofTotal: boolean;                // form a: words prefixed by words.OF_THE
  readonly whyHere: string | null;          // root why_here (cap1)
  readonly shareOf: Line | null;            // form b only: "↳ " + shortOf(relTier)
  readonly terms: readonly string[];        // the daily_quota ⓘ
  readonly nolimit: boolean;
}
export interface OriginVM { readonly says: Line; readonly tag: string | null; readonly howtell: boolean; readonly book: Line }

/** prototype tnode / tierH / tierWords / kt */
export type TreeNodeVM = TierVM | KindVM | ReleaseRowVM;
export interface TierVM {
  readonly t: "tier"; readonly tier: "total" | "own" | "share" | "count" | "size"; readonly tid: string;
  readonly kicker: string | null;           // the moved scope tag over the share (tagShare)
  readonly line: Line;                      // tierWords: boldN(says, n) + size extra's 2nd number "big2" + root ⓘ + book
  readonly sub: string | null;              // own tier: cap1(why_here)
  readonly origin: OriginVM | null;         // tier.origin with How to tell and the root's book when group.origin is null
  readonly band: boolean;                   // tier === "size": the tinted SizeBand wraps this node
  readonly short: string;                   // shortOf(tier) for the pin bar (T28)
  readonly children: readonly TreeNodeVM[];
}
export interface KindVM {
  readonly t: "kind"; readonly kind: string; readonly species: readonly string[];
  readonly folded: boolean; readonly zero: boolean;
  readonly says: Line | null;               // null when folded; boldN(says, n) (no bold when n===0); + "How to tell ›" for hatchery-only steelhead
  readonly size: { text: string; rule: boolean } | null;   // the chip (nbsp'd); rule: the bold size_rule chip
  readonly qualifiers: string | null;       // sizeOf(detail).q joined ", "
  readonly pictures: readonly FishPicVM[];  // FIRST after the sentence + chip
  readonly also: readonly string[];         // also_counts[].says (cap1)
  readonly duty: Line | null;               // duties.map(dutyWords).join(" · ") + link "Steelhead stamp ›" {tab:"licence"}
  readonly presence: Line | null;           // presOf(k).says + ⓘ steelhead
}
export interface ReleaseRowVM { readonly t: "release"; readonly names: string; readonly pictures: readonly FishPicVM[] }
/** caption "Brook trout ›"; sil: no image. name: fishName(c) — a11y "What a <name> looks like". */
export type FishPicVM = { readonly code: string; readonly name: string; readonly caption: string; readonly open: SheetKey; readonly sil: false } | { readonly code: string; readonly name: string; readonly sil: true };
export interface NotKeptVM { readonly status: "closed" | "release"; readonly says: string; readonly whose: Line; readonly mark: "✕" | null; readonly presence: Line | null; readonly pictures: readonly FishPicVM[] }
export interface PlainRowVM {                                         // golden keep.plainRows (T19b)
  readonly row: number; readonly title: string;
  readonly pill: { kind: "release" | "closed"; text: string; term: string } | null;
  readonly scope: string | null; readonly sum: string | null;
  readonly week: { status: string | null; says: string; parts: readonly { days: string; says: string; today: boolean; book: Line }[]; notes: readonly Line[] } | null;   // (T19b) + status, notes (page v36 weekHtml)
  readonly presence: Line | null; readonly names: string | null;
  readonly whichFish: string | null;        // (T19b) the folded "Which fish (N)" label when the names are > 5, else null
  readonly release: { names: string; pictures: readonly FishPicVM[] } | null;   // prototype krel28
  readonly ladder: LadderVM; readonly more: { text: string; open: SheetKey };
}
export interface LadderVM {                                           // "How this was decided" (page v36 ladderHtml)
  readonly label: string;
  readonly sections: readonly { q: string | null; result: string | null; steps: readonly LadderStepVM[] }[];   // q null: the members form (T36's other: sheet)
  readonly lost: readonly { text: string; status: string; rule: RuleKey; book: Line }[];
}
export interface CaveatVM {                                           // page v36 inPartHtml / sideHtml / anglerClosures / timedHtml / uncertainHtml (T15b)
  readonly kind: "inpart" | "side" | "angler" | "timed" | "uncertain"; readonly hot: boolean;
  readonly title: string; readonly calm: string | null;
  readonly items: readonly CaveatItemVM[]; readonly later: readonly CaveatItemVM[] | null;
}
export interface CaveatItemVM { readonly what: Line; readonly where: string | null; readonly rule: RuleKey | null; readonly fish: string | null; readonly book: Line }

export interface OtherFishRowVM { readonly thumb: string | null; readonly name: string; readonly value: string;
  readonly tone: "keep" | "release" | "closed" | "nolimit";   // decided in core, never the component
  readonly week: string | null; readonly sizeHint: string | null; readonly open: SheetKey }   // open = `other:o${i}`
export interface WhereNotVM { readonly heading: string; readonly scope: string | null; readonly items: readonly { text: Line; rule: RuleKey }[] }

// ── 2.3 Gear tab (prototype buildGear; page v36 gearParts) ─────────────────────────────────────────────────────────
export interface GearVM {
  readonly closed: string | null;           // closedUntil() — then nothing else
  readonly day: string | null;              // dayLine() (golden gear.day; null on a closed day)
  readonly mode: { value: string; terms: readonly string[]; changes: readonly ChangeVM[]; src: SourceLineVM | null } | null;
  readonly lines: readonly GearLineVM[];    // Lines · Hooks · Weight on the line, only parts the summary sentence names
  readonly bait: BaitVM | null;
}
export interface GearLineVM { readonly k: "lines" | "hook" | "weight"; readonly label: string; readonly values: readonly string[]; readonly terms: readonly string[]; readonly changes: readonly ChangeVM[]; readonly src: SourceLineVM | null }
export interface ChangeVM { readonly chip: "Now" | "Next"; readonly when: string; readonly says: string; readonly inForce: boolean }
export interface SourceLineVM {
  readonly who: string;                     // includes the trailing " ›" exactly as golden
  readonly aria: string;
  readonly open: SheetKey;                  // srcs:…
  readonly keys: readonly RuleKey[];        // feeds the srcs: sheet
  readonly pages: readonly { printed: number; pdf: number }[];
}
export interface BaitVM { readonly label: string; readonly notes: readonly Line[];
  readonly dated: readonly ChangeVM[]; readonly list: YesNoVM; readonly src: SourceLineVM | null }
export type YesNoVM = readonly { col: "ok" | "no"; head: "Allowed" | "Not allowed"; items: readonly { mark: "✓" | "✕"; name: string; note: string | null }[] }[];

// ── 2.4 Methods tab ────────────────────────────────────────────────────────────────────────────────────────────────
export interface MethodsVM {
  readonly closed: string | null;
  readonly ways: { label: string; list: YesNoVM; src: SourceLineVM | null } | null;
  readonly boats: { label: string; items: readonly string[]; src: SourceLineVM | null } | null;
  readonly reference: { label: "Reference"; title: string; sub: string; groups: readonly { head: string; items: readonly string[] }[]; src: SourceLineVM | null } | null;
}

// ── 2.5 Licence tab (prototype licence(); page v36 licParts) ────────────────────────────────────────────────────────
export interface LicenceVM {
  readonly question: string | null; readonly questionTerms: readonly string[];
  readonly axes: readonly { axis: "age" | "res"; aria: string; options: readonly { value: string; text: string; checked: boolean }[] }[];
  readonly label: string;                   // static licence.profile_labels[profileIx]
  readonly blocks: readonly LicenceBlockVM[];          // draw order
  readonly src: { who: string; aria: string; open: SheetKey } | null;   // open = `licsrc:${profileKeyOf(who)}`
}
export interface NeedItemVM { readonly name: string; readonly terms: readonly string[]; readonly when: string; readonly price: string | null }
export type LicenceBlockVM =
  | { cls: "classbox"; head: string; terms: readonly string[]; body: string; source: SheetKey | null }
  | { cls: "classbox youth"; head: string; terms: readonly string[]; body: string }
  | { cls: "flag"; text: string } | { cls: "scopenote"; text: string; source: SheetKey | null }
  | { cls: "needs"; label: string; items: readonly NeedItemVM[]; onlyIf: { label: string; items: readonly NeedItemVM[] } | null; priceNote: string | null }
  | { cls: "paper"; lead: string; strong: string; tail: string }
  | { cls: "nymbox"; label: string; says: string; note: string }
  | { cls: "muted"; text: string }
  | { cls: "lostlist lopen"; label: string; cards: readonly { forWho: string; label: string; paths: readonly string[] }[] };

// ── 2.6 Sheets ─────────────────────────────────────────────────────────────────────────────────────────────────────
export interface SheetVM { readonly key: SheetKey; readonly title: string; readonly sub: string | null; readonly blocks: readonly SheetBlock[] }
export type SheetBlock =
  | { k: "para"; text: Line; tone?: "plain" | "small" | "note" }        // note = "⚑ …" rule notes
  | { k: "example"; text: Line }
  | { k: "head"; text: string }
  | { k: "quote"; text: string }
  | { k: "list"; items: readonly Line[]; ordered?: false }
  | { k: "fold"; summary: string; body: readonly SheetBlock[]; open: false }   // ALWAYS closed by default
  | { k: "term"; id: string; title: string; body: readonly SheetBlock[] }
  | { k: "rule"; title: string; level: string; applies: readonly string[]; book: Line; open: SheetKey }
  | { k: "value"; text: Line }
  | { k: "weekBox"; day: string; scope: string | null; status: string; says: string }
  | { k: "ladder"; q: string | null; result: string | null; steps: readonly LadderStepVM[] }
  | { k: "sources"; groups: readonly { where: string; rows: readonly SourceRowVM[] }[] }
  | { k: "pictures"; pics: readonly FishPicVM[] }
  | { k: "image"; code: string; file: string; alt: string; credit: string; width: number; height: number }
  | { k: "adipose"; figures: readonly { variant: "wild" | "clipped"; caption: Line }[] }
  | { k: "pages"; line: Line }
  | { k: "source"; line: Line }
  | { k: "link"; line: Line }
  | { k: "choice"; items: readonly { text: string; sub: string | null; status: string; long: string | null; checked: boolean; part: number }[] };
export interface LadderStepVM { readonly level: string;
  readonly lines: readonly { text: Line; tag: string | null; rule: RuleKey | null; won: boolean; applies: readonly string[]; book: Line; today?: boolean; days?: string;
    notes?: readonly string[] }[] }   // (T19b) notes: the data's restates notes under a winning line (prototype buildKeep rnote)
export interface SourceRowVM { readonly key: RuleKey | null; readonly title: string | null; readonly quote: string; readonly role: string; readonly book: Line }
export interface SheetCtx { readonly inp: RegsInputs; readonly page: Omit<RegsPage, "sheet">; readonly today: Day }
