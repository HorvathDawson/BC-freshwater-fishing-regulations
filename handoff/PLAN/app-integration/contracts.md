# contracts.md — the RegsPage view model and where every field comes from

Target: regs **v33** (`app/design/regs-v11.html` @ `04765ab5`, UI-NOTES v33), answers sections ladder 1 · rows 26 ·
gear 12 · licence 6 · display 16 · glossary 5, store `regs-store/4`. The golden file that pins every string in this model
is `app/design/regs-v11/port/golden/golden.json` (generator `port/golden.mjs`); its per-state shape mirrors this model
on purpose, so a parity test is `plain(vm.x) === golden.states[id].x`.

Three layers, three type families:

| layer | file | types | who builds them |
|---|---|---|---|
| store wire (decoded blobs) | `app/packages/core/src/answers.generated.ts` (exists, generated) | `RowsValue`, `GearAnswer`, `StatusFrame`, `LicenceValue`, `RuleFacts`, `PartFacts`, `Picker`, `Place`, `Moment`, `GlossaryTerm`, `StripState`, … | Python (`pipeline.deliver.answers.model`) |
| inputs (one tap, hydrated) | `app/packages/core/src/regs/inputs.ts` (new) | `RegsInputs`, `RegsYear`, `Statics`, `RuleView`, `RuleIndex`, `CellIds`, `LadderFrameWire`, `DisplayFrame`, `Settled`, `CardModel` | `@app/data` `loadRegsInputs()` (T07) + core `hydrate()` (T06) |
| view model (what components draw) | `app/packages/core/src/regs/model.ts` (new) | `RegsPage` and everything below | core `buildRegsPage()` (T21) |

`app/packages/core/src/regulations.ts` is now only `export * from "./regs"`; its old exports (`RegulationsAvailability`, `REGULATIONS`,
`RegulationRecord`, `WaterRegulations`, `regulationsFor`) are removed (T00). `regs/versions.ts` holds `REGS_FORMAT = "regs-store/4"`,
`KNOWN_VERSIONS = {ladder 1, rows 26, gear 12, licence 6, display 16, glossary 5}`, `SectionName` (T03). `@app/core/regs/testing`
is a subpath export (types + eq, no I/O), never re-exported from the core index (T12).

Rule 22–25/33 (AGENTS): components never compute; every finished sentence is in `RegsPage` and pinned by a core test.

---

## 1. Rich text — `Line`

Every visible text in the prototype is a sentence that can carry: a number set big (`b.tn`, `b.ghn`, `b.kn`, `.gln`), a
glossary ⓘ (`button.gl` / `.glk`), a Rule book link (`a.book`), an in-line link button (`How to tell ›`, `Source ›`,
`See the rules from then ›`), a non-breaking "50 cm". The app models it once:

```ts
// app/packages/core/src/regs/sheetKey.ts — the app's keys carry NO state id (a RegsPage is one state)   // (T36)
export type SheetKey =
  | `rule:${RuleKey}` | `gl:${string}`            // gl: one id, or ids joined by "," (multi-term sheet)   // (T36)
  | `sp:${string}` | "hatch" | "parts"
  | `srcs:${"bait" | "ways" | "always" | "boats"}` | `srcs:gl|${"mode" | "lines" | "hook" | "weight"}`   // (T36)
  | `licsrc:${ProfileKey}` | `other:o${number}` | "pos:pos" | `src:r${number}`;   // (T36)
export type ProfileKey = "adult" | "adult_non" | "adult_alien" | "u16_res" | "u16_non" | "u16_alien";   // (T36)
export function parseSheetKey(k: string): SheetKey | null;             // refuses unknown shapes   // (T36)
/** golden ⇄ app (test-only use; T37 deep links and the parity harness agree on it) */
export function appKeyOf(goldenKey: string, sid: string): SheetKey;     // "other:vedder_1010|o0" → "other:o0"; "srcs:gl|vedder_1010|hook" → "srcs:gl|hook"; "licsrc:lic|vedder_1010|u16_res" → "licsrc:u16_res"   // (T36)
export function goldenKeyOf(k: SheetKey, sid: string): string;         // the inverse; parity tests compare goldenKeyOf(vm.open, id) === golden open   // (T36)
export const profileKeyOf: (who: Who) => ProfileKey;                    // {u16, non_resident} → "u16_non"   // (T36)

// app/packages/core/src/regs/line.ts
export type RuleKey = string;            // `${entry_id}::${rule_id}` — the export rule's own ids (store `rule.j`)
export type Run =
  | { t: "text"; s: string }             // data words or a words.ts constant; "50 cm" already carries U+00A0 (nbsp())
  | { t: "num"; s: string; size: "big" | "big2" | "inline" }  // "big": tier/head number (TYPE.answer); "big2": the size box's second number
  | { t: "term"; ids: readonly string[] }                    // ⓘ — one id: "What does <title> mean?"; 2+: "What these words mean"
  | { t: "book"; printed: number; pdf: number | null; bare?: true }   // "Rule book p.<printed> ↗" (label from words.ts BOOK); bare: "p.<printed> ↗" (glossary pages under a "Rule book" label, prototype pageLink 935); pdf null → "p.<printed>" unlinked   // (T36)
  | { t: "link"; s: string; open: SheetKey | { day: Day } | { tab: Tab } };  // "How to tell ›", "Source ›", "See the rules from then ›"; {tab}: "Steelhead stamp ›" (T18)
export type Line = readonly Run[];
export function text(s: string): Run;                                  // { t: "text", s }   // (T00)
export function plain(l: Line | null | undefined): string;   // the golden comparison form (see §5): runs concatenated AS IS, then collapse + trim
export function plainAll(ls: readonly (Line | null | undefined)[]): string;   // separate Lines/fields joined with ONE space, then collapse (golden gap())   // (T00)
export function squash(s: string): string;                            // s.replace(/\s+/g, " ").trim()   // (T00)
export function nbsp(s: string): string;                      // /(\d) (cm|in)\b/ -> "$1 $2"  (prototype nbsp, regs-v11.html buildKeep.nbsp)
export function boldN(s: string, n: number | null, size?: "big" | "inline"): Line; // first \b<n>\b as a num run (buildKeep.boldN)
```

`plain()` reproduces the golden text form: **within one Line, runs are concatenated as is** (a presenter writes the spaces a
sentence has into its text runs: `"Up to "`, num `"2"`, `" may be …"`); `num` → its digits; `term` → `"ⓘ"` (one id) or
`"What these words mean"` (2+); `book` → `"Rule book p.<printed> ↗"` (`bare`: `"p.<printed> ↗"`; `pdf` null: `"p.<printed>"`); `link` → its `s`. **Between two Lines / fields that the
page draws as separate elements, the golden has exactly one space** (golden.mjs `gap()`: a space wherever two elements
abut — `"2 a day per angler from streams" + " " + "of the 4 …"`, `"Kokanee" + " " + "release"`, a sentence + `" "` + its
`"Rule book p.21 ↗"`). Then `/\s+/g → " "`, trim. The test helper `eqPlain(vm, golden)` applies exactly this; a VM field
made of several Lines is compared as `lines.map(plain).join(" ")`. (Goldens before commit `b999cfd7` used raw `textContent` and fused such fields; any ticket that quotes a fused string
is stale — `port/quotes.mjs` finds them.)
---

## 2. `RegsPage`

```ts
// app/packages/core/src/regs/model.ts
export interface Day { readonly month: number; readonly day: number }            // the calendar day; year only for the weekday
export type { Weekday } from "../calendar.generated";   // REUSE the generated type ("Monday" … "Sunday"), never a second literal union   // (T00)
export interface Who { readonly age: "adult" | "u16"; readonly residency: "resident" | "non_resident" | "non_resident_alien" }
export type RegsTarget = { kind: "part"; item: string; partIx: number } | { kind: "section"; handle: number };
export type Tab = "keep" | "gear" | "methods" | "licence";

export interface RegsPage {
  readonly version: "regs-v38";             // (T00) target 7fb6809f = golden meta.ref (IMPLEMENT-HANDOFF §8 C1)
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
export function buildRegsPage(inp: RegsInputs, opts: { today: Day; who: Who; tot?: "a" | "b"; pid?: string }): RegsPage;   // (T30) pid: test-harness page id only — sheet keys never embed it (§6 #1)
```

### 2.1 Header — `HeaderVM` (prototype `hdr()` 1005, `placeRow` 994, `placeGo` 996, `shortLabel` 985, `hereLabel` 977, `fitLabel` 1001)

```ts
export interface HeaderVM {
  readonly name: string;                    // st.name ← store item.name (or place.names[kind] for an unnamed section)
  readonly dayWord: string;                 // "Today" when day==today, else weekday short "Fri" (words.WD3)   golden header.day
  readonly date: string;                    // "Oct 10" (words.MON3)                                           golden header.date
  readonly dateAria: string;                // "Change date, now Oct 10"                                       golden header.dateAria
  readonly dot: "open" | "closed" | "faint";// status colour of the pill dot                                   golden header.dot
  readonly place: { full: string; short: string } | null;  // two candidates; the component shows full if ≤2 lines else short (fitLabel)
  readonly action: { kind: "map" | "parts"; label: "Map" | "Parts"; aria: string } | null;   // golden header.action
  readonly tabs: readonly { tab: Tab; label: string }[];   // "What you can keep" · "Gear" · "Methods" · "Licence"  golden header.tabs
}
```
Sources: `full` = page v36 part `t` minus `/ · \d+ sections?$/` when the subtitle names it, else `PartFacts.short` if uncut
(no "…") else `PartFacts.label`; `short` = `PartFacts.short` if uncut else `label`; both "" for a one-part water (empty
slot kept). `action.kind`: `map` when the app can frame the part on the map (always true in the app for a named part —
decision D1), `parts` when the item has >1 part and no map, null for an unnamed section. Store: `part.label_s/short_s`
(str refs), `item.picker` → `Picker.choices`.

### 2.2 Keep tab — `KeepVM` (prototype `buildKeep()` 1020–1304)

```ts
export interface KeepVM {
  readonly order: readonly KeepBlock[];     // the draw order, = golden keep.order (strip, inh, banner, groups…, other, posrow, nofish)
  readonly strip: StripVM | null;
  readonly inherited: InheritedVM | null;
  readonly banner: BannerVM | null;         // only on a closed day
  readonly groups: readonly GroupVM[];      // every article.row.ledger.g28 (a row with group.headline)
  readonly plainRows: readonly PlainRowVM[];// a row page v36 draws without a card (release-only "Char" etc.)
  readonly otherFish: readonly OtherFishRowVM[] | null;
  readonly caveats: readonly CaveatVM[];    // top caveat blocks, in draw order (T15b)
  readonly possession: PossessionVM | null; // (T15)
  readonly whereNot: WhereNotVM | null;
  readonly outside: string | null;          // words.OUTSIDE_BC "This part of the water is outside B.C.: no B.C. rules apply." (page v36 renderCard 701)   // (T42)
}
export type KeepBlock = "strip" | "inherited" | "banner" | { caveat: number } | { group: number } | { plain: number } | "otherFish" | "possession" | "whereNot"   // (T21)
  | "outside";                              // page v36 p.foot; no v33 golden state is outside B.C. (unit only); timed caveats stay { caveat } (§6 #2)   // (T42)
export interface PossessionVM { readonly label: string; readonly value: string; readonly says: string; readonly rule: RuleKey; readonly term: "possession_quota"; readonly open: SheetKey }   // (T15) open = "pos:pos" (T36; §6 #3)
```

#### StripVM (page v36 `waterStrip0` 459–468 + prototype `flagBar` 938 / `flagLegend` 942 / snap `runsOf`)
```ts
export interface StripVM {
  readonly status: "open" | "closed";        // golden keep.strip.statusClass (tidal/outside → StripVM null)   // (T13)
  readonly line: readonly { kind: "status" | "when" | "soon" | "soonOpen"; text: string }[];   // (T13) cls wsstat / muted small / wsoon / wsoon open;  // golden keep.strip.line: ["Open","today"], ["Closed","on Jun 15","Opens Jul 1"]
  readonly runs: readonly { bits: number | null; days: number }[];   // per-day display_frame.strip over the VIEWED year, run-length; = golden bar[].flex
                                            // a common year skips leap-calendar day 60 (Feb 29): 365 days. VERIFIED 2026-10-20: 134/134 golden states equal
                                            // (taps.json displayByDay → display_frame.strip, day 60 dropped; x31p_1010 is null all year → StripVM null),
                                            // and the status word (closed iff display_frame.status === "closed") agrees on 134/134
  readonly todayAt: number;                 // bookIndex(day)/365 (page v36 segBar idx/365) → the marker; golden today "left:calc(77.26% - 1px)"   // (T13)
  readonly months: readonly string[];       // "J","F",… (words.MONTH1)
  readonly legend: readonly { bit: number; mark: "✓" | "✕" | null; label: string }[];   // (T13) golden text = (mark ? mark + " " : "") + label;  // golden legend; text ← static display.strip[bit].label
}
```
Sources: `display_frame.status`, `.strip` (bitmask: closed 1, release 2, fly 4, flies 8, bait 16 — `static display.strip`),
the run per day = the display frame of `cell(akey, segmentIndex(starts, d))` for each day d of the viewed year on the leap
calendar (taps.json `displayByDay` holds all 366; drop index 59 = Feb 29 in a common year). Each day is resolved at ITS OWN
weekday (`inside=false`) — page v36 `segOf(w, md)` = `segAt(w, md, weekdayOf(md), false)`, which snap `runsOf` used.

#### InheritedVM (page v36 `inheritedHtml` 611; prototype 1032–1035)
```ts
export interface InheritedVM { readonly label: string; readonly lines: readonly { says: Line; caution: string | null }[] }   // (T15)
```
`label` = "By another water’s rule" (words), `says` = `display_frame.inherited[].says` + `book` runs of `display_rule[by].book`,
`caution` = `.caution`.

#### BannerVM (page v36 `closersAt` 517 / `closedBanner` 520–541; prototype dedupe 1054–1062)
```ts
export interface BannerVM {
  readonly kind: "closed" | "tidal";        // (T14) tidal: page v36 tidalHtml
  readonly big: readonly string[];          // headline phrases left after "say each sentence once" (often empty)
  readonly paras: readonly string[];        // "No fishing for any game fish Jun 1–30."
  readonly chain: readonly { text: string; scope: string; rule: RuleKey; book: Line }[];  // .closechain li   // (T14)
  readonly reopen: { text: string; day: Day } | null;  // "See the rules from then ›" → sets the day
  readonly merged: { label: string; items: readonly { text: string; level: string | null; rule: RuleKey | null; book: Line }[] } | null;   // (T14) page v36 mergedHtml
}
```

#### GroupVM (prototype per-article block 1084–1286; page v36 `cardBlock` 946 / `treeBlock` 936 / `rowTitle` 436 / `scopeBadge` 640)
```ts
export interface GroupVM {
  readonly row: number;                     // index into rows frame rows[]
  readonly title: string;                   // "Trout and char" ← page v36 rowTitle(row) (species groups via static export.species)
  readonly scope: string | null;            // head kicker, null when moved to the share (moveTag)
  readonly head: GroupHeadVM;
  readonly origin: OriginVM | null;         // the .ghd.gsub block
  readonly tree: readonly TreeNodeVM[] | null;   // root's children when header.tier === tree[0].id, else tree[]
  readonly notKept: readonly NotKeptVM[];
  readonly worked: { label: string; says: string | null; lines: readonly { at: "here" | "elsewhere"; q: string; a: string }[] } | null;   // (T19) q carries ":" for at=here
  readonly quiet: Line | null;              // page v36 noRulesLine (STEEL_LINE) + ⓘ steelhead   // (T19)
  readonly more: { text: string; open: SheetKey };   // "See every regulation line ›" → src:r<i>   // (T19)
  readonly pinLine: PinLine | null;         // groupLine(title, root, headline, form === "a") — what the pin bar draws (PinLine from pins.ts)   // (T28)
}
export interface GroupHeadVM {
  readonly form: "a" | "b" | null;          // 20h: a REGION total over a stream share; null otherwise
  readonly rootTier: string | null;         // header.tier id
  readonly lead: Line | null;               // form a: headline words with its n big ("2 a day per angler from streams")
  readonly leadN: number | null;            // the share lead's number (PinAnchor.headlineN); golden head.leadN is its string form "2"   // (T29)
  readonly words: Line;                     // the root tier's words, its n big (form a: shown small after "of the")
  readonly ofTotal: boolean;                // form a: words prefixed by words.OF_THE ("of the")
  readonly whyHere: string | null;          // root why_here (cap1)
  readonly shareOf: Line | null;            // form b only: "↳ " + shortOf(relTier)
  readonly terms: readonly string[];        // the daily_quota ⓘ — always [] in v33 (prototype qtip finds no glifts when group.tree exists)   // (T17)
  readonly nolimit: boolean;
}
export interface OriginVM { readonly says: Line; readonly tag: string | null; readonly howtell: boolean; readonly book: Line }
```
Field sources: `RowsValue.rows[row].group` (`GroupTotal`): `.headline{n, says, unit, also}`, `.header{says, tier, tag}`,
`.tree[0]` (`TreeTier`), `.origin`, `.total_line`, `.not_kept[]`, `.worked[]`, `.possession`; `Row.elsewhere.says`;
`Row.scope` (Badge) → kicker words (page v36 `scopeBadge` + `rowHtml`'s "All Region 2 streams"/"Region 2-wide" rewrite —
port as logic, see T17).

#### TreeNodeVM (prototype `tnode`/`tierH`/`tierWords`/`kt` 1212–1248)
```ts
export type TreeNodeVM = TierVM | KindVM | ReleaseRowVM;
export interface TierVM {
  readonly t: "tier"; readonly tier: "total" | "own" | "share" | "count" | "size"; readonly tid: string;
  readonly kicker: string | null;           // the moved scope tag over the share (tagShare)
  readonly line: Line;                      // tierWords: boldN(says, n) + size extra's 2nd number "big2" + root ⓘ + book (only if a page is fresh on this branch)
  readonly sub: string | null;              // own tier: cap1(why_here)
  readonly origin: OriginVM | null;         // tier.origin (20g) with How to tell and the root's book when group.origin is null
  readonly band: boolean;                   // tier === "size": the tinted SizeBand wraps this node
  readonly short: string;                   // shortOf(tier) for the pin bar (T30)
  readonly children: readonly TreeNodeVM[];
}
export interface KindVM {
  readonly t: "kind"; readonly kind: string; readonly species: readonly string[];
  readonly folded: boolean; readonly zero: boolean;
  readonly says: Line | null;               // null when folded; boldN(says, n) (no bold when n===0); + link "How to tell ›" for hatchery-only steelhead
  readonly size: { text: string; rule: boolean } | null;   // k.size_rule || sizeOf(detail).size — the chip (nbsp'd); rule: the bold v33 size_rule chip   // (T18) = T26's "sizeRule" (§6 #4)
  readonly qualifiers: string | null;       // sizeOf(detail).q joined ", "
  readonly pictures: readonly FishPicVM[];  // FIRST after the sentence + chip (20f)
  readonly also: readonly string[];         // also_counts[].says (cap1)
  readonly duty: Line | null;               // duties.map(dutyWords).join(" · ") + link "Steelhead stamp ›" {tab:"licence"}   // (T18)
  readonly presence: Line | null;           // presOf(k).says + ⓘ steelhead
}
export interface ReleaseRowVM { readonly t: "release"; readonly names: string; readonly pictures: readonly FishPicVM[] }
export type FishPicVM = { readonly code: string; readonly name: string; readonly caption: string; readonly open: SheetKey; readonly sil: false } | { readonly code: string; readonly name: string; readonly sil: true };   // caption "Brook trout ›"; sil: no image (T18)
                                            // name: fishName(c) = static fish[c].name || caption — a11y "What a <name> looks like" (not the caption, which may be a stand-in's)   // (T26)
export interface NotKeptVM { readonly status: "closed" | "release"; readonly says: string; readonly whose: Line; readonly mark: "✕" | null; readonly presence: Line | null; readonly pictures: readonly FishPicVM[] }
export interface PlainRowVM {                                         // golden keep.plainRows (lines of the article)   // (T19b)
  readonly row: number; readonly title: string;
  readonly pill: { kind: "release" | "closed"; text: string; term: string } | null;   // "↩ Release" ⓘ catch_and_release / "⊘ Closed" ⓘ no_fishing_for
  readonly scope: string | null; readonly sum: string | null;
  readonly week: { says: string; parts: readonly { days: string; says: string; today: boolean; book: Line }[] } | null;
  readonly presence: Line | null; readonly names: string | null;
  readonly release: { names: string; pictures: readonly FishPicVM[] } | null;   // prototype krel28
  readonly ladder: LadderVM; readonly more: { text: string; open: SheetKey };
}
export interface LadderVM {                                           // "How this was decided" (page v36 ladderHtml); T36's other: sheet reuses it   // (T19b)
  readonly label: string;
  readonly sections: readonly { q: string; result: string | null; steps: readonly LadderStepVM[] }[];
  readonly lost: readonly { text: string; status: string; rule: RuleKey; book: Line }[];
}
export interface CaveatVM {                                           // page v36 inPartHtml / sideHtml / anglerClosures / timedHtml / uncertainHtml   // (T15b)
  readonly kind: "inpart" | "side" | "angler" | "timed" | "uncertain"; readonly hot: boolean;
  readonly title: string; readonly calm: string | null;
  readonly items: readonly CaveatItemVM[]; readonly later: readonly CaveatItemVM[] | null;
}
export interface CaveatItemVM { readonly what: Line; readonly where: string | null; readonly rule: RuleKey | null; readonly fish: string | null; readonly book: Line }   // (T15b)
```
Sources: `TreeTier{id,tier,n,says,by,children,whose,why_here,replaces,over_cm,extra,origin}`, `TreeKind{species,name,n,says,
detail,also_counts,duties,presence,folded}`; book pages ← `display_rule[by].book`; the fish caption ← fish manifest
`caption` (T22), name ← `static answers.fish` / export species.

#### OtherFishRowVM, WhereNotVM
```ts
export interface OtherFishRowVM { readonly thumb: string | null; readonly name: string; readonly value: string;
  readonly tone: "keep" | "release" | "closed" | "nolimit";   // prototype class k-release / k-closed — decided in core, never the component   // (T27)
  readonly week: string | null; readonly sizeHint: string | null; readonly open: SheetKey }   // (T19) sizeHint "· size limit"; open = `other:o${i}` (i = index in keep.otherFish)   // (T36)
export interface WhereNotVM { readonly heading: string; readonly scope: string | null; readonly items: readonly { text: Line; rule: RuleKey }[] }   // (T15)
```
`OtherFishRowVM` ← page v36 `rowsHtml` short forms (`"15 a day"`, `"release"`, `"closed"`, `"no limit"`) over non-trout rows
when ≥ 2; `week` ← `Row.week.status`. `WhereNotVM` ← page v36 `whereHtml` (ladder `shown` rules with `RuleFacts.where`,
`.plain`) + heading words "Where you can’t fish".

### 2.3 Gear tab — `GearVM` (prototype `buildGear()` 1454–1543; page v36 `gearParts` 1191–1259)
```ts
export interface GearVM {
  readonly closed: string | null;           // closedUntil(): "No fishing here until Jul 1." — then nothing else
  readonly day: string | null;              // dayLine(): "Today, Oct 10" / "Fri, May 15"   (golden gear.day; null on a closed day)   // (T30)
  readonly mode: { value: string; terms: readonly string[]; changes: readonly ChangeVM[]; src: SourceLineVM | null } | null;
  readonly lines: readonly GearLineVM[];    // Lines · Hooks · Weight on the line (GLAB), only parts the summary sentence names
  readonly bait: BaitVM | null;
}
export interface GearLineVM { readonly k: "lines" | "hook" | "weight"; readonly label: string; readonly values: readonly string[]; readonly terms: readonly string[]; readonly changes: readonly ChangeVM[]; readonly src: SourceLineVM | null }
export interface ChangeVM { readonly chip: "Now" | "Next"; readonly when: string; readonly says: string; readonly inForce: boolean }  // plain: "<when>: <says>[ · in force today]"
export interface SourceLineVM {                                       // "Province · Region 2 · This water’s ›" + "Rule book p.N ↗"…   // (T30)
  readonly who: string;                     // includes the trailing " ›" exactly as golden   // (T30)
  readonly aria: string;                    // "Sources: " + words joined ", " (or "rules")   // (T30)
  readonly open: SheetKey;                  // srcs:… (§1)
  readonly keys: readonly RuleKey[];        // feeds the srcs: sheet (the prototype's global SRCS)   // (T30)
  readonly pages: readonly { printed: number; pdf: number }[];
}
export interface BaitVM { readonly label: string; readonly notes: readonly Line[];   // the bait_ban ⓘ is a term run; golden plain ends " ⓘ"   // (T31)
  readonly dated: readonly ChangeVM[]; readonly list: YesNoVM; readonly src: SourceLineVM | null }
export type YesNoVM = readonly { col: "ok" | "no"; head: "Allowed" | "Not allowed"; items: readonly { mark: "✓" | "✕"; name: string; note: string | null }[] }[];
```
Sources: `GearAnswer.summary{says, parts[{part, says, cites, terms}], terms, changes[{when, says, cites}]}`, `.bait[]
{element, ok, why, excepted_from, carry_kg, also_allowed}`, `.bait_ban`; dated bait runs ← strip bit 16 runs; who-words ←
rule level (export `prov.rank` → words.LEVEL: "Province", "Region 2", "This water’s", …) — see T33 for the exact table.

### 2.4 Methods tab — `MethodsVM`
```ts
export interface MethodsVM {
  readonly closed: string | null;
  readonly ways: { label: string; list: YesNoVM; src: SourceLineVM | null } | null;
  readonly boats: { label: string; items: readonly string[]; src: SourceLineVM | null } | null;
  readonly reference: { label: "Reference"; title: string; sub: string; groups: readonly { head: string; items: readonly string[] }[]; src: SourceLineVM | null } | null;
}
```
Sources: `GearAnswer.ways[]{method, allowed, why, not_for, for, while, conduct, while_rules, devices}`, `.vessel`, boat rules
(`display_rule` kind boat/vessel + export `label`), `.conduct{release|keep|carry|never|also}`, `.caught`, statics
`gear.moments`, `gear.conduct_means`, `export.conduct`.

### 2.5 Licence tab — `LicenceVM` (prototype `licence()` 1359–1403; page v36 `licParts` 1309–1370)
```ts
export interface LicenceVM {                                          // (T33) replaces the v32 {cls, lines: string[]} blocks
  readonly question: string | null; readonly questionTerms: readonly string[];   // "Who is fishing?" + ⓘ under_16   // (T33)
  readonly axes: readonly { axis: "age" | "res"; aria: string; options: readonly { value: string; text: string; checked: boolean }[] }[];   // (T33) aria
  readonly label: string;                   // static licence.profile_labels[profileIx] e.g. "Canadian, other province, under 16"
  readonly blocks: readonly LicenceBlockVM[];          // draw order; `needs` is one of them (top-level `needs` dropped)   // (T33)
  readonly src: { who: string; aria: string; open: SheetKey } | null;   // open = `licsrc:${profileKeyOf(who)}` (T36)   // (T33)
}
export interface NeedItemVM { readonly name: string; readonly terms: readonly string[]; readonly when: string; readonly price: string | null }   // (T33)
export type LicenceBlockVM =                                          // (T33)
  | { cls: "classbox"; head: string; terms: readonly string[]; body: string; source: SheetKey | null }
  | { cls: "classbox youth"; head: string; terms: readonly string[]; body: string }
  | { cls: "flag"; text: string } | { cls: "scopenote"; text: string; source: SheetKey | null }
  | { cls: "needs"; label: string; items: readonly NeedItemVM[]; onlyIf: { label: string; items: readonly NeedItemVM[] } | null; priceNote: string | null }
  | { cls: "paper"; lead: string; strong: string; tail: string }
  | { cls: "nymbox"; label: string; says: string; note: string }
  | { cls: "muted"; text: string }
  | { cls: "lostlist lopen"; label: string; cards: readonly { forWho: string; label: string; paths: readonly string[] }[] };
export function licenceBlockLines(b: LicenceBlockVM): readonly string[];   // plain form = golden blocks[].lines   // (T33)
```
Sources: `LicenceValue{holds, profiles[60], under_16}`; profile index = `profileIndex(who ⊕ {guidance:"non_guided", status:"none"},
static licence.profile_dims, static licence.profiles)`; documents/prices ← `ProfileAnswer.documents[]`, `export.licences`;
designations ← `lic[lix]` export words. Golden `needs[].items[].text` is derived from the `needs` block:
`[name, "ⓘ" per term, when, price].join(" ")`, `onlyIf` items included in order (T33).

### 2.6 Sheets — `SheetVM`
```ts
export interface SheetVM { readonly key: SheetKey; readonly title: string; readonly sub: string | null; readonly blocks: readonly SheetBlock[] }   // (T36) key
export type SheetBlock =                                              // (T36) replaces the v32 list
  | { k: "para"; text: Line; tone?: "plain" | "small" | "note" }        // note = "⚑ …" rule notes
  | { k: "example"; text: Line }                                        // "For example: …" (bold lead in the run list)
  | { k: "head"; text: string }                                         // .lbl: "The exact words", "What to look for", "How this was decided", "These decide it (12)"
  | { k: "quote"; text: string }                                        // blockquote.exact
  | { k: "list"; items: readonly Line[]; ordered?: false }
  | { k: "fold"; summary: string; body: readonly SheetBlock[]; open: false }   // ALWAYS closed by default
  | { k: "term"; id: string; title: string; body: readonly SheetBlock[] } // a glossary section inside a multi-term or the hatch sheet
  | { k: "rule"; title: string; level: string; applies: readonly string[]; book: Line; open: SheetKey }   // srcs: rows (a button row)
  | { k: "value"; text: Line }                                          // the row's verdict chip "↩ Release ⓘ" / "5 a day"
  | { k: "weekBox"; day: string; scope: string | null; status: string; says: string }
  | { k: "ladder"; q: string | null; result: string | null; steps: readonly LadderStepVM[] }
  | { k: "sources"; groups: readonly { where: string; rows: readonly SourceRowVM[] }[] }
  | { k: "pictures"; pics: readonly FishPicVM[] }
  | { k: "image"; code: string; file: string; alt: string; credit: string; width: number; height: number }   // credit "<caption>[ · <phase>] · <credit> (<source>)" (T43)
  | { k: "adipose"; figures: readonly { variant: "wild" | "clipped"; caption: Line }[] }
  | { k: "pages"; line: Line }                                          // a paragraph of book runs
  | { k: "source"; line: Line }                                         // "Source ›" link run + book runs
  | { k: "link"; line: Line }                                           // "See every regulation line ›"
  | { k: "choice"; items: readonly { text: string; sub: string | null; status: string; long: string | null; checked: boolean; part: number }[] };   // parts sheet rows
export interface LadderStepVM { readonly level: string;               // also LadderVM.sections[].steps (T19b)   // (T36)
  readonly lines: readonly { text: Line; tag: string | null; rule: RuleKey | null; won: boolean; applies: readonly string[]; book: Line; today?: boolean; days?: string }[] }   // (T36) text Line, applies, today, days; book (T19b) — §6 #5
export interface SourceRowVM { readonly key: RuleKey | null; readonly title: string | null; readonly quote: string; readonly role: string; readonly book: Line }   // (T36)
export interface SheetCtx { readonly inp: RegsInputs; readonly page: Omit<RegsPage, "sheet">; readonly today: Day }   // sheets.ts   // (T36)
export function sheetOf(ctx: SheetCtx, key: SheetKey): SheetVM | null;   // null: the key does not apply here (never throws)   // (T36)
export function sheetLines(vm: SheetVM): { title: string; sub: string | null; folds: { summary: string; open: false }[];
  pages: { text: string; pdf: number | null }[]; lines: string[]; nested: string[] };   // the golden comparison form (§1 join rule)   // (T36)
```
`sheetLines(sheet)` must equal golden `sheets[goldenKeyOf(key, id)]` `.lines` (body), `.title/.sub`, `.folds`, `.pages`.

### 2.7 Component-side contracts (ui-native; not in `RegsPage`)
```ts
// AnchorEvent — declared by T29, forwarded by T26's Group/TreeNodes through `onAnchor`
export type AnchorEvent =                                             // (T26)
  | { k: "group"; row: number; y: number; height: number; ref: React.RefObject<View> }            // the article (Group root View)
  | { k: "pin"; row: number; y: number; height: number; ref: React.RefObject<View> }              // the head block (GroupHead root)
  | { k: "tier"; row: number; tid: string; y: number; height: number; ref: React.RefObject<View> } // a tier node (TierHeader + children)
  | { k: "head"; row: number; tid: string; y: number; height: number; ref: React.RefObject<View> } // the TierHeader alone
  | { k: "worked"; row: number; y: number; height: number; ref: React.RefObject<View> };   // ref: measureLayout(contentRef) → content offset   // (T29)
// y is relative to the reporting View's parent (RN onLayout); useAnchors converts to content offsets.

// app/packages/ui-native/src/Sheet.tsx — optional props, existing callers unchanged
export function Sheet(p: { /* …existing… */ scroll?: boolean; sub?: string | null }): JSX.Element;   // (T37)
//   scroll (default false): children in <ScrollView maxHeight 0.82·window, paddingHorizontal GUTTER, paddingBottom SPACE.s4, keyboardShouldPersistTaps="handled">
//   sub: a TYPE.small `sub` line under the title row (rule / src sheets)
```
**testID prefixes** (every regs component T23–T35; the palette/census gate's selectors — RN has no classes):   // (T39)
status words `st:<status>`, big/inline numbers `n:big|big2|inline`, chips `chip:size|change`, segmented buttons/pills
`btn:<name>`. An element without one of these prefixes is role `text`.

### 2.8 Hooks (`@app/ui` `regs.ts`, T11)
```ts
export type RegsOpenDb = (url: string) => Promise<Db>;                 // injected (App.tsx passes loadBundle)   // (T11)
export function useRegsStore(source: RegsSource, regsUrl: string | undefined, openDb: RegsOpenDb | undefined): Async<RegsStore | null>;   // (T11)
export function useRegsTarget(store: RegsStore | null, item: ItemId | null, section: SectionId | null): Async<RegsTarget | null>;   // (T11)
export function useRegulations(store: RegsStore | null, target: RegsTarget | null, mapDay: Date | null): Async<RegsInputs | null>;   // (T11) keyed on the calendar date
export function useRegsPage(inputs: RegsInputs | null, who: Who, today: Day): RegsPage | null;   // useMemo(buildRegsPage(inputs, {today, who})); null while inputs null   // (T11, §6 #8)
export function regsDay(mapDay: Date | null, now?: Date): { day: Day; weekday: Weekday; calendarYear: number; today: Day; isToday: boolean };   // (T11)
export function mapDayOf(day: Day, calendarYear: number, now?: Date): Date | null;   // (T11)
export const DEFAULT_WHO: Who;                                          // { age: "adult", residency: "resident" }   // (T11)
export function useAnglerProfile(): readonly [Who, (who: Who) => void];   // session-only, shared across mounts   // (T11)
```
Screen props (T01/T11): `Shell` `regs?`, `openRegsDb?`; `WaterScreen` `regsUrl?`, `openRegsDb?`, `day`, `onDay`, `section`.

---

## 3. Inputs — `RegsInputs` (built by `@app/data`, hydrated in core)

```ts
// app/packages/core/src/regs/inputs.ts  (T09 FINAL form)
export type DisplayFrame = StatusFrame | TidalStatus;     // answers.generated; a tidal key's frame is {status:"tidal", tidal, note, see, licence}   // (T04)
export interface CellIds { readonly ladder: number; readonly rows: number; readonly gear: number; readonly licence: number; readonly display: number }   // (T00)
export type VerdictWire = readonly [speaks: readonly number[], beside: readonly number[], shown: readonly number[],
  nym: readonly number[], partly: readonly (readonly [number, readonly number[]])[], lost: readonly (readonly [number, number, number])[]];   // (T00)
export interface LadderFrameWire { readonly common: VerdictWire; readonly rows: readonly (readonly [fish: string, none: VerdictWire, hatchery: VerdictWire, wild: VerdictWire])[] }   // (T00) replaces the untyped LadderFrame (T04)
export interface MergedPart { readonly partIx: number; readonly label: string; readonly sections: number; readonly broad: RuleView | null }   // (T09/T14)
export interface RegsYear {                                                                                          // (T09)
  readonly segStarts: readonly number[];
  readonly moments: readonly Moment[] | null;
  readonly cells: readonly CellIds[];                     // index = s, length = key.nseg   // (T09)
  readonly ladderBySeg: readonly (LadderFrameWire | null)[];   // (T07)
  readonly rowsBySeg: readonly (RowsValue | null)[];      // (T13/T08) the joined wire value; rowsAt hydrates it
  readonly displayBySeg: readonly (DisplayFrame | null)[];   // (T07)
  readonly displayByDay: readonly (DisplayFrame | null)[]; // 366, leap calendar, index d-1; the SAME object for equal frame ids   // (T09)
}
export interface RegsInputs {                                                                                        // (T09)
  readonly target: RegsTarget; readonly day: Day; readonly weekday: Weekday;
  readonly calendarYear: number;                            // the selected date's year: names every day's weekday (page YEAR)   // (T09)
  readonly item: { readonly id: string; readonly ord: number; readonly name: string; readonly kind: string; readonly sections: number;
                   readonly parts: readonly (PartFacts | null)[]; readonly partSections: readonly number[];
                   readonly picker: Picker | null; readonly steelhead: 1 | 2 | null } | null;   // (T09) item.sub removed — T16 composes the subtitle (§6 #7)
  readonly partIx: number;
  readonly place: { readonly label: string; readonly name: string } | null;   // an unnamed section: place label + names[kind]
  readonly key: AkeyRow | null;                             // null = outside B.C. (or an item with no rule set on this part)
  readonly s: number | null; readonly cell: CellIds | null;   // (T09)
  readonly frames: { readonly ladder: LadderFrameWire; readonly rows: RowsValue; readonly gear: GearValue;
                     readonly licence: LicenceValue; readonly display: DisplayFrame } | null;   // (T04/T09) GearValue = GearAnswer | TidalState
  readonly year: RegsYear | null;                           // (T09)
  readonly ruleset: { readonly reach: readonly number[]; readonly trib: readonly number[] } | null;   // (T09)
  readonly rules: RuleIndex;                                // every store rule, viewed through THIS part's via/rank; get() never misses (noRule)   // (T09/T06)
  readonly lics: ReadonlyMap<number, unknown>;              // export licensing words by lix
  readonly statics: Statics;
  readonly mergedParts: readonly MergedPart[] | null;      // page mergedHtml: only when the part's picker group is closed and has ≥ 2 parts   // (T09/T14)
}
export interface AkeyRow { akey: number; ruleset: number; licensingSet: number | null; steelheadWater: boolean; steelhead: 1 | 2 | null; steelheadRules: boolean; kind: string; tidal: boolean; seg: number; mom: number | null; nseg: number }
export interface RuleView {                                // widened; a view is PER PART (via/rank) — never shared across parts   // (T06)
  readonly rix: number; readonly key: RuleKey;             // `${entry_id}::${rule_id}`
  readonly export: ExportRule; readonly facts: RuleFacts | null;
  readonly f: Readonly<Record<string, unknown>>;           // = export.fields (page r.f)   // (T06)
  readonly kind: RuleFacts["kind"] | null;                 // page r.k   // (T06)
  readonly baseRank: number;                               // export.prov.rank   // (T06)
  readonly via: "reach" | "trib";                          // the part's ruleset list holding it (absent: "reach")   // (T06)
  readonly rank: number;                                   // via trib && baseRank >= 0 → 1, else baseRank (page makePlace.R)   // (T06)
  readonly species: readonly string[]; readonly speciesExcept: readonly string[];   // (T06)
  readonly wins: readonly (readonly [Md, Md])[]; readonly tcond: string;           // (T06)
  readonly part: string | null;                            // fields.undrawn_part   // (T06)
  readonly notes: readonly string[];                       // WORDS.UNCERTAIN + why | WORDS.NO_REASON ; WORDS.CURATOR + review_reason   // (T06)
  readonly missing: boolean;                               // the NORULE stand-in (noRule(rix): key "#<rix>", rank 4)   // (T06)
}
export interface RuleIndex { get(rix: number): RuleView; has(rix: number): boolean; readonly size: number }   // (T06)
export interface ExportRule { entry_id: string; rule_id: string; type: string; family: string; label: string; verbatim: string; fields: Record<string, unknown>; parts?: Record<string, string>; prov: { rank: number; entry_name?: string; who?: string; why?: string }; dimension?: string; binds?: string }   // (T00) dimension, binds
export interface Statics {
  glossary: { version: number; terms: readonly GlossaryTerm[] };
  strip: readonly StripState[]; reasons: unknown; fish: unknown; moments: readonly Moment[];
  countNouns: Record<string, readonly string[]>; gearMoments: unknown; conductMeans: Record<string, string>;
  profiles: readonly string[]; profileDims: readonly (readonly [string, readonly string[]])[]; profileLabels: readonly string[]; residencyLabels: unknown;
  exportSpecies: SpeciesBook; exportLicences: unknown; exportConduct: Record<string, string>;   // (T06) SpeciesBook {fish, groups, protected?, salmon?} (names.ts)
  ladderReasons: unknown; ladderReasonState: unknown;
  pdfPages: Readonly<Record<number, number>>;   // printed → pdf, for glossary pages (printed only); source pipeline.common.synopsis_pages — never compute   // (T36)
}
// CardModel (T08 defines it; card.ts) — Infinity is a legal daily; roles keyed by RuleKey.   // (T08)
// app/packages/core/src/fish.generated.ts (T20's generator) also emits:
export const FISH_TIPS: Readonly<Record<string, readonly string[]>>;   // app/design/regs-v10/fishart.js FISHART.SPECIES[code].tips — "What to look for" (not in the store)   // (T36)

// app/packages/data/src/regs/inputs.ts
export async function loadRegsInputs(store: RegsStore, target: RegsTarget, day: Day, weekday: Weekday,
                                     calendarYear: number): Promise<RegsInputs>;   // (T09) 5th param calendarYear
export async function targetFor(store: RegsStore, item: string, section: number | null): Promise<RegsTarget>;   // (T09) lowest part_ix sharing the section's akey
```
Data layer (T03/T04): `BundleInfo.ruleIdsSha256: string | null` (T03; the store refuses a mismatched pairing); new row types
`ItemRow`, `PartRow`, `PlaceRow`, `RulesetRow`, `StoredRule` (T04, `@app/data` only). `frames.ladder` is the wire form with
verdict ids resolved, and core decides the states (T04).

Store mapping (verified 2026-10-20 on `data/generated/bundle/regs.sqlite`, see T03/T04):

| input | store read |
|---|---|
| item | `item` row by `item_id` (str ref; build a `Map<itemId, item_ord>` once — no index) → `name`, `kind`(str), `picker`→`picker.j`, `x` |
| parts | `part` rows `(item_ord, part_ix)` → `akey`, `ord`, `label_s/short_s/runs_s/place_s/hint_s` (str), `km`, `closed_all_year`, `paper_licence` |
| key | `akey` row → `seg`→`seglist.starts`, `mom`→`segmom.ix`→`static answers.moments[i]` |
| s | `segmentIndex(starts, dayOfYear(day), moments, weekday, inside=false)` (port of `encode.segment_index`) |
| frames | `cell(akey, s)` → ids; `display_frame`/`gear_frame` blob = the frame; `rows_frame` = `[spp, {S:[h,w]}, [rowIds], line]` → `rows_decided`, `rows_row`; `licence_frame` = `[h, d, u?]` → `licence_hold`, `licence_doc` → `licence_answer[]`, `licence_u16`; `ladder_frame` = `[common, [[fix, v…]]]` → `ladder_verdict` |
| rules | ALL rules (T04 `rules()`, once per store) viewed through the part's `via` (`ruleIndex`, T06): `display_rule.j` (RuleFacts, rix-aligned) + `rule.j` (export words: `entry_id`, `rule_id`, `label`, `verbatim`, `fields`, `prov`) |
| unnamed | `section_akey` block `h>>12`, u16le at `2*(h&4095)`; `section_place` byte `h&4095` minus 1; name = `place.names[akey.kind]` |
| blobs | first byte `0x02` → raw inflate with `zdict[table]`; `0x01` raw inflate; else UTF-8 JSON |

---

## 4. Field-by-field: RegsPage ← answers (the short table)

| RegsPage path | answers / store source | port of |
|---|---|---|
| `status` | `display_frame.status` (`closed`→closed, else open); `akey.tidal`→tidal; `key==null`→outside | v36 `statusAt` 183 |
| `header.name` | `item.name` / `place.names[kind]` | — |
| `header.place.full/short` | `PartFacts.label/short` | proto `hereLabel` 977 / `shortLabel` 985 |
| `keep.strip.runs` | `display_frame.strip` per day (366) | snap `runsOf`; proto `flagBar` 938 |
| `keep.strip.legend[].text` | `static display.strip[].label` | proto `flagLegend` 942 |
| `keep.strip.line` | `display_frame.status`, `nextOpen` over the year | v36 `waterStrip0` 459 |
| `keep.inherited` | `display_frame.inherited[]{says, by, caution}` + `display_rule[by].book` | v36 `inheritedHtml` 611 |
| `keep.banner` | `display_frame.closing`, `.unless_opened`, `display_rule.plain/unless_opened`, `export.prov.rank` | v36 `closersAt` 517 / `closedBanner` 520 |
| `keep.groups[].head` | `group.header`, `group.tree[0]`, `group.headline` | proto rootLine 1252–1273 |
| `keep.groups[].tree` | `group.tree[0].children` | proto `tnode` 1246 |
| `keep.groups[].notKept` | `group.not_kept[]` | proto `nk` 1181 |
| `keep.groups[].worked` | `row.elsewhere.says`, `group.worked[]` | proto `worked` 1189 |
| `keep.otherFish` | non-trout `rows[]` (TROUTISH split), `row.week.status`; `tone` from the short form, `sizeHint` from the group's size rule (T27); `open` `other:o<i>` | v36 `rowsHtml` 692; proto `k-release`/`k-closed`, `.osz` |
| `keep.caveats[]` | `Settled.{inpart, side, anglerclosed}`, per-moment segments (timed), `item.unresolved` → `CaveatVM` (golden `details.inpart*` / `div.caveats.top*`) | v36 `inPartHtml` 592 / `sideHtml` 604 / `anglerClosures` 584 / `timedHtml` 553 / `uncertainHtml` 588 (T15b) |
| `keep.outside` | `key == null` → words.OUTSIDE_BC (T42) | v36 `renderCard` 701 foot |
| `keep.groups[].head.leadN` | `group.headline.n` (form a share lead) (T29) | proto rootLine; golden `head.leadN` |
| `keep.groups[].pinLine` | `groupLine(title, root, headline, formA)` (T28) | proto `shortOf`/`pinUpdate` |
| `keep.possession` | possession rules (`decided.roles` role `possession`) → `display_rule.plain` | v36 `possessionHtml` 626 + proto 1043 |
| `keep.whereNot` | ladder `shown` rules with `RuleFacts.where` | v36 `whereHtml` 616 |
| `gear.day` | `dayLine()`; null on a closed day (T30) | proto `buildGear` |
| `gear.lines` | `gear.summary.parts/changes` | proto `buildGear` 1474–1502 |
| `gear.*.src` / `methods.*.src` | cited rixs → `who` (+" ›"), `aria`, `keys` (RuleKey[]), `pages` from `RuleFacts.book`; `open` `srcs:…` (T30) | proto `learnChipWords` 1377 / `itemSrc` 1464 |
| `gear.bait` | `gear.bait[]`, strip bit-16 runs; `notes` Lines carry the `bait_ban` ⓘ term run (T31) | v36 `gearParts` bait + proto 1504–1520 |
| `methods.ways` | `gear.ways[]` | v36 `gearParts` ways + proto 1522–1526 |
| `methods.boats` | boat rules (`vessel`) | v36 `gearParts` boats + proto 1528 |
| `methods.reference` | `gear.conduct`, `gear.caught`, statics | v36 `gearParts` always + `renderKit` tile; proto 1535–1540 |
| `licence.*` | `licence_frame` → holds/profiles/under_16, statics `licence.*`; typed `blocks` (needs/classbox/paper/nymbox/lostlist…), `licenceBlockLines()` = golden `blocks[].lines` (T33) | v36 `licParts` 1309; proto `licence()` 1359 |
| `licence.src.open` | `licsrc:${profileKeyOf(who)}` (T36) | proto `LICSRC` 1417 |
| `sheet("rule:K")` | `display_rule`, `rule.j` | v36 `openRule` 1156; proto `sheetHtml` rule: 1674 |
| `sheet("gl:id[,id…]")` | `static answers.glossary.terms[id]`; pages = bare `book` runs, pdf via `Statics.pdfPages` (T36) | proto `glossBody` 930 / `pageLink` 935 |
| `sheet("pos:pos")` | possession rule + glossary `possession_quota` + `group.possession{example, doubles}` | proto `sheetHtml` pos: 1659 |
| `sheet("other:oN")` | the row + its decided roles (ladder) + `week` | v36 `ladderHtml` 1077 / proto `weekSheet` 1310 |
| `sheet("srcs:…")` | `SourceLineVM.keys` of that block → one `rule` block each (T36) | proto `sheetHtml` srcs: 1652 |
| `sheet("licsrc:<profile>")` | `licence_frame` holds' `from` groups (T33/T36) | proto `LICSRC` 1417 |
| `sheet("src:rN")` | `rowSources(row)` | v36 `openRowSources` 796 / proto `groupedSources` 1633 |
| `sheet("sp:CODE")` | fish manifest (`image` blocks + credit) + `FISH_TIPS[code]` (T36, generated by T20) | proto `sheetHtml` sp: 1684 |
| `sheet("hatch")` | adipose words + glossary `hatchery_wild` (`adipose` + `term` blocks) | v36 `adiposeHtml` 717 / proto 1680 |
| `sheet("parts")` | `item.parts[]` label/short/km + `closed_all_year` → `choice` block (T36) | proto parts branch |

Line numbers: `regs-v11.html` @ 04765ab5 (v33) and `page_v36.js` (unchanged since 18bc2a11) — **always re-read them from
`app/design/regs-v11/port/golden/inventory.json`** (they move when the prototype changes; the names do not).

### 4.1 Mappings already proved against the goldens (`port/verify_map.py`, `port/pinreplay.mjs`, v33)

| mapping | result |
|---|---|
| `keep.strip.runs` = run-length of `display_frame.strip` per day (each day at its own weekday, Feb 29 dropped in 2026) | 134/134 |
| `keep.strip.status` closed ⇔ the day's `display_frame.status === "closed"` | 134/134 |
| groups drawn = rows with `group.headline` among page v36's full rows: rows with any TROUTISH member (`RB CT EB GB LT DV ST BT CT_ws CT_c`), all rows when there are no trout rows or < 2 others; **no rows at all on a closed day** | 134/134 |
| `keep.otherFish` = the non-trout rows when ≥ 2 and the day is open, else null | 134/134 |
| tier line text = `TreeTier.says` (+ ⓘ / Rule book runs) | 130/130 tiers |
| kind sentence starts with `TreeKind.says` (folded kinds draw none) | 273/273 kinds |
| pin bar = pure `pinChain(anchors, y, viewport)` | 134/134 states, every transition |

## 5. Golden comparison rules
- Compare `plain()` of a VM field to the golden string after `/\s+/g → " "`, trim. Golden text fields AND `lines()` insert one
  space where two elements abut (never inside a text run, never between text and a following element) — so `"Open today"`,
  `"Kokanee release"`, `"… from streams of the 4 …"`; see §1.
- Golden `keep.groups[].tree` mirrors `TreeNodeVM` 1:1 (`line` = `plain(TierVM.line)`, `big[]` = the `num` runs).
- Golden `pins.samples[]` are `{y, lines:[{cls, text, sub, big}]}` at 8 px steps over the Keep tab with `pins.anchors` measured
  at scrollTop 0 in the prototype (440×900 window, 390×844 phone frame, fallback fonts — fonts are blocked offline). The
  pinChain unit test feeds the anchors and y, not RN layout.
- A prototype bug is pinned too: `pinUpdate` throws on 25 states (an `article.row.ledger` whose row has no `group`, e.g.
  Cowichan part 1 at y=1296). `golden.states[id].pins.error` records it; samples after the throw are missing. The app's
  `pinChain` must NOT throw (it skips a group without a tree) — those states are excluded from pin parity (T30).

## 6. Resolved conflicts

1. **Sheet keys: state id or not** — T30–T33/T35 build keys with `${pid}` inside (`srcs:gl|${pid}|hook`, `srcs:bait|${pid}`,
   `licsrc:lic|${pid}|…`); T36 says app keys carry no state id. Golden keys embed the state id either way, so both can match
   it; T36 wins because `appKeyOf`/`goldenKeyOf` already bridge them and a `RegsPage` is one state. Presenters emit
   app-form keys (`srcs:gl|hook`, `srcs:bait`, `licsrc:u16_res`), and parity tests compare `goldenKeyOf(vm.open, id)`
   to the golden key. `opts.pid` stays optional for the test harness only; T30's `${itemId}~${partIx}~${mmdd}` app pid is dropped. T30–T35 bodies are updated to match.
2. **Timed caveats: `TimedVM` (T42) or `CaveatVM{kind:"timed"}` (T15b)** — the golden has a single `div.caveats.top`
   (x15p2_1010) and no per-block field. T15b already models it, with its siblings (`details.inpart.hot`, angler, side) as
   `{ caveat: n }`, so T42's `"timed"` KeepBlock, `KeepVM.timed` and `TimedVM` are not added. T42 adds only `"outside"` and
   `KeepVM.outside`. T27's "two golden blocks have no KeepBlock" is also covered by `{ caveat }` (T15b).
3. **Possession sheet key** — T15 used `"pos:main"`, T36 `"pos:pos"`. Golden key `pos:<sid>|pos` → `pos:pos` (T36).
4. **`KindVM.sizeRule` (T26) vs `KindVM.size.rule` (T18)** — the golden `size` is a plain string, so neither shape is pinned.
   T18's `size: {text, rule}` was already applied, so it stays and T26's `sizeRule` reads `size.rule`. No duplicate field.
5. **`LadderStepVM` line text: `string` (T19b) vs `Line` (T36)** — the golden is plain text, and `plain(Line)` reproduces it.
   T36's `Line` (with `applies`, `today?`, `days?`) wins. T19b's `book` is kept, and `LadderVM.sections[].steps` uses the same type.
6. **`head.leadN`** — golden stores the string `"2"`; T29 needs a number for `PinAnchor.headlineN`. The VM carries `number | null`,
   and parity compares `String(leadN)`.
7. **`item.sub` (T16) removed by T09** — T16's "sub names it" test read `inp.item.sub`; T09 drops the field (the subtitle is
   words). T16 composes it from `item.kind`, `item.sections`/`partSections` and the part label, and then tests `includes(long)` (T16 updated).
8. **Hooks: `useRegulations(store, target, day, who)` → `Async<RegsPage>` (T23/T35) vs T11's `useRegulations(store, target, mapDay)` →
   `Async<RegsInputs | null>`** — T11 wins: data loading stays separate from presenting. T11 gains
   `useRegsPage(inputs, who, today)`, a `useMemo` over `buildRegsPage`, and T23/T35 call it. A `who` change rebuilds the page from the same
   inputs without a reload. T23 also uses T01's `regsUrl` WaterScreen prop and T11's `openRegsDb`, not its own `regs` prop.
9. **`ReadonlyMap<number, RuleView>` (T30 `GearCtx.rules`, `learnChipWords`) vs `RuleIndex` (T06/T09)** — T09's `RuleIndex` wins;
   `get()` never misses (it returns `noRule`). T30 is updated.
10. **`StatusFrame` per day (T13) vs `DisplayFrame | null` (T04/T09)** — T09 wins. `displayOnBookDay` returns `DisplayFrame | null`;
   a tidal frame or null gives `StripVM` null, which matches golden x31p_1010.
