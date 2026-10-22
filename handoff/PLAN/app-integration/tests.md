# tests.md — how the port is proved: test layers, the golden parity harness, every check.mjs check → its replacement

Target: **regs v33 = `app/design/regs-v11.html` @ `04765ab5`** (round 20i), its committed `app/design/regs-v11/tools/check.mjs`
(**110 `check()` call sites, 111 results**: `map-status-tokens` runs light + dark), goldens regenerated at v33
(`app/design/regs-v11/port/golden/golden.json` meta `{sha:"04765ab5", version:"v33", states:134, sheets:144}`, commit 325a03f4).
Read the checks with `git show 04765ab5:app/design/regs-v11/tools/check.mjs` (line numbers below are that file's).

Ticket ids are README §2's. Test file names for T36–T43 are binding; for T13–T35 they are the names this file assumes
(the ticket that owns the presenter may rename — then update §2 here in the same commit).

---

## 1. Three layers, and the golden parity harness

### 1.1 The layers

| layer | where | runner (vitest project) | what it may assert | what it may not |
|---|---|---|---|---|
| **A. core unit** | `app/packages/core/src/regs/*.test.ts` | `logic` (node) | a presenter's output for a hand-built `RegsInputs` slice (from `core/src/regs/testing/vm-fixtures.ts` / `inputs-fixtures.ts`) — literal expected strings copied from golden.json **with the golden path in a comment**; grammar regexes (tier words, "Rule book p.N ↗"); invariants (no bare 0, each sentence once) | read the store (core may import nothing but siblings + `../answers.generated` etc.; `node:fs` is banned by `layers.json`) |
| **B. golden parity** | `app/conformance/src/regs/*.parity.test.ts` (conformance is outside `layers.json`, so it may import `@app/core` **and** `@app/data/regs/node`) | `logic` | for every golden state (minus the listed exclusions): `buildRegsPage(loadRegsInputs(store, tap), opts)` → `plain()` of each VM block **equals** golden; skipped when the province store is absent | judge layout, colour, px |
| **C. RN render** | `app/packages/ui-native/src/regs/*.test.tsx` | `render` (jsdom + react-native-web, `tools/render-setup.ts`) | structure, order, roles, press → `onOpen(key)`/`onDay`, `allowFontScaling`, hitSlop, style objects' token names (via `palette` identity), one golden-driven mount per tab | pixel geometry (jsdom has no layout: every px check is layer D) |
| **D. manual / browser gates** | `app/tools/compare-regs.mjs`, `app/tools/regs-walk.mjs` (T41), the reader gate (§4) | node + Chrome (cdp, like `regs-v11/tools/cdp.mjs`) against Expo web (`pnpm -C app/apps/mobile web`) at 390×844 | px: heights, overlaps, line counts, colour census on the RENDERED app; human/agent comprehension | run in `pnpm check` (they need Chrome + a dev server; they are ticket acceptance steps and release gates) |

Rule of thumb for porting a check: if it compares **DOM text to snapshot data**, it becomes A (+ B for all states); if it
compares **DOM order/presence/press**, C; if it measures **px or computed colour**, D (or a token lint when the property is
statically knowable in RN — §2 says which).

### 1.2 The parity harness (built by T12; T21/T35/T36 consume it)

Files (T12): `app/conformance/src/regs/golden.ts` (loader + cases), `app/conformance/src/regs/eq.ts` (`eqPlain`, `diffLines`),
`app/conformance/src/regs/exclusions.ts`. If T12 placed the loader under `core/src/regs/testing/`, keep only the pure parts
there (`eqPlain`, the golden JSON types) — the inputs loader needs `@app/data/regs/node` and therefore lives in conformance.

```ts
// app/conformance/src/regs/golden.ts
import type { RegsTarget, Day, Weekday, Who, RegsPage } from "@app/core";
export interface GoldenState { id: string; w: string; d: number; p: number; water: string; header: unknown; keep: unknown;
  gear: unknown; methods: unknown; licence: Record<`${"adult"|"u16"}|${string}`, unknown>; sheets: Record<string, GoldenSheet | "@">; pins: unknown }
export interface GoldenSheet { title: string | null; sub: string | null; folds: { summary: string; open: boolean }[];
  pages: { text: string; pdf: number | null }[]; lines: string[]; nested: string[] }
export interface Tap { item: string; partIx: number; md: number; date: string /* "2026-05-15" */; weekday: Weekday;
  item_ord: number; akey: number; seg: number; mom: number | null; nseg: number; starts: number[]; moments: unknown[] | null;
  s: number; cell: { ladder: number; rows: number; gear: number; licence: number; display: number }; displayByDay: number[] }
export function golden(): { meta: { sha: string; version: string }; states: Record<string, GoldenState>; sheets: Record<string, GoldenSheet> };
export function taps(): { states: Record<string, Tap>; unnamed: { handle: number; akey: number; place: number; name: string }[] };
/** golden id → the app's request. `today` is Oct 10 2026 for every state (the prototype's "today" is d=1010). */
export function requestOf(id: string): { target: RegsTarget; day: Day; weekday: Weekday; today: Day };
/** the store, or null (skip): env REGS_STORE, else data/generated/bundle/regs.sqlite; null when missing or its
 *  meta.store_digest differs from golden.meta's pairing (taps.json meta.store_digest) — a stale store is a skip with a reason, never a fail */
export function provinceStore(): Promise<RegsStore | null>;
/** every case for a block: [id, inputs] minus exclusions(block), memoised per id (an 8 MB golden, 134 states: load once per file) */
export function cases(block: Block): AsyncIterable<{ id: string; g: GoldenState; inputs: () => Promise<RegsInputs>; page: (who?: Who, tot?: "a"|"b") => RegsPage }>;
/** the memoised inputs of one golden state (presenter tickets T13–T19 call their presenter on it before T21 exists) */
export function inputsOf(id: string): Promise<RegsInputs | null>;
/** UPDATE_REGS_FIXTURES=1: write frozen RegsPage JSON for vedder_1010, vedder_615, vedder_515, klwa_1010, bc1_501, glenn_501 to
 *  app/packages/ui-native/src/regs/fixtures/<id>.json — render tests (jsdom, no store) mount these */
export function writeVmFixtures(ids: readonly string[]): Promise<void>;
export type Block = "header" | "keep" | "gear" | "methods" | "licence" | "sheets" | "pins";
```

**Mapping a golden state → the app's request** (`requestOf`): `taps.states[id]` gives `item` (the store item id, e.g.
`gnis:3062`), `partIx`, `date` (`2026-MM-DD`) and `weekday` → `target = {kind:"part", item, partIx}`, `day = {month, day}`
from `date`, `weekday` as given (do NOT recompute from a different year), `today = {month:10, day:10}` (golden header
`day:"Today"` only on d=1010). The tap the app makes must reproduce `taps.states[id].{akey, s, cell}` — T05's
`segmentIndex` parity test asserts that first, so a parity failure is never a tap failure in disguise.
Unnamed sections (`taps.unnamed[]`, 20 handles → `{kind:"section", handle}`) have no golden state; T42 tests them with
literals.

**Profiles**: golden `licence["adult|resident"]` … six keys → `Who {age, residency}`; `licsrc:lic|<sid>|<key>` sheets map
`adult→adult|resident, adult_non→adult|non_resident, adult_alien→adult|non_resident_alien, u16_res→u16|resident,
u16_non→u16|non_resident, u16_alien→u16|non_resident_alien` (golden.mjs `P` table).

**Comparison** (`eq.ts`, contracts §1/§5, golden ≥ b999cfd7): golden text fields and sheet lines carry ONE space wherever two
elements abut (no fused "streamsof"). The app side is `plain()`: **within a `Line` the runs concatenate as they are**; between
separate Lines / VM fields / blocks one space; then `/\s+/g → " "` and trim. `eqPlain` compares those normalised strings
line by line:
```ts
export const norm = (s: string) => s.replace(/\s+/g, " ").trim();
export function eqPlain(actual: readonly string[], expected: readonly string[]): { ok: true } | { ok: false; diff: string };
export function diffLines(a: readonly string[], e: readonly string[], ctx = 2): string;   // unified, line-numbered, ≤ 40 lines
```
`expect(eqPlain(...)).toEqual({ok:true})` fails with the diff as the message, e.g.
```
keep · vedder_1010 · groups[0].tree  (golden app/design/regs-v11/port/golden/golden.json @ 04765ab5)
  @@ line 3 @@
    "Only 2 of the 4 may be from streams, not counting hatchery rainbow trout"
  - "Only 1 of the fish in this box may be over 50 cm (20 in)"            (expected)
  + "Only 1 of the fish in this box may be over 50 cm (20 in) ⓘ"          (actual)
  first difference at char 57: expected end-of-line, got " ⓘ"
```
Each state is its own `it(id)` (`it.each`), so one wrong state never hides the other 133; the test title carries the
state id and the block path.

**Exclusions** (`exclusions.ts`) — a list, each with a reason and the ticket that removes it; an exclusion without a
reason fails `exclusions.test.ts`:
```ts
export const EXCLUDE: readonly { block: Block; ids: readonly string[] | "*"; key?: RegExp; reason: string; until?: string }[] = [
  // (T28) no PINS_ERROR entry: v38 has no pinUpdate throw (UI-NOTES v35); only the Kitimat ruling keys pins anchors.N.tiers
  { block: "sheets", ids: "*", key: /^parts$/, reason: "the prototype's parts sheet carries a prototype-only line ('This prototype has no map of this water…'); T36 compares its rows only" },
  { block: "sheets", ids: "*", key: /^rule:/, reason: "'Technical detail' fold body (key + fields JSON) is compared by structure, not string: JSON.stringify spacing" , until: "never — T36 test asserts the fold exists, closed, and that its first line starts with the key" },
  { block: "header", ids: "*", key: /^place\.shown$/, reason: "which of full/short is shown is a measurement (fitLabel); the VM carries both candidates — T41 checks the choice on Expo web" },
];
export const PINS_ERROR_IDS: readonly string[];   // computed at load: Object.keys(states).filter(id => states[id].pins.error)
```
Golden-side gaps recorded in `golden.meta.misses` (`[data-share-of]`, `:scope > .kb > .ksn`, `h3 .scope`, `.gtag`) are
selectors that matched nothing at v33 — the VM field stays (form b, a kind's own sub-note, the where-not scope, a group
tag) and is pinned by a core unit test with a hand fixture instead.

**Pre-implementation proofs, also sync-time gates** (both run by `port/sync.mjs`; a failure after a prototype bump means
the spec's mapping, not the app, must change first):
- `node app/design/regs-v11/port/pinreplay.mjs` — a pure `pinChain` over golden/pins.json's geometry (port/pins.mjs) and
  golden.json's strings replays every state's samples (134/134 at v38); T28 transcribes its `pinChain`.
- `.venv/bin/python app/design/regs-v11/port/verify_map.py` — decodes the store the way contracts.md says the app will
  (strip runs, frames, rows → tree, …) and checks it against golden for all 134 states.
- `node app/design/regs-v11/port/quotes.mjs` — every string a ticket quotes is still in golden (a miss is a stale ticket).

**As built (T12, 2026-10-21)**: the store is `parityStore()` (not `provinceStore()`, which stays T05's hand-SQL `TapStore`);
`REGS_PARITY=province|fixture|auto` (default auto) picks the live release paired via the province regs.sqlite's
`answers_sha256`, else the committed fixture release held to taps.json's section_handles/reach_digest/rule_ids_sha256 (+ the
province answers when that store is here — the release meta has no `store_digest`). Use `parityGate()` for `describe.skipIf`.
Golden is v38 @ bdda3586: `PINS_ERROR_IDS` is empty (the pins exclusion appears only when non-empty). `keep.strip` is
compared (bar/legend/months/today/status are `display_frame.strip`, a store answer); only `strip.line`/`strip.lines` of the 11
states with a "Trout …" line (`TROUT_LINE_IDS`) are excluded as PENDING until the pipeline ships the per-row year strip
(owner rule 2026-10-21: read it from the store, never derive it).
`writeVmFixtures` is `syncFixtures()` in `conformance/src/regs/fixtures.ts` (from T21). Loaders live in `oracles.ts`.

**Run**: `pnpm -C app test -- regs` (all three layers' regs files); one block: `pnpm -C app vitest run conformance/src/regs/sheets.parity`.
The parity files print one summary line in `afterAll`: `sheets parity: 131/134 states equal · 3 excluded (parts …)`.

---

## 2. Every check in `check.mjs` @ 04765ab5 → its replacement

Columns: **check** · **means** (one line) · **port** = the test that replaces it (`A`/`B`/`C`/`D` = layer, file ·
`test name`) or PROTOTYPE-ONLY (reason) or MANUAL (gate). Paths: `core/…` = `app/packages/core/src/regs/…`,
`rn/…` = `app/packages/ui-native/src/regs/…`, `conf/…` = `app/conformance/src/regs/…`, `tools/…` = `app/tools/…`.

### 2.1 Top level (rounds 9–17; full suite only)

| check | means | port |
|---|---|---|
| no-hand-facts | no `data-src="design"` / "pending" text in the page source | A+C `rn/no-hand-facts.test.tsx` · "every string a regs component renders is a data string or a words.ts constant" (T27; walks the rendered Keep/Gear/Methods/Licence + every sheet of vedder_1010, subtracts data strings, asserts the rest ⊆ `words.ts`) |
| one-sources-line | gear/methods items carry their own source line; licence has exactly one | C `rn/GearTab.test.tsx` · "one SourceLine per item, one for the licence section" (T34/T35) |
| no-barbless-box | no standalone "Single barbless hook ⓘ" item in Gear | A `core/gear.test.ts` · "no gear line is only the barbless term" (T30) |
| nofish-quiet-note | fishway lines come after the groups + Other fish, never in closed colour | A `core/standing.test.ts` · "where-not lines carry no status" (T15) + C `rn/KeepTab.test.tsx` · "WhereNot draws after OtherFish" (T27) |
| pictures-whole | fish images keep the manifest aspect, contain, never clipped; tap opens `sp:CODE` | C `rn/Group.test.tsx` · "FishPic: aspect = manifest w/h, resizeMode contain, press → onOpen('sp:CODE')" (T26); overlap px → D `tools/regs-walk.mjs` "no overlapping pictures" (T41) |
| gear-sentence | gear lines from summary.parts; pages = union of cites; each change shown with when+says; each term gets ⓘ | A `core/gear.test.ts` · "lines/pages/changes/terms = summary" (T30) + B `conf/gear.parity.test.ts` (T35) |
| gear-no-icons | Gear has no icon glyphs; bait + methods blocks exist | C `rn/GearTab.test.tsx` · "no icons; bait and ways blocks present" (T34) |
| licence-last | the licence section is last and once | C `rn/RegsScreen.test.tsx` · "section order keep, gear, methods, licence" (T23) |
| always-as-reference | the "always" rules are a closed Reference fold at the end of Methods | C `rn/MethodsLicence.test.tsx` · "Reference fold last, closed" (T35) |
| place-in-header | header name/label rules (no kind prefix, no section count, action map vs parts, aria) | A `core/header.test.ts` · "place candidates, action kind/aria" (T16) + B `conf/header.parity.test.ts` (T21); positions → D `tools/compare-regs.mjs` (T41) |
| no-part-strip | no part chips under the header | C `rn/RegsScreen.test.tsx` · "no part strip" (T23) |
| map-status-tokens (light) | map overlay status fills = theme tokens | PROTOTYPE-ONLY (the prototype's water-map overlay; the app map has its own style tests: `tools/style-spec.test.ts`) |
| map-status-tokens (dark) | same, dark | PROTOTYPE-ONLY (as above) |
| map-from-pipeline | overlay data generated by `water_map_export` | PROTOTYPE-ONLY (overlay data file) |
| map-steps-and-back | overlay stepping + Escape restores scroll | PROTOTYPE-ONLY (overlay; D1: the app's Map action goes to the Map tab) |
| card-headline | the head's big number = root.n when header is root, else headline.n | A `core/grouphead.test.ts` · "big number" (T17) + B `conf/keep.parity.test.ts` (T21) |
| card-picture-per-species | one picture per species on a kind row | A `core/tree.test.ts` · "pictures = species with a manifest image" (T18) |
| card-cap-once | no repeated line inside a group; the big number is never the total when the total isn't the head | A `core/tree.test.ts` · "each line once per group" (T18) |
| inherited-line | inherited lines = data, with their book pages, before the cards | A `core/standing.test.ts` · "inherited lines + pages" (T15) + C `rn/KeepTab.test.tsx` · "InheritedLine before groups" (T27) |
| braid-review | braid review links in the poster | PROTOTYPE-ONLY (braid review tooling; braids paused) |
| sheet-book-words | glossary sheet: says, quote in a closed "In the book’s words" fold, page links to the right PDF page | A `core/sheets.test.ts` · "gl: says, closed fold, pages" (T36) + C `rn/SheetHost.test.tsx` · "fold closed by default" (T37) |
| bait-ways-compact | bait/ways items in the right ok/no column; compact rows | A `core/bait.test.ts` + `core/methods.test.ts` · "ok/no lists = data" (T31/T32); px → MANUAL (T41 walk "bait rows ≤ 2 px slack", reported, not gating) |
| dated-rows | dated changes in Gear as rows: when + says | A `core/gear.test.ts` · "changes → ChangeVM" (T30) |
| howtell-has-hatchery | How to tell opens the hatch sheet with the hatchery_wild term (say, closed quote, pages) | A `core/sheets.test.ts` · "hatch sheet" (T36) + C `rn/HowToTell.test.tsx` (T38) |
| release-group-pictures | a release-only group lists its members with picture + name | A `core/tree.test.ts` · "release rows carry pictures" (T18) |
| one-worked-list | one "How it adds up" list; here-lines read "<q>: <a>" | A `core/tail.test.ts` · "worked list" (T19) |
| gear-sentence-plain | gear lines have no em dash | A `core/gear.test.ts` · "no — in gear lines" (T30) |
| header-one-shape | the header does not change on scroll | C `rn/RegsScreen.test.tsx` · "header outside the ScrollView" (T23); D T41 walk "header height at 0 and 900" |
| bella-coola-front | snapshot has Bella Coola 1010 + 501 with pills | PROTOTYPE-ONLY (snapshot coverage + side-panel pills); the 501-vs-1010 difference is kept as golden states `bc1_501` vs `bc1_1010` |
| no-lift-box | no legacy lift boxes | PROTOTYPE-ONLY (legacy markup; the VM has no lift type) |
| not-kept-said | not-kept rows: words, status, ✕ only when closed, whose + pages | A `core/tail.test.ts` · "not-kept rows" (T19) |
| gear-no-any-hook | no "any hook" in gear | A `core/gear.test.ts` · "no 'any hook'" (T30) |
| ape-creek-front | snapshot has Ape Creek 1010 + 501 | PROTOTYPE-ONLY (snapshot coverage) |
| licence-u16-you-need | under-16 "You need" list = the profile's data list | A `core/licence.test.ts` · "u16 needs" (T33) |
| presence-in-section | a presence sentence sits on its own kind/not-kept row, said once | A `core/tree.test.ts` · "presence once, on its row" (T18) |
| week-in-words | the weekday Other-fish sheet says week.says, no dates, no verdict words | A `core/sheets.test.ts` · "other: weekday sheet words" (T36) |
| not-kept-merged | not-kept lines equal `not_kept[].says` in order (merged "rainbow trout and cutthroat trout") | A `core/tail.test.ts` · "merged not-kept" (T19) |
| no-bare-zero | no bare "0" number anywhere on the card | A `core/tree.test.ts` · "no num run '0'" (T18) + B `conf/keep.parity.test.ts` "no big 0" (T21) |
| licence-says-residency | the licence says who it is for, before the needs | A `core/licence.test.ts` · "label" (T33) |
| week-sheet-one-story | weekday sheet: one status box (status, says, parts) before "How this was decided" | A `core/sheets.test.ts` · "other: week box first" (T36) |
| week-parts-decided | each week part (days, says, pages, today) in the ladder; restates once | A `core/sheets.test.ts` · "other: week parts + restates" (T36) |
| closed-card-once | on a closed day each sentence is said once (strip, banner, golink) | A `core/banner.test.ts` · "each sentence once" (T14) |
| nearer-row-tribs-front | snapshot has Glenn + Hamill 615 | PROTOTYPE-ONLY (snapshot coverage) |
| gear-lines | labelled lines from summary.parts, mode first, each part once, pages per line | A `core/gear.test.ts` · "lines: mode first, each part once" (T30) |
| worked-last | the worked list is last in a group | A `core/tail.test.ts` · "worked last in group order" (T19) + C `rn/Group.test.tsx` (T26) |
| rule-titles | sources sheet rows headed by the rule's title, never "ALL STEELHEAD" | A `core/sheets.test.ts` · "src: rows titled" (T36) |
| inherited-caution | an inherited caution follows its line | A `core/standing.test.ts` · "caution" (T15) |
| restates-one-line | a row restate is drawn once, inside the won step | A `core/sheets.test.ts` · "other: restates once" (T36) |
| no-quota-table | no quota table / "Check my catch" | PROTOTYPE-ONLY (retired v10 feature; the VM has no such type) |
| bull-trout-row | Bella Coola May 1: a 30 cm bull trout → 1, chip cm + in | A `core/tree.test.ts` · "bc 501 DV row" (T18) |
| card-is-the-caps | drawn numbers = max of the species' table cells = oracle | PROTOTYPE-ONLY (oracle.json internals). Optional data invariant: `pipeline/tests` already proves `group.caps` |
| phone-real-height | the prototype's 390×844 frame and tab bar | PROTOTYPE-ONLY (prototype frame); the app's equivalent is D T41 "390×844 viewport" |

### 2.2 ONLY=18
| check | means | port |
|---|---|---|
| header-compact | header ≤ 120 px; date on the name row; label ≤ 2.5 lines; action on the name row | A `core/header.test.ts` · "label candidates" (T16) + D T41 "header ≤ 120 px, action on name row" |
| group-head-compact | head: title, number, unit on one line; origin + tag; also "or …" | A `core/grouphead.test.ts` · "head words/unit/origin/tag" (T17); height → D T41 |
| tree-folders | full tree walk: tier words + numbers, kinds, folded, chips, also, one picture strip | A `core/tree.test.ts` · "tree = golden" (T18) + B `conf/keep.parity.test.ts` · "groups[].tree" (T21); flat/indent → C `rn/Group.test.tsx` "children not indented" (T26) |
| own-tier-why-here | an own tier's sub says why_here, not replaces | A `core/tree.test.ts` · "own tier sub" (T18) |

### 2.3 ONLY=19
| check | means | port |
|---|---|---|
| header-label-two-lines | full label if it fits 2 lines else short; action ≥ 44×44 | A `core/header.test.ts` · "full/short candidates" (T16) + C `rn/RegsHeader.test.tsx` "numberOfLines 2 + onTextLayout picks" (T23) + D T41 "action ≥ 44" |
| size-tier-box | size tier = hairline-framed section holding its children; tier origin words | A `core/tree.test.ts` · "size band + origin" (T18) + C `rn/Group.test.tsx` · "SizeBand: borderTop/Bottom hairline, no bg" (T26) |
| size-note-on-kind | a size note sits inside its size tier | A `core/tree.test.ts` · "size note placement" (T18) |
| origin-no-tag | no tag → origin words only | A `core/grouphead.test.ts` · "origin without tag" (T17) |

### 2.4 ONLY=ur1 / ur2 / ur3
| check | means | port |
|---|---|---|
| rule-book-label | every book link reads "Rule book p.N ↗" (or bare p.N under "Rule book") | A `core/line.test.ts` · "plain(book run)" (T00) + B every parity file (the string is in every golden line) + C `rn/RuleBookLink.test.tsx` (T34) |
| ur1-tier-words | tier word grammars (total/share/size/count) | A `core/tree.test.ts` · "tier grammars" (T18) |
| ur1-plain-words | closed not-kept wording; duty wording; "up to 1 kg (2.2 lb)"; KLMB lines | A `core/tail.test.ts` + `core/gear.test.ts` · "plain words" (T19/T30) |
| howtell-current-water | the hatch sheet opens over the current water's header | C `rn/SheetHost.test.tsx` · "sheet over RegsScreen keeps the header mounted" (T37) |
| ur2-headline-per-angler | every tree head reads "N a day per angler" | A `core/grouphead.test.ts` · "per angler" (T17) |
| ur2-worked-plain | Vedder's two here-lines, exact | A `core/tail.test.ts` · "vedder worked" (T19) — v33 line 2: "If you keep 2 hatchery rainbow trout: none from other streams today — up to 2 more of any kind, from here or from lakes." |
| ur2-kind-words | DV / ST kind words, presence, duty "Stop fishing the Vedder River after 2 a day" | A `core/tree.test.ts` · "vedder kinds" (T18) |
| ur2-closed-until | closed day Gear = "No fishing here until Jul 1." only | A `core/gear.test.ts` · "closedUntil" (T30) |
| ur2-bait-allowed-note | the roe row names its ban; no "still OK" on banned rows | A `core/bait.test.ts` · "roe names ban" (T31) |
| ur3-howtell-steelhead | the hatchery-only ST kind carries "How to tell ›" → hatch | A `core/tree.test.ts` · "ST link run" (T18) + C `rn/Group.test.tsx` · "How to tell press → onOpen('hatch')" (T26) |
| ur3-buffer-feet | Vedder where-not: exactly the two fishway lines | A `core/standing.test.ts` · "vedder whereNot" (T15) |
| ur3-count-part-of | count tier "1 cutthroat or rainbow trout in all (part of the 2)" | A `core/tree.test.ts` · "count tier" (T18) |
| ur3-lifted-share | Vedder share tier exactly "Only 2 of the 4 may be from streams, not counting hatchery rainbow trout" | A `core/tree.test.ts` · "vedder share" (T18) |
| ur3-combined-plain | KLWA kokanee restate sentence; never "combined daily quota" | A `core/sheets.test.ts` · "klwa restates" (T36) |

### 2.5 ONLY=20 / 20b / 20c(+20d)
| check | means | port |
|---|---|---|
| r20-header-is-total | the root tier is drawn in the head (words, n, book, why) | A `core/grouphead.test.ts` · "root in head" (T17) |
| r20-share-inside-total | share tiers exact; no "count toward…/(part of)" share wording | A `core/tree.test.ts` · "share words" (T18) |
| r20-big-numbers | tier numbers ≥ head number size; size tier second number = n + extra.n | A `core/tree.test.ts` · "big2 = n + extra.n" (T18); font size → C `rn/Group.test.tsx` "TierHeader num style = TYPE.answer" (T26) |
| r20b-number-once | a tier whose only child repeats its number is folded | A `core/tree.test.ts` · "folded kinds" (T18) |
| r20b-book-once | a book page shown once per branch | A `core/tree.test.ts` · "pages once per branch" (T18) |
| r20b-worked-ends | Vedder here-lines kept `[[4,2]]`, ≤ 2 here-lines | A `core/tail.test.ts` · "worked ends" (T19) |
| r20b-howtell-inline | the How to tell button sits inline in the kind sentence | C `rn/Group.test.tsx` · "link run is a nested Text" (T26); height → MANUAL (T41 walk, reported) |
| r20d-pinned-stack | pin stack under the header; lines by scroll geometry; ≤ 90 px | A `core/pins.test.ts` · "pinChain(anchors, y) = golden samples" (T28) + C `rn/PinBar.test.tsx` (T29) + D T41 "stack ≤ 90 px" |
| r20d-flat | no side borders / indents on tiers; one text edge | MANUAL (T41 compare-regs side-by-side) + C `rn/Group.test.tsx` · "no borderLeft, no paddingLeft on children" (T26) |
| r20d-compact-head | head ≤ 66 px + lead; total on one line | MANUAL (D T41 measure on Expo web) |
| r20c-style-tokens | ≤ 4 font sizes, ≤ 4 weights, caps only on kickers, hit areas ≥ 32 | C `ui-native/src/regs/style-tokens.test.tsx` (T40, DONE): a type census of the rendered Keep tab (`regs/testing/textCensus.ts`) on vedder_1010 · x35p_1010 · klwa_1012 · glenn_615; hit areas are `a11y.test.tsx`'s 44 × 44 (`regs/testing/a11yAudit.ts`). 200 % text laid out in Chrome: `tools/regs-text-scale.test.ts` (T40) |

### 2.6 ONLY=20e / 20f / 20g
| check | means | port |
|---|---|---|
| r20e-four-tabs | four tabs, one row, pressed one, scroll-to lands under the header | A `core/header.test.ts` · "tabs" (T16) + C `rn/RegsHeader.test.tsx` · "tablist, aria-selected" (T23) + C `rn/PinBar.test.tsx` · "tab press → scrollTo(anchor − header)" (T29) |
| r20e-dated-on-its-line | each change sits on its line (v33: vedder 515 mode "Jul 1–Apr 30: one hook, lure or fly and weight on the line up to 1 kg (2.2 lb)"; cowichan p1 "Nov 16–Jul 3: …") | A `core/gear.test.ts` · "change attaches to its line" (T30) |
| r20e-sources-per-item | each gear item has its own source line; keys/pages = its cites | A `core/gear.test.ts` · "SourceLineVM per item" (T30) |
| r20e-ways-columns | Methods: allowed (with condition) above not allowed | A `core/methods.test.ts` · "ways ok/no" (T32) |
| r20e-licence-selectors | age × residency → label + needs | A `core/licence.test.ts` · "6 profiles" (T33) + C `rn/MethodsLicence.test.tsx` · "WhoSelector radios" (T35) |
| r20e-region-vs-stream | the region scope tag moves to the share; pins say "across <region>" then "from streams" | A `core/tree.test.ts` · "scope tag on share" (T18) + A `core/pins.test.ts` (T28) |
| r20f-pictures-first | pictures come first after the sentence + chip | A `core/tree.test.ts` · "KindVM order" (T18) + C `rn/Group.test.tsx` · "child order" (T26) |
| r20f-headers | tier text edge = head edge; size sections hairlines | MANUAL (T41 compare) |
| r20f-region-words | share words; pin "Trout and char · 2 a day from streams (of 4 across Region 2)" (v33: the bracket may wrap) | A `core/tree.test.ts` + `core/pins.test.ts` · "region words" (T18/T28) |
| r20g-palette | per tab ≤ 6/5/5/4 text colours, all tokens; status colours only on status; numbers ink; one tint | C `rn/palette.render.test.tsx` (T39) + MANUAL light/dark/CVD census on Expo web (T39 `tools/regs-census.mjs`) |
| r20g-changes-line | change line: Now/Next chip, "<dates>:" first, "· in force today" iff now | A `core/gear.test.ts` · "ChangeVM plain" (T30) |
| r20g-possession-fold | possession sheet: plain ≤ 2 sentences, example, closed fold "In the rule book’s words", source | A `core/sheets.test.ts` · "pos: blocks in order" (T36) + C `rn/SheetHost.test.tsx` (T37); height < 300 → D T41 |

### 2.7 ONLY=20h
| check | means | port |
|---|---|---|
| r20h-one-surface | no non-card background ≥ 200×40 inside a group or the pin bar | C `rn/palette.render.test.tsx` · "one surface" (T39) — static: no `backgroundColor` other than `palette.card` on group containers |
| r20h-picture-surface | fish pictures and their parents transparent | C `rn/palette.render.test.tsx` · "pictures transparent" (T39) |
| r20h-pin-drops | a pinned tier line ends when the next head is in view | A `core/pins.test.ts` · "drops" (T28) |
| r20h-total-relation | form a: big = headline n + "of the N a day …"; form b: "↳ N of them …" | A `core/grouphead.test.ts` · "form a/b" (T17) (D12) |
| r20h-roe-names-ban | roe row names its ban; no "the ban" note; no roe in bait changes | A `core/bait.test.ts` (T31) |
| r20h-possession-own-example | the possession sheet lists the card's own examples, generic only as fallback | A `core/sheets.test.ts` · "pos: own examples" (T36) |

### 2.8 ONLY=20i (v33, active)
| check | means | port |
|---|---|---|
| r20i-size-rule-chip | a kind with `size_rule` shows it as the chip, exactly | A `core/tree.test.ts` · "size_rule chip" (T18) |
| r20i-other-size-hint | an Other-fish row with a size rule says "· size limit" | A `core/others.test.ts` · "other-fish size hint" (T19) |
| r20i-duty-line | duty line names the water ("Stop fishing the Vedder River after 2 a day"), full weight ink; ST row has "Steelhead stamp ›" → Licence | A `core/tree.test.ts` · "duty words + stamp link" (T18) + C `rn/Group.test.tsx` · "duty style = bodyStrong ink; stamp press → onTab('licence')" (T26) |
| r20i-licence-stamps | stamps: no ST stamp where ST closed; "(not kokanee)"; "Only if…" under its head | A `core/licence.test.ts` · "stamps" (T33) |
| r20i-closed-span | strip says "Closed Jun 1–30" (not "Closed from") | A `core/strip.test.ts` · "closed span" (T13) |
| r20i-all-year-ban | an all-year bait ban reads "All year: bait ban", now | A `core/bait.test.ts` · "all year" (T31) |
| r20i-glare | `--sub` vs `--card` ≥ 6.5:1; release text bold | A `tools/cvd.test.ts` extension · "sub on card ≥ 6.5:1 in light and dark" (T39, D3) + C `rn/Group.test.tsx` "release weight 700" (T26) |
| r20i-scroll-margins | nothing scrolls under the pin stack | C `rn/PinBar.test.tsx` · "scrollTo offset = header + pin stack height" (T29) |

### 2.9 Counts
111 rows = 111 results (110 names; map-status-tokens light + dark). By primary replacement: **unit (A, most with a
B parity companion) 82 · render (C) 14 · manual (D) 3 · prototype-only 12** (11 names). The manual three are r20d-flat,
r20d-compact-head and r20f-headers; px halves of A/C rows (`bait-ways-compact`, `r20b-howtell-inline`, `header-compact`,
`group-head-compact`, `pictures-whole` overlap, `r20g-possession-fold` height) are reported by T41's walk — none of them
gates `pnpm check`; T41's walk + §4 gate the release.

---

## 3. Arriving with the next sync

Nothing pending at 04765ab5: the eight round-20i checks above are committed and mapped. The next prototype bump follows
README §1's re-sync: `node app/design/regs-v11/port/sync.mjs --ref <commit>`, then diff the check names:
`diff <(git show 04765ab5:app/design/regs-v11/tools/check.mjs | grep -o "check('[^']*'" ) <(git show <new>:… | grep -o …)` —
each new name gets a row in §2 in the same commit as its test; a retargeted check (a changed expected literal) changes only
the literal in its A test (the B parity test picks it up from the regenerated golden by itself).

---

## 4. The comprehension reader gate (manual, release-blocking)

Method of rounds r20g/r20h/r20i (`handoff/PLAN/r20h_report.md` "Readers", `handoff/PLAN/r20i_report.md` §Readers):
a **fresh** opus reader (a new agent, no history), **PNG screenshots only**, a fixed question set, answers + confidence 1–5 +
"what confused you". The prototype's score at v33 is **39 / 39** on the 39-question set (+ 14 / 14 UR4 questions).

1. **Question set.** T41 copies `…/scratchpad/r20i/prompt-gate39.md` (39 questions; it names its PNGs) and
   `…/scratchpad/r20i/reader-prefix.md` into `app/design/regs-v11/port/reader/` (committed — the scratchpad is per session),
   rewriting only the two folder paths. The questions are not edited for the app.
2. **Shoot the app** (`node app/tools/compare-regs.mjs --reader OUT_DIR`, T41): Expo web (`pnpm -C app/apps/mobile web`,
   store served by `tools/serve-tiles.mjs` `/regs.sqlite`), Chrome via cdp at **390×844 DPR 2**, fonts loaded (not offline).
   For each PNG name in the prompt the tool has a state → `{item, partIx, date, tab, sheet?, who?, scroll}` table
   (`port/reader/states.json`, derived from `regs-v11/tools/shoot.mjs` + `frames.mjs` STATES): TALL = the whole tab as one
   image (the app's ScrollView content measured and shot in one tall viewport), FRAMES = consecutive 844-px screenfuls with
   90 px overlap, stepped back so no line sits under the pin stack (frames.mjs's rule). Same file names as the prototype set.
3. **Run the reader** — the human launches it (it is an agent run): one fresh reader on the app set with the same prompt.
4. **Pass bar:** score ≥ the prototype's (39/39 at v33), **no question whose answer differs from the prototype reader's**,
   and no confidence drop of ≥ 2 on any question. A miss is a bug against the ticket that draws the confusing thing.
5. **File results** in `handoff/PLAN/app-integration/reader/YYYY-MM-DD-<version>.md`: version + app commit, the PNG folder
   path, the reader's answers verbatim, the score, the diff vs the prototype reader, and the tickets opened.

Run it when a prototype version's report says a reader round was run on it (README §1 step 5) and before any release.

---

## 5. Existing app gates every new ticket keeps green

| gate | command | what a T36–T43 change must respect |
|---|---|---|
| boundaries | `node tools/check-boundaries.mjs` | core imports nothing; ui-native may import core/data/ui/react/react-native/react-native-svg; **conformance** holds parity tests; asset `require()` only in `apps/mobile` (T38/T43 images go through the image map T20 registers) |
| platform | `node tools/check-platform.mjs` | no `document.`, `window.localStorage`, `Alert.alert`, `accessibilityState` in shared packages — use `aria-checked`/`aria-selected`/`aria-expanded`; a `.native`/`.web` pair must be complete (T43 `openBook.native.ts` + `openBook.web.ts`) |
| style | `pnpm style` | a token change → `pnpm style:build` + commit the regenerated `packages/map/style/*` (T39 D3) |
| colours | `node tools/check-colours.mjs` | **no colour literal** outside `packages/map/style/{tokens,themes/*}.json`; fish drawings ship as raster assets (T38) |
| spinner/fixture/gauge-policy/calendar | as in package.json | untouched |
| deps | `node tools/check-deps.mjs` | any new dependency needs a `deps.md` line (T41 adds none to the app: Playwright is NOT used, cdp is) |
| typecheck | `pnpm typecheck` | — |
| theme.test | `packages/ui-native/src/theme.test.ts` | new palette keys (T39) get a contrast assertion |
| cvd.test | `tools/cvd.test.ts` | status colours stay apart under every deficiency; T39 adds "sub on card ≥ 6.5:1" |
| test | `vitest run` | every regs test above |

Each ticket's Acceptance = `pnpm -C app check` + its own vitest filter + (render tickets) its D step.
