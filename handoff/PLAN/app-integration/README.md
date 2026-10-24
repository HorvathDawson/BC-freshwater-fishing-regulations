# App integration — execution spec for the Regulations page (regs v33 → the app)

2026-10-20. Supersedes the *how* of `~/.claude/projects/…/handoff/PLAN/APP-INTEGRATION.md` (2026-10-19, kept for the
why). Nothing in `app/` source, `pipeline/`, `regs-v11.html` or `check.mjs` was changed to write this; the only code added
is the port tooling in `app/design/regs-v11/port/` (committed, run, outputs in `app/fixtures/golden/`).

| file | what |
|---|---|
| `README.md` | this: order, conventions, how a ticket is verified, how to re-sync with the prototype |
| `facts.md` | the code as it is (exact signatures), and 25 places the 10-19 plan was wrong |
| `contracts.md` | `RegsPage` (the view model every component draws), `Line`/`Run`, `RegsInputs`, field → answers map |
| `tokens.md` | prototype tokens light/dark vs the app theme, the type scale, spacing, per-role styles |
| `tests.md` | every check.mjs check → its unit / render test or manual gate; the golden parity harness; the reader gate |
| `decisions.md` | owner questions with a default so work never blocks; which tickets each one touches |
| `tickets/NN-*.md` | the ordered backlog, ≤ ~1 day each |
| `self-check.md` | three tickets dry-run against the real code |

## 0. Prerequisites: app prep + the minimal release (added 2026-10-20)

This spec now depends on `handoff/PLAN/app-data/` (revision 2, 2026-10-20):
- `DATA-MAP.md`: every data product, from source to screen.
- `MINIMAL-PACKAGING.md` (revision 3): a few small gzip files in `data/release/`, separate from the build artifacts
  in `data/generated/`:
  - **paint** (0.88 MB: the status index + flow panels) is the only file before first paint;
  - **core** (1.11 MB: names, search, sid → water);
  - **water** (regs-common + part topology, 1.94 MB);
  - **regs-<region>** × 9 (frames, 0.19–1.32 MB each est.);
  - **regs-sid** (1.22 MB), **conditions** (2.66 MB), **near** (1.99 MB).
  Native is always offline: it holds the full tiles + every file, and the update check downloads only changed files.
  Web fetches them lazily; a tap in a new region fetches that region. Every access is keyed and measured. Tiles are
  geometry-only and versioned separately.
- `APP-PREP.md`: tickets N1–N2 (the native map), P01–P18, P20, P21.
- `QUESTIONS.md`.

The owner decided (2026-10-20):
- the prototype layout, with the **bottom bar hidden in water mode**;
- (SUPERSEDED 2026-10-22, owner call 3, T71: the view strip is the one Map entry; no header Map, no "Conditions here")
  "Conditions here" **next to Map**, opening the midpoint section of the **part being read**, and one back rule to the
  normal map views;
- the map's up/downstream navigation drawn from the tiles; no geometry store;
- **a few small files, each with a reason**, written by one bundling step into `data/release/` (apart from the build
  artifacts), fast to query, gzip;
- **tiles versioned separately**, so a data release never rebuilds them;
- **native-first** (Expo/React Native), with web compiled from the same tree. The native map is built early (N1, N2);
- native always offline with an update check that downloads only changed files;
- the middle-man JSON is debug-only (P20).

**Gate before phase 0:**
- **Before T00 / T02 / T05:** P03 (CI runs `answers --check`) and P06 (one Feb 29 rule; T05's `weekdayOf`).
- **Before T01 / T03 / T04 / T10 / T11:**
  - P08 (`deliver release`);
  - P11 (the release reader: native offline install/update, web lazy).
  - The regs store is NOT a file beside `/province.sqlite`. Its tables are split across the **water**,
    **regs-<region>** and **regs-sid** release files, opened over the `Db` seam (decisions D22).
- **Before T16 / T23 / T29:**
  - N2 (the native map);
  - P13 (route state);
  - P14 (water-mode shell);
  - P15 (the water map).
- **Before any release build:** P01, P02, P04, P05, P07, P18.
- **Every RN ticket below is accepted on native first (iOS + Android dev builds), then web parity.**

| integration ticket | what changes because the data source / shell changed |
|---|---|
| T01 | No `/regs.sqlite` route and no `REGS` constant. The water screen receives the `ReleaseLoader` (P11) and asks it for `water`, `regs-<region>` and `regs-sid` on first need (native: already on disk). The props plumbing (`day`, `section`) stays. |
| T03 | `openRegsStore(loader)` opens a store view over three release files. The format/version checks read the water file's `meta`; the digest checks compare every file's `section_handles` with the manifest's tile version (P10 `pair`); `rule_ids_sha256` is checked across the three. A mismatch refuses the whole release (keep the last good pair). |
| T04 | The same reads, routed by table: `akey`/`seglist`/`part`/statics → water; `cell` + frames → the `regs-<region>` file named by water's `akey_region`; `section_akey`/`section_place` → regs-sid. The per-access latency budgets are in MINIMAL-PACKAGING §3 (A1–A3, A10). |
| T05 | Feb 29 reads Feb 28's reading day with its own real weekday (P06, D31). The `taps.json` parity gains the 2036-02-29 (Friday) Zymoetz case. |
| T09 | `targetFor(section)` resolves a tapped sid through core `item_section` → water `part_section` (named), or regs-sid (unnamed). |
| T10 | The fixture is the fixture RELEASE (`deliver release --items …`, P11). It also replaces `packages/data/dev/bundle.sqlite`. D13 is amended. |
| T11 | `useRegsStore(loader)` opens lazily on the first water screen. A "loading the regulations for Region N" state appears only on web when a shard is not cached. |
| T12 | The parity harness reads the release files (P20 stops writing `regs.sqlite`; until then either works). |
| T16 | `placeAction` gains `"conditions"`. The header carries `[Map] [Conditions here]` (owner). **Superseded (T71, call 3):** the header carries `[Parts]` or nothing; Map is the view strip's. |
| T23 | `RegsScreen` mounts inside P14's `WaterMode`. There is no FaceBar row (D2 amended) and no bottom bar (D1 amended). `onConditions` opens P14's Conditions view at `mid_sid`, and `onMap` opens P15's water map. |
| T29 | The pin bar's top is the regs header alone. |
| T41 | The parity walk is native first, then web. It starts from the map, enters water mode, uses Map / Conditions here, and backs out. |
| T42 | The unnamed-section door is P17 #3. "Conditions here" on an unnamed section uses the section itself. |

## 1. The frozen target and the generators

**Target: regs v33 = `app/design/regs-v11.html` at commit `04765ab5`** (UI-NOTES "regs v33 (round 20i …)", check.mjs
111 checks). It is the latest committed prototype AND the one whose snapshot was taken from the data the built store holds
(rows 26 · gear 12 · licence 6 · display 16 · glossary 5 — the store's `meta.layout.versions`). The spec started on v32
(`18bc2a11`); v33 landed mid-writing and was absorbed by re-running the generators — the procedure below, for real. Some
ticket line refs may still read `@18bc2a11`: function NAMES are authoritative, `app/fixtures/golden/inventory.json` has the current
ranges. The spec never copies prototype strings by hand; it points at generated files, so a new version flows in by
re-running one command:

```sh
node app/design/regs-v11/port/sync.mjs --ref <commit>      # ≈ 25 s; needs Chrome, .venv, data/generated/bundle/regs.sqlite
```

| generator | output (`app/fixtures/golden/`) | what the port uses it for |
|---|---|---|
| `golden.mjs` | `golden.json` (8.8 MB): per state (134 = 41 waters × days × parts) the header, every Keep block (strip, inherited, banner, groups with the whole tree, not-kept, worked, other fish, possession, where-not), Gear, Methods, Licence for all 6 (age × residency) profiles, every sheet the state can open (+144 global sheets: rules, glossary, fish, how-to-tell), and the pin bar sampled every 8 px with the anchors it was measured from | the expected strings of every core presenter test; the pin fixtures |
| `tokens.mjs` | `tokens.json`: every `:root` custom property as computed (light, dark), the app theme values, a diff, and a style census per role (font family/size/weight/line-height, colour **named by token**, background, border, padding) | `tokens.md`, the RN `StyleSheet`s, the palette render gate |
| `inventory.mjs` | `inventory.json`: every top-level + nested function of `regs-v11.html`'s script and `page_v36.js`, line range, body hash; `--diff old.json` lists changed bodies | the "ports" line refs in tickets; which tickets to revisit after a bump |
| `taps.py` | `taps.json`: per golden state the item id, part, date, weekday → the store's item_ord, akey, segment starts, moments, segment index, the five frame ids, the display frame of each of the 366 days; 20 unnamed handles → akey, place, name | the phase-0 parity oracle (Python reference decoder vs the TS one) |
| `hydrate.mjs` | `hydrate.json` (1.3 MB): page v36's own hydration run in node (lines 1–242, YEAR 2026) per state — `describe()` of the 726 rules the states cite, `settle()` (status, s, broad, inpart, lifted, standing, anglerclosed, timed, side), `buildModel()` rows, `dayStatus` per row; exits 1 unless its `s` equals taps.json on 134/134 | the expected values of T06–T08 (the hydration layer has no other golden) |
| `pinreplay.mjs` | — (prints `pin replay: N / 134`) | the reference pure `pinChain` over `golden.pins.anchors`; reproduces every golden pin transition on 134/134 states — T28 transcribes it. If a bump drops the count, `pinUpdate` changed: port the delta |
| `verify_map.py` | — (prints passed / 134 per check) | proves the contracts.md store → view mapping BEFORE any TS: strip runs, status word, which rows are drawn as groups (TROUTISH split, none on a closed day), the other-fish rows, every tier line = `TreeTier.says`, every kind sentence starts with `TreeKind.says` — all 134/134 (130 tiers, 273 kinds) at v33 |
| `quotes.mjs` | — (prints `quotes: N / M found`) | every page sentence a ticket quotes as an expected value, looked up in the goldens; after a bump it names the tickets whose expected strings went stale |
| `stage.mjs` | — | stages the prototype at `--ref` so a dirty working tree never leaks in (`--ref WORKTREE` reads it in place) |

`sync.mjs` keeps the previous outputs, regenerates, and prints: changed function bodies (→ tickets that port them), per
block how many states' goldens changed (→ which app tests will fail), changed token values (→ `tokens.md`/themes).
Proved on the v32 → v33 bump (2026-10-20): the inventory diff reported `buildKeep, buildKeep.sizeOf, buildKeep.kt,
buildKeep.bigWords, buildGear, buildGear.note2, render, pinUpdate` changed and `buildKeep.dutyWords` added (page_v36.js
unchanged); the golden diff: keep 93, gear 96, licence 107, sheets 94, pins 77 of 134 states changed (e.g. the size chip
"30 cm (12 in) or longer" → "keep 30 cm (12 in) or longer; release the rest", "Single barbless hook" → "Single barbless
hook, no trebles"); tokens: `--sub` #5F666C → #555C62, dark #A3ABB2 → #B0B8BF. Nothing in the spec's prose had to change
except the version line — the tickets cite golden paths.

**Re-sync procedure (a prototype version bump):**
1. `node app/design/regs-v11/port/sync.mjs --ref <new commit>`; read its report.
2. If `meta.misses` gained a selector, the prototype renamed a class — update `EXTRACT` in `golden.mjs` (the only hand-kept
   part of the generators) and re-run.
3. For each changed function in the inventory diff, open the ticket whose "Ports" section names it; port the delta in the
   core presenter; run that ticket's test (it now reads the new golden). For a changed token, follow `tokens.md` §4.
4. `pnpm -C app test -- regs` until green; commit `app/fixtures/golden/` and the app change together, message "regs vNN".
5. The comprehension reader gate (tests.md §4) only when the version's report says a reader round was run on it.

## 2. Order

```
Prep (app-data/APP-PREP.md)   N1 → N2 ; P01–P07 (parallel) → P18 → P08 → P09 → P10 → P11 ; P12 ; P13 → P14 → P15 ; P16 P17 ; P20 P21
                               phase 0 waits for P03 P06 P08 P11; T16/T23/T29 wait for N2 P13 P14 P15 (§0)
Phase 0  data + decoder        T00 → T01 → T02 → T03 → T04 → T05 → T06 → T09 → T07 → T08 → T10 → T11 → T12
                               (T09 before T07/T08: their parity tests need loadRegsInputs; T05 starts conformance/src/regs/golden.ts,
                                T09 adds requestOf/inputsOf, T12 completes it)
Phase 1  Keep tab (static)     T13 T14 T15 T15b T16 T17 T18 T19 T19b T20 → T21 ; T22 → T23 → T24 T25 T26 → T27
Phase 2  pinned stack + tabs   T28 → T29
Phase 3  Gear/Methods/Licence  T30 T31 T32 T33 → T34 → T35
Phase 4  sheets, dark, a11y    T36 → T37 → T38 ; T39 T40 T41 T42 T43
```
T13–T20 (incl. T15b, T19b) are independent of each other once T12 lands (parallel). All golden PARITY tests live in
`app/conformance/src/regs/*.parity.test.ts` (core may not import `@app/data`/`node:fs`); core `*.test.ts` are unit tests over
hand-built inputs; render tests mount frozen VM fixtures written by `UPDATE_REGS_FIXTURES=1` (tests.md §1.2). Phase 3's core tickets (T30–T33) may start after T12; its
RN tickets after T27. See each ticket's "Depends on".

| # | ticket | phase |
|---|---|---|
| T00 | core `regs/` skeleton: model + line + words, `regulations.ts` rewritten | 0 |
| T01 | serve `/regs.sqlite`, `REGS` URL, prop plumbing to the water screen | 0 |
| T02 | blob codec (fflate dict-inflate) + UTF-8 decode | 0 |
| T03 | `openRegsStore`: format, digests, section versions, refusal; `Db` export; aliases | 0 |
| T04 | store reads: items, parts, picker, akey, segments, cells, frame codecs, rules, statics, section blocks | 0 |
| T05 | core tap: `segmentIndex`, `weekdayOf`, `profileIndex`; taps.json parity; `answers --check` in `pnpm check` | 0 |
| T06 | core hydrate 1: rule views, species names, `describe()` | 0 |
| T07 | core hydrate 2: ladder verdicts, `settle()` context | 0 |
| T08 | core hydrate 3: rows model (`rowsAt`, `hydRow`, `buildModel`) | 0 |
| T09 | `loadRegsInputs()`: the whole tap → `RegsInputs` (+ the year's display frames) | 0 |
| T10 | regs fixture store (subset tool) + conformance suite | 0 |
| T11 | ui hooks: `useRegsStore`, `useRegulations`, `useAnglerProfile`; the one day | 0 |
| T12 | golden parity harness (loader, per-state inputs cache, `eqPlain`) | 0 |
| T13 | presenter: year strip + status line | 1 |
| T14 | presenter: closed banner + chain | 1 |
| T15 | presenter: inherited line, where-you-can't-fish, possession row | 1 |
| T15b | presenter: caveat / in-part blocks ("Fishing is different in some spots today", 9 states) | 1 |
| T16 | presenter: header (labels, date words, action, tabs) | 1 |
| T17 | presenter: group head (title, scope kicker, forms a/b, origin line) | 1 |
| T18 | presenter: the quota tree (tiers, size band, kind rows, pictures) | 1 |
| T19 | presenter: not kept, How it adds up, other-fish list | 1 |
| T19b | presenter: page v36 plain rows (non-pool rows drawn without a card, 31 rows) + the shared "How this was decided" ladder | 1 |
| T20 | fish manifest → `core/regs/fish.generated.ts` + the app's image map | 1 |
| T21 | `buildRegsPage()` Keep slice + Keep golden parity on all states | 1 |
| T22 | theme + type tokens for the page | 1 |
| T23 | RN: `RegsScreen` in `WaterScreen`, `RegsHeader`, the one scroll | 1 |
| T24 | RN: `YearStrip` | 1 |
| T25 | RN: `ClosedBanner`, `InheritedLine`, `WhereNot`, `PossessionRow` | 1 |
| T26 | RN: `GroupHead`, `OriginLine`, `TierHeader`, `SizeBand`, `KindRow`, `FishStrip`, `NotKeptRow`, `HowItAddsUp` | 1 |
| T27 | RN: `OtherFish`, `KeepTab` composition, Keep render gates | 1 |
| T28 | core: `shortOf`, `pinChain` + golden pin fixtures | 2 |
| T29 | RN: anchors, `PinBar`, tab-in-view, tab scroll-to | 2 |
| T30 | presenter: gear lines, changes line, item sources, closed note | 3 |
| T31 | presenter: bait list | 3 |
| T32 | presenter: ways, boats, reference fold | 3 |
| T33 | presenter: licence (selectors, label, answer, sources) | 3 |
| T34 | RN: `GearLines`, `ChangesLine`, `SourceLine`, `RuleBookLink`, `ClosedNote`, `YesNoList`, `BaitList` | 3 |
| T35 | RN: `WaysList`, `Boats`, `ReferenceFold`, `WhoSelector`, `LicenceAnswer`; Gear/Methods/Licence parity | 3 |
| T36 | presenter: every sheet | 4 |
| T37 | RN: `SheetHost` + sheet bodies | 4 |
| T38 | RN: `AdiposeArt`, How to tell | 4 |
| T39 | dark theme + palette gate (light/dark/CVD) | 4 |
| T40 | a11y, 200 % text, hit areas, style-token gate | 4 |
| T41 | web parity: side-by-side shots, Playwright walk, reader gate | 4 |
| T42 | edge states: unnamed section, tidal, outside B.C., hours windows | 4 |
| T43 | Rule book links on native, fish credits + permissions | 4 |

## 3. Conventions every ticket follows

- **Layers.** Pure logic in `app/packages/core/src/regs/*` (no imports but `../answers.generated`, `../statusIndex`,
  `../calendar.generated`, siblings). Store access in `app/packages/data/src/regs/*`. Hooks in `app/packages/ui/src/regs.ts`.
  Components in `app/packages/ui-native/src/regs/*` — **no logic**: a component receives a VM slice + `Palette` and draws it.
- **Words.** Every string a presenter emits is either a data string (frame, static, rule) or a constant in
  `core/src/regs/words.ts`. Never inline an English literal in a presenter or component (the `no-hand-facts` render test
  subtracts data strings and asserts the rest ⊆ `words.ts`).
- **One presenter per prototype function family**, named after it; each file header lists the prototype/page-v36
  functions it ports with `inventory.json` names (not line numbers — those are in the ticket for the frozen version only).
- **Tests.** Core: `*.test.ts` beside the file (project `logic`), reading `app/fixtures/golden/golden.json` through
  `core/src/regs/testing/golden.ts` (T12), skipped with `it.skipIf(!hasProvinceRegs())` when the province store is absent
  (as `hasDevBundle()` does). RN: `*.test.tsx` in `ui-native/src/regs/` (project `render`) over hand-built VMs from
  `core/src/regs/testing/vm-fixtures.ts` + one golden-driven mount per tab.
- **Verifying a ticket:** its "Acceptance" block — always `pnpm -C app check` green plus the named vitest filter; for a
  presenter, 100 % of golden states equal (or an explicit, listed exclusion with its reason).
- **Commits:** one ticket per commit, `git commit -- <paths>` (other agents work in this tree — never `git add -A`).
- **Never** run the LLM parser or anything calling the `claude` CLI; heavy jobs only via
  `~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/handoff/tools/heavy.sh`.

## 4. Ticket template

```
# TNN — title
Phase · Depends on · Blocks · Decisions (D#) · Size (≤ 1 day)
## Goal            one paragraph: what exists after, what the reader of the app sees (if anything)
## Files           create / modify, full paths
## Signatures      every exported TS type/function, verbatim
## Ports           prototype / page_v36 functions (inventory names + line refs @ 04765ab5), and what changes in the port
## Reads           the RegsInputs / answers field paths
## Tests           file, and each case: fixture (state id × profile) → expected (golden path or literal)
## Acceptance      commands
## Notes           traps found while writing the spec
```
