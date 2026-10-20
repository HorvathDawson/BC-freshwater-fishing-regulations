# IMPLEMENT-HANDOFF — start here (app data packaging · app prep · regs prototype port)

Written 2026-10-20, documentation only. This is the single entry point. It links to the detailed specs and does not
repeat them. Where two plans disagree, §8 says which one wins.

Read next, in this order: `AGENTS.md` → this file → `app-data/APP-PREP.md` §0–§1 → `app-data/MINIMAL-PACKAGING.md` →
`app-integration/README.md` → the ticket you are on. Background: `app-data/DATA-MAP.md`, `app-data/QUESTIONS.md`,
`app-integration/{decisions,contracts,facts,tests,tokens,self-check}.md`, and the audit at
`~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/handoff/PLAN/AUDIT-2026-10-20.md`.

---

## 1. Goal

1. Replace today's "download the whole 103 MB `bundle.sqlite`" with a few small gzip release files, written by one
   bundling step into `data/release/`, with tiles versioned on their own.
2. Make the Expo/React Native app ready for the port: a real native map, one data reader (native fully offline, web
   lazy), route state with water mode, and no logic in components.
3. Port the regulations prototype (`app/design/regs-v11.html`) into the app. It is proved state by state against
   generated goldens (`app/design/regs-v11/port/golden/`), on native first and then on web.
4. Every string the app shows comes from data or from `core/src/regs/words.ts`. Nothing is hand-copied.
5. The work is done when the native and web apps pass the golden parity tests, the performance budgets and the
   comprehension-reader gate.

### Owner decisions (2026-10-20; binding)

Sources: `app-data/QUESTIONS.md` (Q1–Q19), `app-integration/decisions.md` (D1–D32), and the memory note
`app-port-direction`.

- **Native-first.** Build the Expo/React Native app; web is compiled from the same tree with Expo web. Every RN ticket is
  accepted on native (iOS + Android dev builds) first, then checked for web parity. (D28)
- **Build the native map.** Use MapLibre Native through an RN binding, behind the existing adapter.
  `Map.native.tsx` is a placeholder today. Web and native use the same pmtiles bytes, style and feature ids; only the
  source (R2 range reads vs a local file) and the renderer differ. (Q9; N1, N2)
- **The native app is always offline.** First run installs the full pmtiles and every data file (resumable,
  sha-verified). An update check diffs the manifest by sha and downloads only the changed files, then swaps atomically.
  Tiles are downloaded only when the tile version changes. (Q5; P10, P11)
- **Several small packages, each with a stated reason:** `paint` (0.88 MB gz, the only file needed before first paint),
  `core`, `near`, `water`, `regs-<region>` × 9, `regs-sid`, `conditions` (MINIMAL-PACKAGING §2). (Q1, Q2; D22)
- **Regulations are split by region and fetched on demand on web.** A tap in a new region fetches that region's file;
  native holds all of them. (Q3)
- **Geometry lives only in the tiles.** There is no geometry store. Highlights use feature-state on `part_section` sids;
  the camera comes from `part_geo`; navigation comes from `part_mark` / `water_exit`. (D23; P09, P15)
- **Tiles are geometry-only and versioned on their own** (`<handles16>.<tilebuild12>`). Each data manifest declares
  `tiles.compatible`, a data-only release never rebuilds tiles, and on a mismatch the app keeps the last good pair.
  (Q1; D27)
- **The outside-BC mask stays a separate small file** (`bc_outside.geojson`, 62.5 KB) in the tile version folder,
  listed in the manifest. It never goes into the tiles. (Q16; D32)
- **gzip everywhere.** Brotli was measured and was not "a LOT better". (Q4; D30)
- **Build artifacts stay in `data/generated/`**, apart from the stripped release dir `data/release/<r>/`.
  `regs.sqlite` is not shipped. (Q12; MINIMAL-PACKAGING §1)
- **Regs mode:**
  - The bottom bar is hidden while a water is open.
  - The header reads `‹ · name · date · [Map] · [Conditions here]`.
  - "Conditions here" opens the midpoint (`part_geo.mid_sid`) of the part being read.
  - Back always returns to where you came from: the tab, camera, query and drill are restored.
  - No FaceBar.
  - (Q6–Q8; APP-PREP §0; supersedes D1/D2)
- **Prototype layout.** Header tabs Keep · Gear · Methods · Licence over one scroll; the pinned stack. (D1 answer)
- **Conditions shows the same content, better formatted.** Riffle is not involved. (Q10; P16)
- **Feb 29 reads Feb 28's regulations, with its own real weekday.** The year stays at 365 days. (Q14; D31; P06)
- **answers/export JSON is debug-only** (`--debug-json` → `data/generated/debug/`). (Q11; D29; P20)
- **Stocked/surveyed layers are greyed out and disabled** until their data exists. (Q15; P17 #6)
- **Self-hosted glyphs and sprites.** The tile step writes `map-assets-<n>/`; nothing is fetched from a CDN at run
  time. (Q18; P18)
- **Tile size work is deferred** (atlas z13 overzoom, basemap landuse filter, about −345 MB est.). It is scheduled
  before the first public native release. (D32)
- **Query speed budgets are part of acceptance.** Every hot access is keyed; the budgets are in MINIMAL-PACKAGING §3
  (A1–A11), e.g. tap → regs page drawn < 100 ms. (§4 below)
- Also settled: the card gate runs on the shipped answers in `deliver all` and blocks packaging (D26; P05). The day is
  one state, `mapDay` (D7). The profile lives in memory per session (D8). Native rule-book links use `Linking.openURL`
  (D6). Stop writing `stats.json` and delete `record()` (Q16; P20). Stop writing `blk_chains.pkl` (Q17; P21).

---

## 2. Execution order (every ticket)

Format: **id** · goal · depends on · ticket · acceptance. Paths are relative to `handoff/PLAN/`. `AP` means
`app-data/APP-PREP.md` (the line number of the ticket's heading is given); `T/` means `app-integration/tickets/`.
★ = critical path. ∥ = can run in parallel with the item above it.

There is **no P19** (the numbering skips it).

### Stage 0 — test tiers (do this FIRST, before anything else)
0. ★ **P00** · split the tests into a fast SMOKE set and the FULL set (owner: the tests take far too long) · needs
   nothing · this doc §3 rule 4 · Measure the current durations first (pytest `--durations=50`, vitest per-file times,
   check.mjs per check, the card gate, the build). Then:
   - pytest: a `smoke` marker on a small representative set (one test per gate/invariant, no live-bundle scans), run
     with `-m smoke`; mark the slowest tests `slow`, and make the default CI run fast-but-complete;
   - vitest: a `smoke` project or filter; check.mjs: an `ONLY=smoke` list of ~10 checks covering every round;
   - a `make`-style entry point per tier (e.g. `scripts/test-smoke.sh`, `scripts/test-full.sh`) and a one-line doc in
     AGENTS.md. Acceptance: Tier 1 (smoke) ≤ 2 min wall clock on the laptop; Tier 2 unchanged in coverage (same tests,
     just run rarely); a table of before/after times in the commit message. No test deleted without the owner's OK.

### Stage A — native map, safety and gates (start together)
1. ★ **N1** · MapLibre Native spike: local pmtiles, feature-state, taps, frame time; device factor for A1–A11 ·
   needs nothing · AP L116 · `app-data/N1-SPIKE.md` gives go/no-go and numbers on an iPhone and a mid Android. The
   expo-sqlite-from-`documentDirectory` timing is part of this.
2. ★ **N2** · the native map behind the adapter; web/native parity · N1 · AP L153 · `pnpm -C app check`; on device:
   status colours, tap opens a water, frame time within N1's budget; web Playwright walk unchanged.
3. ∥ **P01** · spots store: no silent loss, atomic save, serialised writes · needs nothing · AP L203 · vitest
   `spots SpotCapture`.
4. ∥ **P02** · feed freshness: no fabricated `fetchedAt` · needs nothing · AP L258 · vitest `feed freshness`.
5. ∥ **P03** · CI gaps: generated-TS paths, app-ci = `pnpm check` + the `--check`s, fixture schema test · needs
   nothing · AP L291 · a TS-only edit to `answers.generated.ts` fails CI.
6. ∥ **P04** · retire `update-hydro/in-season/stocking.yml` · needs nothing · AP L314 · only `app-ci.yml` and
   `pipeline-ci.yml` remain.
7. ★ **P05** · the card gate runs on the shipped answers inside `deliver all` and blocks · needs nothing · AP L336 ·
   `deliver all --out-dir <tmp>` (heavy) passes and stamps; `test_card_gate_in_chain.py`.
8. ∥ **P06** · Feb 29: one calendar rule for page, app and pipeline · needs nothing · AP L368 · on 2036-02-29 every
   reader returns day 59 with weekday Friday (the Zymoetz closure applies).
9. ∥ **P12** · logic and sentences out of components (3 commits) · needs nothing · AP L686 · `check`; no sentence
   templates in `DonorPanel`/`ConditionsPanel`.
10. ∥ **P13** · `useRoute`: browse / water stack, one back rule · needs nothing · AP L731 · vitest `route Shell`.
11. ∥ **P21** · stop writing `blk_chains.pkl` · needs nothing · AP L1017 · `test_no_slivers.py -m slow` at the next
    human-run atlas build. Deleting the live file needs the owner's OK.
12. ∥ **T22** · theme and type tokens (D3 `sub` from `port/golden/tokens.json`) · needs nothing ·
    T/T22-theme-type-tokens.md · `style`, `colours`, cvd vitest.

### Stage B — packaging (the bundling step; MINIMAL-PACKAGING §8)
13. ★ **P07** · bundle format version + atomic `deliver all` (out-dir, promote all, `vintage.report_all`) · P05 ·
    AP L430 · `pytest -k "deliver_atomic or vintage"`.
14. ★ **P18** · tiles geometry-only and versioned; mask + glyphs/sprites in the tile version folder; `region_id` fix ·
    needs nothing (code); the ≈1 h tile rebuild is HUMAN-RUN · AP L940 · tile tests after the rebuild, promoted file by
    file; a data-only `pipeline build` dry run skips tiles; no CDN request.
15. ∥ **P09** · `part_geo` (with `mid_sid`), `part_mark`, `water_exit` · needs nothing · AP L507 ·
    `test_part_geo.py -m needs_bundle` on a side bundle (never on the live one).
16. ★ **P08** · `deliver release` → `data/release/<r>/` + manifest; parity, perf and size gates · P05, P07, P18 (P03 for
    CI; P09's tables are copied in once they exist) · AP L451 · `test_release*.py -m needs_bundle` (unfiltered),
    `emit_pack --check`, vitest `release-perf`.
17. ★ **P10** · endpoints config (no `localhost` literals), `fetchVerified`, manifest v2, `diff`/`pair`; `publish`
    (human-run) · P08, P18 · AP L564 · vitest `fetchVerified manifest`; a device build reads the manifest from the LAN
    dev server.
18. ★ **P11** · the release reader: native install, update check and atomic swap; web lazy; device perf · P08, P10, N2 ·
    AP L613 · airplane-mode relaunch; a data-only release downloads only the changed files; A1–A11 within budget; web
    cold start = manifest + paint ≈ 0.9 MB.
    - **Publish (human-run):** after P10/P11, `pipeline/tools/publish.py` uploads tiles + assets, then data, then
      `manifest.json` last (MINIMAL-PACKAGING §6.4).

### Stage C — shell and map for the port (needs N2)
19. ★ **P14** · water-mode shell: bottom bar hidden, header, "Conditions here" at `mid_sid`, back · P13, P11, P09, N2 ·
    AP L775 · native walk: Map → Vedder → Conditions here → ‹ → ‹ restores the camera; tap → water mode < 100 ms.
20. ∥ **P15** · water map: part highlight from tiles, ▼/▲, marks, mouth exit · N2, P09, P11, P13 · AP L818 · side by side
    with the prototype for Vedder, Cowichan and KLWA; a ▼/▲ step < 50 ms.
21. ∥ **P16** · Conditions: the same content, better formatted · P02, P12 · AP L865 · before/after shots (native, then
    web) for 3 sections; owner review.
22. ∥ **P17** · navigation/UI clean-up; stocked/surveyed greyed out · P13 · AP L911 · a manual walk of every tab and back
    path, recorded in the commit.
23. **P20** · retire the middle-man JSON; no `regs.sqlite`; drop `stats.json` + `record()` · P08, P11, and the port
    tooling switched to the release files · AP L983 · `pytest -m "not slow"`; `sync.mjs --ref HEAD` runs on the
    packages.

### Phase 0 — data and decoder (gate: P03 + P06 before T00/T02/T05; P08 + P11 before T01/T03/T04/T10/T11)
24. ★ **T00** · core `regs/` skeleton; `regulations.ts` rewritten · P03, P06 · T/T00-core-regs-skeleton.md · vitest
    `packages/core/src/regs`.
25. ∥ **T02** · blob codec (fflate dict-inflate) · T00 · T/T02-blob-codec.md · vitest `data/src/regs/blob`.
26. ★ **T01** · plumb the `ReleaseLoader` to the water screen (**no `/regs.sqlite` route**, README §0) · P08, P11 ·
    T/T01-serve-and-plumb.md · vitest `WaterScreen`.
27. ★ **T03** · `openRegsStore(loader)` over water + regs-<region> + regs-sid; digests and refusal · T00, T02, P11 ·
    T/T03-open-regs-store.md · vitest `data/src/regs/store`.
28. ★ **T04** · store reads, routed by table to the three files · T02, T03 · T/T04-store-reads.md · vitest
    `data/src/regs/reads`.
29. ★ **T05** · core tap: `segmentIndex`, `weekdayOf` (P06), `profileIndex`; taps.json parity incl. 2036-02-29 · T00,
    T04 · T/T05-core-tap.md · vitest `tap tap.parity`; `answers` in `pnpm check`.
30. ★ **T06** · hydrate 1: rule views, `describe()` · T00, T04 · T/T06-hydrate-rules.md · `describe.parity` against
    hydrate.json.
31. ★ **T09** · `loadRegsInputs()` · T04, T05, T06 · T/T09-load-regs-inputs.md · vitest `data/src/regs/inputs`.
32. ★ **T07** · hydrate 2: ladder, `settle()` · T05, T06, T09 · T/T07-hydrate-ladder-settle.md · `settle.parity`.
33. ★ **T08** · hydrate 3: rows model · T07, T09 · T/T08-hydrate-card.md · `card.parity`.
34. ★ **T10** · the fixture RELEASE (`deliver release --items …`), which replaces `dev/bundle.sqlite` + conformance · T03,
    T04, T09 · T/T10-regs-fixture-conformance.md · deterministic re-run, no diff.
35. ★ **T11** · hooks `useRegsStore(loader)`, `useRegulations`, `useAnglerProfile`; one day · T01, T03, T09, T10 ·
    T/T11-ui-hooks.md · vitest `ui/src/regs`.
36. ★ **T12** · golden parity harness · T05, T09, T10 · T/T12-golden-parity-harness.md · green with `REGS_PARITY=fixture`.

### Phase 1 — Keep tab (T13–T20 run in parallel once T12 lands)
37. **T13** strip + status · T07, T08, T09, T12 · T/T13-strip.md · 134/134.
38. **T14** closed banner + chain · T06, T07, T09, T12, T13 · T/T14-closed-banner.md · 134/134.
39. **T15** inherited, where-not, possession · T06–T08, T12 · T/T15-inherited-wherenot-possession.md · 134/134.
40. **T15b** caveats / in-part · T06, T07, T12 · T/T15b-caveats-inpart.md · 9/9 blocks, 0 extra.
41. **T16** header (`placeAction` gains `"conditions"`) · T09, T12, T13 · T/T16-header.md · header.parity 100 %.
42. **T17** group head · T06, T08, T12 · T/T17-group-head.md · 97/97 groups.
43. **T18** quota tree · T17, T20 · T/T18-tree.md · 130/130 tiers, 273/273 kinds (+ `verify_map.py`).
44. **T19** not kept, How it adds up, other fish · T17, T18, T20 · T/T19-notkept-worked-otherfish.md · 97/97, 134/134.
45. **T19b** plain rows + the "How this was decided" ladder · T06–T08 · T/T19b-plain-rows.md · 31/31.
46. **T20** fish manifest → `fish.generated.ts` · T00 · T/T20-fish-manifest.md · `fish:build` + check.
47. ★ **T21** `buildRegsPage()` Keep slice · T07–T09, T12–T20 · T/T21-build-keep.md · 134/134 with only the listed
    exclusions.
48. ★ **T23** RN `RegsScreen` **inside P14's `WaterMode`** (no FaceBar, no bottom bar) · T11, T16, T21, T22, P14, P15, N2 ·
    T/T23-regs-screen-header.md · render tests; native walk, then web.
49. **T24** `YearStrip` · T13, T22, T23 · T/T24-year-strip.md · render test + side-by-side check.
50. **T25** banner / inherited / where-not / possession components · T14, T15, T22, T23 ·
    T/T25-banner-inherited-wherenot-possession.md · render test + platform.
51. **T26** group components · T17–T19, T22, T23 · T/T26-group-components.md · render test.
52. ★ **T27** `OtherFish`, `KeepTab`, Keep render gates · T19, T21, T23–T26 · T/T27-otherfish-keeptab.md · 4 waters side
    by side with the prototype.

### Phase 2 — pinned stack and tabs
53. **T28** core `shortOf`, `pinChain` (may start after T12/T17/T18) · T/T28-core-pins.md · `pinreplay.mjs` 134/134 =
    vitest `pins`.
54. ★ **T29** RN anchors, `PinBar`, tab scroll (the pin top is the regs header alone) · T28, T23 ·
    T/T29-pinbar-anchors-tabs.md · render test + a slow manual scroll.

### Phase 3 — Gear / Methods / Licence (core T30–T33 may start after T12; RN after T27)
55. **T30** gear lines · T/T30-gear-lines.md · 134/134 `gear`.
56. **T31** bait list · T30 · T/T31-bait-list.md · 134/134 `gear.bait`.
57. **T32** ways, boats, reference · T30, T31 · T/T32-ways-boats-reference.md · 134/134 `methods`.
58. **T33** licence (`patchNeeds()` per D21) · T04, T05 · T/T33-licence.md · 804/804.
59. **T34** RN gear components · T27, T30, T31 · T/T34-rn-gear.md · render test.
60. ★ **T35** RN methods + licence; G/M/L parity · T11, T21, T27, T30–T34 · T/T35-rn-methods-licence-parity.md ·
    `gml.parity` + render.

### Phase 4 — sheets, dark mode, a11y, release gates
61. ★ **T36** every sheet presenter · T06–T08, T12, T15 · T/T36-presenter-sheets.md · `sheets.parity`.
62. ★ **T37** `SheetHost` + bodies · T36, T23, T22, T20, T34 · T/T37-sheet-host.md · render test; possession sheet
    < 300 px.
63. **T38** adipose art, How to tell · T36, T37, T20 · T/T38-adipose-art-howtell.md · render test + side by side.
64. ∥ **T39** dark theme + palette gate (owner looks at the fish once, D4) · T22, T27, T35, T37 ·
    T/T39-dark-theme-palette-gate.md · palette/cvd vitest + dark census.
65. ∥ **T40** a11y, 200 % text, hit areas, style-token gate · T23–T29, T35, T37, T39 · T/T40-a11y-text-scale-style-gate.md
    · vitest a11y/style-tokens/hit + a font-scale shot.
66. ∥ **T42** edge states: unnamed, tidal, outside B.C., hours · T05, T09, T13–T21 · T/T42-edge-states.md · `edge.parity`.
67. ∥ **T43** book links, fish credits, permissions · T20, T25/T34, T36/T37 · T/T43-book-links-credits-permissions.md ·
    `check-fish-credits.mjs --ship` must pass before a store build.
68. ★ **T41** native-first parity walk, web walk, reader gate · T27, T29, T35, T37–T40 ·
    T/T41-web-parity-walk-reader.md · `regs-walk.mjs` all PASS in light and dark; reader gate passes (§4).

**Release gate (before any release build):** P01, P02, P04, P05, P07 and P18 are done; the T43 `--ship` check and the
reader gate pass.

### Critical path and parallelism
- **Critical path:** P05 → P07 → P18 (+ the human tile rebuild) → P08 → P10 → P11 → T01/T03 → T04 → T05 → T06 → T09 →
  T07 → T08 → T10 → T11 → T12 → (T13–T20) → T21 → T23 → T27 → T29 → T35 → T36 → T37 → T39/T40 → T41.
- **A second long pole joins it at P11:** N1 → N2 (P11, P14, P15 and T23 all need N2). Start N1 on day 1.
- **Parallel while packaging runs:**
  - P01–P04, P06, P12, P13, P16, P17, P21 and T22;
  - P09;
  - T00 → T02 (only P03/P06 gate them);
  - T06's pure core and T20.
  The data-touching tickets (T01, T03, T04, T10, T11) wait for P08 + P11.
- **After T12:** T13–T20, T28 and the core tickets T30–T33 and T36 are independent of each other.

---

## 3. Ground rules (non-negotiable)

1. **Never run the LLM parser or the `claude` CLI** (`run_parse.sh`, `pipeline.regs.parsing.dispatch`, `claude -p`).
   Give the owner the command instead. "Run it" means *they* will.
2. **Heavy jobs run one at a time, through**
   `~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/handoff/tools/heavy.sh <label> <min_free_pct> -- <cmd>`.
   Use 70 for atlas/reach and 40 for deliver/pytest; use ≤ 4 workers.
   - "Heavy" means atlas, reach, `deliver`/`pipeline build`, full pytest, the golden regeneration (`sync.mjs`), answers
     builds and the node harnesses.
   - A heavy job needs the owner's go-ahead first. While `~/.claude/heavy-job.pause` exists, do light work only.
   - The atlas, tile and fixture rebuilds named in the tickets are human-run.
3. **Test-first.** Write a short CI test that fails before the change and passes after it (record the "fails before"
   evidence). Mutation-pin gates that could be tautologies (memory `checks-seeded-from-input-launder`).
4. **Tests are tiered; long tests run rarely** (owner, 2026-10-20: "the tests are too long… long tests run less often,
   a faster set run more frequently"). Three tiers:
   - **Tier 0, while iterating (seconds):** only the test file / vitest filter / `ONLY=…` check for what you touched.
   - **Tier 1, the SMOKE set, per ticket commit (target ≤ 2 min total):** `pnpm -C app check` without the slow bits, plus
     the pytest smoke marker set, plus `ONLY=smoke` in check.mjs. Built by ticket **P00** (below), first thing.
   - **Tier 2, the FULL set, once per phase or before a merge (never per edit or per ticket):** the full fast + slow
     pytest, the full check.mjs, the card gate on shipped answers, the golden re-sync, the comprehension reader. Perf
     gates run here too.
   Never rerun a tier on an unchanged tree. If a Tier 2 run fails, fix it and rerun only the failing files, then Tier 2
   once more at the end.
5. **Gating runs are unfiltered.** Never decide on an `rtk` test run (it once showed a red suite as clean). Use
   `.venv/bin/python -m pytest …` / `pnpm -C app exec vitest run …` directly (or `rtk proxy …`), and read the summary
   counts. `rtk` is fine for iterating.
6. **Commit only your own paths:** `git commit -m "…" -- <paths>`. Never `git add -A` or `git commit -a`; another agent
   works in this tree. Check the files-changed count. One ticket per commit.
7. **Never pipe `rtk ls`/`ls` into `rm`/`xargs`.** The hook appends sizes. Use `find … -maxdepth 1` or `/bin/ls`, and
   name files explicitly. Never replace the tiles dir; promote `atlas.pmtiles` + `atlas.meta.json` + `layers/*.geojsonl`
   file by file. Keep a known-good copy before any delete.
8. **Zero hand-written facts.** Every displayed string is a data string or a constant in `core/src/regs/words.ts`
   (the `no-hand-facts` render test). Counts come from data; a missing value renders nothing (AGENTS 32, 33). No
   reader may read curated files at run time; the release is the only source (memory `bundle-is-the-only-source`).
9. **Card gate = 0 problems** on the shipped answers, always (P05). A non-zero result blocks packaging.
10. **Perf budgets are acceptance criteria**, not aspirations (MINIMAL-PACKAGING §3). No hot access may plan a scan.
11. **Native first, then web parity**, for every RN ticket and every map ticket.
12. **The prototype is the source of truth.** Never copy prototype strings into tickets or tests by hand; read the
    goldens. When the prototype changes, re-sync:
    `node app/design/regs-v11/port/sync.mjs --ref <commit>` (heavy; ≈ 25 s; needs Chrome, `.venv` and the province
    store), then follow README §1's re-sync procedure. Do not edit `app/design/regs-v11*` or
    `pipeline/deliver/answers/` yourself; that is the prototype/data agent's lane.
13. **Layers** (AGENTS 22–28): logic in `core`, hooks in `ui`, components draw a VM and nothing else. Map layers are
    added only via `layers.source.json`. Every dependency gets a line in `app/deps.md`.
14. **Tiles and data share a vintage.** Before you trust a colour, check `vintage.report_all(strict)` and the
    `tiles.compatible` pairing (memory `tiles-bundle-same-vintage`).
15. **Report what you did not verify** (AGENTS 44).

---

## 4. How to verify

| gate | command | pass |
|---|---|---|
| app gate | `pnpm -C app check` (boundaries → platform → style → colours → spinner → fixture → gauge-policy → calendar → deps → typecheck → test; P03/T05 add `answers`) | exit 0, read unfiltered |
| card gate | `.venv/bin/python -m pipeline.deliver.answers.card_check` (P05 adds `--all` + the JSON report; inside `deliver all` after P05) | `problems: 0` |
| answers model | `.venv/bin/python -m pipeline.deliver.answers.model --check` (+ `emit_calendar --check`, `emit_gauge_policy --check`, `emit_pack --check` after P08) | exit 0 |
| prototype checks | `node app/design/regs-v11/tools/check.mjs` (`ONLY=<round>` while iterating) | all pass (130/130 at v37) |
| golden parity | `pnpm -C app exec vitest run conformance/src/regs` (province store) and `REGS_PARITY=fixture …` (CI) | each ticket's N/N in §2 |
| data → view map | `.venv/bin/python app/design/regs-v11/port/verify_map.py` | 134/134 per check (130 tiers, 273 kinds) |
| pins | `node app/design/regs-v11/port/pinreplay.mjs` | 134/134 |
| quotes | `node app/design/regs-v11/port/quotes.mjs` | no stale ticket quotes after a sync |
| perf | vitest `release-perf` (laptop: p95 ≤ 2 ms for A1/A3/A5/A6/A7/A9, ≤ 5 ms for A2, paint decode ≤ 30 ms, `EXPLAIN QUERY PLAN` shows only SEARCH); device: A1–A11 on the N1 phones; tap → page drawn < 100 ms; tap → water mode < 100 ms; ▼/▲ < 50 ms | all within budget |
| vintage | `vintage.report_all(strict=True)` (P07) | every file's `section_handles` = the held tile version's |
| pipeline suites | `.venv/bin/python -m pytest pipeline/tests -q` (+ `-m slow` when the atlas/reach/tiles change) | read the counts line |

**Comprehension-reader gate** (manual and release-blocking; full procedure in `app-integration/tests.md` §4):
1. Copy the 39-question prompt and the reader prefix into `app/design/regs-v11/port/reader/` (T41 step 1). Today they
   exist only in a session scratchpad (§5).
2. Shoot the app with `node app/tools/compare-regs.mjs --reader OUT` at 390×844, DPR 2: TALL images plus 844 px
   FRAMES, with the same names as the prototype set.
3. The **owner launches** one fresh Opus reader, with no history and only the PNGs, using the same prompt.
4. Pass: the score is ≥ the prototype's (**39/39 at v37**, and 8/8 on the UR6 questions); no answer differs from the
   prototype reader's; no confidence drops by ≥ 2.
5. File the results in `app-integration/reader/YYYY-MM-DD-<version>.md`.
6. Run it before a release, and whenever a prototype version's report says a reader round was run on it.

**What "done" means for each stage:**
- **Prep:** every N/P acceptance above passes. `data/release/<r>/` is produced by `pipeline build`. Native installs and
  updates offline from the LAN dev server. Web cold start is manifest + paint only.
- **Phase 0:** T12's harness is green on the province store and on the fixture release. taps.json parity holds,
  including the 2036-02-29 Zymoetz case.
- **Phase 1:** Keep parity 134/134, apart from listed exclusions that have reasons. The Keep tab renders on native and
  web side by side with the prototype for the Vedder, Glenn, Bella Coola p1 and KLWA.
- **Phase 2:** the pin replay is 134/134 in core, and a manual scroll matches the prototype's pin behaviour (UR6: a pin
  stays until the section's last row has gone under the stack).
- **Phase 3:** gear, methods and licence parity are 134/134/804; the RN tabs render.
- **Phase 4:** the sheets are at parity; the dark/CVD palette, a11y, 200 % text and edge states pass; the
  `regs-walk.mjs` walk passes in light and dark; the T43 `--ship` check passes; the reader gate passes.

---

## 5. Current state (2026-10-20)

- **Branch** `redesign/stream-sections`, HEAD `cc73e5ef`. `main` is untouched; nothing is merged.
- **Key commits:**
  - `4b02d27b`: the plans.
  - `8e09563f`, `93ed8e3a`: app-data rev 3 and the mask decision.
  - `1581bc6e`: old prototypes removed.
  - `04765ab5`: regs v33, the target the integration spec was written against.
  - `b7294e63`: UR6 data, rows 28 / display 17.
  - `d4a0ccd9`: **regs v37**, the last committed prototype.
  - `cf500b33`: goldens re-synced to v37; verify_map 130/130, 273/273, 134/134.
  - `cc73e5ef`: regs-diagram rebuilt.
- **Final prototype round (UR7, DONE):** data `813ec879` + `fdce06e4` (rows 29), page **regs v38** `bdda3586` +
  `7fb6809f`, goldens re-synced `7ba60cfd` (verify_map full marks; the golden label says "v38 @ bdda3586", generated from
  the `7fb6809f` tree). **The port target is `7fb6809f` (regs v38).** Published. Its final runs: fast suite 3462 passed /
  6 skipped (unfiltered), check.mjs 136/136, card gate 0. On day 1, confirm the goldens are current (§7).
- **Data versions, live store** (`data/generated/bundle/regs.sqlite`, `regs-store/4`):
  - rows **29** (UR7), gear **12**, licence **6**, display **17**, ladder **1**;
  - glossary **5** (`pipeline/deliver/answers/glossary.py` `VERSION`);
  - handles `d768d6e135e62b99`, reach `cad1c21fe6eb9ba9`, rule ids `16be720be08b0fca`.
  - The goldens are at v38 (7ba60cfd): 134 states, 144 sheets, 6 profiles.
- **Published links:**
  - prototype: https://claude.ai/artifact/VmvnzcpAcpxWU2bgsQM7z2
  - reference page: https://claude.ai/artifact/K2CoUr5UYj9Uj9xAFheAyS
  - diagrams: https://claude.ai/artifact/EbLF9vd7DhccM8RGtuWUKn
  - v37 itself was committed "NOT published"; check the prototype link's version before you compare against it.
- **Known caveats:**
  - **Suites:** the last full fast suite (UR7) passed unfiltered on the final state; still run it once unfiltered as your
    day-1 baseline.
  - **App suite:** the audit ran no vitest or `pnpm check`, and whether the app suite is green is unknown. Establish a
    baseline on day 1.
  - **Native map:** a placeholder (`app/packages/map/src/Map.native.tsx:16-26`). Whether MapLibre RN supports pmtiles
    and feature-state is **not verified** (that is N1).
  - **Expo SQLite:** opening SQLite files from `documentDirectory` in Expo is **an unproven spike**. Its timing belongs
    to N1 and P11.
  - **URLs:** they are `localhost` literals today (`App.tsx:35-37,57,75,77`). P10 adds the config. The R2
    bucket/domain and credentials are the owner's (publish is human-run).
  - **Sizes and timings:** package sizes and perf numbers are **laptop estimates** made on scratchpad copies (`est.`);
    the phone factor is unmeasured until N1.
  - **Scratchpad material:** the reader prompt (`…/scratchpad/r20i/prompt-gate39.md`, `reader-prefix.md`) and the
    packaging scripts (`…/scratchpad/pack/`) live in a session scratchpad under `/private/tmp`, which a reboot wipes.
    Copy the reader files into the repo early (T41 step 1).
  - **Golden extraction:** `golden.json` `meta.misses` lists 4 selectors (`[data-share-of]`, `:scope > .kb > .ksn`,
    `h3 .scope`, `.gtag`). After the final sync, compare them with the previous misses; a new miss means updating
    `EXTRACT` in `golden.mjs` (README §1 step 2).
  - **Stale line refs:** APP-PREP line refs are at `49c1f299`, and ticket line refs at `04765ab5`. Function NAMES are
    authoritative; `port/golden/inventory.json` has the current ranges.
  - **CI:** CI does not yet run the answers `--check`s (P03), and the dev fixture is 6 tables behind `schema.sql`
    (audit #5).

---

## 6. Open questions (unanswered across the docs)

**ANSWERED by the owner 2026-10-20** (do not re-ask):
1. **Fish image permissions:** the owner HAS permission for every image. Not a ship gate; drop that part of T43.
2. **Salmon images:** recoded (chum CM, coho CO, pink PK, sockeye SK) in `bb47bde5`. Show them where the species appears.
3. **D19 `klmb_1010` pooled row:** still technical: default is a data-side `group`; verify at T19b/T21 whether rows 27/28
   already resolved it.
4. **D18 bait `also_allowed` (dead fin fish as bait):** keep HIDDEN.
5. **D21(b):** move the licence patches (`patchNeeds()`) into the pipeline (default stands; technical).
6. **Prototype-side items:** the licence default STAYS (no change; the user changes the selectors); the pinned size line
   staying to its section's last row is FINE; Glenn's worked line, Chilliwack p2's rainbow row and stamps on each fish's
   section are done by the final prototype round (see §5).
7. **Audit leftovers:**
   - the lake splitter: REMOVED (`167aa27b`);
   - the "unsure tools": ARCHIVE them after confirming nothing runs them, EXCEPT the tools the owner keeps: the DFO dossier
     and the review/curation tools (keep those);
   - `test_answers_reference` (always skips because the module it imports was renamed): FIX it so it runs;
   - #12 glossary curated read at answers time: stamp a digest of the curated book text (technical default);
   - fish image / font trims: DROPPED (the images were already trimmed once).
8. **Devices and hosting:** the owner does physical-device testing and production hosting (R2 bucket, domain,
   credentials) LATER. Use emulators/simulators meanwhile; perf numbers from emulators are provisional.
9. **Heavy jobs:** allowed for this implementer via `heavy.sh`, one at a time, but SPARINGLY (don't let builds dominate the
   timeline; batch changes before a rebuild).
10. **P21:** OK to delete the existing `blk_chains.pkl` once the build stops writing it.

Still open: none.

Already answered elsewhere, so do not re-ask:
- old prototypes and the archive dir (deleted, `1581bc6e`);
- the scheduled workflows (retired, P04);
- the trimmed bundle and the codec (superseded by D22/D30);
- tile zoom and landuse (deferred, D32);
- Feb 29 (D31);
- the native map (build it, N1/N2);
- `regulations.ts` (rewritten by T00);
- the T14 "." chain line, `pinUpdate` throwing on 25/134 states and the tidal Gear text: all fixed in prototype v35.

---

## 7. First day

1. Read `AGENTS.md`, this file, APP-PREP §0–§1, MINIMAL-PACKAGING §1–§3 and §6, integration README §0–§3, and
   `decisions.md`.
2. `git log --oneline -15`. Confirm that `7fb6809f` has been filled and that the other agent's round
   is committed. `ls ~/.claude/heavy-job.pause` (if it exists: light work only). Ask the owner for the heavy-job
   go-ahead.
3. Copy the reader prompt files out of the session scratchpad into `app/design/regs-v11/port/reader/` and commit them
   with `-- <paths>`. Ask the owner first if that folder is in the prototype agent's lane.
4. Get a baseline, unfiltered and once: `pnpm -C app check`; `.venv/bin/python -m pytest pipeline/tests -q`. Record the
   counts in your progress note.
5. Re-sync the goldens to the final prototype, through heavy.sh:
   `node app/design/regs-v11/port/sync.mjs --ref 7fb6809f`. Read the report: changed functions,
   per-block golden changes, token changes, `meta.misses`. Then run `verify_map.py`, `pinreplay.mjs` and `quotes.mjs`.
   Commit `port/golden/`.
6. **The first three tickets** (independent; start all three):
   - **N1:** the native map spike. It is the longest pole and needs devices, so book them now.
   - **P05:** the card gate in `deliver all`. It heads the packaging critical path (P05 → P07 → P18 → P08).
   - **P03:** the CI gaps. Together with P06 it unblocks T00/T02/T05. It is half a day; P06 can follow the same day.
7. Keep a progress note in `handoff/PLAN/` (e.g. `implement_progress.md`): ticket, commit, gate counts, and what was not
   verified.

---

## 8. Contradictions between the plans, and which doc wins

| # | conflict | wins |
|---|---|---|
| C1 | Port target: integration README §1, D5, tests.md, T41 and ticket line refs say **v33 @ `04765ab5`**; the goldens are at **v37 @ `d4a0ccd9`** (`cf500b33`), and a final round is running | **`7fb6809f`, i.e. whatever `port/golden/golden.json` `meta.ref` says after the day-1 sync.** Line refs are stale; function names hold. |
| C2 | Data source: T01 (serve `/regs.sqlite`), T03/T10/T12, T41 and tests.md §4 (store from `/regs.sqlite`; `dev/regs.sqlite` fixture) vs README §0, D22 and P20 | **README §0 + D22:** the release files (water / regs-<region> / regs-sid) through the `ReleaseLoader`; there is no `/regs.sqlite` route, and the fixture is a fixture release. `regs.sqlite` remains only for the port tooling until P20. |
| C3 | Shell: T23 ("under the FaceBar", `TabBar` stays), D1/D2 defaults, T29 (pin top = both rows) vs APP-PREP §0 / P14 | **APP-PREP §0 + P14:** water mode, bottom bar hidden, no FaceBar, `[Map] [Conditions here]` in the header; the pin top is the regs header alone. |
| C4 | Prep order: integration README §2 says "P01–P07 (parallel) → P18 → P08 → P09 → P10 → P11" vs APP-PREP §1 and the ticket headers | **APP-PREP ticket headers:** P05 → P07 → P18 → P08; P09 has no prerequisites (parallel; P08 copies its tables when present); P11 also needs **N2**. |
| C5 | decisions.md "Owner questions" lists the T14 "." line and `pinUpdate` throwing on 25/134 states as open; D20 calls the tidal golden a prototype bug that T42 excludes | **UI-NOTES v35:** all three are fixed in the prototype. Port `pinChain` as "no lines" for a group-less row (not "skip"); T14 does not port the "." bug; T42 drops the x31p_1010 exclusion if the re-synced golden shows the banner words. |
| C6 | Heavy jobs: the README says only "via heavy.sh" | **Memory `heavy-jobs-one-at-a-time`:** via heavy.sh, AND only after the owner's go-ahead, AND never while `heavy-job.pause` exists. |
| C7 | AGENTS rule 3 says to prefix every command with `rtk` | **Memory `rtk-pytest-false-pass`:** a run that gates a decision is unfiltered (`rtk proxy` or the direct runner). |
| C8 | D9 (ship `regs.sqlite` whole) and D13 (the `regs.sqlite` fixture subset) | Superseded / amended by **D22** (already marked in decisions.md). |
| C9 | The request names "P01–P21" | **There is no P19**: P20 and P21 follow P18. |
| C10 | tests.md §4 sets the reader bar at "39/39 at v33" | Same set and score at v37 (39/39; UR6 8/8 also passed). Use the final version's report. |
