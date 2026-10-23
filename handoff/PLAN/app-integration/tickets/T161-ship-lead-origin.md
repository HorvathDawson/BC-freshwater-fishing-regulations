# T161 — The pipeline ships each fish's leading origin; the app's mainRes only reads it

Status: DONE, REVIEWED — 3e89b917 (code + pins) + review 3bf1e1e1 (pins hardened), d0bc72fd (the scan
leaves core: boundaries). Rebuild: deliver + fixture/golden
re-cut (below).

Source: Q66 decided by Claude 2026-10-23 (owner rule: computed where the data lives). The app picks a fish's "main"
origin itself; common.lead_origin decides it in the pipeline. Ship it in the store/release, make mainRes read it, pin
no second copy. Ships: deliver + fixture re-cut + app. Size: S.

## As built
- **Pipeline (rows 32 → 33).** `common.lead_name(h, w)` names the origin `common.lead_origin` picks ("hatchery" | "wild" |
  None) — it never re-decides. `rows.fish_answer(h, w)` builds every stored fish answer `{hatchery, wild, lead}`
  (`produce` calls it). Wire: a rows frame's fish is `[decided h, decided w, lead]` (`RowsCodec`; SPEC text says so);
  the decoder refuses a 2-slot fish. `model.FishAnswer.lead` + a validator that holds every stored lead to
  `common.lead_name` (a copy that disagrees fails the build). Section version: `answers.py` Section rows 33, SPEC "33".
  `release/shards.frame_refs` and the app's `regs-fixture.test.ts` closure read the two answer slots only.
  `reference/page_data.py` drops the slot (the page v36 reference decides for itself, as before).
- **App.** `versions.ts` rows 33 (a rows 32 store is refused at open, never re-picked). `reads.ts` decodes the third
  slot (a 2-slot fish is `damaged`). `card.ts` `rowsAt` hydrates `R[S].lead` (the HydRes the lead names; a lead naming an
  absent answer, a null lead beside an answer, or no lead is thrown); `mainRes(fish) => fish?.lead ?? null`;
  `KEEPISH` (the copy's predicate) removed. `grouphead.separateFrom` and `caveats` (moments) call `mainRes(card.R[S])`.
  `answers.generated.{ts,json}` regenerated (`FishAnswer.lead`).
- **Pins.** `pipeline/tests/test_answers_lead_origin.py` (+4): `lead_name` whole-value over every status pair; the stored
  fish answer whole; the model refuses a disagreeing / missing / dangling lead; the wire round trip and the rows-32
  refusal; the shard closure. App `core/src/regs/mainRes.test.ts` (moved by the review to `app/tools/lead-origin.test.ts`): no app source file (packages/*, conformance/src, tests
  and generated files aside) picks an answer by keep status (`KEEPISH`, a keep ternary, an if/return), `mainRes`'s body
  is the read. **Mutations run:** (1) `rows.fish_answer` computing its own lead ("hatchery" unless absent) → fails
  `test_the_stored_fish_answer_carries_its_lead_whole`; (2) a keep-ternary `pick` re-added to card.ts → fails
  `mainRes.test.ts` test 1 naming card.ts; (3) the old `mainRes` body, an if/return copy and a `nolimit` ternary are
  caught by the scan's own mutation test, a one-answer filter (`m ? m : null`) is not.
- Version-text tests moved to 33: `store.test.ts` (+ a rows-32 refusal case), `golden.test.ts` liveReleaseAt (+ a rows-32
  case), `reads.test.ts` (+ a 2-slot damaged case), the VERSIONS constants of reads/store/ruleSources tests,
  `test_answers_pipeline_needs.test_the_versions_say_what_changed`.

## Measured deliver diff (scratch, read-only against live)
`scripts/heavy.sh T161-deliver 30 -- python -m pipeline.deliver all --out-dir <scratch>/T161-deliver/out` from the live
atlas + reach run (handles d768d6e135e62b99, reach b9be92114323aa22): vintage agrees, **card_check 0 problems**
(2,266 keys, 7,159 cards, 16,004 groups; answers sha 27db377d…), release **r-796311fd3f8c 13,663,310 B gz** (live
r-8b761969206f 13,639,686; +23,624 B, gate 14,000,000).
- Answers: sections display, gear, ladder, licence **identical**; rows `at`, `decided`, `rows`, `strips` identical;
  4,204 rows frames → 4,204, **identical once the lead slot is dropped**; `version` 32 → 33; top-level `spec`/`schemas`
  (the SPEC text and FishAnswer schema) and `about.bundle.build` (the worktree's path — the merger's run names the main
  checkout) differ. Nothing else.
- The one changed row class: **every fish of every rows frame gains `lead`** — 62,327 fish answers, all `"hatchery"`
  today: (closed, closed) 32,936 · (keep, keep) 20,216 · (release, release) 7,777 · (keep, release) 775 (e.g. RB/ST/CT of
  frame 0) · (no_limit, no_limit) 623 (LMB/SMB/YP of frame 27). No frame has hatchery release/closed beside a wild keep,
  or a wild-only answer, so no fish leads with wild.
- **The app's former TS pick agrees with the shipped lead on 62,327 of 62,327** → no card, group head or caveat line
  changes; the change is where the choice is made, not what it says.
- Store `regs.sqlite`: `rows_frame` 4,204 of 4,204 rows re-encoded, its `zdict` dictionary retrained (1 row), `meta`
  6 rows (layout versions, digests), `static` 3 rows (spec/schemas). Every other table byte-identical. Release files:
  regs-1…8 +341 to +5,858 B gz each, water −1,471 B.
- Scratch outputs were lost in the 2026-10-23 restart; the review re-ran the deliver (below) and deleted its scratch.

## Rebuild needed (merger)
1. `scripts/heavy.sh deliver 30 -- .venv/bin/python -m pipeline.deliver` (rows 33; expect the diff above).
2. Re-cut the goldens and the regs fixture from that release — **not done here**: `.venv/bin/python
   app/design/regs-v11/port/taps.py` (taps.json `meta.store.layout.versions.rows` 32 → 33, store digests), then
   `pnpm -C app regs-fixture` (`packages/data/dev/regs-release/r-*`: each region file's `rows_frame` blobs gain the third
   slot), then the UPDATE_REGS_FIXTURES harness as in earlier re-cuts. Until then, the app suites that read the committed
   fixture/golden fail by refusal ("rows section version 32, this app reads 33"): conformance regs.fixture,
   golden.test openPaired/resolveSource, keep/caveats/plainrow parity, regs-fixture.test (358 tests in the worktree run,
   all fixture-backed); `pipeline/tests/test_moments.py::test_the_card_answers_the_weekday_asked` reads the live answers
   (rows 32) and fails until the deliver.
3. `REGS_STORE_REQUIRED=1 pnpm -C app check` after the re-cut.

## Checks
- `pytest -m smoke` (unfiltered, heavy.sh): 207 passed. Targeted answers/store/release tests: see the lane report.
- App: `tsc -b` clean; vitest core/src/regs + data/src/regs 581 passed; mainRes.test 3/3.
- Strict app check not run green here: it needs the fixture re-cut above (the lane may not write the committed fixture).

## Left / follow-ups
- T125's latent-duplicate list can drop the app `mainRes` entry (Q-T120): there is no TS copy any more.
- The wire spends ~10 B per fish on a word that is "hatchery" in every frame today; a 0/1 code would save most of the
  +23.6 KB gz if the release gate tightens.

## Adversarial review (2026-10-23)
- **Scratch deliver re-run** (`scripts/heavy.sh T161-deliver 30 -- python -m pipeline.deliver all --out-dir <scratch>`,
  worktree code, live atlas + reach): vintage agrees (handles d768d6e135e62b99, reach b9be92114323aa22), **card_check 0
  problems** (20,456 parts, 2,266 keys, 7,159 cards, 16,004 groups; answers sha 27db377d…), and **the same release id
  r-796311fd3f8c** as the builder's run — so the build is deterministic over this change and the builder's size figures
  stand. Against the live outputs: sections ladder / gear / licence / display identical; rows `at`, `decided`, `rows`,
  `strips` identical; 4,204 frames identical once the third slot is dropped, every fish 3 slots; `about.sections.rows`
  32 → 33, `spec`/`schemas` and `about.bundle` (worktree paths) the only other differences. 62,327 fish answers, every
  lead "hatchery" (closed/closed 32,936 · keep/keep 20,216 · release/release 7,777 · keep/release 775 · no_limit/no_limit
  623); **the former TS pick agrees 62,327 / 62,327**. Store: `rows_frame` 4,204 rows, `meta` 6, `static` 3, `zdict` 1
  changed; every other table identical. The diff reproduces the builder's exactly.
- **No wild lead exists in today's data**, so the release never exercises `lead: "wild"`; that path is held by unit
  tests only (card.test 2 feeds a wild lead beside a hatchery keep and a hatchery lead beside a wild keep).
- **Version handling:** rows 33 in `answers.py`, the encode SPEC, `versions.ts`; a rows-32 store is refused at open
  (store/golden tests), a 2-slot frame is refused by the pipeline decoder and marked `damaged` by `reads.ts`;
  `card.ts storedLead` throws on a lead naming an absent answer / a null lead beside one / no lead. Model validator holds
  every stored lead to `common.lead_name` during the build (`encode.py` validates every section value against the model before encoding).
- **Mutations (each run, then restored):**
  - card.ts `lead: H ?? W` (ignore the stored lead) → card.test 2 and 4 fail. Holds.
  - `common.lead_name` hatchery-unless-absent → 3 pipeline tests fail; model validator disabled → 1 fails. Holds.
  - **Passed (defects, fixed in 3bf1e1e1):** a `lead_name` that re-decides from its own keep test (same answers, no
    `lead_origin` call) passed all 14 pipeline tests — the AST scan looked for a returned ANSWER, not a returned origin
    word; now flagged (+1 mutation pin). In the app, a property-path ternary (`f.H && f.H.status === "keep" ? f.H :
    f.W`), a `[f.H, f.W].find(<keep test>)` copy, and any copy under `apps/` passed `mainRes.test.ts` (verified by
    writing each into a real source file); the scan now takes property paths, catches find/filter, and scans `apps/`;
    each of the three real-file mutations fails it, the baseline passes, a one-answer filter is still spared.
  - Still a shape scan: a copy written another way (a keep-status lookup table, a sort) would pass. The semantic guard is
    card.test 2 (the store's lead wins over a keep-first pick) for card.ts only.
- **Fixture re-cut, rehearsed in scratch only:** `python -m pipeline.tools.regs_fixture_release --release
  <scratch>/release/r-796311fd3f8c --out <scratch>/fixture` cuts r-3a505323508d cleanly from the rows-33 release (closure
  reads two slots). The committed `app/packages/data/dev/regs-release/r-83dc2fe9564f` was not touched. The merger's
  order stands as written in "Rebuild needed": deliver → `taps.py` (taps.json rows 33 + store digests) →
  `pnpm -C app regs-fixture` → the UPDATE_REGS_FIXTURES harness → strict app check.
- **Defect: the app check never got past its 2nd step.** `mainRes.test.ts` lived in `packages/core` and imported
  `node:fs` / `node:path`; `check-boundaries` refuses both (2 violations), so `pnpm check` stopped at `boundaries` and no
  strict check could run. The builder ran vitest and tsc directly, never `check`. Fixed in d0bc72fd: the file moved
  whole to `app/tools/lead-origin.test.ts` (tools may read files; root adjusted), references in card.ts and the pipeline
  pin follow; still 3/3, still fails on a copy written into packages/ui.
- **Checks (review):** `REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh` — every gate through typecheck
  green; vitest 49 files / 80 tests failed, 3,732 passed. **Every failure is the rows-32 pairing**: 34 "REGS_STORE_REQUIRED
  is set: release … is rows 32 (this app reads 33)", 5 "rows section version 32, this app reads 33 — refused", the rest
  are fixture-backed screens (back, WaterMapView, WaterScreen, WaterMode, Spots/MapDay shells, water-frame-*, ui regs /
  waterMap) whose fixture store is refused, so their testids never render; plus ONE unrelated flake
  (`tools/one-day-table.test.ts` ENOENT on `_plat.tsx`, the temp file `check-boundaries.test.ts` writes and removes in
  the same run). Clears with the deliver + fixture/golden re-cut. `pytest -m smoke` (unfiltered, heavy.sh) 207 passed;
  targeted answers tests 65 passed.
