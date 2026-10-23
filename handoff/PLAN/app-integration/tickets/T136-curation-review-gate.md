# T136 — Curation review phase 5: parity gate and pilot before the full review

Status: DONE (tool side) — b42fc8cd, 12739188, review fixes 371f1a85 (lane-T136). The 30-entry PILOT is prepared, not run: it is a
person's review time (owner question 1). Owner approved PLAN.md 2026-10-23 (defaults Q34–Q41).
Takes effect on merge: no rebuild. (The new CLI flags change no default; `pipeline build` output is unchanged.)

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 5: gate before full review" (and §5); split out of T97.

## Work
See PLAN.md §4 "Phase 5: gate before full review" — built as written there.

**Owner 2026-10-23 — splits mid-review:** "Make sure we could add splits and rebuild then continue review with the new
splits." In fix mode the owner adds or moves a split → the tool prints the rebuild commands (atlas → binding diff →
promote → reach → deliver; it never runs them) → after they run, the tool picks up the new vintage without losing
anything: the new split appears in tier S0 to confirm, entries whose binding moved go stale with reason "binding moved",
every other mark stands, and review continues where it was.

## Acceptance
PLAN.md §4 "Phase 5: gate before full review" "Accept when", run unfiltered (not through rtk).
- End to end on a scratch copy: mark entries, add a split in fix mode, run the printed rebuild via heavy.sh, reload —
  0 false-stale marks, the moved entries stale as "binding moved", the new split in S0. Then the 30-entry pilot.

Depends: T97, T133, T134, T135. Size: S + build time. Owner: no (plan approved).

## As built
- **Parity gate (AGENTS 16)** — `pipeline/regs/review/parity.py` (`python -m pipeline.regs.review.parity`, through
  heavy.sh): for every rule, the review app's binding (`reuse.entry_reaches`, the code the screen runs) against the
  served reach run — outcome, reason and the SET of sections. The 20 GB `rule_section.jsonl` is streamed in its own
  order (one rule's sections at a time; the writer's form split on quotes, anything else `json.loads`; out of order
  refused); unresolved rules come through `runfacts.RunFacts` (the one reader). A difference is explained only by a
  cause the code states (`EXPLAINED`: `needs_corpus`, the book-known steelhead waters). The CLI refuses a run of
  another corpus or atlas (`check_vintage`).
- **The printed rebuild targets what the tool serves** — `vintage.Target` (entries, splits, build, reach, bundle,
  release, tiles) and `rebuild_commands(reasons, target)`, threaded through `entry_state` / `split_state` / the
  pending-build panel / the queue's "no run" words (`reach_run_commands`). Live tree: `pipeline build --no-tiles`
  for an entry; after a split: `pipeline build --atlas`, a reach run on `<build>_next`, the binding diff between the
  two runs (AGENTS 63 — the old line named no runs, and nothing made the side run), then `pipeline build --promote`
  WITH tiles. **Found:** the old `--promote --no-tiles` could never finish — `deliver all`'s strict vintage check
  refuses a bundle whose tiles are another atlas's. A tool on COPIES (every output elsewhere) prints the chain spelled
  out on its own paths (atlas `--splits --out`, reach `--entries --out`, binding diff, `promote --target`, tiles
  `--release-root --bundle`, `deliver all --tiles --out-dir`); a half-live target prints no command, only why.
- **CLI flags so a rebuild can target copies** (defaults unchanged): `pipeline.deliver all|bundle --tiles`;
  `pipeline.deliver.release --tiles --tiles-release-root`; `pipeline.deliver.tiles --release-root --bundle`;
  `pipeline.atlas.promote --target --tiles --bundle`. The review app reads `CURATION_BUILD`, `CURATION_BUNDLE`,
  `CURATION_TILES` beside the existing `CURATION_*` (answer.py passes the bundle path to `effective_rules`).
- **False stale** — `marks.moved(mark, now)` is THE one comparison (which parts moved: text, records, meaning,
  binding, reach, answer, unbuilt); `stale_reasons` names it through `MOVED_REASONS`; `BUILT_PARTS` = the bundle's
  half. `gate.stale_report` splits stale marks into `edited` (a curated part moved) and `build_only` (only a build's
  parts — across a rebuild of unchanged inputs, a false stale). CLI `python -m pipeline.regs.review.gate stale`.
- **Ready signal** — `CURATION_WARM=1` (run.sh sets it) warms registry, matcher, bundle facts, vintage, run facts,
  graph, geometry, then the queue once, in a thread at startup (`reuse.warm`); `GET /api/ready` reports each step with
  its second, `ready`, `error`. Off by default (tests).
- **Pilot** — `Mark.seconds` (the screen's clock: from the target opening, or the previous mark of a zone entry's
  rules, to the mark; `useMarking` sends it); `gate.pilot_pick` (per tier, spread evenly through the queue's order
  among unmarked + built targets; quotas S0 4 · P1 6 · Z 4 · P2 4 · P3 4 · P4 6 · sweep 2) and `gate.pilot_report`
  (time per target / tier, defects = wrong/unsure, tool blockers = a mark whose note starts `TOOL:`; carried copies not
  counted). `handoff/PLAN/curation-review/PILOT.md` (one-page how-to + the 30 targets) and `pilot.json` (baseline
  r-012fb43aa175, reach b9be92114323aa22).
- Docs: curation-review/README.md "The gate before the full review (T136)".

## Measured (baseline: reaches/full 08:19 + release r-012fb43aa175, after the batch-5 rebuild)
- **Parity: 3,405 rules — 3,396 identical, 9 explained (`needs_corpus`), 0 unexplained** (99.74 % identical, gate
  passes). The 9: the steelhead twins `zp:steelhead` r1b/r2b/r4b (run binds 15 sections each), `z2` r7b (2), and
  `z1` r5b / `z1:hg_quota` r6b / `z3` r5b / `z5` r6b / `z6` r9b (run: `no_sections`). 57 min (the app resolves zone
  rules one entry at a time). (PLAN's 3,406 is the day's drift.)
- **End to end on scratch** (APFS clones of catalogue, splits, marks, atlas, reach run, bundle, release, tiles under
  the scratchpad; backend in fix mode on :8893, own Vite :5293):
  - marked through `POST /api/marks`: every entry (zone entries rule by rule) and every split point — 2,163 marks,
    0 refused; `gate stale`: 2,163 held.
  - added `t136_e2e_354154447` (Adam River, a point mid-section, 11 km above the Hwy 19 bridge) via `POST /api/splits`:
    the panel listed exactly `r1:adam_river_except_eve_river@1-10` + the split ("not in the served atlas") and printed
    7 commands, every one on the scratch paths; `gate stale`: 1 stale (Adam, "binding moved", part `binding`).
  - ran the 7 printed commands as printed (only `python` = the venv's), heavy ones through heavy.sh: atlas rc 0, side
    reach 754 s, binding diff (233 rules' section sets differ — 22 fewer sections, 7 cut points lost on Fraser /
    Raspberry / Exchamsiks / Two Forty: see follow-ups), promote, reach 1,038 s, tiles 663 s, deliver 682 s; deliver's
    strict vintage check: every artifact agrees.
  - reload, SAME server, no restart: pending build empty; the new split in S0 (`unmarked`, current); `gate stale`:
    2,163 marks, 2,162 held, **1 stale = Adam "binding moved"; build_only 0 → false-stale rate 0**. The bundle halves
    of all 3,376 bundle rules (reach summary, answer, water parts) were identical between the live and rebuilt bundles.
    Review continued: the split mark and a fresh Adam mark were taken (`verified`, `seconds` stored). Screen checked
    in the browser on :5293 (queue + split card load). Scratch removed after.
- Not measured: a person's time (the pilot). This rebuild moved no unnamed block (T97's "unnamed count may flutter"
  stays unobserved, not disproved).

## Checks
Unfiltered via `scripts/heavy.sh` (`-m "slow or not slow"`): **344 passed** — test_curation_review_{vintage,api,
safety,screen,writes,queue}, test_review_{screen,reaches,loop,gate,vintage,marks,queue}, test_binding_diff. Frontend
`tsc --noEmit` clean. New `test_review_gate.py` (21): rebuild commands whole values (live, scratch, half-live, release
mismatch); no live path in a scratch chain; EVERY printed scratch command parses with its own CLI (each run to its first
real work); deliver all threads `--tiles` to bundle/release/vintage; promote `--target`; `moved` vs `stale_reasons`
over 9 cases; stale_report whole value; parity compare / streamed run (vs the writer, quotes, unicode, order) /
compare_all / vintage refusal; pilot pick, report, markdown; `Mark.seconds`; warm/readiness. No-second-copy pin:
`def moved(` one copy.
Mutation pins (each mutant fails the named check): `Target.kind` always "canonical" → `test_a_scratch_target_prints_no_live_path`
finds live paths; `BUILT_PARTS` without `reach` → `test_stale_report_splits_an_edit_from_a_rebuild` sees 0 false stales;
`explain` explaining any unresolved → `test_compare_whole_values`' no_sections case; a pick ignoring the vintage →
`test_pilot_pick_spreads_each_tier_over_what_can_be_finished_now` changes.
Not run: Tier 2 (`scripts/test-full.sh`), `pnpm -C app check` (no app changes), the Playwright browser test file.

## Adversarial review (2026-10-23)
Read the diff against PLAN §4 phase 5, mutated every claimed pin for real (edited the source, ran the file), re-measured
parity on real waters, and checked that no printed scratch chain or new flag writes the live tree.

**Confirmed:**
- Parity spot check on the live atlas + reaches/full (run b9be92114323aa22, corpus 4e3f729204098d06) for
  `r1:adam_river_except_eve_river@1-10`, `r2:stave_river@2-8` and `zp:steelhead`, through heavy.sh: 9 rules. Adam and Stave
  are identical, rule by rule. The 3 steelhead twins r1b/r2b/r4b show the app `needs_corpus` against the run's 15 sections
  each (e.g. 356000423:0, 356362768:637), matching the ticket's "Measured". 0 unexplained.
- The builder's real mutations hold: `kind()` -> "canonical" fails 5 tests; `BUILT_PARTS` without reach fails
  `test_moved_is_the_one_comparison…` and `test_stale_report_splits…`; inverting edited/build_only fails the stale-report
  tests; dropping the vintage filter fails `test_pilot_pick_spreads…`.
- Safety: the scratch chain names no live path. The `atlas.build --out` carrier check and `tiles` `assets.fetch()`
  only READ the live tree (or a download cache). `deliver all --out-dir` stages beside the out-dir. `promote --target`
  touches only its own directory and `.prev`. `/api/ready` is a GET. A mark's `seconds` passes through `post_mark`.

**Found and fixed (371f1a85):**
1. **The gate grew a third copy of "a mark's target prints now"** (alongside `tickets.statuses_now` and `reuse.marks_view`).
   Fix: `marks.target_prints`, which both `gate.stale_report` and `tickets.statuses_now` now call. Pin:
   `test_a_mark_s_target_prints_have_one_reader` (whole value plus a source check). Mutation: passing the
   mark's `rule_id` (a renumbered rule would read as gone) fails it.
2. **The pilot counted a zone entry "marked" after one rule mark.** One rule of ten marked made the target done, and
   `passes` could be true with most of the entry unread. The time also under-counted. Fix: `pilot_report(…, rules_of)`
   (the CLI passes each entry's rule ids), and an entry marked rule by rule is done only when every rule is. Pin:
   `test_a_zone_entry_is_marked_in_the_pilot_only_when_every_rule_is`. Mutation: `_finished` -> True fails it.
3. **The pilot could pick a rule-less zone entry** (the 7 with licensing records only). It can never be finished.
   Fix: `pilot_pick` skips `kind == zone` with `n_rules == 0`. Pin: `test_a_rule_less_zone_entry_is_no_pilot_target`
   (mutation: drop the filter). The committed pilot.json picked none of them, so it is unchanged.
4. **Parity skipped a run-unresolved rule id that the corpus's entry lacks.** Only the run's BOUND rules were added to the
   corpus's own. `check_vintage` makes this unreachable on a matching corpus, but the function claimed to cover it.
   Fix: compare it. Pin: `test_compare_all_compares_a_run_unresolved_rule_the_corpus_lacks`.
5. **A scratch `CURATION_BUNDLE` not named `bundle.sqlite`** got a chain whose `deliver all --out-dir` writes
   `<dir>/bundle.sqlite`. The tool would then wait on its own file forever. Fix: no command, only why. Pin:
   `test_a_scratch_bundle_not_named_bundle_sqlite_gets_no_command`.
6. **Two parity guards had no pin.** Fix: added tests for the `run.outcome != "absent"` guard in `explain`
   (`test_needs_corpus_is_explained_only_when_the_run_answered`) and the backslash guard in the fast row reader
   (`test_the_fast_row_reader_never_returns_an_escaped_value`). Each fails under its mutation.

**Noted, not changed:** the builder's `test_*_is_pinned_by_mutation` tests apply their mutation to themselves via
monkeypatch. The real-source mutations above show that the whole-value tests carry the pins. `binding` holds the
registry's absorbed-id fold, which is a build output, so a registry change from a rebuild alone would count as "edited",
not "build_only". That would hide a false stale. In the E2E, edited was 1 (Adam, a real split), so nothing was hidden.
See follow-ups.

Checks: unfiltered via heavy.sh, with read-only symlinks to the live data/generated + data/release (removed after):
**301 passed**. Files: test_review_{gate,marks,vintage,queue,loop}, test_curation_review_{queue,vintage,api,safety,writes}.
test_review_gate now has 28 tests. Ships on merge, no rebuild.

## Owner questions (built with the default; say if not)
1. **The pilot needs a person other than the builder.** *Default:* prepared — PILOT.md + pilot.json (30 targets);
   you or a reviewer run it (~1 h) and `gate pilot-report`. PLAN's "no tool blocker" stays open until then.
2. **After a split, the printed live chain now ends `pipeline build --promote` WITH tiles (~40 min)** — the old
   `--no-tiles` cannot pass deliver's vintage check. *Default:* tiles included.
3. **A tool blocker is reported as a deferred mark whose note starts `TOOL:`** (no new verdict or field). *Default:* that.
4. **The 9 `needs_corpus` rules are explained, not fixed**: the screen shows them unresolved ("needs corpus") while the
   bundle binds 2–15 sections or none. *Default:* explained by design (AGENTS 54); say if the app should read the run.

## Left / follow-ups
- (review) `fingerprint`'s `binding` folds ids through the BUILT registry, so a rebuild that changes the registry alone reads
  "edited" in `gate stale`. That would under-count false stales. Split the registry fold into its own part (a built part)
  if the measure is to be exact across a registry change.
- **The live atlas is older than its inputs**: a rebuild from today's curated files lost 7 cut points (Fraser 98 → 94,
  Raspberry, Exchamsiks, Two Forty) and 22 sections — gauge matches or splits changed since the served atlas was built
  (it predates `split_anchors.json`, so the tool checks split ids only). Diagnose before the next real promote.
- Parity takes ~1 h (zone rules resolved per entry in the app); a re-run is worth it after any reach-code change.
- Pilot not run (owner Q1); then compare the median per tier with PLAN §3's hours.
- 7 zone entries hold licensing records only, no rules (z3:steelhead_stamp, z4:classified_waters_licence,
  z4:creston_valley_permit, z4:kootenay_annual, zp:classified_waters_licence, zp:basic_licence,
  z5:dean_river_classified): a zone entry is marked rule by rule, so they can never read "verified" (seen in the E2E:
  "never marked (7)"). Needs a way to sign off a zone entry's licensing records (entry mark for a rule-less zone entry).
