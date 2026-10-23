# T134 — Curation review phase 3: a review screen separate from fixing

Status: DONE — 45b15369, 21e800b7 (lane-T134). Owner approved PLAN.md 2026-10-23 (defaults Q34–Q41).
Takes effect on merge: no rebuild. The legible crops need the `row_boxes.json` sidecar, a one-off the owner runs
(`python -m pipeline.regs.extraction.row_boxes`, no credits, ~2 min); until then the evidence pane says so with that
command and offers the extractor's own crop.

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 3: a review screen, separate from fixing" (and §5); split out of T97.

## Work
See PLAN.md §4 "Phase 3: a review screen, separate from fixing" — built as written there.

**Owner 2026-10-23 — splits mid-review:** "Make sure we could add splits and rebuild then continue review with the new
splits." In fix mode the owner adds or moves a split → the tool prints the rebuild commands (atlas → binding diff →
promote → reach → deliver; it never runs them) → after they run, the tool picks up the new vintage without losing
anything: the new split appears in tier S0 to confirm, entries whose binding moved go stale with reason "binding moved",
every other mark stands, and review continues where it was.
Fix mode keeps split add / move / delete / rename (SplitEditor) and shows the pending-build panel (§5).

## Acceptance
PLAN.md §4 "Phase 3: a review screen, separate from fixing" "Accept when", run unfiltered (not through rtk).
- Fix mode: add a split, save → the pending-build panel lists it and the exact commands; review mode never writes it.

Depends: T97, T133. Size: L (about a week). Owner: no (plan approved).

## As built
- **Two modes, decided once at import** (`curation-review/backend/app.py`). REVIEW MODE (default) registers the reads
  and ONE write, `POST /api/marks`. FIX MODE (`bash curation-review/run.sh --fix` → `CURATION_FIX=1`; anything else is
  review mode) adds the editors' routes by `register_fix_routes`: `POST /api/check`, `PUT /api/entries/{id}`,
  `POST /api/splits` (NEW: add a split), `PUT` / `DELETE /api/splits/{id}`, `POST /api/splits/{id}/rename`. In review
  mode those are refused by the router (404/405), not by a flag check in a handler. The frontend asks `GET /api/mode`
  and hides every editor in review mode (no live `/api/check` either). `PORT` / `CURATION_VITE_PORT` move the two
  ports (Vite and the backend's CORS origin follow), so a second copy runs beside the owner's.
- **Split add (fix mode)** — `reuse.add_split`: into an existing splits.json group or a new group on the item, validated
  by the pipeline's split loader, duplicate ids refused; its `_note` opens "Verify: added in fix mode <date> — confirm on
  the built map", so it is tier S0 (`tiers.split_needs_verify`). The AddSplit form sits under the boundaries (fix mode,
  previewing the point on the map). Move = the SplitEditor's anchor save (unchanged); delete / rename unchanged. The
  pending-build banner prints the commands in order; in fix mode it also lists every entry and split waiting.
- **Evidence first** (`backend/evidence.py`, `components/Evidence.tsx`): the heading is the RAW name ("ADAM RIVER (except
  Eve River)"; the display name beside it). A water row is cropped from the repo PDF at 2.6 px/pt, water column above
  regulation column (Adam River: 975 x 67 px; the book's ~6.5 pt text renders ~17 px — the old crop was the 530 pt row
  shown at ~24 %); "show page N with it marked" draws the box on the page. The boxes come from the new one-off helper
  `pipeline/regs/extraction/row_boxes.py`: it re-runs the extractor's own `extract_rows` with `save_row_crop` swapped
  for a recorder (no credits, writes only the sidecar), refuses a box whose row is not the extraction's (key + water),
  reports glyph-order drift (page 18 Heber River reads "Folpye fnishing" one run, "Fly fishing" the next) and records the
  raw-data sha the tool checks. Run into the scratchpad: 1,393 rows, every box reproduces its existing crop's pixel size
  (0 mismatches). A zone passage is FOUND BY TEXT on its printed page (pdfplumber words in reading order,
  `SequenceMatcher` runs of ≥3 words); measured over the 132 zone entries: 113 find ≥ 90 % of their words, 1 under 50 %
  (`zp:quota_defaults`, a paraphrase); the screen states "found N of M words".
- **Coverage strip** — `pipeline/regs/review/coverage.py`: the row (`clean_verbatim`) cut into quoted / unquoted / gap
  stretches, a quote found by `squash` (the gate's normaliser) through `_folded`, pinned equal to `squash` over every
  row of the corpus; quotes not in the row are listed.
- **Checklist + verdict bar** (`Checklist.tsx`, `useMarking.tsx` — the one place a mark is made, entries and split cards
  alike): T R P A each y / n / ?, the PLAN §2 questions; any n asks for a problem category (`marks.PROBLEMS`, by check)
  and a note; defer needs a note; the verdict is the server's (`marks.verdict_from_answers`, `POST /api/marks` with no
  `verdict`), pinned to agree with the `Mark` model over all 256 answer combinations. While awaiting build P and A wait.
  The verdict bar is sticky at the bottom; one mark in flight at a time.
- **Keys**: t r p a / y n ? / Enter (record, then next) / j k / u (next unsure) / o (the app page) / m (map large). URL
  hash = the open entry (`#split:<id>` for a card): a reload resumes in place. The marked row stays in the list
  (`review.ts keepSelected`), so the position never reads "–".
- **Zone entries** are marked rule by rule (scope rule); each rule shows its impact line ("binds 119,559 sections in
  region 4", the bundle's `rule_section_counts`) and its own app link; Enter moves to the next open rule, the last one
  to the next entry. The `matched` hint for a zone entry no longer says "empty means it binds nothing".
- **The answer** (`answer.app_part`, `app_link`): the app's start link `?regs=<item>&part=<i>&date=<day>` opened in a new
  tab (Q34 default) — the bundle part where the entry's (or rule's) rule set holds, own water first, a reached part
  before a walked one, then the largest; `CURATION_APP_URL` (default `http://127.0.0.1:39218/`, the owner's app — the
  tool only links to it). Only while current; a date picker sets the day.
- **Tiers, progress, ETA** — `pipeline/regs/review/tiers.py` (`entry_tier`, `risks`, `flagged`, `split_grade`,
  `split_needs_verify`; the closure is `rules.closure_grade`; `reuse._flagged` / `split_review` now call it). Corpus now:
  P1 226, Z 132, P2 261, P3 116, P4 730, sweep 60 (PLAN: 227 / 132 / 261 / 116 / 730 / 59); S0 150 splits (119 L).
  `GET /api/review/progress` (done / to do / stale per tier and overall), queue rows carry `tier` and filter on it.
  The ETA is the reviewer's own median over their last 20 timed entries (localStorage), none before 3.
- **Split cards (S0)**: "work → split points (S0)" lists `GET /api/review/splits` (to do first, grade L first), each card
  with label, water, grade, note, build state, the map with the point, and one check, P (a `split` mark).
- Docs: README "Two modes" + "The review screen", API.md (modes, the review endpoints, `POST /api/splits`).

### The timed run (honest numbers)
Scripted browser run (Playwright) on my own ports — review-mode backend :8891 over temp copies of the catalogue /
splits / marks (via `scripts/heavy.sh`), Vite :5291 — 40 consecutive P4 ("plain") entries of region 1 in book order
(Alice Lake … Harewood (Extension) Lake), each answered `t y y y y` + Enter, timed from the first key to the NEXT entry
on screen with its crop loaded and the checklist ready: **batch 1 (20) mean 1.95 s, median 1.65 s, max 3.6 s; batch 2
(20, after a reload that resumed on the hash) mean 1.99 s, max 3.7 s.** 40 verified marks written to the temp store, no
page errors. That is the TOOL's time per entry, with zero human reading time: it says the screen adds about 2 s per
entry, not that a person reviews a simple entry in ≤ 1.5 min — that needs a person (the T136 pilot). Cold start: the
first entry after the backend started took 21.9 s (registry + graph loads). Also checked in the browser: a zone entry
marked rule by rule (r1 verified → r2 active, impact lines shown), a wrong mark refused without problem + note, the A
link, a split card marked and the next card opened, `k` back to a marked row (still listed, "40 / 108").

## Checks
Unfiltered (`rtk proxy`, via `scripts/heavy.sh`, `-m "slow or not slow"`): **265 passed** —
`test_curation_review_{api,writes,vintage,safety,screen}.py`, `test_review_{marks,vintage,screen,reaches}.py` (incl. the
slow helper test). Frontend `tsc -p tsconfig.json --noEmit` clean. Two tests added to `smoke.txt`.
New tests: `test_review_screen.py` (coverage whole values + corpus fold = squash + mutation; tiers whole + corpus vs
PLAN §3; S0; row-box refusal / drift; the helper on page 16 (slow); no-second-copy of tier / grade / flag / strip /
verdict / passage / app part), `test_curation_review_screen.py` (Adam River raw name + crop size + page; missing
sidecar names the command; zone passage + impact; impact line whole; EVERY review-mode read route then a mark → only
the marks store and its backup changed; progress), safety (review mode = exactly `POST /api/marks`; fix mode = exactly
the editors; any value but "1" is review; a fix request in review mode refused and writes nothing; verdict rule = model
over 256 combos), vintage (fix-mode add split → panel lists it with the exact atlas commands, review mode cannot,
the rebuild lands → S0 card current → a review-mode split mark confirms it).
Mutation pins (each named mutant fails the test named): fix router registered regardless of the flag →
`test_review_mode_serves_one_write_a_mark`'s check (`test_the_one_write_check_is_pinned_by_mutation`); a
`verdict_from_answers` that lets ? through as verified, or calls an n unsure → `test_verdict_of_and_the_mark_model_are_one_rule`
(`test_the_verdict_rule_is_pinned_by_mutation`); a fold that keeps case → `test_the_fold_is_squash_over_every_row_of_the_corpus`
(`test_the_fold_check_is_pinned_by_mutation`).

## Owner questions (built with the default; say if not)
1. **S0 is 150 split points, not PLAN's 165.** S0 = a LEADING `[auto-proposal X]` tag or a note containing "Verify:".
   PLAN counted 15 more whose note mentions "VERIFY" in an `[osm-candidate … VERIFY]` stamp later answered by a
   `[human-review …]` stamp. *Default:* 150 (the answered ones are history); "every split, not only S0" shows all 409.
2. **The answer opens your app on :39218 by default** (`CURATION_APP_URL`). *Default:* deep link in a new tab until you
   approve the iframe on the reviewer's own serve-tiles port (Q34).
3. **A new split added in fix mode is stamped "Verify: added in fix mode …"** so it is S0 until a split mark confirms it.
4. **`POST /api/check` is fix-mode only** (it writes nothing, but only the editor asks it).

## Left / follow-ups
- The owner runs `python -m pipeline.regs.extraction.row_boxes` once (no credits) to put `row_boxes.json` beside
  `synopsis_raw_data.json`; rerun after any re-extraction (the tool refuses a sidecar of another extraction).
- T135 builds the risk-ordered queue on `tiers` (adds the ambiguous-cut / unresolved factors from `rule_diagnostic.jsonl`,
  which PLAN's P1 counts and these tiers cannot see from the entry alone).
- The ≤ 1.5 min per simple entry target is unmeasured for a human (the T136 pilot); the tool's own share is ~2 s.
- The crop is shown scaled to the pane (review mode gives the evidence 66 % of the width; `m` enlarges the map instead).
  A per-rule highlight inside a zone passage (marking each rule's own sentence) is not built.
- A review-mode error message passes the model's text through ("Value error, wrong needs a problem category and a note").
- Not run: Tier 2 (`scripts/test-full.sh`), `pnpm -C app check` (no app changes).
