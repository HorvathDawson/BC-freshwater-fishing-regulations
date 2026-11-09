# T522 — Rebuilds rerun only what changed: content-keyed stages and delivery steps

Status: IN REVIEW — merged 2026-11-09 after the proof below; the stage-level code key starts on the next real
`pipeline build` (reach and deliver rerun once for the new key form).

Source: owner 2026-11-08, "the constant rebuilds take so long".

## Found (manifest + build-logs, 2026-11-06 … 11-08)
- A typical rebuild is reach ~11 min (rules 523 s) + deliver ~10.5 min, serial (deliver reads the reach run); tiles
  9–19 min run beside reach (`--jobs 2`); atlas 28 min only with `--atlas`. Deliver = bundle 345 s, verdicts 49,
  status index 5, export 17, answers 64, card gate 74, store 62, release 17.
- The stage keys already skip unchanged stages, but their CODE part was every .py under broad roots: deliver = all of
  `pipeline/`, reach = `pipeline/regs` + `pipeline/common` + … So an edit to the review app (`pipeline/regs/review`,
  the most-edited area: 92 of the last 300 pipeline commits), an audit tool or the DFO suggester reran reach AND
  deliver (~22 min) for code neither imports.
- Inside deliver every step always ran: the T517 side lane ran deliver 12 times on 11-08 at ~640 s each, mostly for
  answers/bundle edits — an answers edit rebuilt the 345 s bundle each time.

## Work (done)
1. Recorder `pipeline/_trace.py` (on only with `PIPELINE_TRACE`, installed by `pipeline/__init__.py`): every process
   and worker under a step writes the repo modules it imported (hashed as imported), files read, dirs listed, paths
   probed.
2. `pipeline/common/stepcache.py`: fingerprint from a trace; `StepCache` keeps each delivery step's outputs + key.
3. `pipeline.deliver all` reuses bundle / verdicts / status_index / export / answers / card_check / store when
   arguments, staging inputs, everything read, and declared inputs (source + atlas stamps; bundle also the curated
   tree minus the review state — GDAL and ATTACH read in C) are unchanged. `--no-step-cache` runs all;
   `pipeline build --force deliver` passes it.
4. `pipeline build`: reach and deliver (`TRACED`) key on the modules their last run imported (`code_files` in the
   manifest); before a recorded run, the old roots. tiles/clim keep their narrower hand-picked roots (P18).
5. Docs: `pipeline/README.md` "The fast path"; tests `pipeline/tests/test_stepcache.py` (mutation-style: each input
   kind moved reruns the step; a tampered kept output is never served).

Not done, on purpose: parallel delivery steps. They are a chain (export → answers → card gate/store → release);
verdicts is independent but already uses 4 workers, so running it beside answers (4 more) breaks the ≤ 4-worker
memory rule. reach ‖ tiles was already parallel.

## Acceptance
- Proof (`data/generated/side/t522/proof.sh`, through heavy.sh): cold `deliver all` (fills the cache), warm (reuses),
  fresh `--no-step-cache`; every output file byte-identical across the three. Numbers: see Results.
- `pytest pipeline/tests/test_stepcache.py pipeline/tests/test_build_command.py` green.

## Results
Proof 2026-11-09, clean worktree (HEAD + T522 only), T517 side inputs, heavy lane 1, while T517's deliver ran on lane
2 (so absolute times are ~25% above the 11-08 numbers):
- cold `deliver all` (records): **781 s** (bundle 446, verdicts 60, status 5, export 21, answers 73, card 90, store 65,
  release 20) — recorder overhead nil (fresh bundle untraced: 456 s).
- warm, nothing changed: **31 s** (all 7 steps REUSED, bundle restore 9 s; release + runtiming run).
- fresh `--no-step-cache`: **783 s**.
- a / b / c: **65 files byte-identical** (sha256), incl. bundle, verdicts, status index, export pair, answers, card
  stamp and the release folder (same r-id).
- The deliver stage's traced code: 237 of 446 pipeline modules; nothing under `pipeline/regs/review`.
- Expected by edit kind (from the step times): review app / tools / DFO suggester → 0 (both stages up to date, was
  ~22 min); answers or wording → answers + card + store + release ≈ 4 min (was ~10.5); bundle code (parts/naming) or
  a rule edit → as before (the bundle reruns and everything after it reads it).
- Unit: `test_stepcache.py` 6 + `test_build_command.py` 23 green.

Depends: none. Size: M. Owner: no.
