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

## Review fixes (coordinator review of 0dbb9e6ad, 2026-11-09; tonight's release built with `--force deliver`)
1. Inputs stamped WHEN READ (R size:mtime, L listing digest, E kind); `record()` re-checks them after hashing and keeps
   nothing if one moved during the run (mutation: the step rewrites what it read → not kept, older record dropped).
2. Key holds the interpreter + every installed distribution (`env_fingerprint`), steps and traced stages.
3. One flock per step across lookup+restore and across record; restored outputs re-hashed in place.
4. The bundle's newest-by-mtime reach pick is keyed: report.json stamps are declared when no `--reaches` is given.
5. `os.access` / `os.lstat` (lexists) recorded; `dir_fd=` relative stat and `scandir(fd)` mark the run unkeyable
   (X) — only `shutil.rmtree`'s deletion scans are skipped, by name.
6. Tests: wiring (`_step` run→reuse→stage trace; `all` passes/omits the cache), `step_declared`, stage_code on a
   module edit, a spawned worker's reads, a mid-run change, the lock, a torn restore; each checked against a code
   mutation (removing the check fails its test). `test_deliver_atomic` / `test_card_gate_in_chain` fakes take `steps`.
7. `modules_of` removed; `_trace` docstring corrected.
Proof of the fixes (2026-11-09 midday, clean worktree HEAD + fixes, T517 side inputs, lane 1): cold 841 s, warm 29 s
(7 steps reused), fresh 741 s; MUTATION — the reach run's rule_section.jsonl.gz re-gzipped (same 3,376 rows, new
bytes): bundle reran ("reads changed: …rule_section.jsonl.gz"), rebuilt byte-identical, so every later step was reused
(453 s). a/b/c/m: 65 files byte-identical. Tests unfiltered: test_stepcache 19 + build/deliver/review gates green.

## The varying read count (coordinator, 2026-11-09): explained and fixed
Cause: the recorder logged the IMPORT SYSTEM's reads of module sources. On a .pyc miss (a fresh worktree, or one
where tests had compiled only some modules), each module's .py was read and recorded; otherwise the .pyc was read
(skipped). 198 / 142 = cold runs in fresh worktrees; 82 = the same run with .pyc warm and no `cache/` dir in the
worktree (`cache/dfo_salmon` is gitignored, so those 9 reads became recorded `missing` probes). No hole: every such
read was a module that is also keyed by its M line. Fix: import-system reads and scans are not recorded (only the
immediate caller counts, so a module's own import-time file reads still are); every repo-local module, not just
`pipeline.*`, gets an M line. Test: `test_the_read_set_does_not_depend_on_whether_a_pyc_exists` (fails on the old
recorder). Evidence: three traced bundle runs on identical inputs (data/generated/side/t522/rx): the same 149 modules,
92 reads, 3 listings and 129 probes with identical stamps; bundle.sqlite sha256 identical (5df79c0b…).

Depends: none. Size: M. Owner: no.
