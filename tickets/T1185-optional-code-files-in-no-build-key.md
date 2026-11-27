# T1185 — Code files a stage probes or lists are in no stage key

Status: OPEN — a #286 follow-up; scoped 2026-11-26: a real gap in the key, no live case found

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1185 | Code files a stage probes or lists are in no stage key | OPEN — a #286 follow-up; scoped 2026-11-26: a real gap in the key, no live case found | — | a lane: after the next real build, list each stage's E / L trace lines under the code roots; key the ones that can change output, or close with the list |

Source: PR #286's third review, L3 (follow-up, not fixed): "an optional code file probed (E) or a code directory listed (L) under `pipeline/` is in no stage key.
This is not small: E and L stamps are kinds and listings, not content, and a listing under `pipeline/` would move with every new `__pycache__`."

## The gap (on main, 1e2dc8a3b)
- The recorder writes E (a path probed: exists / is_file) and L (a directory listed) lines (`pipeline/_trace.py:12-13`); `stepcache.Trace` parses them into
  `probes` / `lists` (`stepcache.py:274-275, 286`).
- A stage key uses only R reads under `CODE_READ_ROOTS` (`traced_code`, `stepcache.py:334`; `code_key`, L353-360). E and L are never in it. (The step cache inside
  deliver does fingerprint them, `fingerprint` L380-381, but only once the stage already runs.)
- So a file a stage probes and finds missing, then reads when it appears, or a new file in a code folder a stage lists, can arrive without moving the key.

## What a static search found (2026-11-26)
- One stage-time listing under `pipeline/`: `card_check.code_sha` (`pipeline/deliver/answers/card_check.py:1005-1015`) rglobs `pipeline/deliver/answers/*.py`.
  A new, unimported file there leaves deliver "up to date" with a stale `code_sha` in the stamp; nothing compares that value, so no answer can be wrong.
- The optional-looking code reads checked stop when the file is missing (no silent default): `classified_waters.load_pins` (`pipeline/regs/classified_waters.py:938-941`),
  `passage_boxes` (`pipeline/regs/extraction/passage_boxes.py:226-227`).
- A static search cannot see every probe; the real E / L lines of each stage appear only in a real build's trace (`run_stage` deletes the trace dir, `build.py:816-818`).

## Fix (one sentence)
Have `_traced_key` print each stage's E / L lines under `CODE_READ_ROOTS` (report only) on the next real build; key a probe by its kind and a listing by its
`*.py` / data names (no `__pycache__`) only for the lines that show, else close this with the list.

## Acceptance
The list from one real build is in this ticket; each line is keyed (with a test that adds the file and sees the stage rerun) or shown harmless.
Severity: low (was medium; no live case found). Model: Opus. Size: S.
