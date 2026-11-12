# T680 — Release: find fixture-pinned test failures before the push

Status: DONE (d14e2565b)

Source: release r-483b295de57c stopped four times on 2026-11-11, each found one CI round at a time:
1. a changed golden page (by design, the fixtures were re-cut on the candidate);
2. an untracked ticket file failing the ticket guard;
3. `one-part-one-page.test.ts`, Nicomen Slough wording (old value typed in);
4. `WaterMode.test.tsx`, section number 578 became 586 (old value typed in).

## Work
- Run the whole app check locally right after the fixture re-cut and list every failure at once, before the push.
- Extend the test-anchor scanner to flag a section number typed inside an expected string.
- Offer re-cut + app tests as a dry run once a candidate is built, so the failures show before the release starts.
- Make the worker `tsc` part of `pnpm -C app check`.

Depends: none. Size: S.

## Done (d14e2565b)
- `python -m pipeline release --staging`, follows stage: after the re-cut on the candidate it runs the pipeline tests
  found by rule AND the whole app check — the app vitest suite, the feeds worker suite, each `check:fast` gate on its
  own (read from `app/package.json`) — every one whatever the one before did; the one stop names every failing test
  (from the vitest JSON / pytest junit reports; a check with no report is named with its rc). Before the push gate.
- `python -m pipeline release --staging --dry-run --rehearse-follows`: on a built CANDIDATE, re-cuts in place, runs all
  of the above, lists every failure, the golden pages and the `--accept-fixtures` token, then puts the fixture paths back
  byte for byte (a mismatch after is printed).
- `app/tools/check-test-anchors.mjs`: a section / part number typed into an expected string where the file's own
  template puts one ("panel for 578 at …", "regs on gnis:8634 part 3") is a raw-section / raw-part. One live hit fixed:
  WaterMode.test.tsx names the Chilliwack parts by their ends.
- `pnpm check:fast` runs `typecheck:worker` (`tsc -p workers/feeds/tsconfig.json`); so does fast-fail through it.
- Tests: `pipeline/tests/test_release_command.py` (every failure listed by name; gates read one by one; the rehearsal
  puts the fixtures back — pinned by mutation), `app/tools/check-test-anchors.test.ts` (hit + near misses, mutation).
