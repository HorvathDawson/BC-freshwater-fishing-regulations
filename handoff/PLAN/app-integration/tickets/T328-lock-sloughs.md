# T328 — Archive the old slough inputs after the T322 promote

Status: DONE (2026-11-04, 1a27dd3af) (owner 2026-10-30; the T322 gate is mechanical, no owner review). The lock step is gone: T325 replaced it with write-through, so a saved mark already updates `data/curated/waters/slough_calls.json`.

## Work
After T322 is promoted, archive the old slough inputs that nothing reads any more (slough page-marks, the retired artifact export, `decided/sloughs.json` if the build no longer reads it). `waters/slough_calls.json` stays the final file.

Depends: T322 promote. Size: S.

## Done 2026-11-04
Checked each old input against its readers: `decided/sloughs.json` (the apply step, `tools.slough_calls`), the
automark + `candidates_v2.json.gz` (the review app's slough page and the stale-mark check `slough.prints_of`) and
`sloughs/page-marks-2026-10-28.json` (the review page's pre-marks, config `sloughs.page_marks`) are all still read, so
they stay. The one dead input, the retired artifact page builder `handoff/PLAN/sloughs/v2/page/`, is deleted.
