# T328 — Archive the old slough inputs after the T322 promote

Status: OPEN (owner 2026-10-30; the T322 gate is mechanical, no owner review). The lock step is gone: T325 replaced it with write-through, so a saved mark already updates `data/curated/waters/slough_calls.json`.

## Work
After T322 is promoted, archive the old slough inputs that nothing reads any more (slough page-marks, the retired artifact export, `decided/sloughs.json` if the build no longer reads it). `waters/slough_calls.json` stays the final file.

Depends: T322 promote. Size: S.
