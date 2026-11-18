# T788 — Gates that hide their cause or do not fire when they should (found by the audit and by last night's release)

Status: OPEN (pipeline release / deploy scripts; none let a bad release through).

Source: general audit 2026-11-16, code and guards checkers (`handoff/PLAN/audit-2026-11-16/code.md`, `handoff/PLAN/audit-2026-11-16/guards.md`).

1. `pieces_gate.fetch_status` swallows every error: a 403, a DNS failure, a timeout and bad JSON all read "the worker
   cannot be asked"; it prints nothing on a pass. Print the cause. `deploy/staging/upload.sh` line 21 takes
   `FEEDS_URL` from the caller's shell (a stray value asks another worker silently): set it in the script.
2. The gate's question to the feeds worker is asked only inside the upload, after the promote: ask it in preflight
   and in the rehearsal too.
3. fast-fail's mini-release check does not trigger on `pipeline/regs/**` (the path that turned CI red on 11-15).
   **FIXED** (lane/bugs-080, 2026-11-17, the commit naming T788 item 3): `regs` is in `mini_chain`'s trigger.
4. `deploy/staging/site.sh` run on its own does not refuse a dirty `app/`; `deploy/prod/site.sh` stamps the commit at
   deploy time, not the export's.
5. `--no-basemap` / `--no-synopsis` are accepted for the live release root.
6. `pipeline/atlas/reach/channels.py` caches by `id(registry)` without holding the object (the review app could serve
   an old build's side-channel table after a promote without a restart).
7. Review backend: `_region_rows` returns `{}` when `splits.resolved.json` is missing; `cut_label` swallows
   `FileNotFoundError` (no silent fallbacks).
8. The release ledger's record that the 407-rule diff was accepted was overwritten in the last attempt with "same
   reach run" (the diff file is still on disk): an accepted diff's record must survive a rerun after the promote.
9. `taps.py` stops at the first stale golden declaration (three attempts last night): list them all. (The closed-part
   pin is the app chat's T765.)
10. The release's own in-season commit reran deliver for 9 minutes and produced the same release id.
11. No free-disk check anywhere (`scripts/heavy.sh` checks memory only).
12. Five comments still cite `exclusions.ts`, removed by T439 (`taps.py`, `golden.ts`, `tree.ts`).
