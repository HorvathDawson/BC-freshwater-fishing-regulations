# T865 — Production data clean-up is automatic (stay under R2's free 10 GB)

Status: IN PROGRESS (lane app/t865-prod-prune).

Owner 2026-11-18, verbatim:
- ~18:20: "We will also what to delete the old data for old app version once everything is settled to avoid being over 10gb"
- ~18:23: "That clean up should be automatic"
- ~18:25: "when we flip the switch i will get you to clean that stuff up too"

Today `deploy/prod/upload.sh` clears nothing (old releases are pruned by hand, PRODUCTION.md "Not yet"), and the v1
layout (the old Vite site's data) is deleted by hand at PRODUCTION.md step 11.

Work:
1. After the pointer moves, `deploy/prod/upload.sh` deletes every release-scoped object that neither
   `release/manifest.json` nor `release/manifest.previous.json` names (rollback keeps working). Delete by an allowlist
   of release-scoped prefixes, never "everything else"; refuse (and say so) when a manifest cannot be read or parsed, or
   a file a kept manifest names is missing. Prints what it deleted and the bytes freed; runs under `--dry-run` and
   against a `--bucket` rehearsal.
2. The v1 layout goes automatically once canifishthis.ca serves the new site (a marker the new web export has and the
   old Vite site lacks); without it, v1 is kept and the output says why. Run by `upload.sh` and `deploy/prod/site.sh`.
3. Tests with no network; one proven to fail when the guard is broken.
4. PRODUCTION.md: step 11's delete becomes automatic. Turning off the old `bc-fishing-cron` worker and `main`'s
   `update-hydro.yml`, `update-in-season.yml`, `update-stocking.yml` is CLAUDE's step at the switch, on the owner's
   word (owner ~18:25), not before.
