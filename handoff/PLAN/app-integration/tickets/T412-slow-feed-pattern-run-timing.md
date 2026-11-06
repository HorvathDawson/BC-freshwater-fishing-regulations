# T412 — One "slow feed" pattern, run timing first (HYDAT clim and bathymetry follow it)

Status: DONE in code 2026-11-05 — 67f8be2a6 (live with the next staging release) — was OPEN (owner 2026-11-05, T408 walkthrough; `handoff/PLAN/data-flow-decisions.md` "Run timing").

Run timing is made two ways today (Mac deliver, T373, and the manual `feeds-runtiming.yml`, which refuses until the
owner uploads a binding base after each atlas promote). Owner: a slow feed like HYDAT and later bathymetry — "make sure
it is producing same output … deployed to R2 directly".

## Work
1. One path: the `feeds-runtiming.yml` workflow, own R2 folder + `index.json`. Deliver stops making run timing.
   **Started by a button or by the worker — never a GitHub schedule** (owner 11-05: GitHub as scheduler is flaky and
   unpaid). The worker emails when a source looks changed (or starts the run itself, T413's token).
2. `upload.sh` uploads the binding base with every release, so the workflow never waits on the owner.
3. Content-hashed files; publish only on a real change, with a diff report (runs / waters added, dropped, changed).
4. A build-twice test asserts identical bytes. Staging automatic; prod the owner's choice.
5. Write the pattern down so HYDAT clim and bathymetry follow it when they are next touched.

Depends: T297 (release command). Size: M. Owner: no (prod enable is theirs).


## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T199, T225 — read each file's open items and finish them here; each closes pointing here.

## Owner 2026-11-05: how a release and run timing stay in step
Asked: the workflow as the only publisher breaks T373's guarantee (a paint index for another atlas paints nothing).
Owner: "upload waits on workflow? why couldn't it happen in parallel then wait if it finishes first … we want whatever
would be the industry standard." → **Atomic publish:** the staging upload puts the release's immutable files and the
binding base, starts the run-timing workflow for the new handles IN PARALLEL with the rest of the upload, and flips the
release pointer (`release/manifest.json`) LAST, only once the workflow has published run timing for those handles.
The workflow is the only publisher; the Mac release no longer builds run timing.

## Done in code 2026-11-05 (goes live with the next staging release + worker deploy)
- **One publisher:** `feeds-runtiming.yml`. `pipeline deliver` no longer builds the feed; it stages the release's
  BINDING BASE (`runtiming.feed.release.stage_base` → `data/generated/runtiming/feed/release/{base-<h>.json.gz,
  base.json}`), and the vintage check refuses a delivery whose base names other tiles. `feed.release --local` keeps the
  local test bed's feed (`feed/out`, never uploaded).
- **Atomic switch (owner's choice):** `deploy/staging/upload.sh` checks the base and that the branch is pushed, puts the
  base in `private/runtiming/base/`, starts the workflow with `tiles=<the release's>` and `upload=true`, uploads the rest
  meanwhile, waits for the run (`gh run watch`), checks `runtiming/index.json` names the release's tiles, and only then
  writes `release/manifest.json`. A failed run leaves the pointer unwritten. (The bucket clear at the start is unchanged,
  so staging was never atomic before the pointer; that is the existing staging behaviour.)
- **Workflow:** `tiles` input; `--tile-version`; a diff step (`pipeline.runtiming.feed.diff`: runs added / dropped /
  changed, in the run summary); uploads only a real change (files by sha256 or other tiles; `built_at` alone is not).
  No schedule. A `push` of the workflow file only REGISTERS it (job skipped): GitHub cannot dispatch a workflow that is
  not on main until it has run once (checked: 404 before; `feeds-sync-inputs.yml` dispatches because it registered
  that way). Same registration added to `feeds-hydat.yml` for T413.
- **Worker:** weekly (Mon 10:40 UTC) NuSEDS check (`src/runtiming.ts`): a newer "All Areas NuSEDS_<date>.zip" than the
  live feed's `sources.dfo.updated` starts the workflow (T413's `dispatchOnce`, owner's token) and emails; without the
  token the email names the button. `POST /run/runtiming` by hand. PSF has no machine-readable date: yearly by hand.
- **Tests:** build twice → same bytes (only `built_at` moves); the diff says a real change only; the release stages
  its own base; vintage/deliver tests on the base. Worker: 4 new (208 pass).
- **Pattern for HYDAT clim and bathymetry** (when next touched): one workflow publishes; R2 folder + `index.json`
  written last; content-named immutable files; publish only on a real change, with a diff in the run summary; started
  by a button, the worker (source changed) or the release upload — never a GitHub schedule; a release that depends on it
  flips its pointer only after it.
- **Not yet run end to end:** the first real run happens with the next staging release (it also proves the workflow on
  a GitHub runner, which has never run it: T199 ran the same commands locally).
- T199's open item (data refinement, step 2) and T225's remaining items (prod switch-on, a week with no hand runs) are
  not part of this; they stay listed in those files' history and move to T404 (prod) / a new ticket if the owner wants
  the refinement.
