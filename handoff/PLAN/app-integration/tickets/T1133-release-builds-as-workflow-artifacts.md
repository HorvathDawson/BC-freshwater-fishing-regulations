# T1133 — Releases built by GitHub workflows as artifacts, then uploaded from them

Status: OPEN — after the repo move and T1062's build; a plan page first, then in stages

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1133 | Releases built by GitHub workflows as artifacts, then uploaded from them | OPEN — after the repo move and T1062's build; a plan page first, then in stages | | owner 2026-11-25; the plan says what each stage costs and which secrets move |

Source: owner 2026-11-25 (planning chat), verbatim: "after repo move maybe we should have the releases built and
released as gha artifacts? which are then uploaded? artifacts will use current store info and everything in the
package too. so that everything that is true for the release is there. also will have android, ios and web builds?
or is this too much?" Related: T1062 (release candidates: an rc is built once and the same files are promoted),
T1120 (a fresh clone can rebuild everything), T1131 (the store pictures by workflow), T993 (the release runbook),
T1058 / T1063 / T1097 (builds starving each other on the one Mac), `confirm-prod-owner-only`, `no-github-schedules`.

## The planning chat's answer to "is this too much?" (a proposal; the plan page checks each claim)
Not too much if it is staged, and the data build stays out of it.
- Fits a standard Linux runner (free on a public repo): the web site, the feeds worker, the Android bundle (with
  its R8 mapping file).
- Fits a standard macOS runner, harder: the iPhone build (signing certificate and profile in the workflow).
- Probably does not fit: the data and tile build (the atlas and its sources are many GB and the build has taken
  this Mac's memory before). It stays on the Mac; its result is already a versioned release in R2 that the app
  builds read.
- What "everything true for the release" means: one bundle per release tag holding the Android file, the iPhone
  file, the web export, the worker bundle, the store listing words and pictures as committed at that tag, the
  What's-new text, the data release id it was built against, and checksums; kept on the GitHub Release for the tag
  (workflow artifacts expire after 90 days at most).
- CONFIRM_PROD stays the owner's: every upload job (Play, TestFlight, production site, production worker) sits in
  a protected environment that needs his approval click; nothing uploads on a push or a schedule; build jobs get no
  upload secrets.
- The cost that is new: signing keys and store keys move into GitHub secrets on a public repo. They are not
  readable from fork PRs, but the plan has to say exactly which workflows can read them and how a leaked one is
  replaced.

## Work
1. A plan page for the owner to agree, nothing built: the stages below with runner minutes and wall time measured
   on one trial run each, the list of secrets that move and their scope, how it joins T1062 (the rc tag starts the
   build; the final release promotes the same files, no rebuild), what stays on the Mac, and what the runbook's
   steps become.
2. Stage A: web, worker and Android built on the release tag and attached to the GitHub Release; no upload. The
   Mac's build stays as the way to ship until a release has gone out from A's files once.
3. Stage B: the upload jobs behind the protected environment (Play Internal / Closed, staging site and worker,
   then production with the owner's approval).
4. Stage C: the iPhone build and TestFlight upload on a macOS runner.
5. The store listing words and pictures in the bundle, and the checks that they are the ones at the tag (T1127's
   list, T1114).

Acceptance (stage A): a release tag produces the files without anyone's Mac, byte-for-byte reproducible where the
tools allow (say where they do not), and one release ships from them. Severity: none (tooling; it removes the
single-machine risk and the build contention). Release: after the repo move; the plan with T1062's build at the
start of 0.10.1, stage A in 0.10.1 if the plan is agreed, B and C after. Model: Opus for the plan and the secrets
design, Sonnet for the workflows. Size: L in stages (the plan is S). Owner: yes (agrees the plan; puts the secrets
in; the approval clicks).

## Design 2026-11-25 night (read-only; build-ready)
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `ship-workflow-design.md` (every release step today, the workflow sketch, the secrets list,
the migration in steps). The owner's words that set it, 2026-11-25: "could we make the feeds workers prod and
staging get deployed from gha? would that be a good idea?" and "ideally we have all the cloudflare items pushed
from gha I think? like when a release needs new worker code pushed then it gets pushed and similar push other
stuff it needs like data or webapp etc? maybe a seperate work deploy is bad."
- ONE ship workflow per environment, started from the rc tag (staging) or the final tag (production): the worker
  only if it changed since the last shipped tag, then the data pointer, then the web app; a check after each;
  production behind a GitHub Environment with the owner as required reviewer. The data build stays on the Mac.
- The data is already addressed by content (the release id is a hash of every file's hash, `rebase.py:38-42`), so
  the Mac uploads and tags and CI only checks every hash and the one-vintage rule, then moves the pointer. The
  stamped manifest bytes are outside that hash: the rc tag must carry their sha256. The worker inputs and
  `map-assets/v1` are fixed keys: the Mac stages them under the release id.
- **The approval gap:** agents use the owner's `gh` login, and GitHub accepts any `repo`-scoped token of a
  required reviewer, so an approval click alone is no stronger than CONFIRM_PROD is today. Fix first: agents push
  with their own GitHub identity, then "Prevent self-review" (T1160).
- Today's exposure: no environments exist; the R2 keys and an unused `CLOUDFLARE_API_TOKEN` (permissions unknown)
  are repo-wide; actions are pinned by tag; no tag ruleset. Cloudflare offers tokens limited to named Workers; no
  Cloudflare OIDC was found.
- Production becomes one approval click (about 25-35 min unattended: a guess) instead of 5-7 commands at the Mac.
  Phones stay on the Mac for now (MapLibre is built from source); prerequisites for later: the MapLibre Android
  library built once per pin in CI as a release asset; the SEO "previous snapshot" kept in R2.

## Owner

- [ ] 2026-11-25 — Picks for the ship workflow (defaults first): prune inside the approved run, exactly the keys the plan listed; the production site built after your click; agents get their own GitHub identity; approval required for all outside contributors' PR runs; token expiry 12 months; order: the clean-up step now, the rest after T1144's gate workflow and 0.10.0. Yours when it starts: create the two environments, the scoped tokens and the secrets (steps handed to you).
