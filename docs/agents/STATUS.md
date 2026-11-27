# Where things stand

**Start the review app yourself:** `bash tools/curation-review/run.sh`, then open http://127.0.0.1:5173. It's local only and
spends no credits; Ctrl-C stops it. If Claude's copy is still running on port 8787, either use Claude's copy at
http://127.0.0.1:5183, or run your own beside it with `PORT=8788 CURATION_VITE_PORT=5174 bash tools/curation-review/run.sh`.

Plain summary for the owner, updated as work lands. Newest at the top. Ticket numbers are in brackets for lookup only.

---


## 2026-11-24: 0.9.0 shipped
- Production: the data (r-0aec88ed3edb, you ran it), the website and the feeds worker are on 0.9.0; the production
  smoke test passed 18 of 18.
- Phones: Android build 44 on Play Internal and Closed testing; iPhone build 45 uploaded to App Store Connect. The
  Play Production rollout and the App Store submission are yours (`views/OWNER.md`).
- Before shipping, the device pass found and fixed two defects (background checks never ran under the code shrinker;
  a crash moving a frame to another water). One known issue shipped: a cold-start tab link can land short (fixed for
  0.9.1).
- The release branch merges back into main by PR #140. Restart pages: `docs/agents/START-HERE.md`.

## 2026-11-22 20:15: 0.9.0 batch chat, stopped early (low credits; no release)
- Seven, then fourteen parallel lanes worked the 0.9.0 batch plus every small open ticket (your rule: no bathymetry,
  stocking or big builds). Each PR gets a second agent's review before it merges; the reviews found real defects in
  four of them, all sent back and fixed or being fixed.
- On main now: pages for the 156 border waters and former lake names, the region page's map link framed on its region
  (phone too), source maps served, the crash triage (most crash reports were one search engine's page renderer) with
  four fixes, Runs data warmed in the background.
- Waiting to merge: label overlap (T997), Apple push proven on staging + incremental backup, forecast errors, R8 on
  every Android build (obfuscation 1% → 72%), the Serpentine ditches, out-of-B.C. drawing, app UI smalls, ledger cleanup,
  and four draft lanes.
- Your questions and production steps are in `views/OWNER.md` (built by `node app/tools/ledger.mjs`).

## 2026-11-22 13:40: build/CI streamlining chat finished (no release)
- `main` is now the protected trunk: every change is a pull request with four checks, and it merges itself when they pass.
- Version numbers now follow the commits (SemVer). The next app release is 0.9.0; it waits for the next chat's work.
- One release runbook for you: `docs/runbooks/release-by-hand.md`, plus a page explaining every release stop.
- Android builds use one pinned MapLibre commit instead of a patched copy (T978); test suites faster (T866); docs match
  the code (T987).
- Production steps are yours only (you, 2026-11-22). Claude prepares and checks, then gives you the command.
- New tickets from your finds today: T1003 + T1009 (Play's code-optimization score), T1007 + T1008 (website Lighthouse),
  T1010 (region page map link), T1011 (156 waters crossing the border have no web page; old lake names not on pages).
- Your list of 100 pages to submit to Google: `docs/seo/index-requests-top-100.md`.

Older entries (0.8.x and earlier) were cut in T1081; git history holds them.
