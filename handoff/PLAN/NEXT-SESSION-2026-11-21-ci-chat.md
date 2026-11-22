# Next session (written 2026-11-21 23:25, build/CI streamlining chat; no release in it)

## What this chat is
Owner picked T993 (release / CI / CD cleanup; T992 folded in) and T978, then widened it: every build/audit ticket
(T791, T788, T777, T792 B, T768, T661 #3, T987, T866, T931, T736, T485 step 3), a production-level code audit (T994),
GitHub Actions cleanup, and a real branch structure (T995). Owner rule (2026-11-21): change things to industry best
practice. No release is cut in this chat; 0.8.6 comes later.

## Done (wave 1, merged + pushed on `redesign/stream-sections`)
- Reports: `handoff/PLAN/audit-2026-11-21/t993-inventory.md` (inventory, Actions waste, every 0.8.4/0.8.5 stop
  traced, gate glossary, 17-item plan) and `t994-audit.md` (30 findings).
- Merged, each independently reviewed (fixes made): T978 (one build precheck, data checks out of the app build, mini
  release staleness caught), T994 F1 (feeds worker), F2 (pipeline + review backend), F3 (app data seam), T993 G1
  (workflows split, docs-ci, feeds-worker-ci, permissions, two staging workflows deleted), G2 (derived minimum app,
  rehearsal, size headroom, conservative auto-accept, `scripts/before_push.py`), G3 (deploy gates, heavy.sh disk check).
- GitHub cleanup: 8,399 runs of deleted workflows being deleted (background script in the chat's scratchpad,
  `gha-clean/delete.log`); caches 10.4 → 1.24 GB; retention 30 days; Copilot environment removed.

## Since (2026-11-22)
- **T995 landed:** `main` is the protected trunk (PRs, four required checks, merge commits, admins bypass only on a PR
  merge); `redesign/stream-sections` is read-only until 0.8.6 redeploys the production feeds worker. Every change now
  goes by PR from a `feat/ fix/ ci/ docs/ chore/ test/` + ticket branch. T788 + T791 DONE; T931 reviewed (T1000 tile
  gap left); T998 (versioning) and T999 (worker backups) filed.

## In flight when written
- GitHub's first runs of the new workflows on the pushed head: read `gh run list --commit <head>`.
- MapLibre 0002 measurement (T978, on lane `lane/t978-maplibre-measure`, emulator on heavy lane 1): owner decides on
  the numbers (OWNER-TODO).

## Next (in order)
1. Ledger statuses for T978 / T993 / T994 / T788 / T791 after CI is green; post-merge fixups (worker emails still say
   `update-hydat.sh --upload`, `feeds-hydat.yml` comment, worker `fetchSeries` 404 rule, boto3 pin, F1 backup past
   ~4,500 reports → ticket).
2. **T995** branch switch: `main` = protected trunk (required checks = G1's jobs), merge commits, release branches at
   scope lock, tags by the release command, versioned pre-push hook (`before_push.py --hook`), prune merged branches
   and worktrees, AGENTS + memory rule updates.
   **T998** with or right after it: SemVer, the bump from Conventional Commits (commitlint hook + CI + before_push),
   1.0.0 at public launch, `v*` tags, generated developer changelog. Commits already use the format
   (`fix(T983): …`).
3. Wave 2 lanes: test isolation (T777 + T792 B + plan 6), heavy.sh fast-fail gate (T661 #3, plan 5), T768 rest,
   T931 merge, T866 trim (owner sees the list), T736 + T485 step 3, T987 cleanup.
4. Last: the one human runbook + T992 explanation (plan 11).

## Next chat / next release (owner)
- **T996** test the Apple push service; **T997** overlapping boundary labels (Nicomekl).
- From the previous restart page: TestFlight external review state; tester feedback; T991, T981, T940, T977, T987
  (now in this chat), T709/T715 postponed.

## Waiting on the owner (also in OWNER-TODO)
- MapLibre 0002 decision on the numbers; turn off the Copilot agent in the repo's Copilot settings.
- T973 CO question; Q182 (T723); T725; T760.
