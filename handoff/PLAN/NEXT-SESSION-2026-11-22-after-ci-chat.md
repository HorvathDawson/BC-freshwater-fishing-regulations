# Next session (written 2026-11-22 13:20, end of the build/CI streamlining chat; no release in it)

## First, every chat
`handoff/PLAN/START-HERE.md` step 0: read every report source (both feeds workers' reports + crashes, TestFlight
feedback + external review state, Play), compare with OWNER-TODO's last "Store checks" line, ticket anything new with
its report id / crash hash. The 11-22 check found T1004 (a report) and T1005 (~20 untriaged crash kinds).

## What the last chat did (all merged to `main` by PR)
- **T995 branch structure:** `main` is the protected trunk (4 required checks: `docs ok`, `app ok`, `feeds worker ok`,
  `pipeline ok`; merge commits; PRs only, auto-merge). `redesign/stream-sections` is read-only until the production
  feeds worker is redeployed from `main`.
- **T998 versioning DONE:** Conventional Commits `type(Tnnn): subject` checked in the commit hook, docs-ci and
  `before_push.py`; header ≤100, **PR title ≤93**. The next app version computes to **0.9.0** (owner accepted). Owner:
  0.9.0 is NOT cut until the next chat's work is in it.
- **T993:** the one human runbook `handoff/PLAN/runbooks/release-by-hand.md` + `release-gates-explained.md` (T992),
  PR #24. Owner reads it (OWNER-TODO). Pipeline CI job limit 30 min, `--durations=25`.
- **T978:** build checks fail only on code; Android builds use one pinned upstream MapLibre main commit (no patch),
  PR #15.
- **T866 DONE** (vitest on 4 shards, 5 duplicate tests removed, 30 wording files folded), **T987 DONE** (docs match the
  tree; history-only pages removed, PR #27), **T994** fix lanes F1-F3 + the T993 deploy lane merged.
- **CONFIRM_PROD is the owner's only** (owner 2026-11-22: "only me unless i say otherwise that time"): the production
  site, production feeds worker, production data / R2, Play production, App Store, phones. Claude prepares, verifies
  and hands over the command. Staging stays Claude's.
- New tickets from the owner 2026-11-22: T1003 (R8 on in production; Play "Obfuscation 1%"), T1004-T1006 (reports),
  T1007 (Chrome `StorageType.persistent` warning), T1008 (web source maps: serve or keep hidden, owner picks, default
  serve), T1009 (Play "DEX code optimization: Low", all three categories), T1010 (region page "Open the map" is not
  framed on the region; page should lead into the app), T1011 (156 waters partly outside B.C. get no web page: the
  page rule reads `outside_bc`, a section COUNT, as a flag; renamed waters never print their former name; home page has
  no crawlable links).
- `handoff/PLAN/seo/index-requests-top-100.md`: the owner's 100 URLs to submit by hand in Search Console.

## Next (in order)
1. Step 0 (reports).
2. **T996** test the Apple push service end to end; **T997** overlapping boundary labels (Nicomekl Part 2).
3. The 0.9.0 batch: T1003 then T1009 (R8 and the DEX score; T1003's Play deadline date: owner reads it in Play
   Console), T1010 + T1011 (website), T1007, T1004, T1005, T999, T1000, T1001, T1002, and the earlier leftovers
   (T991, T981, T940, T977). Then cut 0.9.0 with the new runbook; production steps are the owner's.
4. T994: close or ticket any finding its ticket does not mark fixed. T995 left: prune merged branches; delete
   `redesign/stream-sections` and `cache/maplibre/fslean-m2` after the 0.9.0 production feeds worker redeploys.
5. Small, ticket when touched: **the pre-push commit check should check `merge-base(origin/main, HEAD)..HEAD`, the
   range docs-ci checks** (today it checks `<remote old>..<local>`: it re-checks main's commits arriving by a merge,
   and it MISSED #15's three over-long headers already on the remote branch, which docs-ci then refused); dev-tooling
   commits still count as app paths in `release_version.is_app_path`; a stale memory reference near
   `release.py:2044`; `prod_instructions` names PRODUCTION.md; T866 inventory line numbers.

## Waiting on the owner (also in OWNER-TODO)
- Read the release runbook and the gates page (T993).
- Turn off the Copilot coding agent (repo Settings → Copilot).
- T1003's Play deadline date; T1008 serve or hide source maps; T1010 picks the region-page look (pictures first).
- The claude.ai runbook copy (artifact 5r4yEqNwqygin1oKo5Zxjk) and the Owner To-Do page (Lh2ougAwfxx1wNmCV18m8f)
  could not be opened from the CI chat's account ("not found / not shared"): update them from a session that can, or
  ask the owner whether to make new ones.
- From before: T973 CO question; Q182 (T723); T725; T760.
