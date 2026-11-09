# After chat 8: start here

Written 2026-11-09. It replaces `NEXT-SESSION-after-chat7.md`.

## Paste this to start the chat
> Read `handoff/PLAN/START-HERE.md`, the restart page it names, and the STATUS.md top entries. Do the list in order.
> Start ticket work without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## The goal of this chat
The owner wants two things (2026-11-09): **finish the loose ends below**, and **get everything ready for a proper workflow review
with Fable 5.2**. For the review that means:
- the ledger is whole and every IN REVIEW ticket is closed or has a named owner step;
- the docs match the code (AGENTS.md, the runbooks, pipeline/README);
- the tree is clean, with no stray worktrees or side folders;
- fast-fail covers what failed late.

The bigger goal is unchanged: **T495**, Play closed testing on production.

## Where chat 8 left things
- **Build 22 (0.7.0, versionCode 22) is on Play INTERNAL** (3d807829d). It carries:
  - T517: one part per connected run, names from the run's ends.
  - T527: stock-style gauge alerts, including the percentile for the day.
  - T470: map colours meet WCAG contrast in dark mode.
  - T526: DFO counts as stale after 24 h without a check.
  - T523: cut ids keep their direction.
  - The boot page's "Try again" is now centred.
- **Data r-d14bdb180f55 is LATEST and on staging**, with `minAppVersion 0.7.0`:
  - It adds the Barrow Town cut and the owner's Chilliwack marks.
  - The Squamish crossing moved onto OpenStreetMap line 5L32.
  - T510's connectors are named.
  - The binding diff was read in full. Every one of the 136 rules traces to Sumas, Squamish, the Columbia length cut or T510; Williamson Slough is tidal, so its lake is correctly unbound.
- Staging feeds worker deployed (53b90e7: it reads `inseason-links/3`); gauges, DFO and in-season ok.
- Emulator (build 22 code, test channel):
  - Install: PASS.
  - Offline: PASS except two steps, **"Check now offline"** (flow `open-updates-log`) and **"slow line"**. The report is under `app/tools/testbed/out/runs/`.
- **Play listing graphics:** the icon and feature graphic upload, but every listing save gets 403. The owner granted "Manage store presence" to `play-uploader@…`, so it is most likely still propagating. Rerun `deploy/staging/play-listing-images.py --package ca.canifishthis.app`.

## Loose ends, in order
1. Fast-fail is DONE (6f5790a44, T483 "2026-11-09 additions"); `./scripts/fast-fail.sh` runs in 9 s. It now covers:
   format pairs, staging site version vs release (`site_gate.py`), goldens paired, build keeps LATEST's reach, accept-diff
   by content, the skip guard, and the heavy precheck. Gaps left: `reach.cli` run directly still overwrites without a copy;
   only session.sh opts into the precheck.
2. Emulator: fix the 2 failing offline steps (app or flow). Then run the feeds and big-update scenarios.
3. Staging site: the deploy (export → build-seo → site.sh) was still running at hand-off; the site served 0.6.9, so the new
   site check blocks every staging upload until it is current. Confirm the deploy and a green smoke. Then run `release --staging --min-app-version 0.7.0` once more and confirm it says everything is done.
4. Play listing: rerun the script above. If it still gets 403, re-check the permission with the owner.
5. **T525 round 3** (owner, strongly): the review app is "still really bad, so confusing". Make it radically simpler:
   - one proposal;
   - plain status ("Saved. Takes effect after the next rebuild builds the new cut …");
   - internals hidden.

   The owner saves the two Forks fixes (proposals rewritten in chat 8: 3 arms, no tributaries) and the Chilliwack ones.
6. **Section E** (Region 6): it must be the region minus B, C, D and F (owner). Check that it is computed that way, not a hand list.
7. **Wording 54 = wording 56** (old typo): apply the 56 proposal.
8. Prod feeds worker placeholder: check whether it exists. Create it if missing; the owner has permitted this.
9. Test gap: in `app/tools/regs-recut/taps.py`, PART_GROWN only checks that the label differs, so a wrong part passes. Tighten it to name the expected part.
10. Owner decisions waiting in OWNER-TODO:
    - Q176: casing for map lines in light mode.
    - T527: the percentile line is fixed when the alert is saved; tapping the notification opens Conditions; there is no Edit.
    - The build 22 phone check.
11. Clean up:
    - Worktrees: `.claude/worktrees/agent-*`, `~/dev/personal/bcfr-t360`, `bcfr-t360b`, `bcfr-baseline-r6dce`.
    - Old side runs under `data/generated/side/`.
    - `data/generated/atlas/full.prev`, once nothing needs the old baseline.
12. Clean OWNER-TODO.md using the Owner Desk answers: https://claude.ai/artifact/DNAAwMJiUEHLhWvBHmhE8U (db "answers").
13. Later, not now:
    - T528: River Forecast Centre advisories. Mockup first, approved by the owner.
    - Prod release: the owner runs `CONFIRM_PROD=r-d14bdb180f55 python -m pipeline release --prod r-d14bdb180f55` after DFO labelling.

## Rules learned in chat 8
- **Play INTERNAL uploads need no wait.** The owner is the only user: upload any time and run the checks anyway.
- **A release whose data needs a newer app** must bump the app version, set `--min-app-version` to it, and redeploy the staging site before the upload.
- **Never run `pipeline build` outside a release on the live paths without keeping LATEST's reach run first** (fast-fail lane item 4). Use `--side` for experiments.
- **Worktree subagents (`isolation: worktree`) came up on an old tree.** Run lanes in the main checkout and commit with `-- <paths>`.
