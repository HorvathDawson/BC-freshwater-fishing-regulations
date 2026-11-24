# Restart page: the 0.9.1 follow-ups chat (roadmap row 2)

Written 2026-11-24 04:35 MST, mid-night, by the 0.9.1 chat. It ran overnight beside the 0.9.0 chat and never touched
the main checkout. Scope is frozen to `handoff/PLAN/ROADMAP-after-0.9.0.md` row 2. The name is not NEXT-SESSION-* on purpose: the guard keeps at most two of those, and they are the 0.9.0 chat's. The 0.9.0 chat has its own restart
page (START-HERE step 2); this page is only for 0.9.1.

## Where each item stands

| Item | State |
|---|---|
| T1048 the cut writes the app_reads row | Merged (#95, #98). The cut now REFUSES while a shipped version has no row on main: cut 0.9.1 only after `release/0.9.0` is merged back into main. |
| T1021 data-path gate | Merged (#97). |
| T1041 review lows | Merged (#99). Left, low: no test that the "one level up" test list stays sorted. |
| T1036 "All sources" sheet | Merged (#96). Owner question 1 below. |
| Trout per-row source pages (T1031) | Merged (#100), app only. Owner question 2 below. |
| T1016 web tab across releases | Merged (#101). Owner question 3 below; the feeds worker must be deployed for the reports to be classed `expected`. |
| T1020 answers_diff licence gap | Reviewed, merging (#104). A tool gap, not a real change. |
| T905 salmon feed and head sizes | STOPPED after measuring (#94, docs): no shrink is safe for installed 0.9.0 apps. Owner question 4 below. Budgets unchanged. |
| T1047 datum-change list | In review fixes (#103). Five rows, all proposed. Owner question 5 below. |
| T1030 changed-waters count | In review (#105). Needs the 0.9.1 data release and app build; the first release carrying it shows no count. |
| T1027 R8 scores | Lane running (the only emulator lane: AVD n1_pixel_b, port 5556). |
| T1024 stub-polygon double outline | Lane running. |
| T1029 one-bank stripe | Pictures only, as a draft PR that is not to be merged (same lane, after T1024). Owner Q4–Q6. |
| T1049 review lows | Lane running. |
| One file per ledger row / OWNER-TODO item | Not started: last, after everything above has merged. |
| 0.9.0 device-pass bugs | None handed over yet. |

## Owner questions (pictures are in the repo; the chat sends them with the questions)

1. T1036: should a species sheet's own rules, and a row that only opens a sheet (Possession), be listed in the page's
   "All sources" sheet? Built: no. `handoff/PLAN/app-integration/T1036-shots/`.
2. T1031: a trout row's Source line now lists every page behind the row (A, built). B also lists every rule in the
   row's breakdown; it needs a Python change and a data release. `handoff/PLAN/app-integration/T1031-shots/`
   (`rowpages-*`).
3. T1016: the two notice texts. Built: "This page's data was replaced by a newer release. / Load the new data ›" and
   "The app was updated since this page opened. / Reload ›". `handoff/PLAN/app-integration/T1016-shots/`.
4. T905: pick one of the four options in `handoff/PLAN/app-integration/tickets/T905-shrink-live-salmon-feed.md`
   (keep the budgets; move the worker's own state out of the feed; new feed and head formats in 0.9.1; or the same
   with the feed waiting until most installs are on 0.9.1).
5. T1047: accept or reject each proposed datum change (table in the ticket). 08HD006 and 08EF001 can be accepted now.
   07FD001 and 08KH019 changed zero inside this year and can be accepted only after the this-year line is fixed.
   08MH153: recommended reject (a slow creep, not a step). `handoff/PLAN/app-integration/T1047-shots/`.
6. T1029 Q4–Q6: with the lane's pictures, once its draft PR is up.

## Next, in order

1. Finish the running lanes; each PR gets one independent review, then auto-merge.
2. The one-file-per-ledger-row lane, last.
3. Put the owner questions in `handoff/PLAN/OWNER-TODO.md` and the owner's to-do page; one push notification.
4. After 0.9.0 is on production AND `release/0.9.0` is merged back into main (the T666 fixture re-cut comes with it):
   cut 0.9.1 (`python -m pipeline release --cut`), release to staging, Play Closed, then hand the owner the
   production commands (`handoff/PLAN/runbooks/release-by-hand.md`). CONFIRM_PROD is only ever typed by the owner.
5. Update `handoff/PLAN/play-production-access/releases.md`, the feedback log's "Fixed in" column and the
   questionnaire (Q3, Q7, Q8) for 0.9.1.

## For the planning chat (new finds; this chat makes no tickets)

- `pipeline reports` with the worker tokens lists 36 old items as NEW (app 0.7.x–0.8.0, up to 2026-11-18, mostly
  the staging worker).
- T1016: the release pointer has no `previous` field, so a 404 on the kept previous release is classed as replaced
  (a too-early prune would be masked); the web has no app-version gate on the in-session apply.
- T1047: 08MH044 and 08MH053 are level-only and relocated (the off-rating check cannot see them); 08HA003 and
  08NK026 are too recent in HYDAT; the this-year line mixes zeros for 07FD001 and 08KH019.
- T1021: `pipeline/gauges/feed/publish.py` hides a failed HYDAT release check (`except Exception: latest = None`);
  the DFO cache readers never refuse a missing directory.
- T1048: on main a format change fails `app_reads --check` for the old app version with no clean way out.
- T1030: "N waters changed" prints no thousands separator; the count is against the immediate predecessor only;
  wording-only changes are not counted (the owner may want to say otherwise).
- T1020: `LIC_KEYS` in `answers_diff` does not list the licensing refs `alt`, `displaced_by`, `displaced`,
  `also_printed`.
- Tests: `app/packages/ui/src/chart.render.test.tsx` is timing-dependent (a fixed 30 ms wait); vitest times out
  ("Timeout calling onTaskUpdate") under machine load.
