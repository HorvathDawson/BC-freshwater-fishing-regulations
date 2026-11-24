# Restart page: the 0.9.1 follow-ups chat (roadmap row 2)

Written 2026-11-24 06:45 MST by the 0.9.1 chat, at the end of its overnight run beside the 0.9.0 chat. It never
touched the main checkout. Scope is frozen to `handoff/PLAN/ROADMAP-after-0.9.0.md` row 2. The name is not
NEXT-SESSION-* on purpose: the guard keeps at most two of those, and they are the 0.9.0 chat's. The 0.9.0 chat has its
own restart page (START-HERE step 2); this page is only for 0.9.1.

## Where each item stands

| Item | State |
|---|---|
| T1048 the cut writes the app_reads row | Merged (#95, #98). The cut REFUSES while a shipped version has no row on main: cut 0.9.1 only after `release/0.9.0` is merged back into main. |
| T1021 data-path gate | Merged (#97). |
| T1041 review lows | Merged (#99). Left, low: no test that the "one level up" test list stays sorted. |
| T1036 "All sources" sheet | Merged (#96). Owner question open. |
| Trout per-row source pages (T1031) | Merged (#100), app only. Owner question open (A built, or B too). |
| T1016 web tab across releases | Merged (#101). Owner: the two notice texts; the feeds worker must be deployed for the reports to be classed `expected`. |
| T1020 answers_diff licence gap | Merged (#104). A tool gap, not a real change. |
| T1049 review lows | Merged (#107). Slow-phone case proven by test only. |
| T1047 datum-change list | Merged (#103). Five rows, all proposed; nothing changes until the owner accepts rows. |
| T1027 R8 scores | Merged (#108): 72 → 76 on all three, floors 75. The WorkManager keep rule and the `notify-fire` walk merged first as #109; the 0.9.0 chat cherry-picks that fix into 0.9.0. |
| T905 salmon feed and head sizes | STOPPED after measuring (#94, docs): no shrink is safe for installed 0.9.0 apps. Owner picks 1 of 4 options. Budgets unchanged. |
| T1030 changed-waters count | Approved, on auto-merge (#105), last seen fixing a conflict with main and a stale mini release. Needs the 0.9.1 data release and app build; the first release carrying it shows no count. Owner question open (what counts). |
| T1024 stub-polygon double outline | Approved, on auto-merge (#111). Needs the 0.9.1 tile build (a full water export). Owner look. |
| T1029 one-bank stripe | Pictures only, draft PR #112: NOT to be merged. Build after the owner answers Q4–Q6. |
| One file per ledger row / OWNER-TODO item | Lane L12 building it (scope T1041, branch `chore/T1041-one-file-per-row`). Its PR is to be re-imported on the then-current main and merged only AFTER `release/0.9.0` has merged back (the release branch edits the old three files). |
| 0.9.0 device-pass bugs | None handed over. |

## Owner questions

All eight, with pictures, are on https://claude.ai/artifact/UZhX9gBLs6JiynhgVcWuFR and in `handoff/PLAN/OWNER-TODO.md`
(section "2026-11-24: 0.9.1 overnight questions"). Ask them in chat in batches, most blocking first (T905, T1047 rows,
T1030 count, T1029 Q4–Q6, T1016 words, T1031 A or B, T1036, T1024 look), and write each answer into OWNER-TODO and
its ticket in the same turn.

## Next, in order

1. Check #105 and #111 merged; review the one-file-per-row PR (one independent review) and hold its merge as said.
2. Build what the owner's answers unlock: T1029 (the stripe), T905 (the option picked), T1047 (flip accepted rows
   through `curated_write`, rebuild clim.json), T1030 (A or C if picked).
3. After 0.9.0 is on production AND `release/0.9.0` is merged back into main (the T666 fixture re-cut comes with it):
   merge the one-file-per-row PR, cut 0.9.1 (`python -m pipeline release --cut`), release to staging (a full tile
   build: T1024 needs the water export), deploy the staging feeds worker (T1016's report class), Play Closed, then hand
   the owner the production commands (`handoff/PLAN/runbooks/release-by-hand.md`). CONFIRM_PROD is only ever typed by
   the owner.
4. Update `handoff/PLAN/play-production-access/releases.md`, the feedback log's "Fixed in" column and the
   questionnaire (Q3, Q7, Q8) for 0.9.1.

## For the planning chat (new finds; this chat makes no tickets)

- `pipeline reports` with the worker tokens lists 36 old items as NEW (app 0.7.x–0.8.0, up to 2026-11-18, mostly
  the staging worker).
- T1016: the release pointer has no `previous` field, so a 404 on the kept previous release is classed as replaced
  (a too-early prune would be masked); the web has no app-version gate on the in-session apply.
- T1047: 08MH044 and 08MH053 are level-only and relocated (the off-rating check cannot see them); 08HA003 and
  08NK026 are too recent in HYDAT; the this-year line mixes zeros for 07FD001 and 08KH019 (a two-sided publisher
  change; until it is built those two rows cannot be accepted).
- T1021: `pipeline/gauges/feed/publish.py` hides a failed HYDAT release check (`except Exception: latest = None`);
  the DFO cache readers never refuse a missing directory.
- T1048: on main a format change fails `app_reads --check` for the old app version with no clean way out.
- T1030: "N waters changed" prints no thousands separator; the count is against the immediate predecessor only;
  `answers_diff` treats a licence `when` change as wording only.
- T1020: `LIC_KEYS` in `answers_diff` does not list the licensing refs `alt`, `displaced_by`, `displaced`,
  `also_printed`.
- T1027: a notification SCHEDULED before an update would not load after it (trigger classes have no
  serialVersionUID; the app schedules none today; AGENTS rule 34b says so); expo-modules-core and react-native-svg
  rules cannot be narrowed without a pnpm patch; the `offline` bench step opens a water on its Map tab and fails;
  one 315 / 544 ms cold-start stall seen under load.
- T1049: a clamped start jump can still be taken for the reader's when the page grows before the scroll event.
- T1024: a selection rule inside a layer filter splits the stream bucket (`lab.test.ts` guards it); a Kildala-style
  polygon route is written in 10 pieces.
- Tests: `app/packages/ui/src/chart.render.test.tsx` is timing-dependent (a fixed 30 ms wait); vitest times out
  ("Timeout calling onTaskUpdate") under machine load.
