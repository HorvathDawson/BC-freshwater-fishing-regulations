# Restart page: the 0.9.1 follow-ups chat (roadmap row 2)

Rewritten 2026-11-24 22:35 MST by the 0.9.1 chat. 0.9.0 is on production and `release/0.9.0` is merged back (#140), so
the cut is no longer blocked by it. Scope: `handoff/PLAN/ROADMAP-after-0.9.0.md` row 2, the owner's four staging
reports of 11-24, and the tickets the planning chat put into 0.9.1. The planning chat owns ticket numbers: this chat
numbers nothing and sends it finds. This chat works only in scratchpad worktrees, never the main checkout.
A network outage stopped every lane from about 16:15 to 22:15 on 11-24; all were restarted, nothing was lost.

## Where each item stands

| Item | State |
|---|---|
| T1048, T1021, T1041 lows, T1020, T1027, T1024, T1031 A+B, T1047 (two accepted), T1030, T1049 lows + cold start, T1016, T905 | Merged. T1024, T1030 need the 0.9.1 data and tile build. T1049: a device check on a release build is left. T1047: 07FD001, 08KH019, 08MH153 stay proposed. |
| T905 / T1016 feeds worker | Staging worker deployed from main 11-24 14:24 (version c5dc2e47). Live staging index is 129,702 B against the ticket's 121,688 B (a fixture): believed to be larger live inputs; confirm before the cut. Production deploy: the owner, with the 0.9.1 commands. |
| T1036 "All sources" second group | DONE (#126). The owner kept the built head, weight and empty-group rule. |
| T1050 ice fishing | #130: review fixes checked (Kootenay main body keeps its ice line count), auto-merge on. Needs the data release. |
| T1051 treble / barbed | #144 open, independent review running. After it: ask the owner the two partial wordings ("Treble hooks allowed" / "Barbed hooks allowed") and which info sheet, with `T1051-shots/`. |
| T1052 fully closed says it once | #131: reviewed, the owner picked wording A; a conflict with main resolved 11-24 22:25; auto-merge on. Follow-up NOT started: the 130 part-year waters (owner: "Yes, merge those too"), pictures first. |
| T1053 head lakes (+ T1086, T1087 step 1) | Branch `feat/T1053-walk-climbs-head-lakes`, lane running, DRAFT PR to come. Owner rulings are in OWNER-TODO (13:30–16:10 section). The owner reads the before/after list (with its "own row kept" section) before it ships. T1087 step 1 verdict: the Brunette r1 binding is RIGHT. Tell the planning chat the PR number (it closes Q124). |
| T1029 one-bank stripe | Draft #112, lane finishing (review fix, merge main, a Standard-map reshoot). Then one independent review, the owner sees the Standard-map shots, un-draft, merge. `wt-l18/side` (about 4 GB) is deleted after the review. |
| T1058 Gradle Metaspace, T1059 strict CI run limit | Both built; PRs opening. Needed before the Play build. One review each. |
| T1083 web start link refuses `wsc:` ids (HIGH, on production) | NOT started: the next lane. |
| T1061 Skeena "Flows into" blank band | NOT started: after T1083. Emulator n1_pixel_b on port 5556 only. |
| T1093 location crash kind | 0.9.2 unless an app lane is free before the build. |
| One file per ledger row / OWNER-TODO item | Built and reviewed (#114, #143). HELD, last: on `chore/T1041-one-file-per-row` run `node app/tools/ledger.mjs --merge origin/main`, check it carries rows T1055 onward and every OWNER-TODO section, merge, tell the planning chat at once. |

## Next, in order

1. Take each open PR through its one independent review, check the fixes, auto-merge. Ask the owner in chat, with
   pictures: the T1051 wordings, the T1053 list, the T1029 Standard-map look, the T1052 part-year pictures.
2. T1083, the T1052 part-year follow-up, T1061. At most 3–4 lanes and one heavy job at a time (five lanes starved the
   0.9.0 production builds on 11-24).
3. Merge #114 last. Before the cut, check a release cut from main carries `minAppVersion` in its manifest.
4. Cut 0.9.1 (`python -m pipeline release --cut`), a full data and tile build (T1024, T1029, T1050, T1053), staging,
   Play Closed, then hand the owner the production commands (`handoff/PLAN/runbooks/release-by-hand.md`), the
   production feeds worker deploy among them. CONFIRM_PROD is only ever typed by the owner.
5. Update `handoff/PLAN/play-production-access/releases.md`, the feedback log's "Fixed in" column (D08–D12 too) and
   the questionnaire (Q3, Q7, Q8) for 0.9.1. One push notification when the work is done.

Owner to-do passed to the planning chat: submit iPhone 0.9.0 (45) for Beta App Review (his answer "Submit it"; the
dry run agrees; this chat's permission check refused the real run):
`node deploy/staging/testflight-distribute.mjs --version 0.9.0 --build 45`.

## For the planning chat (new finds; this chat makes no tickets)

- Sent 2026-11-24 12:57: rule 28 vs runtime stream overlays (the layers DSL cannot express an offset half-line keyed
  by feature-state); T1016 a too-early prune is now auto-refreshed under an `expected` report; "Not allowed: Water
  insects and crayfish · Not on lakes." reads oddly on a lake's Gear tab; T1049 late fonts/images and growth after a
  still page are not covered.
- Build machine: a Gradle Metaspace hang and a build-apk timeout were seen on 11-24; vitest worker timeouts while a
  production build loads the machine (a retry passes).
- The salmon card fetches the DFO index on every cold start (the cause of T1049's late layout).
- Blank band seen on the Skeena Map tab (0.9.0 device pass; not reproduced here).
- Owner, via the 0.9.0 chat: "in future we can use release candidates ?" (release-candidate tags).
- T1031 B / T1049 review lows are in their tickets' "left" lines.
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
- From the 0.9.0 chat: `scripts/ci_status.py` `strict_judge` searches an ancestor's runs with gh's default limit
  of 30 across all branches, so on a busy night `build-apk.sh --dry-run` says "docs-ci.yml: no finished run" although
  a green run exists on an ancestor (it will bite the 0.9.1 build too); a stale FAILED nav answer can flash
  "unavailable" for one render (`app/packages/ui/src/frameView.ts`); `useRegsTarget`'s `found` is stale for one
  render on a frame with no part; the app has no React error boundary, so any render throw is a full app crash.
- Tests: `app/packages/ui/src/chart.render.test.tsx` is timing-dependent (a fixed 30 ms wait); vitest times out
  ("Timeout calling onTaskUpdate") under machine load.

- 11-24 evening: T1050 the page's ice clause is uncited (`app/packages/core` `methods.ts` `waysVM`; the citation is in
  the pipeline only). `scripts/heavy.sh` has no fair queue (T1063). A lane could not stop its own queued heavy job
  (the permission check refused the kill). The pre-push hook's wide vitest pass times out under load (a retry passes).
