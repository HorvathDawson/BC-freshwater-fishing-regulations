# Restart page: the 0.9.1 follow-ups chat (roadmap row 2)

Rewritten 2026-11-24 13:00 MST by the 0.9.1 chat. It works beside the 0.9.0 chat and never touches the main checkout.
Scope: `handoff/PLAN/ROADMAP-after-0.9.0.md` row 2, plus the owner's four staging reports of 11-24 (he said they go
into 0.9.1). The name is not NEXT-SESSION-* on purpose: the guard keeps at most two of those, and they are the 0.9.0
chat's. The planning chat owns ticket numbers since 11-24: this chat numbers nothing and sends it finds.

## Where each item stands

| Item | State |
|---|---|
| T1048, T1021, T1041 lows, T1020, T1027 (+ WorkManager keep rule), T1024 | Merged (#95 #98, #97, #99, #104, #108 #109, #111). T1024 needs the 0.9.1 tile build. The cut REFUSES while a shipped version has no `app_reads` row on main: cut only after `release/0.9.0` is merged back. |
| T1031 trout row sources | A merged (#100), B merged (#119; owner: "Go on to B"). |
| T1047 datum-change list | Merged (#103); 08HD006 and 08EF001 accepted and merged (#121). 07FD001, 08KH019, 08MH153 stay proposed (not asked). |
| T905 salmon feed size | Owner: move worker state out. Merged (#120): feed 132,987 → 121,688 B. Needs a feeds worker deploy (staging: after the 0.9.0 chat says it is done with staging; production: the owner). |
| T1030 changed-waters count | Merged (#105, and #123 hide 0). Needs the 0.9.1 data release and app build. |
| T1049 start link | Lows merged (#107); the slow cold-start fix merged (#122), proven on the emulator. PARTIAL: a device check on a release build is left. 0.9.0 ships without it (owner's call). |
| T1016 web tab across releases | #101 merged. Owner: "Always auto refresh": PR #125 open, review fixes in progress (a reload-loop guard across two codes, a bounded wait on the report). Needs the feeds worker deploy too. |
| T1036 "All sources" sheet | #96 merged. Owner: "Add back but maybe have them separate?": PR #126 open, review fix in progress (collect every sheet kind). The group's heading is NOT picked: ask the owner with `T1036-shots/other-group-peace-aug1-headings-{light,dark}.png`. |
| T1029 one-bank stripe | Draft PR #112. Owner: look B (split + closed dashes, from z13); polygon rivers along the bank edge; the book's west-half rule striped too; no stripe on creeks; the 12 Kitimat creeks closed, "calculated from extents". Being built: FWA LEFT_RIGHT_TRIBUTARY into the atlas, a bank-side filter in the reach step, delivery as an ordinary place. Gates: atlas parity, binding diff moves nothing, salmon diff = exactly the 12 creeks Open → Closed. Then a review. |
| T1050 ice fishing, T1051 treble/barbed | Owner answered (OWNER-TODO 12:33). One lane, two PRs to come, each needs a review. T1050 needs the data release. |
| T1052 fully closed says it twice | Owner answered. Lane running, PR to come, needs a review. |
| T1053 head lakes | Owner: "Climb through every head lake". Lane running, DRAFT PR to come; the owner reads the before/after list before it ships. |
| T1054 tides layer | Future (0.10 or later), ticket only. |
| One file per ledger row / OWNER-TODO item | Built and reviewed (#114, branch `chore/T1041-one-file-per-row`). HELD: after `release/0.9.0` has merged back and the other PRs are in, run `node app/tools/ledger.mjs --merge origin/main` on that branch, check, then merge. Last. |

## Next, in order

1. Take each lane's PR through one independent review, check the fixes, auto-merge. Ask the owner the T1036 heading
   and show him the T1053 list, with pictures, in chat; write each answer into OWNER-TODO and its ticket the same turn.
2. Staging feeds worker deploy (T905, T1016's report class) once the 0.9.0 chat says it is done with staging.
3. After 0.9.0 is on production AND `release/0.9.0` is merged back into main: merge #114, cut 0.9.1
   (`python -m pipeline release --cut`), a full data and tile build (T1024, T1029, T1050, T1053), staging, Play
   Closed, then hand the owner the production commands (`handoff/PLAN/runbooks/release-by-hand.md`). CONFIRM_PROD is
   only ever typed by the owner.
4. Update `handoff/PLAN/play-production-access/releases.md`, the feedback log's "Fixed in" column (D08–D12 too) and
   the questionnaire (Q3, Q7, Q8) for 0.9.1.

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
