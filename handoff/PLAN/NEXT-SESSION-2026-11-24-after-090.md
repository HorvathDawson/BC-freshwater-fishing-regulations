# Restart page: after 0.9.0 shipped (written 2026-11-24, the 0.9.0 chat's last page)

The 0.9.0 chat (roadmap row 1) is finished. Nothing of it is left to do in a build chat. The next chat comes from
`ROADMAP-after-0.9.0.md` (the planning chat keeps it and writes each chat's prompt). The 0.9.1 chat keeps
`RESTART-091-chat.md`.

## What shipped (2026-11-24)

The release id and the build numbers are in the top entry of `STATUS.md`.

| Piece | State |
|---|---|
| Data | on production (the owner ran `release --prod`); smoke 18/18 after the site |
| Website | canifishthis.ca on 0.9.0 (web key `0.9.0-b44-65f8402`, served source maps) |
| Feeds worker | production redeployed; gauges, dfo and inseason jobs all ok |
| Android | the production build (65f84020c) on Play Internal + Closed; R8 mapping sent with the AAB |
| iPhone | the production build uploaded to App Store Connect |
| Tags | the release's data tag; `v0.9.0` moved to 65f84020c (owner's OK; release candidates are T1062) |
| Merge-back | PR #140 (`chore/merge-back-0.9.0`): the release's commits, the Play record (releases row, Fixed in, Q3/Q7/Q8) and a T1048 test fix |

Known issue shipped: a shared link naming a tab, opened from a cold start on a slow line, lands short of the tab
(T1049; fixed on main for 0.9.1, PR #122).

## Owner items (the planning chat holds them, `OWNER-TODO.md`)

Play Production staged rollout of the Android build (waits on production access, T1046, from 2026-12-07) and the App
Store submission of the iPhone build (T1071).

## Loose ends from this chat (all ticketed by the planning chat)

- T1058: Gradle daemon ran out of Metaspace (512m) and the client hung for an hour. The workaround is machine-wide
  (`~/.gradle/gradle.properties`, MaxMetaspaceSize=1024m); the repo fix is the ticket.
- T1059: `ci_status --strict` looks back only 30 runs, so docs-ci read as a false STOP. Both 0.9.0 phone builds used
  `CI_CHECK_SKIP` with that reason.
- T1063: release builds waited hours on the heavy lock behind another chat's test tiers (load average up to 136);
  release builds need priority.
- T1076: the store-picture reshoot needs a staging site built with `EXPO_PUBLIC_STORE_SHOTS=1`; skipped for 0.9.0,
  the listing keeps its pictures.
- T1060, T1061: review lows and the Skeena "Flows into" blank band.
- The Owner To-Do artifact (claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f) answers "no access" to both chats; the owner
  re-shares or makes a new one.
