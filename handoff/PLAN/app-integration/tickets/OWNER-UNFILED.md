# Owner asks with no ticket yet (T1081)

Things only the owner can do that belong to no open ticket. One `## <title>` section each, owner lines as in README.md
("Owner blocks"). The planning chat numbers a ticket for a section, moves its lines into that ticket's `## Owner`
block and deletes the section here. `node app/tools/ledger.mjs` lists the open lines in `views/OWNER.md`.

## 0.10.0 release
- [ ] 2026-11-25 — Run the production commands for 0.10.0 in the order the 0.10.0 chat hands them over with their dry runs: data (`pipeline release --prod`), then the site (it puts the new privacy page live), then the feeds worker (CONFIRM_PROD is only yours). 0.10.0 is held for real part names (T1140, T1146, T1145; your word 2026-11-25 ~16:55); after that staging ships as `v0.10.0-rc.1` and your look, then these commands, probably 2026-11-26; the phone builds and the App Store submit (T1071) follow.
- [ ] 2026-11-25 — Clear the What's-new words the 0.10.0 chat drafted as a proposal (its morning summary).

## Remove the machine-wide Gradle Metaspace workaround (T1058 follow-up)
- [ ] 2026-11-24 — T1058 follow-up: open `~/.gradle/gradle.properties` and delete the Metaspace line you added for the hang (the repo sets the same value now, PR #155; `build-apk.sh` prints a note while it is there). Nothing breaks if you leave it.

## Turn off the Copilot coding agent
- [ ] 2026-11-21 — Repo Settings > Copilot: turn off the Copilot coding agent (its environment is already deleted).

## Cloudflare Bots / JavaScript Detections (optional)
- [ ] 2026-11-22 — The Chrome "StorageType.persistent" warning on the site comes from Cloudflare's injected bot script: turn off Cloudflare Bots / JavaScript Detections to silence it.

## The game-fish bar's open colour (T1145 follow-up)
- [ ] 2026-11-25 — T1145 is merged (#260): look at the Capilano before/after pictures of the open colour (`handoff/PLAN/app-integration/T1145-shots/`); say if it is wrong.

## Re-issue the feeds worker's GitHub token before it expires (2027-11-25)
- [ ] 2026-11-25 — The worker's GitHub token on the new org expires 2027-11-25 (your words: "365 days away is expiry"). Two weeks before: re-issue it on the org, `wrangler secret put` on staging and production, run check-github on each.
