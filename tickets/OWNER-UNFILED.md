# Owner asks with no ticket yet (T1081)

Things only the owner can do that belong to no open ticket. One `## <title>` section each, owner lines as in README.md
("Owner blocks"). The planning chat numbers a ticket for a section, moves its lines into that ticket's `## Owner`
block and deletes the section here. `node app/tools/ledger.mjs` lists the open lines in `views/OWNER.md`.

## T1146 follow-ups (side channels)
- [x] 2026-11-25 — T1146 group D: Strathcona Park is membership-only (`cut: false`), so ~365 straddling sections carry the park rule up to ~478 km outside the park (Rees Creek on 9.6 km from a 31 m overlap). Cut the park at its boundary (curated + atlas rebuild; Burman, Bedwell, Rees and probably Comox channels then merge) or keep it as is? Default: keep as is. → the cut, in the 0.10.0 re-cut: "Hold the re-cut for all of them" (2026-11-26; T1240 class 2, T1149)
- [x] 2026-11-25 — T1146: is Strathcona-Westmin Park part of "Strathcona Park waters"? Thelwood Creek's channels lie in it, outside Strathcona Park. Default: no (as bound today). → "Yes, treat it as Strathcona Park (Recommended)" (2026-11-27, T1240)
- [x] 2026-11-25 — T1146: look at the MacMillan Creek channel draining into Diana Lake (360244113:889, 815 m): it now carries the Skeena bait ban because FWA and the registry call it MacMillan Creek; say if it should not. → "how does it flow out? need map etc to know how to handle" (2026-11-27); with the map: "not into skeena also i dont know how the side channel happened but why does it not have the same regs as the rest of the creek?" (2026-11-27); with the rules compared: "so we jsut decided that macmillan doesnt get skeena riles so this is mute but why did tribugtary not go to that part? seems weird. it should have been same before" (2026-11-27); asked once more: "i thought we went over this??" (2026-11-28). Decision: the channel is not Skeena water and does not take the Skeena River's own row. Reading: nothing changes for the angler (the Skeena-Nass bait ban binds it by its code; salmon reads DFO's Kloiya place); built after 0.10.0 with the outflow guard (T1164 item 2) unless he pulls it in; it reverses T1146 for this one channel. His "why": before T1146 the walk climbed only upstream, so a channel that leaves and never rejoins was not reached.

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
- [ ] 2026-11-25 — T1145 is merged (#260): look at the Capilano before/after pictures of the open colour (`docs/app-integration/T1145-shots/`); say if it is wrong.

## Re-issue the feeds worker's GitHub token before it expires (2027-11-25)
- [ ] 2026-11-25 — The worker's GitHub token on the new org expires 2027-11-25 (your words: "365 days away is expiry"). Two weeks before: re-issue it on the org, `wrangler secret put` on staging and production, run check-github on each.
