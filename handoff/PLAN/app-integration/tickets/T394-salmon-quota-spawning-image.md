# T394 — Salmon quotas still show the ocean picture, not the spawning picture

Status: DONE 2026-11-05 (77e738b54; owner report 2026-11-05, 0.5.0 build 8, water gnis:19986, Regulations).

T343 item 6 (via T367): collapsed salmon section = the ocean-phase chinook; OPENED, each species row = its own
spawning-phase image (`<species>-salmon-spawning-phase.webp`, app/apps/mobile/assets/fish/). On this water the quota rows
still use the ocean images. Find the path that draws them (quota table vs the species rows) and make it the same one.

## Done (77e738b54)
The salmon card's species rows picked the OCEAN look on purpose (T318, "ocean image only"); T343 item 6 (owner
10-31, later) said the rows wear each species' SPAWNING look, and that path was missed. `SalmonCard.tsx` `spawningOf`;
the credits line follows the look shown. The ocean look stays in each fish sheet. Test: every row's picture is the
spawning file (fails on the ocean pick). Phone check with the next APK.
