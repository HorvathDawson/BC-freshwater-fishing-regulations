# T428 — One sync control in More: the "Updates" row really syncs, and looks like the map's sync button

Status: DONE — owner phone check 2026-11-08 passed (0.6.9). Was: IN REVIEW — e8f72da1c; proof pending: phone look (More → Data on the 0.6.x build: one sync button, lines under it). Web shots T428-shots/. (owner 2026-11-05, 0.6.2, More → Data; screenshot `shots/T428-updates-row.png`): "Sync button here is
confusing too — maybe the sync button at top in More menu can be combined with this so it actually syncs when you
click it. Also use same look as sync button with fish near search bar." Do sometime (not urgent).

Today More → Data has two refresh-looking things: "Refresh data: up to date" at the top and the circled "Updates" row
(a sync icon over the per-feed "checked … · updated …" lines), which looks like a button but does nothing on tap.

## Work
1. ONE control: the top "Refresh data" and the "Updates" row become one. Tapping it checks every feed now (the same
   path as the top refresh: one load path), shows progress, then the per-feed lines with what changed.
2. The same look as the map's sync button next to the search bar (the fish one): same icon, spinner and states.
3. Nothing that looks tappable without doing something.
4. Test: tap → each feed asked again (the Updates log shows it); offline → said, not silent.
Size: S. Owner: phone look.
