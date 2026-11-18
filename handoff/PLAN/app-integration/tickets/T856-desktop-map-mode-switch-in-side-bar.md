# T856 — Desktop: the Trout / Salmon map switch moves to the left side bar, under the Map button

Status: OPEN (app chat; 0.8.1).

Owner 2026-11-18 ~14:20 (to the main chat, relayed), verbatim: "also on desktop can we move the salmon/trout toggle for
map to the left side bar? maybe under map button?"

On a wide (desktop) window the map's mode switch (Trout / Salmon, and Runs) sits on the map; put it in the left side bar
directly under the Map button, where the other places to go are. Phones and narrow windows keep it where it is. The map
key must still follow the switch (the 0.7.7 check: flip quickly, the key always matches the colours). Proof: desktop and
phone-width screenshots on staging web.

2026-11-18: built on branch worktree-agent-a9320341d38308cce (b05790e0a): from 1280 px the switch is in the nav rail under
Map (NavRail `mapExtra` slot; RegsLayerToggle `inRail`); narrower keeps it on the map; 4 render tests. Screenshots sent
(desktop Trout/Salmon, phone Salmon). Owner ~14:58, verbatim: "the desktop salmon/trout switch should be styled nicer to fit
in the bar better rn it looks slapped in". So: restyle the rail version to read as part of the rail (two looks to pick from,
with pictures), then merge with 0.8.1.

Round 2 (a9a448cf2: rail looks A sub-items / B split block) shown to the owner. Owner, by dialog, verbatim: "actually maybe
we shouldnt have in side bar because it will make the buttons be jumpy when it disappears? where is a betetr desktop spot"
(the switch shows only on the Map tab, so the rail items below it would jump on a tab change). Offered: key bar left end,
top after the day strip, or back on the map's right. Owner picked: "Top, after the day strip". So: on desktop the switch is
a Trout | Salmon pair on the top row right after the day strip (< Today >), styled like that strip; the side bar goes back
to as it was; phones unchanged.
Owner, right after, verbatim: "the side bar should have some opacity to it too in desktop will look cleaner i think". So
(part of T856): on desktop the left side bar is see-through (the map runs under it; paper at partial opacity, a blur on
web), text and icons keep full contrast; shots in light and dark.
