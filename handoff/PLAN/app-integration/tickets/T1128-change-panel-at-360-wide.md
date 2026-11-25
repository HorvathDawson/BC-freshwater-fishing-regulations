# T1128 — Change panel at 360 px wide: "Steady" struck through by its tick, the day strip's last column clipped

Status: OPEN — 0.10.1; must land before the Play production release (the Play pictures wait for it)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1128 | Change panel at 360 px wide: "Steady" struck through by its tick, the day strip's last column clipped | OPEN — 0.10.1; must land before the Play production release (the Play pictures wait for it) | | store chat 2026-11-25; visible in the Play phone store picture, frame 3 |

Source: the store content chat, 2026-11-25, seen on the phone-width reshoot of store frame 3 (the conditions map's
Change panel; PR #209). Not checked by the planning chat. Related: T1072 (the picture set), T1115.

1. In the Change panel at 360 px wide the "Steady" label is struck through by its own tick mark.
2. The day strip's last column is clipped at the right edge.

Both are app layout defects at a common Android width, not shot defects, and both show in the Play phone store
picture until fixed.

Work: reproduce at 360 px on web and on an Android emulator of that width; fix the label and tick spacing and the
strip's width; a render test at 360 px that fails without the fix. Then the store chat's tool reshoots frame 3 for
play-phone (one command), so the listing picture is clean.

Severity: low (cosmetic; on a store picture). Release: 0.10.1. Model: Sonnet. Size: S. Owner: no.

Owner 2026-11-25 13:43, in the store chat (relayed): "hold play p[ictures until before prods release". No Play
picture upload until just before the production release; by then this ticket is fixed and play-phone frame 3 is
reshot (the store chat's tool, or T1131's workflow). So this ticket now gates the Play listing pictures: whoever
closes it tells the planning chat and the chat that holds the picture tool.

The reshoot (store chat, 2026-11-25, as it closed): the ready store-shots export was in a worktree that is gone, and
this ticket changes the app, so build a fresh export, without `--reuse`:
`scripts/heavy.sh store-shots 30 -- node app/tools/store-shots.mjs --theme light --scenes <nn> --only play-phone`.
