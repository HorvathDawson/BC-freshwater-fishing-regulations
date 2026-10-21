# P15: the water map view, checked on the web (2026-10-21)

This is a phone viewport (390×844, touch). The worktree's expo web ran on :39631 against release r-1bb06fa28daf, served by
the existing tile server on :39217 (read-only). The prototype (`app/design/regs-v11.html`) ran on :39632. To repeat it:
`node web-check.mjs <out dir> [app url] [prototype url]`. `web-check.log.json` is the log from the last run.

## What the app shows (the `app-*` screenshots)
| water | parts, mouth first | ends | exit at the lowest part | marks |
|---|---|---|---|---|
| Chilliwack River | Vedder Crossing Bridge → Tamihi · Tamihi → 100 m below Slesse · 100 m below Slesse → Ecological Reserve boundary · Ecological Reserve boundary → Source | "Lowest part" / "Highest part", disabled and dashed | Flows into Vedder River › (it opens the Vedder River) | Tamihi Rapids Bridge · fishing boundary signs 100 m below the Slesse Creek confluence · Chilliwack River Ecological Reserve boundary |
| Vedder River | Vedder River (1 part) | both disabled | Flows into Sumas River › | none |
| Cowichan River | Mouth → CNR Mile 66 · → CNR Bridge (70.2) · → Greendale Trestle · → Cowichan Lake outlet | both | none (it runs to the sea) | CNR Mile 66 Trestle · CNR Bridge (mile 70.2) · Greendale Trestle |
| Kootenay Lake (any closure area) | Lower West Arm · Upper West Arm · Main Body | both | Flows into Kootenay River › | none placed (see below) |

These were also checked:
- A tap opens the map on the tapped part. The Cowichan run opened on Part 3.
- A tapped mark opens its label with the parts upstream and downstream of it (`app-chilliwack-4-mark`).
- ⤢ frames the whole water and changes to "This part".
- "Show regulations for this part" returns to Regulations on that part.

## Compared with the prototype (the `proto-*` screenshots)
- **Cowichan:** the part order, names, "Part n of 4 · Cowichan River", the three marks and the ends are the same.
- **KLWA:** the order (Lower → Upper → Main Body) and the names are the same. **Not the same:** the prototype draws two
  marks between the closure areas, labelled "Lower West Arm / Upper West Arm" and "Upper West Arm / Main Body". The
  release has no `part_mark` row between two items. The step graph still holds these two boundaries (no position, and
  the label is derived the same way), so ▼/▲ work, but nothing is drawn for them.
- **Vedder:** the prototype's "vedder" is ONE group of five parts, the Vedder River plus the Chilliwack River. It was
  made by hand-picking the group in `water_map_export --group vedder=gnis:3062,gnis:8634`. In the release they are two
  waters. The Chilliwack's lowest part shows "Flows into Vedder River ›", and the Vedder River (1 part) has no way up
  into the Chilliwack. The release has no link that says "continues as" (the Vedder River's `water_exit` predecessors
  also include Stewart Slough, Street Creek and Lewis Slough). Matching the prototype needs a pipeline answer, either a
  continuation link or a group. The app must not guess this.
- The prototype also has a status chip on the bar and a colour legend. The app's map already carries status colours,
  and the chip was left out.

## Seen, not resolved
- Some marks sit a little off the lit line at about z12 (Tamihi Rapids Bridge, the Slesse signs). The mark coordinates
  are the release's `part_mark.lon/lat`, so this is worth checking in the pipeline.
- Feature-state `water` (the other parts of the water) only raises the stream line width floor
  (`width.stream.water` 2.4). Lake fills of the other parts do not change.
