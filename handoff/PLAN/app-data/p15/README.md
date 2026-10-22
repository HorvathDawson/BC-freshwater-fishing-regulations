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
- ~~Some marks sit a little off the lit line at about z12, which pointed at the pipeline's `part_mark.lon/lat`.~~ The
  cause was an APP bug, fixed in the review round. The mark's element set `position:relative` inline. That overrode
  `.maplibregl-marker{position:absolute;top:0;left:0}`, so the marks stacked in normal flow and mark *i* was drawn
  44 px × *i* below its point. The coordinates were right: Tamihi (-121.83788, 49.07184) is at the west edge of part 1.
  The element is now a bare wrapper that GL JS places, with the 44 px button inside it. This is pinned in
  `adapter-contract.render.test.tsx` ("P15 review: a mark's element is left to GL JS's absolute placement"). The
  check was run again on the web (`node mark-check.mjs <out> <app url>`, log in `mark-check.log.json`). Every mark's
  root is now `position:absolute` with `offsetTop` 0, and its centre minus its translate is 136 for all three marks.
  Before the fix it was 136 / 180 / 224. These screenshots were retaken after the fix: `app-chilliwack-2-mouth`,
  `-3-up1..3` and `-4-mark`. In them the Tamihi and Slesse rings sit on the two ends of the lit part.
- Feature-state `water` (the other parts of the water) only raises the stream line width floor
  (`width.stream.water` 2.4). Lake fills of the other parts do not change.

## The colouring engine (review round)
The first report said no branch was generalising `profile-paint.ts`. That was wrong. The engine is `ad3898d5`
("conditions colour: one colouring engine", `profile-paint.ts` → `colouring.ts`, branch `worktree-wf_b01b27c4-44c-1`),
and it is NOT on `redesign/stream-sections` yet. P15 therefore lights the parts through the existing adapter contract:
one `MapAdapter.highlight(h, selected, water)` call through `stateHandle`, which is the same call the engine's
`restore` makes. A merge with `ad3898d5` conflicts in three places:
- `Map.web.tsx`: the destructured props, and the `restore` block. Take ad3898d5's block, then add the water ids to its
  one highlight call: `adapter.current.highlight(r.states(() => []), [...wasLit.current], [...wasWater.current])`.
  `wasLit` still means only the selected ids, as it did before P15, so the engine's unmodified restore line can never
  put the other parts back as `selected`. Left unmerged, those parts would only lose their `water` floor after a
  release swap.
- `map/src/index.ts`: keep both, `MapMark` and the `./colouring` export.
- `ui-native/src/MapScreen.tsx`: keep both prop sets (`standing` and `water`/`marks`/`onPressMark`).

## Follow-ups, checked on the web (review, 2026-10-21)
The web check used a phone viewport (390×844 @2x, touch, 4G). Expo web ran on :39762 against release
**r-f579fe5eac3e**, served read-only on :39761 by `serve-tiles.mjs` (`RELEASE_ROOT=.scratch/release`). That release was
built in the worktree with the fixed `water_continue`, and its tiles are the promoted ones. To repeat it, run
`node followups-check.mjs <out dir> <app url>`. Its screenshots and its log (`followups-check.log.json`) are in `fu/`.

- **The outline (owner: "keep the colour, add an outline").** On a stream, the current part keeps its regulation colour
  and is cased in `color.highlight`. On a lake, the current part keeps its fill and gets a ring. Screenshots:
  `fu-chilliwack-3-up1..3`, `fu-cowichan-2-mouth` / `-3-up1..3`, `fu-klwa-2-down1..2` (the lake ring) and
  `fu-klwa-3-up-again`.
- **The marks, re-taken.** `fu-cowichan-4-mark` shows Greendale Trestle open on the cased part, with its ring on the part's
  end. KLWA still has no placed marks between its closure areas (as noted above).
- **Across waters (owner: "like vedder from chilliwack").** On the Vedder, ▲ reads "▲ Upstream into Chilliwack River".
  Pressing it opens the Chilliwack's map at Part 1 of 4, Vedder Crossing Bridge → Tamihi Rapids Bridge
  (`fu-vedder-2-up-into-chilliwack`). On that part, ▼ "into Vedder River" opens the Vedder (`fu-vedder-3-down-into-vedder`).
  Back walks the waters that were opened on top, newest first: the Chilliwack, then the Vedder (`fu-vedder-4-back-twice`).
  The Chilliwack run steps through the same continuation from the other side (`fu-chilliwack-5..7`).
- **The stepped part is the frame's part.** After ▲ three times, Regulations opened on the stepped part
  (`fu-chilliwack-4-regs-after-steps`), and Map opened on Part 4 of 4 again. Part 4 (part_ix 3) and part 3 (part_ix 0)
  are both closed all year. The release's picker makes them ONE choice ("Closed all year on 2 stretches"), so the page's
  heading names the choice's first stretch. That is the picker as it already works, not a defect in this lane.
- **Step time.** One press of ▼ / ▲ was timed from the press to the first frame where the bar had changed and the
  map's selection had been set, at 4× CPU. Over 19 steps (5 of them across waters) the median was **24 ms**, the p95
  **41 ms** and the max **41 ms**, all under the 50 ms target. The map's selection was set 10–15 ms after the press. The
  camera's ease to the part ends at the map's next idle, 0.8–1.5 s later.
- **The continuation list.** See `continuations.md`. The first cut shipped 92 rows, and 89 of them were wrong
  (tributaries, lake outlets, confluences), because it read the gauge-shed-only `section_down`. The fix reads the atlas
  graph, and the release now ships 18 rows.
