# T747 — A water's Map opened from search frames the part its bar names, not the whole water

Status: DONE — 7b60c75f1, 3f6e558ec (closed on the owner's word 2026-11-20; see the last section)

Source: owner 2026-11-14, staging website (dff2c7a22, desktop, light), verbatim: "i clicked search skeena then clicked
view skeena river then it went to this it is the section view but it is fully zoomed out instead of on the first
section". Screenshot: Skeena River, Map tab, "PART 1 OF 18 · Tidal boundary → Skeena River Ecological Reserve boundary
(km 63.5)", the corner button "This part", the map at a 100 km scale (BC from the coast to Fort St. John), the end label
drawn twice a few pixels apart.

## What was happening (reproduced on staging and a local web export of b90f73922)
- **Search → a result's Open** (`›`, "Open Skeena River") lands on `/water/skeena-river?view=map&whole=1`: the frame is
  WHOLE (T203 item 2, 2026-10-25: "From search, ALWAYS open the Map tab with the whole water shown"), while the bar names
  part 1 and lights only part 1. The fitted box is right — the Skeena's own (part_geo union: lon -130.11..-127.35,
  lat 54.14..57.16, 330 km tall) — but on the desktop strip (1512 × 738 window: the map is 243 px tall) the whole
  Skeena needs a 100 km scale, which reads as the province. Not a null box and not a fallback; not a regression of
  today's T717 / T730 / T731 / T734 (the search open has been whole since T203). Pressing "This part" framed part 1.
- **A `/water/<slug>?view=map` link with no place** (multi-part water) opened whole the same way (`place.ts`
  `ownPart`, T369).
- **Every other way in framed the part**: a map tap then Map, the `/water/<slug>` link then the Map tab, a step ▲ then ▼.
- **The doubled label**: the Skeena's part 1 meets parts 2 and 5 at two marks 4.2 km apart with the SAME end token
  (`area:SKEENA RIVER ECOLOGICAL RESERVE@km63.5`). T443's one-label rule merges marks of one end only within 250 m, so
  both were labelled: a few px apart on the whole frame (doubled text), two boxes saying the same words on the part.
- **No frame at all** (a part with neither a box nor a midpoint) would have handed the map no camera and left it where it
  was, unreported: a silent fallback.

## Fix
- `openWater.ts` `searched`: a result opens the Map at the frame's part (the part the Regulations view opens,
  `useFrameView`), never `whole` — the T203 item 2 decision is replaced by the owner's 2026-11-14 words. ⤢ "Whole river"
  is one press away and stays the only way to frame the whole water (and `whole=1` in an address it left).
- `place.ts` `ownPart`: a multi-part water's link with no place opens its Map on the part too (as it asks).
- `waterMap.ts`: two marks with the same whole end token are one label at any distance (the ring and its screen-reader
  words stay); a part or a tidal water with no frame is reported (`reportOnce`) and the map says unavailable.
- Native: the same code path (`openWater.ts` / `waterMap.ts` are shared `@app/ui`), so the phone app opened search
  results whole too (less visible on a tall phone map); fixed by the same change.
- Not changed: `place-gone` (an address whose place this release no longer holds) and `carry.ts` (a release whose part
  cannot be carried) still open whole, with their notice: there the part is unknown, not named.

## Tests
`WaterMapFraming.shell.test.tsx` (new, real Shell, phone and 1512 px): search → Open, and Open → Regulations → Map,
draw what the part's own frame draws (bar part, "⤢ Whole river", the part's camera); ⤢ frames `:whole` and says "This
part". Mutation-checked: with `whole: true` put back in `searched` both cases fail. `waterMap.test.ts`: the same-token
twin 4 km away is one label, a different-place token is its own (mutation pin). `openWater.test.ts`, `WaterMapView.test.tsx`
(one load path: "search Open" now draws the reload's frame with no pick) updated.

## Proof (local web exports, staging data; `before-*` = b90f73922, `after-*` = 7b60c75f1)
Screenshots in `~/bcfish-session-backup-2026-11-12/water-map-framing/` (`<build>-<way>-<width>.png`; 1512 × 738,
1920 × 1000, 390 × 844; `staging-search_open-1512.png` is the owner's case on staging).

| way in | before: corner button / frame | after |
|---|---|---|
| search → Open (Skeena, Fraser, Vedder; 1512, 1920, 390) | "This part" / whole water (`whole=1`) | "⤢ Whole river" / part 1 |
| search → Open → Regulations → Map | "⤢ Whole river" / part 1 | same |
| `?regs=` link → Map tab | "⤢ Whole river" / part 1 | same |
| `?regs=…&view=map` link (no place) | "This part" / whole water | "⤢ Whole river" / part 1 |
| map tap → Map tab | "⤢ Whole river" / the tapped part (14) | same |
| ▲ then ▼ | "⤢ Whole river" / part 1 | same |
| a lake (Harrison, solo) | opens its Regulations (no Map view) | same |
| ⤢ pressed (`after-whole_press-1512.png`) | — | "This part" / whole Skeena, ONE km 63.5 label |

## Closed 2026-11-20

Closed 2026-11-20 on the owner's word ("Couldn't we just assume most of these are done already?"): search opening the part the bar names (map framed on it) is taken as checked.

Status line before closing: IN REVIEW (fix 7b60c75f1 + proof 3f6e558ec on `redesign/stream-sections` since 2026-11-14; in Play build 33 (0.7.11) and on the staging and production websites since; left: the owner's look, owner: OWNER-TODO "the water map on its part (T747)")
