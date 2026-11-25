# T1134 — App Store product page: the Header and Search Results pictures

Status: IN PROGRESS — left (Claude): the videos land in the PR's second commit; the owner uploads by hand in App Store Connect

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1134 | App Store product page: the Header and Search Results pictures | IN PROGRESS — left (Claude): the videos land in the PR's second commit; the owner uploads by hand in App Store Connect | PR feat/T1134-appstore-creative-assets | owner uploads by hand; confirm the video codec rules on the upload screen |

Source: owner 2026-11-25, in the iOS chat's App Store walk-through at the version page (relayed): "i need search
and header asset too". Related: T1072 (the picture set, direction A), T1071 (the submission), T1131 (the pictures
by workflow: these two join its list).

What the iOS chat read on the App Store Connect screen (not checked against Apple's help page by the planning chat;
whoever builds this reads the current spec first and writes its link here): two optional product page pictures, a
"Header" (3840 x 1646 PNG) and a "Search Results" picture (3:2, up to 3840 x 2560), or one universal 5244 x 2950
PNG that serves both. Apple calls them optional.

## Work
1. Read Apple's current spec (sizes, safe areas, what text is allowed, where each is shown) and write it here.
2. Two or three looks in direction A as pictures; the owner picks. Words on them come from the one listing source
   (`app/store-listing/`), no new claims.
3. The store-shots tool makes them (a scene per asset), with a pixel-size check, and the push list (T1127) and the
   App Store push script know the two slots, or the upload steps say "by hand" if the API has no slot yet.
4. Hand the files to the iOS chat; it adds the slots to the submission page.

Acceptance: both pictures at Apple's exact sizes on main, made by the tool, uploaded by the owner. Severity: low
(optional slots; a better product page). Release: with the 0.10.0 App Store submission if ready before the build
lands (about 18:00), else added to the live page after. Model: Sonnet. Size: S. Owner: yes (picks the look; uploads).

## Apple's spec (read 2026-11-25 via a summarised fetch; confirm on the App Store Connect upload screen)
Links: https://developer.apple.com/help/app-store-connect/reference/app-information/creative-assets-specifications/ and
https://developer.apple.com/app-store/asset-best-practices/
- Header image: 21:9, exactly 3840 x 1646, PNG. Art safe area 1646 x 661 at (x 1097, y 493).
- Search results image: 3:2, 1920 x 1280 to 3840 x 2560, JPEG/PNG. Art safe area (at 3840 x 2560) up to 2168 x 1030 at (836, 765).
- Header video 21:9, 3840 x 1646; search video 3:2, 1920 x 1280 to 3840 x 2560; 30 or 60 fps, 5-30 s, .mov/.m4v/.mp4.
- No alpha channel or transparency. Key art inside the safe area. No prices, URLs, other platforms' names or logos, awards; a short phrase only.
- NOT READABLE from the page, so UNCONFIRMED: the video codec, bitrate and audio rules. The tool writes H.264 High, yuv420p, bt709, 30 fps, no audio track.

## The owner's pick (2026-11-25)
"product page header can be a gif? if so we shoul dhave the animation. also say rules and conditions by the water. i like the gif coho with the map behind cream. search result will use the same."
Apple takes no GIF, so the animation ships as video, each slot with a still PNG as well.

## What was built
`app/tools/store-shots/creative.mjs` writes `handoff/PLAN/app-integration/store/graphics/store/light/appstore-creative/`: `header-3840x1646.png/.mp4`,
`search-3840x2560.png/.mp4`. The coho loader fish (the owner's generator, untouched; 120 frames over its 4 s loop at 30 fps; light-theme coho ink/red
tokens) centred over the app's own map (one headless-Chrome capture of the deployed web app, moved north so the grey out-of-coverage band is
outside the crop; the app's chrome and the map credit lie in the cropped-off margin), faded under a 70 % veil of the icon-ground cream
(`color.ui.coho.ground` is the white card colour, not cream); the name and the new `creativeAssetLine` ("BC fishing rules and conditions, by
the water") from `app/store-listing/listing.json`. Videos are 8 s (two loops, seamless). The tool checks exact sizes, no alpha, ffprobe facts and
that the fish and words lie inside the art safe area. `app/tools/store-shots.json` lists them under `creative`; the T1127 push list and
`appstore-screenshots-push.py` read only their size folders, so nothing uploads them: BY HAND. Test: `app/tools/store-shots/creative.test.ts`.
Regenerate: `scripts/heavy.sh creative 30 -- node app/tools/store-shots/creative.mjs` (`--stills-only`, `--reuse-map`, `--web <url>`).
Not confirmed: the map comes from staging, which shows the released app; a PNG capture at 4800 x 3400 hangs headless Chrome, so the map is a JPEG q96 under the veil.
