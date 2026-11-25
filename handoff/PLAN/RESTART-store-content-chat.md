# Restart page: store content chat (roadmap row S; T1072 + T1076)

Goal: one new set of store pictures and one set of listing words, for the App Store and Google Play. The iPhone
App Store submission (T1071, iOS release chat) waits for the picture set. Scope is frozen to T1072 + T1076.

Rules of this chat: Fable orchestrates, Sonnet lanes (at most 2), one review round; every heavy job through
`scripts/heavy.sh`; no store uploads (the owner gets the files and the steps); finds and owner-only items go to the
planning chat; work only in scratchpad worktrees, never the main checkout.

## State (2026-11-24, chat opened ~23:30)

1. **T1076 (reshoot tool builds its own export): lane running.** Branch `fix/T1076-store-shots-own-export`. It also
   shoots the current app at all five sizes for the direction pictures.
2. **Directions for the owner: not shown yet.** They need the T1076 captures. Three to show, as pictures, in chat:
   the look, each frame and its caption. Then wait for his pick.
3. **The one set: not started.** After the pick. It is committed where `deploy/staging/appstore-screenshots-push.py`
   looks (`handoff/PLAN/app-integration/store/graphics/store/light/`, folders `appstore-6.9`, `appstore-6.5`,
   `play-phone`, `play-tablet7`, `play-tablet10`); that folder is git-ignored today, so the ignore rule changes.
4. **Listing words, one source: lane running** (plumbing only, wording unchanged). Branch
   `feat/T1072-listing-words-one-source`. Wording proposals go to the owner as multiple choice after it lands.
5. **Tell the iOS release chat** (session `bc-freshwater-fishing-regulations-06`) the moment the set is merged.

## What the two stores do not let us share (checked 2026-11-24 on their pages)

- Device frames: Play says avoid device imagery; Apple allows only Apple bezels. So the shared set has no device frame.
- Apple 2.3.10: no other platform's name, icon or imagery in App Store pictures or text. So no Android status bar,
  no Android system buttons, no "Google Play" or "Android" wording in the shared frames.
- Play: no store badges, no "Best / #1 / Top / New / Free", no "Download now"; a caption takes at most 20 % of the
  picture; nothing time-bound. Apple 2.3.7: no prices in pictures or text.
- Sizes differ only in pixels: Play phone 1080x1920 (2 to 8 pictures), tablets 7" 1200x1920 and 10" 1600x2560;
  App Store 6.9" 1320x2868 (required slot) and 6.5" 1242x2688 (optional once 6.9" is given), 1 to 10, no alpha.
- App Store pictures change only with a new version; Play's can change any day.

## Reports (step 0, 2026-11-24)

`pipeline reports --no-save`: 0 new. TestFlight's six items all cite T982. The two worker sources could not be read
from a worktree (the run tokens live in the main checkout).
