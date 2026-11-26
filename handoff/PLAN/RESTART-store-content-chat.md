# Restart page: store content chat (roadmap row S; T1072 + T1076)

Goal: one new set of store pictures and one set of listing words, for the App Store and Google Play. The iPhone
App Store submission (T1071, iOS release chat) waits for the picture set. Scope is frozen to T1072 + T1076.

Rules of this chat: Fable orchestrates, Sonnet lanes (at most 2), one review round; every heavy job through
`scripts/heavy.sh`; no store uploads (the owner gets the files and the steps); finds and owner-only items go to the
planning chat; work only in scratchpad worktrees, never the main checkout.

## State (2026-11-25 14:00; the chat opened 2026-11-24 ~23:05). The chat's scope is DONE; what is left is below.

**On main**
1. **T1076 DONE (#172):** the tool builds and serves its own store-shots export and refuses a normal site; the runbook
   step is `scripts/heavy.sh store-shots 30 -- node app/tools/store-shots/run.mjs`.
2. **The one set (T1072; #192, then #209, merge 4b21a95bc):** direction A, the angler's questions, 7 frames (the owner's
   pick, 2026-11-24). 21 PNGs in `handoff/PLAN/app-integration/store/graphics/store/light/`: `appstore-6.9`,
   `appstore-6.5`, `appstore-6.3` (added 2026-11-25), `play-phone`, 7 each. No tablet sets (owner 2026-11-25: "Drop tablet set"); frame 3 reshot so its Change
   panel names the chart's watershed (a check refuses any other name).
   Regenerate: `scripts/heavy.sh store-shots 30 -- node app/tools/store-shots.mjs --theme light` (one frame: add
   `--reuse --scenes <nn> --only <target>`).
3. **Listing words (#170, #175):** one source `app/store-listing/listing.json` (+ `full-description.txt`);
   `play-listing-push.py --appstore` prints the App Store fields. The owner's wording answers are in T1072.
4. **T1127 DONE (#207):** both push scripts take each size's list from `app/tools/store-shots.json`; a stray or missing
   picture stops plan / check / commit by name before any network call. The Play push clears the 7-inch and 10-inch slots.

**Left (none of it this chat's to start)**
- **The owner's uploads** (commands in T1072 "Owner's steps"). App Store pictures: with the iOS release chat
  (`bc-freshwater-fishing-regulations-06`; told 2026-11-25 that frame 3 is on main at 4b21a95bc). **Play pictures are
  HELD**, his words 13:43: "hold play p[ictures until before prods release" (the planning chat reads this as the Play
  production release). A text-only Play push before then is `--no-shots`.
- **Reshoot play-phone frame 3 after T1128** (two app faults at phone width: "Steady" struck through, last day column
  clipped). Whoever closes T1128 tells the planning chat; the reshoot is the one-frame command above, then a PR.
- **The 34 old untracked pictures in the main checkout:** the 0.10.0 chat clears them (owner: "we can remove old store
  photos. we want this to be clean"). Until then an upload from that checkout stops by name.
- **T1134 App Store creative assets** (Header + Search Results, stills and 8 s videos in `store/light/appstore-creative/`, made by `app/tools/store-shots/creative.mjs`): the owner uploads them by hand in App Store Connect; Apple's codec rules for the videos are unconfirmed (see the ticket).
- **T1131:** a hand-started GitHub workflow that makes the pictures; with the repo-move chat, after the transfer (owner:
  "can be part of new repo transfer").
- **T1072 items not done (the owner's call):** website landing and share-page words (only the home meta description reads
  the one source), the store video (T709).
- Other tickets from this chat's finds: T1112 (worktree setup), T1113 (salmon scene on a pinned state), T1114 (the iOS
  submission page prints from the one source).

## What the two stores do not let us share (checked 2026-11-24 on their pages)

- Device frames: Play says avoid device imagery; Apple allows only Apple bezels. So the shared set has no device frame.
- Apple 2.3.10: no other platform's name, icon or imagery in App Store pictures or text. So no Android status bar,
  no Android system buttons, no "Google Play" or "Android" wording in the shared frames.
- Play: no store badges, no "Best / #1 / Top / New / Free", no "Download now"; a caption takes at most 20 % of the
  picture; nothing time-bound. Apple 2.3.7: no prices in pictures or text.
- Sizes differ only in pixels: Play phone 1080x1920 (2 to 8 pictures), no tablet sets (the push clears the tablet slots);
  App Store 6.9" 1320x2868 (required slot) and 6.5" 1242x2688 (optional once 6.9" is given) and 6.3" 1206x2622 (the "iPhone with Dynamic Island" slot Apple now requires, 2026-11-25; display-type name unconfirmed), 1 to 10, no alpha.
- App Store pictures change only with a new version; Play's can change any day.

## Reports (step 0, 2026-11-24)

`pipeline reports --no-save`: 0 new. TestFlight's six items all cite T982. The two worker sources could not be read
from a worktree (the run tokens live in the main checkout).
