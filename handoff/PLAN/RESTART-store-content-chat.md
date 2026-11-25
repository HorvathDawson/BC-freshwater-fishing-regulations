# Restart page: store content chat (roadmap row S; T1072 + T1076)

Goal: one new set of store pictures and one set of listing words, for the App Store and Google Play. The iPhone
App Store submission (T1071, iOS release chat) waits for the picture set. Scope is frozen to T1072 + T1076.

Rules of this chat: Fable orchestrates, Sonnet lanes (at most 2), one review round; every heavy job through
`scripts/heavy.sh`; no store uploads (the owner gets the files and the steps); finds and owner-only items go to the
planning chat; work only in scratchpad worktrees, never the main checkout.

## State (2026-11-25 02:15; the chat opened 2026-11-24 ~23:05)

1. **T1076: merged (#172).** The tool builds and serves its own store-shots export, refuses a normal site at the start
   (`store-shots-build.json` marker), and the runbook step is `scripts/heavy.sh store-shots 30 -- node app/tools/store-shots/run.mjs`.
   Its ticket still says IN PROGRESS: flip it to DONE with the merge hash.
2. **Owner's pick (2026-11-24 ~23:42): direction A, the angler's questions**, 7 frames, map first, then "What can I keep?"
   showing the limits table, not the season calendar. 2026-11-25: map views one zoom step wider; captures quicker; most
   scenes may be shot from the website (`--web <url>`; only 07-saved-spot and 20-help need the store-shots build).
3. **The one set: PR #192** (auto-merge set, CI running at 02:15). 35 PNGs in
   `handoff/PLAN/app-integration/store/graphics/store/light/` (`appstore-6.9`, `appstore-6.5`, `play-phone`,
   no tablet sets: dropped 2026-11-25); both push scripts read it with no arguments (plan modes clean, nothing sent).
   Regenerate: `scripts/heavy.sh store-shots 30 -- node app/tools/store-shots.mjs --theme light` (about 6 minutes;
   one frame: add `--reuse --scenes <nn> --only <target>`). One review round is spent. Known nits, owner asked:
   both answered 2026-11-25 ("Drop tablet set. And you can get them to fix picture"): tablets dropped, frame 3's watershed name fixed (see T1072).
4. **Listing words: merged (#170, #175).** One source `app/store-listing/listing.json` (+ `full-description.txt`);
   `play-listing-push.py --appstore` prints the App Store fields. Owner's answers (2026-11-24 ~23:55) are in T1072:
   promotional text = his short line, keywords without the name's words, Navigation / Sports, 500 MB, graphic kept.
5. **Owner's steps (no uploads by Claude):** in the final message of the chat and in T1072. FIRST clear the 34 old
   untracked pictures from the main checkout's `store/light/` or upload from a clean checkout (the push scripts take every
   picture in a folder; `--plan` / `--check` must show 7 per size; a planning-chat ticket, PR #202, fixes the scripts). Play:
   `.venv/bin/python deploy/staging/play-listing-push.py --commit`. App Store: `.venv/bin/python
   deploy/staging/appstore-screenshots-push.py --check`, then `--commit`, once the version exists in App Store Connect.
6. **iOS release chat** (`bc-freshwater-fishing-regulations-06`): told the words are ready (00:15); tell it the set is
   on main the moment #192 merges.
7. Planning chat numbered the finds: T1112 (worktree setup), T1113 (salmon scene on a pinned state), T1114 (the iOS
   submission page prints from the one source).

## What the two stores do not let us share (checked 2026-11-24 on their pages)

- Device frames: Play says avoid device imagery; Apple allows only Apple bezels. So the shared set has no device frame.
- Apple 2.3.10: no other platform's name, icon or imagery in App Store pictures or text. So no Android status bar,
  no Android system buttons, no "Google Play" or "Android" wording in the shared frames.
- Play: no store badges, no "Best / #1 / Top / New / Free", no "Download now"; a caption takes at most 20 % of the
  picture; nothing time-bound. Apple 2.3.7: no prices in pictures or text.
- Sizes differ only in pixels: Play phone 1080x1920 (2 to 8 pictures), no tablet sets (the push clears the tablet slots);
  App Store 6.9" 1320x2868 (required slot) and 6.5" 1242x2688 (optional once 6.9" is given), 1 to 10, no alpha.
- App Store pictures change only with a new version; Play's can change any day.

## Reports (step 0, 2026-11-24)

`pipeline reports --no-save`: 0 new. TestFlight's six items all cite T982. The two worker sources could not be read
from a worktree (the run tokens live in the main checkout).
