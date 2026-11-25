# T873 — A much shorter Help page: the map's key first, short labels, one-line facts

Status: DONE — c369c225a (owner OK 2026-11-18 ~20:53). Landed on redesign/stream-sections with app/integrate-081-web after the full tier.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T873 | A much shorter Help page: the map's key first, short labels, one-line facts (owner) | DONE | c369c225a | app chat; owner 2026-11-18 (T872–T874 approved ~20:53) |

Owner 2026-11-18 ~20:32, on More → Help, verbatim: "This help page is way way to verbose it is not helpful..."

Today (`core/src/help.ts` `HELP_WORDS` / `helpVM`, drawn by `ui-native/src/menu/HelpPage.tsx`): an intro paragraph;
Walkthrough (a sentence + Replay); Syncing and updates (3 paragraphs); Offline (2 paragraphs + Offline maps); then Line
colours, Line styles and areas, Salmon colours, Runs, Badges, The bell, The refresh button, each with an intro sentence and
every swatch with a title plus an explanatory sentence. 736 words on the phone (390 wide), 3,370 px tall.

Proposal (built in the lane, 196 words, 1,898 px tall):
- The map's key first: Map colours (the day shown), Marks and areas, Salmon mode, Runs tab. Each swatch keeps the
  legend's own name (shared with the map's legend, unchanged) and a note only where the name alone would mislead or
  leave out what to do.
- Then "Offline and updates": three one-line facts (the app) — "Regulations, the map and your spots work with no
  signal." / "Gauges and weather need a connection." / "Big updates wait for Wi-Fi." — then the Offline maps and Replay
  the walkthrough buttons. The web: "This page needs a signal; the phone app works without one." / "Spots are saved in
  the phone app only."
- Then The refresh button, The bell, Data badges (More tab). No intro, no section intros. One speaker: no "we" (T860).
- Every swatch the map draws is still there (help.test.ts holds every status, gear mark, salmon status, run state, sync
  state; the Help page test draws every row).

Notes kept (word for word): Base regulations only "Only the region's rules apply."; No public access "Regulations still
apply; check access first."; Tidal water "DFO's tidal rules apply here."; No run recorded "Not the same as no fish.";
Needs a refresh "Tap it, or restart the app."; Could not update "Tap the red mark to retry."; In-season changes "Changes
not on the map yet."; Likely out of date "Refresh before relying on it."; Unknown "No date recorded."; Date only "Map
base data: age doesn't matter."

Cut: the intro; the walkthrough sentence; "checks … whenever you are online", "the refresh button at the top checks
now"; "unless you allow mobile data" and "applied when you are not reading anything"; the bell paragraph (now its row's
note); "the last ones fetched stay, with how old they are"; "Detailed map tiles are kept region by region" (the Offline
maps button); the web's "Accounts are coming"; every section intro (incl. the Runs how-to: slide, zoom out, tap); the
second sentence of every swatch except the notes above. NOT_FISHING_WORDS.help (Help-only) removed.

Two legend names run past six words ("Check DFO (not mapped yet, or season not set)", "In the water: spawning or
surveyed here"): they are the map legend's own strings, left as they are.
