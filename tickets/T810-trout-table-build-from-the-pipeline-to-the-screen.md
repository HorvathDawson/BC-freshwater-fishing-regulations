# T810 — Trout and char block as the new table, built from the Python pipeline to the screen (umbrella; the spec is written, the build waits for the owner's go)

Status: PARTIAL — built, merged and shipped through step 11: the cutover landed e0434e1b8 (2026-11-18 06:27, CI green); owner signed off the finals from staging 2026-11-18 12:18; 0.8.0 to Play internal and TestFlight 13:44 (8c73939b7); in production since 0.8.3 (2026-11-19). left (Claude): S2 dead-branch cleanup (optional; needs a coverage run, no provider is installed: see 2026-11-22). The MAP / AGENTS notes are written and T809's cutover diff is run and recorded (2026-11-22).
or a release (owner 2026-11-15).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T810 | Trout and char block as the new table, built from the Python pipeline to the screen (umbrella; spec written; steps 3 to 7 wait for the owner's go) | PARTIAL — cutover landed and shipped (0.8.0, now production 0.8.3); notes + T809 diff done 11-22; left: S2 dead-branch cleanup (optional, needs a coverage run) | e0434e1b8, 8c73939b7 | owner 2026-11-16 15:18; `git show 828c5f7c4:docs/design/trout-table-build-spec-2026-11-16.md` |

Source: owner 2026-11-16 15:18: "have another opus agent spec out what would be involved in transitioning the trout
portion of the app to the new table format. want it implemented clean from pythion up to front end. etc etc. use opus
agent". The spec: `git show 828c5f7c4:docs/design/trout-table-build-spec-2026-11-16.md` (tickets A to L for the main chat, M to S
for the app chat; this ticket stands for all of them until each is filed with its own number when it starts).
Design record: `git show 828c5f7c4:docs/design/trout-adversarial-review-2026-11-16.md`.

## What
Trout and char drawn as one table under the existing Trout and char entry, every word, number and date decided in
Python (payload types generated from `contract.py`), the app only lays out; one trout path; other fish untouched.
Built, cut over (e0434e1b8) and shipped; only the optional S2 cleanup is left.

## Owner decisions (verbatim; all built)
- 2026-11-16 15:42: "Start steps 1 and 2"; possession "Keep the page line (Recommended)"; size "Raise the limit, shrink
  later (Recommended)"; other fish: "what? we want to migrate the new trout style into the current app not the other way
  around. ".
- 15:47: "we should just have the trout table under the trout entry like it currently is with the better text like 2 or 4 with
  expand arrow etc. " (trout only; spec ticket Q dropped; S keeps the old card code for the other fish).
- 15:48: "i dont like the new wild and hatchewry option you sent we should jsut say hatchery only unless specifically wild has
  a seperate quota to wild like 1 wild and 2 hatchery or something.... otherwise it is hatrchery only or unspecified (the
  same for both no mentiuon needed)".
- 16:37: "i feel like this possession stuff is just kind of broken but fixed in the new trout format? so maybe we can just
  work on the transisiton to the new trout redisgn?" (go for steps 3 to 7).
- 16:50: "we want to make sure the plan is robust so that we change the data all the way in python and the front end and it all
  just works withotu fighting it and makign sure everythign is streamlined etc".
- ~16:55 (labels): fold row "All rows get arrow"; pinned trout top line "Drop it (Recommended)"; lone other fish "Stays as
  today (Recommended)"; year bar's trout line "Drop it (Recommended)"; ~17:15 "Salmon + trout folds" (arrow replaces
  "Details" there; Other fish list as is). 17:40: ship "No, only at the switch"; angler-only notes "Page's notes block
  (Recommended)".
- 2026-11-17 08:50 (Kaslo/Keen): "Dollys and bulls always have same quota per the book.  Why would they
  not count towards the regions 2? That seems wrong" (B: count toward the 2 from streams).
- 11-17 15:34 to 15:50 (G2), 21:33 (picture pack signed off), 23:29 ("Keep it big, 23 px (Recommended)", "Keep the tile
  (Recommended)", pin bar "Remove it (Recommended)", "Yes, name the fish (Recommended)"); 11-18 00:35 to 01:42 rounds 2-4
  ("simplify"; "fraser also never simplified that but then removed the text that allowed a user to infer it"; R4 "A but
  still has to mention the overall limit"; Vedder "only one is an exception"; "A: drop it everywhere"). Verbatim in the
  old owner to-do page; the accepted rules (R1-R5 count column, nested bands, "50 cm (20 in) or less if wild", Vedder
  "Keep 2 trout and char today" + subline) are in the build.
- 2026-11-18 12:18 finals from staging (r-f1f2e8da9666): "Signed off". 0.8.0 notes: "0.8 release notes are fine".

## Done
- Step 1 I0 + A0 (merge 1b9cce5d2); F builder; J proof in the card gate (`answers/trout/check.py`); V-py vectors
  (`app/fixtures/trout/trout-vectors.generated.json`, 53 cases; `scripts/trout-refresh.sh`); A1 measured (cutover under
  every gate, no limit raised); L pipeline half (L 679f790c0, sweep e88cb9991); app M0 / M / N / O / P / origin badge / R
  (app/t810-r bb403c97f); S + cutover e0434e1b8 (2026-11-18 06:27, CI green, bench trout-table 7/7).
- 0.8.0 to Play internal (versionCode 35, cabdb411f, bump 7ed1598e6) and TestFlight 0.8.0 (38) 2026-11-18 13:44
  (8c73939b7); production since 0.8.3.
- 2026-11-22 (lane L13): `pipeline/docs/MAP.md` deliver step 5, `app/MAP.md`, AGENTS rule 29a written; T809's cutover
  diff run and recorded in T809 (no trout answer changed).

## Work (left)
- S2 dead-branch cleanup (optional), below.
- `deliver/answers/trout/__init__.py`'s docstring still says "not yet wired into a release": fixing it re-stamps the
  committed mini release (the docstring is hashed into its water files), so it waits for the next mini re-cut
  (`python -m pipeline.tools.mini_release --write`).
- Size lever if room is needed later: F0's `page_possession` is stored inline per rows frame (+11 to +34 kB gz per
  region); interning it would recover most of that (A1; fits without it).
- Loose ends recorded at build time, not seen closed (kept): Kaslo/Keen follow-up asked, does T3a (Lardeau, Duncan lift)
  stand; Madden's DV clause, whether the DV 2 sits outside the general 2; two-number folds ("Up to 5 or 10") stay (for
  the owner); the River-rule section not built (held by the owner); vectors lack an unnamed Region 6 stream and one of
  the 8 rule sets with no stored headline (both need a handle or a find step the cut does not have); P's choices to
  confirm (pictures under "How to tell it", not in the header; hatchery sheet a link; the row picture's spoken name is
  still "<name> — what it looks like").

- NOT done: S2's dead-branch deletion. `quietOf`, `useAnchors`, `PinBar` and the pin-bar coach spots are already gone; the
  rest of S2 (delete composer branches in grouphead.ts, tree.ts, tail.ts, plainrow.ts that only trout reached) needs a
  branch-coverage run over the other-fish rows of the fixture release, and the app has no coverage provider
  (`@vitest/coverage-*` is not installed; a new dev dependency needs its `app/deps.md` line). Optional per the spec.

