Saved by the main chat from the checker's hand-back (the checker could not write files).

# General audit, checker 5: decided or asked, but not built, not ticketed, or built differently

Read-only. Repo at d0d584159 when started; 0c9988033 (the in-flight page) landed during the audit; `OWNER-TODO.md` was being edited by the main chat (the 2026-11-16 morning list appeared while I read; I read all 1,322 lines). Sources read in full: `AGENTS.md`, `handoff/PLAN/OWNER-TODO.md`, `START-HERE.md`, `INFLIGHT-chat14.md`, `NEXT-SESSION-after-chat13.md`, `STATUS.md` (entries from 2026-11-14 on), the "RESTART HERE" section of `ios/START-HERE-ios.md`, `release/PRODUCTION.md`, T495, T371, and the ledger rows of about 130 tickets. Sections 2 and 4 were done by two Opus sub-checkers; their evidence is `part-done-tickets.md` (with `p1-A.md` to `p1-E.md`) and `part-rulings.md` in the same folder.

Words used: BUILT = on the shared branch (code or curated data found). BRANCH = built on a branch that is not merged. TICKETED = an open ticket holds it. LOST = no code and no open ticket (each LOST was searched for in the code, both ledger files, the ticket folder and `git log --all`).

## 1. Owner decisions and requests, 2026-11-09 onward

### 1a. LOST, or built differently, or the record says the opposite of what he decided

| # | date | his words (verbatim) | state | what should happen |
|---|---|---|---|---|
| 1 | 11-15 evening, round 1 | "I want you to for it to be redrawn. We will finish these tonight so not a big deal... Just be safe" (a place whose last save is "wrong" with no fix still ships its older extent) | LOST. Main chat's reading, written twice: "such a place does not ship its disputed extent"; `NEXT-SESSION-after-chat13.md` says "NOT BUILT". No ticket. `pipeline/regs/review/audit.py:98`: "A 'wrong' or 'unsure' save with no landed fix changes nothing that ships." Only a flag exists (`OLD_EXTENT_FLAG`, audit section B, a chip in the review app). On staging: Fraser Region 3 mainstem (morning list B2), Sproat and Stamp Falls "end on a wrong save". | A ticket: the release stops and lists such places, or they ship DFO's words undrawn. Say on the morning list which places ship against a "wrong" mark. |
| 2 | 11-15 evening rounds 1–6, 23:00 dialogs | "I think what ever still applies there should be the color … However a new wording being there would then mean we have a different indicator on the stream line … Then every stretch in the river will get the banner in the salmon section saying the new unmatched rule until we rematch it"; "Yes: close match needs confirming (Recommended)"; "I like the stripe option best for the ones with banners. Also the stripes should only be on the salmon view with this error mode right?" | BRANCH, and the shared ticket describes the rejected design. `T759-missing-feed-place-reads-more-open.md` still says "fix proposed, owner to confirm the reading" and proposes "reads 'not known'"; the ledger title says "app backstop reads 'not known'". Rewritten ticket only on `data/t759-missing-place-check`. Code: Python dfc95d971, worker 8bd68cce2, app 1289d8c20, stripe 6c24f94a6 (worktree branch, not pushed). Not in 0.7.11. Ledger: "before closed testing". | Land the rewritten ticket text now (docs only). Push the stripe branch. Two confirms open (3a). |
| 3 | 11-15 round 4 | "Build it this way (Recommended)" (one-bank places) | TICKETED, NOT BUILT, shown as unanswered: T754 file and ledger "OPEN (investigated; options for the owner)"; OWNER-TODO line 775 "OPEN, six questions". Answer not in the ticket. Kitimat's east bank reads closed about 750 m (B12; T774). | Record the answer in T754; remove the "six questions" line. |
| 4 | 11-15 afternoon | "also want to make sure we deduplicate the new cuts from proposal (if they rewference same thing they should be matched etc)" | Half done. Data de-duplicated once (63f94918a). The suggester fault ("proposes a NEW cut at the same position as an existing one … match by position") is on the restart page under "not yet ticketed"; no ticket, no commit. | Ticket it. |
| 5 | 11-14 (iOS-day default 7) | a default he was told was built: "the About page gains one sentence saying the app is not affiliated with the government (Apple asks for it; audit G15) [About only …]" | LOST / said built, is not. `git log --all -S"not made by or affiliated"` finds only notes commits; no branch has the sentence; no shared-branch ticket names G15 (it is in `ios/parity-audit.md`). | Ticket + build; add to the store checklist. |
| 6 | 11-15 about 23:00 | (unanswered) the app chat's "merge question" | Question lost. `ios/START-HERE-ios.md:22`: "Held: every other merge (he did not answer the merge question)". Not in OWNER-TODO or the morning list. His 11-15 10:33 words: "ios work could merge when ever I think?" Holds T759, T760, T756, T746, T738, T741, T732, T739, five iOS branches. | Morning list, with his 10:33 words beside it. |
| 7 | 11-11 (T495 gate 1) | gate 1: T462 "findings are fixed or ticketed with the owner's OK" | LOST (7 findings): K6 (Skeena weekend caveat cut at the colon, `core/src/regs/caveats.ts:193`), K7 ("Open, except some parts" when nothing is closed, `core/src/regs/strip.ts:148`), K8 (Shuswap pier exemption dropped), G8 (non-resident-alien weekend closures not on the Licence tab), G9 (fly-only waters say "One hook or fly"), G10 (under-16 non-resident price line), D6 (Region 6 gear line cut mid-word). No ticket, no owner row, no commit. | Ticket them; gate 1 is not met until then. |
| 8 | 11-13 | T710: the side channel's name reads backwards ("Exchamsiks River confluence up to Skeena River Ecological Reserve boundary") | Question lost: "undecided" inside an answered row (OWNER-TODO line 502); T710 DONE. | Morning list. |
| 9 | 11-15 | Shuswap closed stretch: "Also yours: tell DFO about the mismatch?" | OWNER-TODO line 846 only; not carried forward. | Morning list (recommended yes). |
| 10 | 11-15 | review app: "when Confirm is greyed, name the unticked row and jump to it (owner got stuck three times)" | LOST: restart page only; `CheckReview.tsx:781` still only disables the button. | Ticket. |
| 11 | 11-15 | Hope gauge: 66 m between 08MF005 and the new bridge cut reads 08MF076; three gauge cuts never placed (08HD024, 08ME002, 08MH032) | LOST: restart page + two curation reports; no ticket. | Ticket. |
| 12 | 11-15 | "Owner rule question, not asked yet: should fast-fail's review step read committed files instead of the working copy of his proposals file?" | Never asked; not on the morning list. | Ask (recommended yes). |
| 13 | 11-14 | "Production will use the latest version"; "It will also get new dfo places I will label tonight" | Owed: "then fresh production folders and fresh commands here" (line 17). Out-of-date commands still at lines 19–25 (r-f749475d0962, copy at 9048fbc20) and 384–385 (r-a178299f6005). | One current block; delete the old two. |

### 1b. Decided, built on a branch only (not on the shared branch, not in 0.7.11)

| date | his words | ticket | where |
|---|---|---|---|
| 11-14 | "Only need to backup photos and spots the rest of the data isn't important"; "Settings could also be saved" | T732 IN PROGRESS | `android/t732-backup` fa562f0d6; open: screen-lock-only [no], copy size [1600 px] |
| 11-14 | "okay i ticked push notifications for app yes build for iphone."; "i guess if new data is there we could send a notification that the user needs to download it."; "for android too."; "dont need this" (time-sensitive) | T739 IN PROGRESS | `ios/t739-push` ca91889bb; TestFlight 35/36 + staging worker. Android half needs his Firebase project. |
| 11-15 round 6 | "Yes to both (Recommended by it)" (push privacy line; App Store answer) | T739 | Not written anywhere: no "Push token" text in `app/tools/seo/html.ts` on the shared branch or `ios/t739-push`. |
| 11-15 23:00 | "Yes, with Claude's numbers (Recommended)"; "Keep trying by itself (Recommended)" | T760 (ledger OPEN, stale) | `fix/t760-stuck-download` 4e3bfffe9, unit-tested, not on the emulator; T756 "NOT ready" |
| 11-15 | "can we do more fish puns here that are directed at this ?" | T746 | `ios/t746-apply-splash`; TestFlight 36 only |
| 11-14 | "we want it to progress with app closed in both app and android"; "need to just update text for ios" | T722 | `ios/t722-install` / `ios/integration` |
| 11-14 (default 14) | first-install bar "[fix it, ticket T738]" | T738 (ledger OPEN "owner pick first", stale) | `ios/t738-bar`; TestFlight 36 |
| 11-14 | "can you make sure to keep working on optimizations? make app faster and lighter for both ios and android" | T741 | `perf/t741` a02e09fb5 |
| 11-15 | closed areas as live locations; region-default sentences | T757, T758 | Worker halves live on STAGING from `ios/staging-worker`; the shared branch's worker has neither (section 6) |

### 1c. Decided, ticketed, not built (correctly open)
- 11-14 "The highlighting isn't very good in dark mode on regulated streams." → T735 (before closed testing), waits on his pick.
- 11-15 "we should have a way to define one half of the river closing maybe? could color only west or east side? …" → T754 (row 3 above).
- 11-15 night "also another fable agent should take a look at how trout are displayed in the app. …"; "We are iterating on it more not implementing"; "Do not hold stuff for the trout redosgn." → exploration, six rounds; data faults in T782 (OPEN).
- 11-15 mark note: Kitsault tidal boundary name → T773 (OPEN).
- 11-13 "after the closed release we will do stocking and depth maps next. so add that to next after closed tickets."; "these are after closed" → T200, T201, T634; T714, T715.
- 11-12 "Raise now, shrink later, but the ticket should cover ways to shrink data across the board, not just ponds." → T686 IN REVIEW, his pick open.
- 11-12 E27 widenings → T695, postponed by him 11-13.
- 11-13 "will look harder for frog and redfern later" → his.
- 11-09 "not now … at least one mockup that i check off" → T528. Q179 "sure" → T559 IN REVIEW. Q180 "dfo surveys are just in addition to psf curve. psf is primary" → T552 PARTIAL.
- 11-16 "Everything takes so long maybe we should have less tests all the time and do it together at end or something"; "Feels wasteful" → morning list B14.

### 1d. Spot-checked as built on the shared branch
- Privacy wording ("choose the best wording please you know better then I do for that stuff"): `app/tools/seo/html.ts:148` "rounded to about 1 km"; `:160` "Sharing and directions"; `:172` "More → Settings → Privacy → “Send crash reports”".
- "Portrait only.": `app/apps/mobile/app.json:7` `"orientation": "portrait"`.
- "we should only have up or downastream that access all components"; "upstream is fine": T750 e920d4d0d on the branch; no "Which way upstream" string in `app/packages` (ledger row stale: "unpushed").
- Third heavy lane: `scripts/heavy.sh` (`heavy-job-3`).
- Desktop split ("for the layout choose which ever you think is best practice"): d3c7376a0 (ledger/ticket stale).
- "The bad display oddities must be fixed on this build": T762 4e793d61f, T763 7958b3c60 + e3d6d5f26; `test_answers_t763_duty_band.py:75`.
- "Raise to 420 kB (Recommended)": `pipeline/deliver/release/build.py:86` `"salmon-6": 420_000`.
- Pallant: `data/curated/waters/tidal_limits.json:998`.
- Puntledge weir: `cuts.json:1787`; tailrace 7102afb9b.
- Skeena confluence cuts: `cuts.json:8738`.
- Juniper 4 km: `cuts.json:4923`.
- Rest-of ("this iss handled by rest of option in extents"; "all items wih same structure as that shoul duse rest of extent type too"): T761 d65d4e15d, marks 2787–2794.
- Lake loops ("yes do both steps but make sure it is done in a clean way"): a458c1fd4 + 3bbb20082 (ledger stale).
- Kasiks: `pipeline/docs/GOTCHAS.md:192`.
- Mouth/tidal ends: `pipeline/regs/dfo_salmon/suggest.py:1210`.
- BC border pickable: T755 (`reuse.border_end`).
- T679 D4 guard: `pipeline/atlas/reach/build.py:906`, `reach/cli.py:204`.
- T670 G2 ("Why does it matter the river is closed? …"): T670 line 130.
- Skeena lettered section on the card: T668 d199f0d4d ("Region 6 · Skeena River · B(ii)").
- B.C. time: `app/packages/core/src/clock.ts:27`.
- Nakina "Whole channel + note": binding pinned; card words not pinned.
- The 11-12/13 tidal, gauge, vessel, licence, inbox, settings and pinch decisions: T685, T684, T678, T682, T690, T697, T670, T209, T673, T706/T694, T703, T707 (sub-checker). T690's row still says "left: promote + release" though r-f749475d0962 shipped it.

### 1e. Readings he gave that are not in `pipeline/docs/RULINGS.md` (last changed 2026-11-13, a2727a3b0)
1. Leftover-worded DFO places use rest-of, with "areas and times" (in AGENTS rule 13 only).
2. Nass "mainstem" = no tributaries; "excluding tributaries" beats the heading; "X River tributaries" = tributaries only ("All three are right (Recommended)").
3. "need to make sure all rules under a water with (includes tributaries) in the name gets tributaries too."
4. Atnarko: "no it needs to include tributaries".
5. Lake rules ("Keep all three (Recommended)") and lake loops (AGENTS rule 12 only).
6. The T759 rule.
7. One-bank places ("Build it this way").
8. "upstream of signs near the mouth" with no sign = whole river from the tidal boundary ("Whole river is right").
9. "if the book says 4km then we put it 4km unless i had another split it already matched??"
10. Shuswap: "Use the real gauge point so we don't duplicate it and the other coord as the other point even tho it is 2km use as printed".
11. Pallant bridge = tidal boundary.
12. Alexandra Bridge 4.3 km into Region 3: "this is fine".
13. "I don't see how this would be a rest of situation.." (rest-of only where a place overlaps what it lists).
14. The T708 dialog answers of 11-13 (no verbatim quote recorded).
No ticket asks for these to be written in.

## 2. DONE tickets since 2026-11-09 (25 sampled; `part-done-tickets.md`)
All cited commits are on the branch (T743's df546975a and cf7df0dba landed as 8a28913c1 and a458c1fd4).
- True DONE (17): T557, T675, T697, T698, T734, T743 (data side), T763, T764, T628, T694, T706, T680, T683, T685, T713, T712, T691, T703.
- Leftover with no home: T462 (seven findings + "smaller drops" + Bella Coola cutthroat note); T549 ("hold a page with more than one rules table"); T554 (older `clim.json` on the same HYDAT can replace a newer rebuild, `clim_guard.py:10`); T673 (run timing tied to one atlas, "needs a decision of its own"); T600 (provenance on the log line, not the record; never put to him); T710 (backwards name).
- One asked item missing: T778 (no test on built data; side-check gap is T779).
- Stalled (18): T261, T292, T310, T390, T461, T535, T562, T585, T633, T674, T603, T606, T670, T671, T672, T676, T687, T689. Most wait for "the next APK/release" (build 32 on 11-14; r-46e4355a7277 on 11-16). T676 waits "with T673" (DONE 11-13). T585 waits for a yes OWNER-TODO line 472 records.
- Stale rows: OWNER-TODO line 34; T462 Status; T697, T734, T690, T664, T753, T752, T750, T747, T759, T754, T760, T738 rows. 28 DONE + 2 DROPPED rows still in README.md.
- No file and no row on any branch: T744, T745, T748, T765–T769.

## 3. Questions
### 3a. Unanswered, something waits (recommended answer as the documents give it)
1. T759 stripe: legend "Check rule"? strong enough on the dark blue line? — yes/yes — T759 merge — not on the morning list.
2. The merge question — not written; his 10:33 words — every held merge — not on any list.
3. T735 dark highlight — C — not on the list.
4. Play Data safety location, approximate vs precise — none written ("we want precise" for the App Store) — Play forms — not on the list.
5. Babine Lake in August reads open inside 13 unconfirmed closed areas — confirm them; "open, closed in part" until then — B8.
6. Gitnadoix powerline cut — yes — B10 (reverses his 11-15 "Whole river (Recommended)"; say so).
7. Gosnell / Thautil — leave out, keep Lamprey — B9.
8. Eulachon below Terrace — yes — B11.
9. Kitimat east bank — leave closed until T754 — B12.
10. T770 gear lines together — yes — B13.
11. Labels B1–B7 — re-check Campbell; keep Fraser R3, Kanaka, Kispiox; check Chemainus imagery; Cowichan flag off; Middle Shuswap main river only.
12. Fewer, later checks — yes — B14.
13. T686 shrink — (a) — not on the list.
14. T732 backup — no; 1600 px — not on the list.
15. T730/T731/T728 looks — leave as built — not on the list.
16. T498/T508 "Not right" with no note (11-09): which tab? — none — not on the list.
17. iOS picks (Save row, back strip 16 pt, email "linked", link file, iOS 16.4+, five clean-up calls) — the bracketed defaults — not on the list.
18. Q172 backup bucket — `bc-fishing-regulations-backup`.
19. T371 OWNER items — yes as drafted.
20. T689 lake colour — leave.
21. Trout round 6 ×4, round 5 leftovers — yes — list C.
22. T710 name; tell DFO (yes); fast-fail reads committed files (yes).
Asked more than once in the file: Kitimat one bank (lines 749, 775, 951, B12); Gitnadoix (909, 921, B10); Fraser R3 mainstem (799, second-look 6, B2); Sheldens (798, 957, 1039, 1058); Play location (line 172, STATUS 11-14, app chat page); production commands (19, 384, 654).

### 3b. Answered, still shown open
T754 six questions (line 775, ticket, ledger); T759 "owner to confirm" and line 898; T752 "LOOK AND PICK" (line 811, ledger); T753 "OPEN, pick" (line 773); Seton mouth (line 742); T743 "BLOCKS tonight's release" (line 34); "Still open: Q31" (line 606; ledger: answered 10-28); T585's five strings; lines 645–647 and 703 (Q26 name, Q27/T87 dropped); line 705 "iOS BENCHED"; line 727 (app name; coordinate rounding, built); line 391 (T532).

## 4. Rulings (20 checked; `part-rulings.md`)
ENFORCED + PINNED (19): Z4a, Z4b, Q28, L22, L23, L24, S12, Li13, Li13b, E19 (binding), E21, E26, D5, P47, P52, Z4, Q13, K5, K8. Five small tests run and passed.
- G9: CURATED DATA ONLY (`region-4.json:10853-10866`); no code, no test, no ticket; not in section 16.
- E19 card words not pinned; P52 "stays in search" half not pinned.
- File behind the code: L22, Li13, E19, E20–E24 cells; K5 still listed "not enforced"; P57 row malformed; section 17 lacks O6 → O7/O8.
- Section 15: nothing unresolved without a ticket.

## 5. Promises (2026-11-13 onward) with no ticket or list entry
- Fresh production folders and commands (OWNER-TODO line 17; restart page step 7): not done; old commands still stand.
- Suggester position match; review-app greyed Confirm; Hope gauge + three gauge cuts; the unasked fast-fail question; the "wrong, no fix" ship rule: restart page only.
- "Unanswered rows on the To-Do page" to be chased: only partly on the morning list.
- Privacy push line into the page's source: app chat's page only; not done.
- `pieces_gate.py` / deploy-refusals review "owed by main": T757 ticket only; branch unmerged.
- DFO vector fixtures refresh: mention in T757 only. Worktrees to remove: no entry.
- "The lake-shoreline look stays open for pictures." (11-12): T467 OPEN since 11-06, no pictures, no row.
- KEPT: T782 (Kitimat, Ross Lake, "Bull trout" caption); privacy crash-switch path (`html.ts:172`); suggester tidal boundary (`suggest.py:1156-1213`); the one morning list (line 1236).
The restart page holds most of these and is deleted when replaced.

## 6. Between today and (a) Play closed testing, (b) production data
T495: closed testing is on production and "the website has to move from v1 to the new version BEFORE closed testing starts"; PRODUCTION.md step 9 makes the closed build a production build. So (b) first.

(b) Production data
1. Owner: check staging; re-tick list A; answer B1–B13 — T743, T781.
2. Decide if one more staging release is needed (Claude builds).
3. Claude: merge T757 + T758 worker halves into the shared branch; `deploy/prod/upload.sh:46` `pieces_gate` refuses r-46e4355a7277 (four closed areas) until the production worker emits pieces — T757, T758.
4. Claude: fresh production commands + clean site copy — T404.
5. Claude: rehearse every production script (step 1) — T404.
6. Owner: backup bucket (Q172), mail routing, `.run-token.prod` (step 2).
7. Owner: data worker (step 3).
8. Owner: feeds worker (step 5), before or with the data for this release; GitHub prod secrets.
9. Owner: `CONFIRM_PROD=<r-id> … release --prod <r-id>` (step 4).
10. Site: Claude builds, owner deploys; Cloudflare robots/bot settings; www redirect (steps 6, 7) — T240.
11. Owner: smoke `--production`, feeds `/status`, the privacy page (step 8).
12. Owner: Search Console/Bing; retire v1 cron and layout (steps 10, 11).

(a) Play closed testing
1. 0.7.11 on Play internal; his ten-step walk (row `t749-r8-phone-walk`) and the 0.7.9 / 0.7.10 checks — T749.
2. "Before closed testing" tickets: T759, T735, T716, T740, T708, T751, T608.
3. Gate 1: the seven T462 findings and this audit's findings, with his OK — T462, T495.
4. Owner: `/code-review ultra`; Claude triages — T463.
5. Store listing: descriptions, 8 pictures re-shot (T704), preview clip (T709), non-affiliation sentence — T244, T371, T319.
6. Owner: Play forms, incl. the location answer — T371.
7. Owner: production app build to Play internal, install, promote to Closed (step 9) — T404, T371.
8. Owner: tester list, opt-in link; 12 testers × 14 continuous days — T371.
9. Claude: in-app update prompt checked (T390, stalled since 11-06); T514.
Not needed for either: iOS, the trout exploration, T200/T201, T686, Android push.
