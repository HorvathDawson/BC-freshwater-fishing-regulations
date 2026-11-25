order: 30

## 2026-11-16 morning list — ONE list, all regions (written 04:14; the audit's findings are added below it when they arrive)

**Where things stand.** Your labels are built into the app data and on staging: release r-46e4355a7277, staging site
rebuilt on it, smoke check green (19 pass, 0 fail). 194 DFO places changed since the last release, 13 new cuts, four
closed areas drawn (Shuswap, Thompson x3). The app chat is redeploying the staging feed server and building 0.7.11 for
Play internal testing. Production is untouched and stays yours.

### A. Saves to re-tick in the review app (I saved these at your request as "unsure"; each is noted so)
Vancouver Island
- Somass, Stamp, Little Qualicum "everything else" places: rest-of, with the listed stretches answering first on
  their own dates (lines 2787, 2788, 2789; they replace my earlier 2771 to 2773).
- Nitinat below Parker Creek (2792) and Puntledge below the lower hatchery weir (2794): rest-of.
- Cowichan: Sandy Pool sign to the tidal boundary (2774) and 66 Mile Trestle to Sandy Pool (2775): side streams off
  (the September closure no longer lands on the whole Koksilah).
- Nanaimo: Cedar Road bridge to the Hwy 1 overpass (2776), 500 m below Bore Hole to Boulder Creek (2777) and its older
  wording (2778): side streams off.
Interior
- Shuswap closed stretch: the gauge to DFO's other printed point, 2,075 m (2770).
- Thompson, Juniper closed area: the lower signs to 4 km up (2785). Deadman and Ashcroft (2748, 2749) from before.
Skeena, Nass, north
- Skeena sockeye, Terrace to above the Babine: rest-of between the CNR bridge and the gauge, three listed places
  (2790, 2791).
- Skeena "mainstem waters only" (pink): above the CNR bridge, no side streams (2782).
- Morice, lake outlet signs to Gosnell Creek: starts at the lake now (2781).
- Babine Lake sockeye place: the whole lake, side streams off (2784). See question 8.
- Tatshenshini: the Blanchard River whole with its side streams (2779); Kwatini, Stanley and Goat creeks whole (2780).
- Sheldens Creek: the whole creek (2783), from your "i think this is tidal bound maybe".
- Kitimat west bank: side streams off (2786); still closes both banks until one-bank places are built.
- Dean, Crag Creek down to the tidal boundary: rest-of (2793).
From the earlier list, still to re-tick: Nanaimo 500 m (2746); the two Bulkley "all tributaries" places (2732, 2733);
the two Skeena places on the new confluence cuts (2751, 2752); Puntledge 1263 / 2447 / 2687; Sproat and Stamp Falls
(re-save as right: your fixes are in); the Fraser Region 2 places and Squamish (they gained side channels).

### B. Questions (recommended answer first; nothing here blocks staging)
Labels
1. **Campbell River near the mouth**: three stretches still "not sure" from Oct 26; the cement block sign is an
   estimate. Re-check on the satellite view when you can. Until then: kept as drawn.
2. **Fraser Region 3 mainstem** (Spuzzum to the Region 3 / 5 line): you marked it wrong because a label read "Region
   3 – Region 8 boundary"; the start used IS the Region 3 / 5 line. Keep as drawn and re-save as right? Recommended: yes.
3. **Kanaka Creek**: DFO says "112th Street bridge", the only crossing is 112 Avenue. Keep? Recommended: yes.
4. **Kispiox "signs near the Kispiox River Resort"**: no resort by that name; the signs sit at the Kispiox River Lodge,
   13.7 km up (low confidence). Keep until DFO Smithers confirms? Recommended: yes.
5. **Chemainus tidal boundary at 2.1 km** (low confidence; it makes 16 of 24 pieces tidal). Keep, or check imagery?
   Recommended: check imagery.
6. **Cowichan**: the water's "including tributaries" flag looks wrong (only a retired wording says tributaries). Turn
   it off? Recommended: yes.
7. **Middle Shuswap** (Shuswap Falls to Mabel Lake): no side-stream choice is saved anywhere. Main river only?
   Recommended: yes (DFO's words name no side streams).
Skeena (from the review of the built result, `handoff/PLAN/curation-review/skeena-cascade-review-2026-11-15.md`, ticket T781)
8. **Babine Lake in August (errs OPEN today).** Sockeye reads open on the whole lake Aug 1 to 27, including within
   400 m of 12 creek mouths and east of the Gullwing Creek line; those 13 closed areas are only notes on the card
   because they are not confirmed yet. Recommended: (a) you confirm the 13 areas in the review app so they are drawn;
   (b) until then the card gets an "open, closed in part" headline on those days.
9. **Gosnell Creek and Thautil River (errs closed).** The rule book's Morice "No Fishing from Gosnell Creek to Lamprey
   Creek" closes both creeks Jun 16 to Jul 31. Your earlier ruling says a joining creek with a line of its own is not
   inside. Leave Gosnell and the Thautil out, keep Lamprey Creek in? Recommended: yes.
10. **Gitnadoix below the powerline (errs OPEN).** Coho reads open Sep 1 to Oct 31; DFO opens only above the
    powerline. Add a cut at the powerline? Recommended: yes.
11. **Eulachon**: "No fishing for eulachon" is missing on the Lakelse and, to Jun 15, the Kitsumkalum. Always show it
    below Terrace? Recommended: yes.
12. **Kitimat east bank**: reads closed for about 750 m (DFO closes only the west bank). Leave closed until one-bank
    places are built? Recommended: yes.
How rules combine
13. **Gear rules of a whole river on a stretch with its own gear rule that day** (T770): on the Little Qualicum in
    Sep to Nov, "fly fishing only" hides the river's all-year hook rule. Show both? Recommended: yes.
How we work
14. **Fewer, later checks** (your "Everything takes so long…", "Feels wasteful"): (1) while working, only the tests
    for what changed; (2) ONE full run, at the release, on a clean copy, and the staging release stops waiting for
    GitHub's run (GitHub's run stays required for production); (3) checks that take seconds run before anything long.
    Yes? Recommended: yes. The audit's guards checker is costing this out; its numbers go below.
Kept as they are unless you say otherwise (no answer needed): the Tatshenshini "mainstem and all other tributaries"
place (your "I don't see how this would be a rest of situation"); the Tatshenshini place below the border keeps its
side streams (you verified it); the Kitsault tidal boundary's name waits for a small code change (T773); Sheldens,
Juniper, the Skeena sockeye pair, the Tatshenshini headwaters and Maria Slough are done as you answered.

### C. Trout display (mockups only; https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH, round 6)
Round 6: (1) on the Vedder is "Up to 2 today" enough on top when a rainbow column says "Keep 4"? yes; (2) keep
Previous / Next in the sheet? yes; (3) a year per fish in the data so the year bar works on every water? yes, if
built; (4) sources under the table and in each sheet? yes. From round 5, still open: the new wordings, and who
writes the "often mistaken for" lines (you, or someone you trust on fish).

### D. Filed overnight (tickets T770 to T782; none needs you except where a question above names it)
T770 gear rules hide each other · T771 chain order and an equal-places check · T772 Kitsumkalum Lake takes its river's
side channels · T773 tidal boundary name · T774 a place cannot be saved "not drawn" · T775 review app rest-of
leftovers · T776 audit tool faults · T777 tests that read your live saves, and child-process crashes · T778 the
Shuswap delta piece (done) · T779 the save-time check does not show a piece the build will refuse · T780 part names
showing "approximate location (medium confidence)" · T781 Skeena follow-ups · T782 data faults from the trout review.

**Added 04:40: the general audit (five Fable checkers, read-only; four reports in:
`handoff/PLAN/audit-2026-11-16/code.md`, `handoff/PLAN/audit-2026-11-16/labels.md`,
`handoff/PLAN/audit-2026-11-16/decisions.md`, `handoff/PLAN/audit-2026-11-16/guards.md`; the answers-against-sources
checker is still running). And the Play build: 0.7.11, build 33, code shrinker on, is on Play internal testing
(04:37); your ten-step phone walk is the To-Do row `t749-r8-phone-walk`.**
Headlines: the code of Nov 14 to 16 has no defect that changes what an angler is told today (97 commits read); every
one of your 309 labelled places is stored as your last save says, and no place reads open against its own DFO row.

### A2. Re-tick list: additions from the audit
- FIRST: Fraser, Mission to Hope (line 2568, unsure): it is the one open answer live this month (hatchery coho
  Nov 7-30, chum Nov 3-30); 25 of its 62 sections are side channels and Maria Slough (your "Add all five").
- Saves I wrote before line 2770 that are in force: 2672 (Stave, unsure); 2730, 2731 (Bella Coola, verified);
  2748, 2749 (Thompson Deadman and Ashcroft closed areas, verified); Region 6: 2728, 2732 to 2736 (unsure), 2738,
  2739 (verified). With 2770 and 2785 that is all four closed areas standing on a "verified" I wrote.
- Low confidence by their own notes: Juniper's upper end (4 km by the words); the Shuswap stretch (2,075 m where the
  words say "one km").

### B2. More questions, from the audit (recommended answer first)
15. Regions 7 and 5A: the page header "no fishing for salmon until further notice" overrides the dated openings in
    the table (your Oct 31 ruling "the header closure wins"). Nechako, Quesnel River and Quesnel Lake Horsefly Bay
    would read closed next Aug 22 when DFO opens sockeye and pink; DFO's page also says "except for the
    opportunities listed in the table below". A dated row with a fishery notice answers on its dates, the header
    every other day? Recommended: yes.
16. Atnarko below the Tweedsmuir Park Lodge sign (line 2584, unsure): the coho opening also covers 17 side-stream
    sections. Side streams off for this stretch? Recommended: yes (errs closed). Note your earlier "no it needs to
    include tributaries" was about the Atnarko as a whole.
17. Lower Shuswap: the cut is the Mara Bridge; DFO says "signs above Mara Bridge". Are the signs at the bridge? If
    not, a cut at the signs.
18. A place whose last save is "wrong" with no fix: you said "I want you to for it to be redrawn… Just be safe". Not
    built: such places still ship their older drawing (the Region 3 Fraser mainstem; Sproat and Stamp Falls end on a
    "wrong" save though their fixes are in). Build: the release lists them and ships DFO's words undrawn until
    re-saved? Recommended: yes.
19. The app chat's merge question (never shown to you): may the finished app work merge into the shared branch now
    (T759 banner and stripe, T760 stuck download, T756, T746, T738, T741, T732 backup, T739 push, the iPhone
    branches)? Your words on Nov 15 10:33: "ios work could merge when ever I think?" Recommended: yes, one at a time
    with its checks.
20. T759 stripe: legend word "Check rule" OK, and strong enough on the dark blue line? Recommended: yes to both.
21. T735 dark-mode highlight: pick C (dark edge + soft glow)? Recommended: C.
22. Play "Data safety" location answer: approximate or precise? (For the App Store you said "we want precise".)
23. Production backup bucket name `bc-fishing-regulations-backup` (Q172)? Recommended: yes.
24. T686 data shrink: option (a), about 1.94 MB, lossless? Recommended: (a).
25. T732 backup: only with a screen lock? [no]; photo copy size [1600 px]. Recommended: as bracketed.
26. T710: the side channel named "Exchamsiks River confluence up to Skeena River Ecological Reserve boundary" reads
    backwards. Reword?
27. Shuswap closed stretch: tell DFO their two printed points are 2 km apart where the words say one km?
    Recommended: yes.
28. Should the pre-build check read the committed proposals file, not your working copy (it stopped a Play build
    once)? Recommended: yes.
Note on question 10 (Gitnadoix powerline cut): it REVERSES your Nov 15 "Whole river (Recommended)" for the Gitnadoix.

### E. From the audit: recorded wrong or lost (I fix the records and ticket these; listed so you know)
- T754 one-bank places: you answered "Build it this way (Recommended)"; the ticket still said "options for the owner".
- T759: the shared ticket still described the design you rejected ("reads not known"); the banner + stripe design
  you chose is on unmerged branches.
- The About-page sentence "not affiliated with the government": listed as built, is on no branch.
- Seven wording findings from the T462 audit had no ticket (K6, K7, K8, G8, G9, G10, D6); your gate 1 for closed
  testing needs each fixed or ticketed.
- Cut de-duplication: the data was cleaned once; the suggester still proposes a new cut where one exists.
- Review app: when Confirm is greyed, name the unticked row (you got stuck three times).
- Hope gauge: 66 m reads the wrong gauge; three gauge cuts never placed.
- 14 of your Nov 13 to 15 readings are not yet in the rulings file.
- Out-of-date production commands at two places in this file; a fresh block is owed.
- The approved push privacy line is written nowhere yet.

### F. PRODUCTION: what stands between staging and production data (the audit's list, in order)
BLOCKER FIRST: the production upload refuses r-46e4355a7277 (four closed areas) until the PRODUCTION feed server
serves closed areas. That server code (T757, T758) is only on the app chat's side branch; the production checklist
puts the feed server after the data, which is wrong for this release.
1. You: check staging, re-tick list A, answer the label questions.
2. Decide whether one more staging release is needed.
3. Claude: merge the T757 / T758 feed-server halves into the shared branch (needs question 19).
4. Claude: fresh production commands and a clean site copy (T404). 5. Claude: rehearse the production scripts.
6. You: backup bucket (Q172), mail routing, run token. 7. You: data worker.
8. You: feed server, BEFORE or WITH the data. 9. You: the production release command.
10. Site: Claude builds, you deploy; robots settings; www redirect (T240).
11. You: smoke check and the privacy page. 12. You: Search Console; retire v1.

### G. PLAY CLOSED TESTING (after production)
1. 0.7.11 on Play internal; your ten-step walk and the open phone checks (T749).
2. The "before closed testing" tickets: T759, T735, T716, T740, T708, T751, T608.
3. Gate 1: the seven T462 findings and this audit's findings, with your OK.
4. You start `/code-review ultra` (T463). 5. Store listing: texts, the 8 pictures re-shot, the preview clip, the
non-affiliation sentence (T244, T371, T319, T709). 6. You: Play forms, including the location answer (T371).
7. You: production app build, install, promote to Closed. 8. You: tester list; 12 testers for 14 days.
9. Claude: the in-app update prompt checked (T390, stalled since Nov 6) and T514.

### H. Question 14 again, with the audit's numbers (fewer, later checks)
Last night's staging release ran 105 minutes over nine attempts; about 60 of those minutes repeated work that told
nothing new (the same 6,490 tests ran in place and again on the clean copy; the clean copy reran after commits that
could not affect it; the release's own commit rebuilt the data for 9 minutes). Across the night those tests ran 16
times on this Mac. A rehearsal command exists that lists every stop in one pass (the comparison to read, stale test
fixtures, changed pages, failing app tests); I did not run it, and it would likely have made it four or five attempts.
The proposal, for your yes or no (nothing is changed until you say):
- While working: the one test file touched, plus the quick checks (about a minute).
- Before a push: one command that runs exactly what GitHub runs, made automatic at push time.
- In a release: build, REHEARSE on the new release and fix everything it lists, then one real run; the tests run
  once (on the clean copy), not twice; the clean copy reruns only when a commit could affect it; the upload's
  question to the feed server is asked at the start, not at the end.
- One thing ADDED: the tests that read the newly built data run once on the new release. Today most of them (1,208
  tests in 115 files) are run by nobody automatically.
- Not cut: the build refusing a piece it cannot place, the stop to read what changed, the stop on a changed page, the
  clean-copy run once per release, GitHub's runs.
- Saves about 10 minutes on a clean release; about 60 of last night's 105.
Recommended: yes.
Also from that checker, to be fixed (ticketed): the review app can crash when several pages are drawn at once (its
page pictures are freed outside the lock); test crashes on this Mac come from a map library's database handle in
child processes; four fixture states for closed river parts are still checked only for "is closed"; 5 of your 17
reviewed continuation links quietly stopped shipping and 4 new ones wait with no row for you.

### I. The fifth audit report: the app's answers against the printed rule book and DFO's pages
(`handoff/PLAN/audit-2026-11-16/answers.md`; about 98 waters, about 1,060 answers, three days each, all regions.)
No wrong number, size or date was found. Province-wide rules matched on all 18 waters with no entry of their own.
**Seven things the app allows that the sources forbid** (none is new in this release; tickets T793 to T798):
- Wild-trout limits are missing from the keep card on at least 15 lakes (Comox, Sproat, Great Central, Cultus,
  Harrison, Pitt, Chilliwack, Chehalis, Lillooet, Lois, Ruby, Windsor, Makii, Dodd): the book says "No wild rainbow
  trout over 50 cm"; the card says "any size; up to 1 over 50 cm". And the Kitimat with 23 streams, Jul to Oct: "Up to
  5 rainbow trout" without "hatchery". The data is right; the card drops it. FIRST TO FIX [T793].
- The Peace River just below W.A.C. Bennett Dam reads open; the book closes it (the rule sits on Dinosaur Lake only)
  [T794].
- Williston Lake's "No rainbow trout under 30 cm" is missing on Nation Arm and Davis Bay [T794].
- "No angling from powered boats" is never shown (Atnarko / Bella Coola, Horsefly, Chilcotin, Chilko, Clearwater,
  Nicomekl, Serpentine) [T797].
- A stretch the book closes can still show an open salmon card (Skeena Ecological Reserve, the Atnarko's closed
  stretch, the Cowichan below Mile 66) [T796]. Question 29.
- A DFO "Finfish closure" closes only salmon; trout stay open (Stamp, Somass, Sproat, Nitinat, Tahltan) [T796].
  Question 30.
- Coal Creek above the old railway bridge asks for no Classified Waters Licence [T798]. Question 31.
**Things the app forbids that the sources allow:** Fraser Lake, Kotcho Lake and Cold Fish Lake read closed all year
because an ecological reserve touches 0.1 % of the lake [T795]; dead fin fish as sturgeon bait is not shown on the
Region 2 Fraser and lower Harrison; the Shuswap Lake pier exemptions; the Pitt Lake head caveat says "release all"
where the book says wild only [T797].
More questions (recommended answer first):
29. A stretch the rule book closes: the salmon card reads closed too, with DFO's row kept as text? Recommended: yes
    (the book, p.77, says its closures apply to salmon as well).
30. A DFO "Finfish closure" closes the whole page, trout included, on its dates? Recommended: yes.
31. Coal Creek above the old MF&M bridge: Class II when open? Recommended: yes.
32. Williston Lake's 30 cm rainbow minimum on all three Zone A parts (Nation Arm and Davis Bay too)? Recommended: yes.
33. Puntledge above Comox Lake: the book says fly fishing only above the diversion dam "including tributaries" and
    exempts the river from the summer closure; we stop both at the lake. Carry both above the lake? Recommended: yes.
34. Westwick Lakes (26 % inside an ecological reserve) and Priest Lake (59 %): open with a part-closed note, or
    closed? (Fraser, Kotcho, Cold Fish and 15 Mile Swamp, barely touched, are a plain fix: open.)
35. Skeena "Section 4" has no printed upper end; the app runs it to the river's source. Right?
36. DFO "non-retention" reads Closed (your earlier ruling; 20 rows). Keep, or show "release only"?
37. **The wild-trout fix is ready on a side branch (05:28); 22 answer keys change, nothing else.** New card lines, for
    your read: Comox, Great Central, Sproat: "Up to 4 brown, cutthroat or rainbow trout combined — no wild rainbow
    trout over 50 cm (20 in)". Chilliwack, Cultus, Harrison, Chehalis, Pitt, Lillooet: "Up to 4 brook, brown,
    cutthroat or rainbow trout combined — no wild brown, cutthroat or rainbow trout over 50 cm (20 in)". Dodd, Lois,
    Ruby, Windsor, Khartoum, Horseshoe, Nanton, Ireland: "… combined — up to 2 wild brook, brown, cutthroat or rainbow
    trout combined 40 cm (16 in) or less; no wild … over 40 cm (16 in)". Kitimat and 23 streams, Jul 1 to Oct 31: "Up
    to 5 rainbow trout — no wild rainbow trout under 30 cm (12 in); up to 1 wild rainbow trout 30–50 cm (12–20 in)".
    They are long but true. Go as worded (recommended), or tell me how to shorten? On your go: merge, rebuild the
    answers, a staging release [T793].
38. **Dodd, Lois, Khartoum, Horseshoe, Nanton, Ireland, Windsor lakes: does "no wild trout over 40 cm" cover char?**
    The book prints "Wild trout/char daily quota = 2" and then "no wild trout over 40 cm" (Lois and Khartoum: "40 cm
    or more"). We read the second line as trout AND char, so a wild Dolly Varden/bull trout or lake trout of 60 cm or
    more cannot be kept there (the card will say so once the fix lands). Your Region 6 ruling reads "trout" as NOT
    char. Trout only here too (a wild char over 60 cm may be kept inside the 2), or trout and char as now (stricter)?
    Recommended: keep trout and char until you say otherwise (it errs closed). Checked: the four lakes the audit had
    not listed (Khartoum, Horseshoe, Nanton, Ireland) carry the same printed lines as Dodd, so their new card line is
    right.
