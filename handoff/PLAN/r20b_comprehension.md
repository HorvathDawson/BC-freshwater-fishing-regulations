# Round 20b — comprehension gate ("still able to interpret the regs")

Method: the answer key is read off the DATA (the page v36 snapshot's `group.tree` / `table` cells, which the card gate proves
against the engine, `rows[].week`, `rows[].restates`, the gear `summary.parts`, the licence answers, the year strip) — not off
the page's words. A FRESH opus subagent (no DOM, no data: PNG paths only) answers the same 20 questions from the BEFORE
screens (regs v25 @ 4f078b36) and again from the AFTER screens. The after score must be >= before with no new wrong answer.

## Questions and key (source field in brackets)

| # | water / screen | question | key |
|---|---|---|---|
| 1 | Vedder Oct 10 (cards) | Counting every lake and stream in Region 2, how many trout and char may ONE angler keep in a day? | 4 [tree root n=4 "4 a day per angler across Region 2, lakes included"] |
| 2 | Vedder Oct 10 | May I keep 4 hatchery rainbow trout of 45 cm from the Vedder today? What else may I keep after that? | Yes, 4; nothing more that day [own tier n=4; worked "If you keep 4 hatchery rainbow trout: that’s your trout and char for the day, everywhere."] |
| 3 | Vedder Oct 10 | I have kept 2 hatchery rainbow trout (45 cm). May I also keep 2 hatchery cutthroat trout of 35 cm from the Vedder? | Yes (2 of the 4 may be other fish from the stream; CT 30–50 cm cell n=2) [share n=2 not counting RB; worked "If you keep 2 …: up to 2 more from here"] |
| 4 | Vedder Oct 10 | A 40 cm cutthroat trout with its adipose fin intact (wild) — may I keep it? | No, release it (hatchery only; wild trout and char go back) [origin] |
| 5 | Vedder Oct 10 | I kept a 65 cm hatchery steelhead. May I also keep a 55 cm hatchery brown trout today? | Yes — 2 fish over 50 cm when one is a hatchery steelhead; GB over 50 cell n=1 [size tier n=1 extra 1 ST; caps "Up to 2 trout and char over 50 cm … across Region 2"] |
| 6 | Vedder Jun 15 (keep) | Is the Vedder open to fishing on June 15? If not, when does it reopen? | Closed; opens Jul 1 [strip runs closed Jun 1–30; "No fishing here until Jul 1"] |
| 7 | Chilliwack part 2 Oct 10 | A 40 cm hatchery rainbow trout — may I keep it, and how many such a day? | Yes; up to 2 (30–50 cm) [RB kind n=2, detail 30–50 cm] |
| 8 | Chilliwack part 2 Oct 10 | A 55 cm hatchery bull trout — may I keep it? | No — bull trout must be 60 cm (24 in) or longer [DV cell under 60 → 0 "too small"] |
| 9 | Bella Coola May 1 mainstem | I kept a 30 cm cutthroat trout. May I keep a 30 cm rainbow trout as well? | No — 1 cutthroat or rainbow trout in all [count tier n=1] |
| 10 | Bella Coola May 1 mainstem | How many 30 cm bull trout (Dolly Varden) may I keep today? | 1 (of the river's 2) [DV kind n=1, 25 cm or longer] |
| 11 | Bella Coola Oct 10 mainstem | May I fish for steelhead if I release them? | No — no fishing for steelhead, not even catch and release [not_kept ST status closed] |
| 12 | Kootenay LWA Oct 10 (Saturday) + Oct 12 (Monday) | How many kokanee may I keep on Saturday Oct 10? And on Monday Oct 12? | Saturday 5; Monday none (release) [week: Sat–Sun keep 5, Mon–Fri release] |
| 13 | Kootenay LWA | If I keep kokanee from both the Lower and the Upper West Arm on one day, what is the most in all? | 5 together [restates "Kokanee from the Lower and Upper West Arms together: no more than 5 a day."] |
| 14 | Kootenay LWA Oct 10 | Trout: I have kept 1 rainbow trout and 1 bull trout. May I keep a second bull trout? | No — 2 trout/char a day in all, and only 1 may be bull trout [root n=2; DV kind n=1] |
| 15 | Bennett Lake Oct 10 | How many lake trout a day, and may I keep one of 70 cm? | 2; no — 60 cm or less, or 90 cm or longer (70 cm is a protected size) [own tier n=2; LT cells: over 60 under 90 → 0 "protected size"] |
| 16 | Ape Creek May 1 | Whose rule sets the daily trout limit here, and what is it? May I keep a cutthroat trout? | The Bella Coola River's rule (its row covers its tributaries): 1 a day; cutthroat — no, release [root whose/why_here; CT n=0] |
| 17 | Glenn Creek Jun 15 | How many rainbow trout a day? How many OTHER trout (brook, brown, cutthroat, lake) from this stream? | 5 rainbow (the Duncan River's rule, any size); 2 others, all inside 5 a day [own n=5; share n=2 "not counting rainbow trout"; root 5] |
| 18 | Vedder Oct 10 gear | How many lines may I fish, and what hook? | 1 line; a single barbless hook (one hook, lure or fly); weight on the line up to 1 kg (2.2 lb) [gear parts] |
| 19 | Kootenay LWA Oct 10 gear | How many lines may I fish when alone in a boat? | 2 (1 line; 2 while alone in a boat) [lines "1 line (rod); 2 while alone in a boat"] |
| 20 | Vedder licence (16+ and under 16) | Does a 14-year-old B.C. resident need a licence here? Does a 30-year-old B.C. resident, and what does it cost a year? | 14: no licence, own limit; 30: basic angling licence $41.15 a year (+ steelhead stamp $28.57 only if fishing for steelhead) [licence adult / u16_res] |

Scoring: 1 = right, ½ = right on the main point but wrong or missing on a second part, 0 = wrong.

## BEFORE (regs v25 @ 4f078b36) — screens in scratchpad r20b/before/

Fresh opus subagent, 15 PNGs (the 14 listed + the kokanee sheet ur2-kootenay-kokanee-sheet-full.png, added after its first pass
answered Q13 "not shown": the combined limit lives in the kokanee row's sheet, not on the card). **Score 20 / 20.**

| # | answer given (abridged) | score | conf |
|---|---|---|---|
| 1 | 4 a day per angler across Region 2, lakes included | 1 | 5 |
| 2 | Yes, 4 (50 cm or less); then no more trout or char anywhere today | 1 | 5 |
| 3 | Yes — the cutthroat count toward "Only 2 of the 4 … not counting hatchery rainbow trout"; 2 + 2 = 4 | 1 | 4 |
| 4 | No — hatchery only, wild go back | 1 | 5 |
| 5 | Yes — "2 if one is a hatchery steelhead"; after that the stream's 2 are used | 1 | 3 |
| 6 | Closed Jun 1–30; opens Jul 1 | 1 | 5 |
| 7 | Yes, up to 2, 30–50 cm | 1 | 4 |
| 8 | No — 60 cm or longer | 1 | 5 |
| 9 | No — 1 cutthroat or rainbow in all | 1 | 5 |
| 10 | 1, 25 cm or longer | 1 | 5 |
| 11 | No — not even catch and release | 1 | 5 |
| 12 | Sat 5; Mon release | 1 | 5 |
| 13 | first pass "not shown" (no sheet screen); with the sheet: 5 together, weekends only | 1 | 5 |
| 14 | No — only 1 of the 2 may be bull trout, and the 2 are used | 1 | 5 |
| 15 | 2; no — 60 or less or 90 or longer | 1 | 5 |
| 16 | The Bella Coola River's rule (its row covers its tributaries): 1 a day; cutthroat release | 1 | 5 |
| 17 | 5 rainbow (Duncan River's rule); 2 brook/brown/cutthroat/lake from this stream | 1 | 4 |
| 18 | 1 line; one hook, lure or fly; single barbless hook | 1 | 5 |
| 19 | 2 (1 line; 2 while alone in a boat) | 1 | 5 |
| 20 | 14: no licence; 30: basic angling licence $41.15 a year (+ stamps only if …) | 1 | 5 |

What confused it (its words, abridged): whether the second fish over 50 cm must also be a steelhead (Q5, conf 3); whether 2
steelhead use up the whole stream share; **"This river’s rule: up to 4 a day … Replaces Region 2’s 2-from-streams limit" next
to "Only 2 of the 4 may be from this stream" — which 2-from-streams limit was replaced?** (→ cut C2); the Chilliwack's rainbow
tier outside the box but inside the share's bracket; Bella Coola's "(part of the 2)" under "(part of the 5)"; Bennett's slot
size and the "no more than 3 counting any …" line said on two rows; Glenn Creek's two parent waters; the Lower West Arm card
not naming the combined limit (it is in the kokanee sheet).

## AFTER (regs v26, rows 22) — screens in scratchpad r20b/after/

A second fresh opus subagent, 17 PNGs (the BEFORE set with the Gear tab and the new Licence tab shots for the Vedder and the
Kootenay; the kokanee sheet included from the start), the 20 questions plus Q21 on the split tabs. **Score 21 / 21 — no new
wrong answer; after >= before.**

| # | answer given (abridged) | score | conf |
|---|---|---|---|
| 1 | 4 a day per angler across Region 2, lakes included | 1 | 5 |
| 2 | Yes, 4 (50 cm or less); then no more trout or char anywhere today | 1 | 4 |
| 3 | Yes — "Only 2 of the 4 … not counting hatchery rainbow trout"; 4 in all | 1 | 4 |
| 4 | No — hatchery only | 1 | 5 |
| 5 | Yes — "2 if one is a hatchery steelhead" | 1 | 3 |
| 6 | Closed Jun 1–30; opens Jul 1 | 1 | 5 |
| 7 | Yes, 2 a day, 30–50 cm | 1 | 4 |
| 8 | No — 60 cm or longer | 1 | 5 |
| 9 | No — 1 cutthroat or rainbow in all | 1 | 5 |
| 10 | 1, 25 cm or longer | 1 | 5 |
| 11 | No — not even catch and release | 1 | 5 |
| 12 | Sat 5; Mon release | 1 | 5 |
| 13 | 5 together | 1 | 5 |
| 14 | No — only 1 of the 2 may be bull trout; the 2 used | 1 | 5 |
| 15 | 2 (part of Region 6's 5); 70 cm no | 1 | 5 |
| 16 | The Bella Coola River's rule (its row covers its tributaries): 1 a day; cutthroat release | 1 | 5 |
| 17 | 5 rainbow (Duncan River's rule); 2 of the 5 may be brook/brown/cutthroat/lake from this stream | 1 | 3 |
| 18 | 1 line; one hook, lure or fly; single barbless hook | 1 | 5 |
| 19 | 2 (1 line; 2 while alone in a boat) | 1 | 5 |
| 20 | 14: no licence; 30: basic angling licence $41.15 a year (+ stamps only if …) | 1 | 5 |
| 21 | Tabs: What you can keep · Gear · Licence; prices on the Licence tab; the Kootenay lists no steelhead stamp | 1 | 5 |

What confused it (abridged): Glenn Creek — the share "Only 2 of the 5 may be brook, brown, cutthroat or lake trout from this
stream" beside How it adds up's "If you keep 2 rainbow trout: none from other streams today — up to 3 more from here or from
lakes" read as a contradiction (it is the Q23b ruling: the lift holds on this water only, so rainbows kept here count toward
the 2-from-streams on OTHER streams; gate-proven; both lines were on the BEFORE screen too, where Q17 scored 1 at conf 4 —
logged for the owner, not a cut's effect); the Vedder's steelhead size note still takes passes (unchanged words); the river's
own rainbow tier sitting outside the "Only 2 of the 4" share (the header's 4 and How it adds up say they count toward the 4);
Bennett's "no more than 3 counting any …" (unchanged); Bella Coola's nested "(part of the 2)" (unchanged); the kokanee row's
weekday words in small grey (unchanged).

Verdict: every cut kept, none reverted.
