# T793 — Wild-trout limits are missing from the keep card on at least 15 lakes and on the Kitimat and 23 streams under it (the app allows what the book forbids)

Status: DONE — da961f55c. A kind keeping both origins says where the wild fish keeps less on the card (Kitimat wild 1, Comox, Dodd and the wild-2 lakes); build stop `tree.origin_unsaid`; owner OK'd the sentences 2026-11-16.

Closed by the 2026-11-20 audit: the branch fix (f672f4444) landed on redesign/stream-sections as da961f55c (same change, same 22 keys). The owner answered question 37 "Go as worded (Recommended)" and question 38 "Trout and char (Recommended)". The Lois/Khartoum double-count went to T805 part C (0f6fe2868). The check carried into the T810 trout table (trout/check.origin_problems). Released from the 0.8.0 data release on.

Source: general audit 2026-11-16, answers checker A1 and A2 (`handoff/PLAN/audit-2026-11-16/answers.md`); the same fault as T782 item 1.

## What is wrong
The book: Comox Lake "No wild rainbow trout over 50 cm" (p.15); Sproat, Great Central, Makii; Cultus, Harrison (p.23),
Pitt (p.25) "No wild trout over 50 cm"; Chehalis, Chilliwack, Lillooet, Lois, Ruby, Windsor; Dodd Lake "Wild
trout/char daily quota = 2 (no wild trout over 40 cm)". The card says "Up to 4 … any size; up to 1 over 50 cm":
"wild" is nowhere in the keep section. Kitimat River and 23 streams, Jul 1 to Oct 31: "Up to 5 rainbow trout" without
"hatchery" (p.51: hatchery rainbow 5; wild rainbow are 1 from streams, none under 30 cm); on Jan 15 the same card
says "hatchery".
The parse and the stored row are right (`comox_lake.r1`: RB, `lengths [{min_cm 50, take 0}]`, `origin: wild`; the
row's `conds` and `caps` hold it); `group.buckets`, which the card draws, holds only "any size" and "only 1 over
50 cm". It shows only under "See every regulation line".

## What to build
- The origin condition (wild / hatchery) reaches the card's tree wherever a stored row has one; a build check: a row
  with an origin condition whose card tree says neither "wild" nor "hatchery" stops the answers build.
- The sweep found 14 lakes plus Dodd by hand and the Kitimat set; it is a floor (it misses cards that say
  "hatchery" for another fish). List every answer key the fix changes; that list is the review.

## Lane result (05:28, 2026-11-16): fixed on `data/t793-wild-limits-on-card`, commit f672f4444 (based on 0d61db3c5)
- Where it was lost: the pipeline's `group.tree` (the tree the card draws), not the app. `card.py` named an origin on a
  kind only when exactly ONE origin keeps; when both keep with different limits the kind line said nothing. The
  Kitimat rows have no group of their own and are drawn through their parent row's group.
- Fix: `pipeline/deliver/answers/tree.py` (`origin_clauses`, `tree_of`: a kind that keeps both origins gets the clauses
  on its line and is never folded; `origin_unsaid` is the build check), `card.py`, `card_check.py` (each clause proved
  against the engine), `answers.py` (the build stops on a keep row with an origin condition whose drawn tree says
  neither "wild" nor "hatchery"). No app change; format version unchanged. Mini release re-cut in the same commit.
- Tests: 6 new (whole sentences, on data the tests build); 535 answers tests and 45 bundle-backed tests pass.
- Proof rebuild into scratch: ladder, gear, licence and display sections identical; 22 answer keys change, every
  change "an origin condition is now said", 0 unexplained.

| Waters | Before | After |
|---|---|---|
| Kitimat River (4 parts) and the 23 streams under it, Jul 1 to Oct 31 | "Up to 5 rainbow trout" | "Up to 5 rainbow trout — no wild rainbow trout under 30 cm (12 in); up to 1 wild rainbow trout 30–50 cm (12–20 in)" |
| Comox, Great Central, Sproat | "Up to 4 brown, cutthroat or rainbow trout combined" | "… combined — no wild rainbow trout over 50 cm (20 in)" |
| Makii | "Up to 4 brown or rainbow trout combined" | "… combined — no wild rainbow trout over 50 cm (20 in)" |
| Chilliwack, Cultus, Harrison, Chehalis, Pitt, Lillooet | "Up to 4 brook, brown, cutthroat or rainbow trout combined" | "… combined — no wild brown, cutthroat or rainbow trout over 50 cm (20 in)" |
| Dodd, Lois, Ruby, Windsor, Khartoum, Horseshoe, Nanton, Ireland | "Up to 4 brook, brown, cutthroat or rainbow trout combined" | "… combined — up to 2 wild brook, brown, cutthroat or rainbow trout combined 40 cm (16 in) or less; no wild brook, brown, cutthroat or rainbow trout over 40 cm (16 in)" |
| the same set, other fish groupings (5 keys) | "Up to 6 rainbow trout"; "Up to 1 Dolly Varden/bull trout or lake trout combined"; … | "… — up to 2 wild rainbow trout under 40 cm (16 in); no wild rainbow trout 40 cm (16 in) or longer"; "… combined — no wild Dolly Varden/bull trout or lake trout 60 cm (24 in) or longer"; … |

- To check before the merge: Khartoum, Horseshoe, Nanton and Ireland lakes were not in the audit's list (read their
  rows in the book); the Dolly Varden / lake trout line comes from `dodd_lake.r2` parsed as all trout and char where
  the book says "no wild trout over 40 cm" (a parse question); the sentences have had no owner read.

- Checked 2026-11-16 (main chat, on the parsed quotes, not the page images): Khartoum, Horseshoe, Nanton and Ireland
  lakes carry "Wild trout/char daily quota = 2" and "no wild trout over 40 cm" (Khartoum and Lois: "40 cm or more"),
  the same lines as Dodd: their new card line is right. The Dolly Varden / lake trout line follows from reading "no
  wild trout over 40 cm" as trout AND char: owner question 38 (his Region 6 ruling C2 / C4 reads "trout" as not char).

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 37: "Go as worded (Recommended)". Question 38 (does "no wild trout over 40 cm" cover char): "Trout and char (Recommended)". Merge f672f4444, deliver, rehearse, staging release. Known after the adversarial trout review: on Lois and Khartoum lakes the new sentences read 2 wild + 2 wild where the book says 2 in all (T805 item 1); tell him plainly with the changed-answers list.
