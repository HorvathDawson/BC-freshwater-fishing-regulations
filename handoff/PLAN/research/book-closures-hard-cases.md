# Book closures for salmon: hard cases and regression list

The open ticket T827 re-runs this list against the DFO pages. It is the conclusions of the adversarial review of the 71
rule-book closure rows (candidate release r-685b700b2802 against the live r-46e4355a7277, the salmon step with
`with_book_closures`). The review's scripts and outputs were scratch and are not kept: re-point any re-check at the
release being checked. Ownership: W2 is the app's (core `bookToday`, `salmon.ts`); the rest are the data side's.

Result then: of 71 rows, 52 pass, 8 wrong (W1, W2), 11 suspect (S1 to S4). 71,046 sections changed salmon status and every
changed species-day was answered by a book place; card, map and year bar agreed on every day of the year, offline and
under the live feed. Non-book days matched the live release exactly.

## Wrong

- **W1. Campbell River below the Quinsam (rows 45, 56; sections 58487 to 58489, DFO places
  `1:campbell-river:downstream-of-the-confluence-with` and `~2`).** The app closes pink and chinook Jul 15 to Aug 31
  under the Region 1 "Summer closure" (MUs 1-1 to 1-6). The book lists the river as "CAMPBELL RIVER 1-10" (p.14); the
  river is the boundary of 1-6 and 1-10 (`mu_set` "1-10,1-6"), the Quinsam (1-6) prints "Exempt from July 15-Aug 31
  summer closure" and the Campbell does not, and the Campbell's own "No Fishing ... Maple Street ... Aug 1-Oct 31"
  would mean nothing in August under a whole-river closure. Fix direction: a water whose table row names its MU follows
  that MU, not a boundary-straddling `mu_set` (owner call). The trout card has the same reading.
- **W2. An all-year book closure loses its dates when another book place answers on some days** (rows 15, 22, 37, 46, 52,
  58; for example the Stikine reserve chain 881, Statlu Creek chain 568, Morice reserve chain 948). `bookToday` adds "(all
  year)" only when the place answers on every day, and the pipeline verdict for an all-year rule has no `when`. Fix
  direction: decide "all year" from the place's own year (`payload.static[lid]`, every day at least 0), not from the
  answer. Expect 0 "book words carry no dates" after the fix.

## Suspect

- **S1. Thompson River below Kamloops Lake (rows 33, 34, 70; chains 528, 691, 693).** The Region 3 spring closure
  (Jan 1 to Jun 30) applies in June, where DFO said "check DFO". The book's row (p.32, "No Fishing Oct 1-May 31",
  "Additional opening ... May 1-31") prints no "Exempt from spring closure" while the upstream row does. Ambiguous:
  owner's call. On chain 693 the bracket dates contradict the line's printed dates.
- **S2. The line is cut at the tributary mark (rows 04, 26, 27).** The bundle verbatim stops at the star, so the book's
  dates sit only in the brackets (Iskut p.51, Deena Creek p.15; Englishman River r3 shows the same).
- **S3. Ecological reserves close a whole section when part of it is inside** (Skeena rows 41, 46, 13, 15): section
  765427 is 7 % inside, 1912509 (a 65 m line inside a lake) 0 %, 765104 22 %. Most sections are at least 90 % inside.
- **S4. A hard line break inside the book's line (rows 44, 62, 69)**: the verbatim keeps the PDF newline and the card
  breaks mid-sentence (Quinsam r1, r2, Tahltan r1). Cosmetic.

## Outside the 71 (words only)

- **X1. The region-wide DFO closure loses its card shape (320,369 sections in Regions 4, 5 and 7).** A header-closed
  chain that gains a book place drops the header and recomputes, so a closed year bar "Salmon (DFO and rule book)" appears
  where the earlier ruling kept it off a region-wide closure.
- **X2. "Freshwater fishing is prohibited in National Parks unless opened under the National Parks Fishing Regulations"
  is carried as a full closure (10,188 sections)**, overstating a conditional rule. Salmon stay closed either way.
- **X3. Wording fragments:** Buttle Lake tributaries r2 ("except Thelwood Creek is closed all year") and Nahatlatch r2
  quote the book's garbled text.
- **X4. Missed closures:** none found among the 543 carried rules; the Region 2 Campbell "No Fishing from 21:00 to 05:00,
  Aug 1-Dec 31" is correctly not carried (never the whole day).

## Regression cases that pass (keep)

- Nass side streams above the Kitsault Bridge (p.49): coho Jan 1 to Jun 15, 25,937 sections, only streams (the Nass
  mainstem is exempt); the Skeena mainstem above Cedarvale is Jan 1 to May 31; the "Open June 16-Apr 30" lift prints
  brackets that differ from the line.
- The tributary star: every trib-bound row was checked against the page image (Iskut, Klinaklini, Tahltan, Bella Coola,
  Dean, Ksi X'anmas, Tatsatua, Yakoun, Statlu, Hunlen, Mamin, Deena, Brothers, Hays, Fergus); creeks that join inside the
  closed stretch close with it.
- Park and reserve rows (05, 06, 13, 17, 19, 30, 57, 71) include lakes and wetlands correctly. Cut positions on a map and
  lake polygons inside reserves were not checked.
