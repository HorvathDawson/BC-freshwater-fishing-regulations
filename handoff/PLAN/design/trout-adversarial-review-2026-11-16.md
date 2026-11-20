# Trout table (round 6), adversarial review against release r-46e4355a7277

Asked by the owner 2026-11-16 about 10:19: "also as a seperate thing can we get some final reviewing of the trout rework
we want the agent to have an adverserial attempt to trry and see if any regulations can not be represented." A fresh
Fable reviewer (not the designer), read-only. Saved by the main chat from the reviewer's hand-back. Mockups only:
nothing is built. Evidence: the session scratch folder, under `trout-adversarial/` (`enum.ts`, `enum.json`: every
stored trout row; `rules.json`: every rule with its quote and page; `classify.py`, `lib.py`).

## Summary
1. **Verdict: no, not yet.** The table's boxes carry almost every rule shape, but the design cannot truthfully carry
   every trout rule: three things mislead and one has no place at all.
2. **Enumerated [ran]:** all 2,105 answer keys of shown waters, at every season change (8,458 key-and-season answers,
   2,741 distinct stored trout rows). That gives 414 distinct daily trout answers (394 with something to keep) and 491
   distinct year-round rule sets, grouped into 12 structural classes.
3. **Fit cleanly:** 7 of 12 classes, the bulk of the province (about 17,650 of 20,354 water-and-rule-set pairs have
   one box with at most one shared inner box).
4. **Mislead:** 3 classes: the steelhead "stop fishing" duty (2,957 waters), a wild limit shared across two boxes
   (2 lakes), and overlapping groups as the mock draws them (2 waters). A fourth, milder: the top line understates on
   89 waters.
5. **Cannot be shown:** 1 class, possession limits (at least 640 waters with their own printed possession number,
   plus all of Region 7 for lake trout and bull trout). The year bar's admitted limit touches 10,250 waters, not two.

Notes: the file `trout-display.stripped.html` is round 1 (Nov 15 22:33); round 6 without pictures is `stripped.html`
in the same folder, and the design was read from that and the designer's notes. Water counts count a water once per
rule set (19,763 shown waters; 410 have stretches with different rule sets).

## Findings, most serious first

### 1. Possession limits have no place in the design (cannot be shown; misleads by omission)
- Water: Buckinghorse River `gnis:10118` part 0 (same for 579 Liard watershed waters).
- Stored [ran]: child row `members ["DV"], daily 1, bands [[30,50,1]]`, with `{"t":"possession_cap","r":2616}`.
- Printed [read, extracted text], p.65: "Bull trout daily and possession quotas = 1 (none under 30 cm or over 50 cm)
  from all lakes and streams, Oct 16-Aug 14".
- Design shows [read]: "Dolly Varden/bull trout · 30–50 cm (12–20 in) · only 1". Possession appears only inside the
  Sources sheet as the region's "Possession: twice the daily quota", and the fish sheet filters possession lines out.
- Wrong conclusion: on day two of a trip the angler keeps a second bull trout. The book allows one in possession.
- Also: Bowron Lake Park waters (48; "Trout/char daily and possession quotas = 1", p.43), Tagish Lake
  `wbk:328961735` ("Lake trout daily and possession quotas = 2", p.53), Teslin, Morley, Bennett, Cheslatta, Murray,
  Whale, Monkman, Lower Blue, Chunamun, Rainbow Lakes and more: 29 stored possession rules [ran].
- Smallest fix: a new line in the fish sheet's "Here, today" ("Only 1 in your possession, not two days' worth") and a
  short tag on the row. The data already stores it per fish.

### 2. "Stop fishing this water" is hidden under "Licence rules" (misleads)
- Water: Andi Creek `gnis:10031` part 0 from Jul 1 (Skeena-type). 241 answer keys, 2,957 waters [ran].
- Stored: steelhead kind `n 1, duties ["10 a year","stop fishing this stream after 1 a day","record each on your
  licence right away"]`.
- Printed p.49: "When you have caught and retained your daily quota of hatchery steelhead from any water, you must
  stop fishing that water for the remainder of that day".
- Design shows: "Up to 5 today", steelhead inside "only 1 of these 4", and a "Licence rules ›" link. The duty sits in
  the sheet under that heading.
- Wrong conclusion: keep the steelhead in the morning, then carry on fishing for the other four.
- Smallest fix: put "then stop fishing here today" on the steelhead row itself (round 1 had it) and take it out of
  "Licence rules".

### 3. A wild limit shared across two boxes (misleads)
- Waters: Lois Lake `wbk:329197058`, Khartoum Lake `wbk:329197063`.
- Stored [ran]: rule `lois_lake.r1` sits separately on two rows:
  - row A (brook, brown, cutthroat, daily 4): `{"c":"origin2","o":"wild","daily":2,"max":40,"who":["CT","EB","GB"]}`
  - row B (rainbow and steelhead, daily 6, counted apart): `{"c":"origin2","o":"wild","daily":2,"max":40,"who":["RB"]}`
- Printed p.24: "Wild trout/char daily quota = 2 (no wild trout 40 cm or more), hatchery rainbow trout = 6 / Rainbow
  trout/hatchery steelhead quota = 6 in the aggregate".
- Design shows: two columns (Keep 6, Keep 4), with "only 2" on the wild rainbow and "only 2 of these 3" on the other
  wild fish. A white box cannot span columns.
- Wrong conclusion: 4 wild fish. The book says 2.
- Smallest fix: the existing "All together" last line: "only 2 wild fish, each under 40 cm". The data should store the
  wild quota once across rows; **T793's new sentences have the same split** (main chat: check before T793 ships).

### 4. Overlapping groups: a fish in two shared limits (misleads as the mock draws it; fixable)
- Waters: Kamloops Lake `wbk:329563838` from Feb 1 to Oct 14; Adams River `gnis:39257` part 3 from Jul 1.
- Stored [ran]: tier `own n=3 "This lake's rule: up to 3 a day (part of the 5)"` holding bull trout (1, 60 cm or
  longer) and rainbow (2, 50 cm or less). Also `real_daily.shared_count [{"take":1,"members":["DV","LT"]}]`, with the
  lake trout line "no more than 1 counting any Dolly Varden/bull trout you keep".
- Printed: p.30 "Rainbow trout daily quota = 2 (none over 50 cm)", "bull trout daily quota = 1 (none under 60 cm)";
  p.28 "1 bull trout (Dolly Varden) or lake trout".
- Design shows: the mock draws Kamloops only for Jan 15, when lake trout go back, with bull trout and rainbow in a
  white box "only 3". From Feb 1 bull trout also needs the box "only 1 of these 2" with lake trout, and the grammar
  gives one white box per fish.
- Wrong conclusion if kept as drawn: 1 bull trout plus 1 lake trout.
- Smallest fix: never draw the "only 3". It is the sum of 2 and 1, not a printed limit. Then bull trout and lake trout
  share the white box and rainbow keeps "only 2" under its name. The data should mark summed tiers as derived.
- Related, already worded: Tagish (2 waters), Atlin, Laidlaw, Morley and Bennett lakes overlap a count box with the
  over-50 limit; the "lake trout not counted" wording covers them (6 waters) [ran].

### 5. Top line understates (mild; the designer's open question 1)
- Waters: 87 waters with a stored "or N" phrase, plus Kaslo and Keen.
  - Glenn Creek `gnis:10457` and 50 others: headline 2, `also ["5 if they're rainbow trout"]`.
  - Madden Creek `gnis:12076` and 29 others: the same plus 2 bull trout, so three boxes. No top-line form is defined
    for three.
  - Vedder and Stewart Slough (4); Peace River parts 5 and 6 ("3 if they're lake trout").
- Design shows: "Up to 2 today" where 5 is legal. Safe, but false as a maximum.
- Smallest fix: keep the stored second half on the top line for these waters, and define the three-box form.

### 6. The year bar's limit is the common case
- Count [ran]: on 10,250 waters (763 keys) at least one fish flips between keep and release while the water stays
  open. Mostly lake trout and bull trout dates, and Region 6 summer against winter.
- Stored: the row year is one state. Kamloops Lake's main row is `year [["keep",365]]` while lake trout go back
  Oct 15 to Jan 31.
- Only 638 waters (34 keys) have a fish with its own stored year, so "When" in the sheet works there and nowhere else.
- Effect: today's table is still right; the bar would draw a flat "keep" on over half the keepable waters.
- Fix: a per-fish year from the pipeline before any bar is drawn, or no bar where a fish flips.

### 7. T793 wild limits fit, but only with wording not yet defined
- Water: Dodd Lake `wbk:329197061`: `{"c":"origin2","o":"wild","daily":2,"max":40,"who":["CT","EB","GB","RB"]}`.
- Printed p.23: "Wild trout/char daily quota = 2 (no wild trout over 40 cm)".
- Design shows: "two rows per fish" gives 8 to 12 rows, with a white box over the four wild rows.
- Smallest fix: a new box sentence like the size one: "Of the 4, only 2 wild, each 40 cm (16 in) or less".
- Touches 18 lakes: 8 with a wild quota of 2, 6 Region 2 lakes with "no wild trout over 50 cm", 4 Region 1 lakes with
  "no wild rainbow trout over 50 cm". The Kitimat set (26 waters) is already drawn.

### 8. Rules for some anglers only
There are 34 angler-closure rules (youth or disabled accompanied waters, Skeena and Babine weekends). "Up to N today"
is false for those anglers, so it depends on the note line above the table always showing.

## What fits cleanly [ran, then inferred by rendering in the design's grammar]
- Closed or release-only all year: 535 keys, 1,541 waters.
- One box with own limits only: 849 keys, 8,610 waters.
- One box with one shared inner box, including the Skeena shape and Region 4's rainbow or cutthroat over-50 box: 649
  keys, 9,047 waters.
- Fish counted apart in two boxes (brook trout 20, Kootenay rainbow): about 1,050 waters.
- Hatchery-only streams: about 2,200 waters.
- Stepped size limits: 992 waters. Slot sizes: 5 waters.
- No keep row mixes trout with kokanee or any other fish. No trout keep rule depends on the angler. No headline
  exceeds the sum of its fish limits.

## Data gaps (the design would never be asked to show these)
- **Region 7 possession:** p.58 and p.64 print "Lake trout: possession quota = 1 daily quota" and the same for bull
  trout. The rules are parsed (`possession_exceptions.r1`, `r2`) but no trout row refers to them; possession is blank
  on about 14,200 Region 7 water entries [ran].
- **Possession with a size band:** "Lake trout possession quota = 2 (only one over 50 cm)" (Cunningham Lake, p.59) is
  parsed; not found on a displayed row.
- **Cross-border quota:** "Only one daily quota may be retained … regardless of which side of the border" (Atlin p.50,
  Bennett, and by the page text Tagish, Teslin, Laidlaw, Swift, Rancheria) is stored as an advisory, not a limit.
- **Impossible steelhead line:** Shamrock Creek `blk:356196726` stores "Up to 2 hatchery steelhead, 20–30 cm
  (8–12 in)". The book (p.24, Lonzo Creek) says "Trout/char 20-30 cm in length Jun 16-Jan 31".
- **"In the aggregate from both lakes"** (Echoes Lakes, p.36) lives only in the "fished elsewhere" sentence.
- **Region 6 winter streams:** 1,904 waters show brook trout 5 and lake trout 3 as keepable under owner rulings C2
  and C4 (audit F6). The design would say "Up to 5 today" there.

## What the reviewer could not check
- It did not render or open the design. "What the design shows" is from its stored mock data and code.
- Printed lines come from the audit's extracted page text, not page images.
- 215 of the 394 keep shapes were read by eye, which covers every shape on two or more waters. The other 179
  single-water shapes went only through automatic checks, which assume the stored tree is complete.
- One weekday per season was loaded. The 167 answer keys of unnamed waters, tidal parts, DFO and in-season notices
  were not covered.
- T793's branch was not built; its ticket sentences were used.
- Not traced: whether François Lake's outlet-area char rule is bound to the whole lake.

---

# Second attack: round 7 (2026-11-16, about 12:40)

The designer built round 7 against the report above; the owner's answers of about 12:00 shaped it (top line's second
half back, swipe-down sheet, no "often mistaken for", sources in both places). The same reviewer attacked it again.
Read from round 7's `stripped.html` (data and code) and the designer's notes; not opened in a browser.

**Verdict:** round 7 still cannot carry every trout rule truthfully, even with the T805 data gaps fixed. Three of the
eight findings are fixed, five partly, and the new grammar creates new faults. The worst: possession below the day's
number, possession with a size, and wild sentences that reach fish the book leaves out.

## The eight findings
1. **Possession: PARTLY, two new faults.** Right where there is one plain number (Buckinghorse River, Bowron River)
   [ran]. NEW: possession with a size: Tagish Lake, p.53 "Lake trout daily and possession quotas = 2 (only 1 over
   90 cm, none between 60 cm and 90 cm)"; stored `tagish_lake.r2 possession LT take 2, lengths null`; round 7 shows
   "only 2 · only 1 over 90 cm" and "only 2 in your possession": an angler reads one fish over 90 cm on each of two
   days as fine; the book allows one over 90 cm in possession (also Morley, Bennett, Cheslatta). NEW: possession
   smaller than the day's number: Cunningham Lake, lake trout daily 3 with "possession quota = 2 (only one over
   50 cm)" (p.59): "only 3" with "only 2 in your possession" under it reads keep 3 today (five lakes: Cunningham,
   Tchentlo, Tsayta, Indata, Nakinilerak). Smallest fix: when possession is below the day's number, the row's number
   is the possession number. Data: Region 7's "one day's limit" is hand-written; on Bowron park streams the stored
   possession members miss lake trout.
2. **Stop fishing: FIXED**, one wording risk: the row says "stop fishing here today"; the book says "that water" and
   the stored duty "this river"; on a river in stretches "here" can be read as this stretch. Use the stored noun.
3. **Wild limit across two boxes: PARTLY.** Lois Lake's last line "All together: only 2 wild fish, each under 40 cm
   (16 in)" matches p.24, but the top line "Up to 6 rainbow trout or steelhead and 4 other trout and char today"
   promises 10 and the limit most likely to be broken is the table's last line. The two char are drawn "clipped fin"
   on a guess: Lois stores an empty wild range for them (min 60, max 40), Khartoum stores none; on the Region 2
   reading of "trout" (Cultus, Harrison p.23 "No wild trout over 50 cm, 1 bull trout over 60 cm") a wild bull trout
   or lake trout of 60 cm or longer may be kept at Lois inside the wild 2. This is the owner's question 38 (answered
   "Trout and char (Recommended)" on 2026-11-16).
4. **A fish in two shared limits: FIXED** (Kamloops, Adams River part 3, Williston Lake B: the only three summed
   tiers in the release, all gone). A build can detect them: the tier's number differs from the "take" of its rule.
5. **Top line: PARTLY.** Glenn Creek, Vedder, Kaslo are true now. Madden Creek's three-number line is true but leads
   with the tightest number and can be read as 5 plus 2.
6. **Year bar: FIXED as a rule, two traps.** On a closed day the stored row year means something else (Kaslo and Elk
   on Apr 1 store `[keep 90, closed 75, keep 200]`: a bar built from the day's row would show January as "keep"
   where it is release). On Buckinghorse the bull trout has a bar while rainbow and lake trout flip with none: "no
   bar" reads as "no change".
7. **Wild limit inside one box: PARTLY, new fault in the defined words.** Dodd Lake's sentence matches. But the
   size-only form "a wild fish must be 50 cm (20 in) or less" (six lakes: Chilliwack, Cultus, Harrison, Chehalis,
   Pitt, Lillooet) forbids the bull trout over 60 cm the same printed line allows and reaches wild brook trout: it
   must name the fish. Ruby Lake ("Wild cutthroat trout daily quota = 2 (none 40 cm or more)") has no defined form.
8. **Angler-only rules: FIXED.**

## Fresh sweep of the 12 classes
Fit: never-keep; one box with own limits; one box with one shared inner box; two boxes counted apart; overlapping
groups; hatchery-only; stepped sizes; slot sizes; Kitimat's hatchery and wild rows; the steelhead duty.
New or remaining: possession below the day's number (5 lakes); possession with a size (Tagish x2, Morley, Bennett,
Cheslatta, the same 5 lakes); "a wild fish must be…" over-reaching (6 lakes) and char drawn "hatchery" (Lois,
Khartoum, Dodd and 4 like them); Lois and Khartoum top line of 10; a possession line repeated on several rows reads
"each" (Bowron park streams); a bar on one fish beside fish that flip without one (every water among the 638 with a
stored fish year that also has a dated put-back); "here" in the stop line.
Looked for and not found [ran]: a possession line colliding with a box sentence; an "All together" line needing a
wild limit and a size limit across boxes; a three-number top line whose third number is inside the first; a stop
duty on release-only steelhead.

## The designer's three questions, by "which choice misleads fewer anglers"
1. Madden's top line: neither wording; say "Up to 5 today" (the stored maximum) and draw one box of 5 with a white
   box "only 2 of these 4" around brook, brown, cutthroat and lake trout, and "only 2" on bull trout. (Owner the same
   hour: "Yes, that wording (Recommended)" for the three-number line.)
2. Sideways moves: the white-box group is right but misses rainbow / steelhead (one fish split at 50 cm). (Owner:
   "No sideways at all".)
3. Year bar only where stored: yes; but where a fish flips, say the date in words under the fish ("until Oct 14"):
   461 retention rules store their dates. (Owner: drop the bar except an all-fish "through the year" line; "i dont
   know why it needs year tho when it has dates".)

## What still blocks it, ranked
1. Possession below the day's number, and possession with a size (design: no rule or wording; data: the size clause
   is not on Tagish's possession rule).
2. Wild sentences must name their fish, and the char's origin must be settled (design; data: Lois, Khartoum and Dodd
   store the char three different ways for the same printed words).
3. Seasons per fish (data: no per-fish year; design: no line for "this changes on a date").
4. Lois and Khartoum top line (design). 5. Madden-type top line (design). 6. "Here" in the stop line (design).
7. Data only: wild quota stored per row, Region 7 possession on no row, Bowron park possession members missing lake
   trout, Shamrock Creek's 20–30 cm steelhead, the cross-border quota, Echoes Lakes (T805).

Not checked: rendering, swipes, text sizes; the 22 older waters beyond the round 7 patches; page images of the
printed lines; the T793 branch's output.

---

# Third attack: round 8 (2026-11-16, about 13:25)

Round 8 answered the owner's screenshot note on the Skeena card ("it is hard to tell 1 of 5 is over 50cm here applies
to all instead of just brook trout also the brook trou limit feels like the full limit makes it seem like the 5 doesnt
apply to all?"), his round 7 answers (no sideways, the bar dropped, dates in words) and the second attack.

**Verdict:** closer, still not. The frame is better and the possession, stop-line and Cultus / Ruby fixes hold. The
date words are right on 49 of 50 lines checked, but miss three kinds of change and are silent on closures. One fix
made things worse: wild char at Dodd and Lois now look keepable on their rows.

## A. The frame: PARTLY
- Works: on the Skeena the strip "Keep 5 in all / Of the 5, only 1 over 50 cm (20 in)" runs the full width with a
  closed frame round all six fish; the size sentence no longer reads as the brook trout's. Kootenay Lake's two boxes
  read as two.
- Still fails the shapes-only test: the strip and the grey cell beside the first row are the same tint with no line
  between them, so the grey makes an upside-down L hooked onto the brook trout row (the owner's second complaint).
  Smallest fix: a hairline between the strip and the rows, or a lighter tint for the "no tighter limit" cell.
- "No limit of its own" risks reading as "unlimited" (worst on Lois, Dodd, Cultus; Buckinghorse lake trout beside
  "in your possession: one day's limit"; Kitimat's hatchery rainbow beside its wild twin's "only 1"). Say it the
  positive way with the box's number ("up to the 5"); if one of the two, empty grey is safer.
- A box-wide size sentence plus a white box with its own size on one fish (Hummingbird Creek, Tagish): reads as
  nested; no fault.

## B. Sheet, year bar, date words
- Sheet (three blocks, no sideways) and the bar (Elk only, never on a closed day): FIXED.
- Date words: PARTLY. 49 of 50 lines on 21 waters match the rule's dates [ran]. One wrong by a day: Adams River part
  3, bull trout "until Jul 30, then release" (the rule starts Aug 1; a one-day season on Jul 31; 25 answer keys have
  one: a real build must read each day).
- Changes the date words MISS: (1) a fish that goes back as a "zero" line (Bella Coola part 0, Apr 15: the 2-a-day
  rule is dated Apr 1 to May 31; from Jun 1 the box is 1 and cutthroat reads "No cutthroat trout may be kept"; the
  card shows no date on cutthroat or on "Keep 2 in all"); (2) a hatchery / wild change (Kitimat and 25 waters: wild
  rainbow goes to release on Nov 1 with no date while its neighbours say "until Oct 31, then release"); (3) a number
  or size that changes with no flip (Coquitlam River part 1: 20 to 30 cm only Jul 1 to Oct 15, then 30 cm or longer:
  a 25 cm trout is legal Oct 15 and not Oct 16); (4) closures: a change to "closed" is never said on the trout card;
  now that some fish carry dates, a fish without one reads as "no change" (Andi Creek, Hummingbird, Bowron,
  Cunningham). Smallest fix: treat a closure as a date too ("until Dec 31, then closed"), once in the strip.

## C. Second-attack fixes
Cunningham: FIXED. Tagish: FIXED (the size is still not on the stored possession rule). Cultus: FIXED. Ruby: FIXED.
**Dodd and Lois: NEW FAULT**: by the owner's ruling a wild char can never be kept there; round 8 took "clipped fin"
off the two char rows and moved the consequence into the sheet, so the row reads "Dolly Varden/bull trout · 60 cm or
longer · only 1 of these 2": keep a wild 65 cm bull trout. The row needs "hatchery only" back, or "wild ones go
back" (seven lakes; the stored rows also disagree with the ruling). Lois top line: FIXED (make the two wordings
identical). Stop line: FIXED (use the stored noun: "this creek", "this slough", "this channel" exist). Bowron: FIXED.

## The designer's three questions
1. "No limit of its own" or empty grey: neither; "up to the 5"; else empty grey.
2. Cunningham, the box says 2 with the day's 3 quiet: yes.
3. Lois wild limit said twice: yes, in identical words, with the row mark for the char.

## What still blocks it, ranked
1. Design: wild char rows at Dodd, Lois and five lakes like them read as keepable.
2. Design: date words miss zero-number fish, hatchery / wild changes and number or size changes (about 31 waters),
   and closures are silent on the card.
3. Design: "no limit of its own". 4. Design: the grey L in the frame. 5. Design: eleven cards over one screen.
6. Data: per-fish dates are computed, not stored; possession is not on the fish row; Tagish's possession size is not
   on its possession rule; Dodd, Khartoum and Lois store the char three ways against one ruling; "trout" covers char
   at Dodd but not at Cultus in the same table; the T805 list.


---

# Fourth attack: round 9, by an Opus reviewer (2026-11-16, back at 15:16)

Saved by the main chat from the reviewer's hand-back. Its files: session scratch folder `trout-adversarial-4/`
(`alldays.json` every day of every answer key, `auto.py` + `check.py` the simulated date words and their check,
`an6.py` the changes with no keep / release flip, `fraser.txt` the Fraser pages from the app).

**Verdict: yes, with conditions.** No new structural fault. What remains is in the date rules and their wording, not
in the table; round 9 could go to a builder with the list below as conditions, once the T805 data gaps are fixed.

## What held [ran unless marked]
- The automatic date rule was simulated on all 2,105 answer keys for every day of 2026 (1.9 million fish-days): it
  never printed a wrong "until X, then release" or "until X"; no change comes back within 10 days (shortest closure
  after open days: 15 days, Denetiah Creek; no other under 27).
- A1 "up to the N": fixed (every row without a tighter limit has its own cell). Mild risk at Dodd: "up to the 4"
  beside brook trout while the strip says "only 2 wild" (true for hatchery fish).
- A2 Cunningham: fixed; rows match the stored year. Possession is still not on the stored row (T805).
- B1 Dodd, Lois, Khartoum: fixed as design, NOT backed by the live data (release r-46e4355a7277 stores no wild rule
  on any trout row there; the lines come from the T793 ruling).
- B3 frame, B4 heights (34 of 42 fit one 640 px screen), B5 Lois: as claimed. Leftover: Lois's "All together" bar
  sits outside both frames and can look like the header of an empty third box.

## Faults, most serious first
1. **"Until X, then closed to all fishing" is false where only trout and char close.** Fraser River gnis:39325,
   parts 5, 9, 11, 16, 18, 19 (6 answer keys): the rule that wins is "No fishing for trout and char, Oct 1–31"
   (`fraser_river.r1`); whitefish (15 a day) and burbot (5) stay open and trout reopen Nov 1. The sheet's "the water
   is closed: no fishing at all" is also false at Bish Creek (gnis:3480, from May 15: only steelhead close). Fix:
   read the closure's scope ("then closed to trout and char", "closed for steelhead") and give the reopening date
   when it is inside the season.
2. **The box strip's date can contradict a row.** Andi Creek, Jul 15: strip "Keep 5 in all … · until Oct 31, then
   release"; brook trout row "until Dec 31, then closed" (stored: brook trout keep 5 a day through Dec 31). Fix: when
   any row of the box carries a later date, "the others: until Oct 31…" or an undated strip. How often it happens
   across the release is not measured.
3. **Hand-written date changes do not scale.** Only 11 answer keys (39 waters) change a number, size, wild rule or
   possession with no keep / release flip; round 9 hand-wrote 4 (Bella Coola part 0, Coquitlam part 1, Kitimat part
   0, Adams part 3). Not written: Humphrys Creek gnis:2046 (23 waters, Kitimat's shape), Kitimat parts 1 and 3, Bella
   Coola part 1, and nine Bowron park streams (Istsi Creek 6, Wolverine River part 1 2, Sootheran Creek part 1 1:
   the park's possession limit, p.43, appears on the stored row only on days lake trout cannot be kept: a data
   artifact). The automatic rule stops at the first change, so a first change that is only looser, or a number /
   size change, hides a later release. Fix: a build rule: skip looser-only changes, date the first tighter or state
   change, store the "why" sentence per rule instead of by hand.
4. **Data, unchanged (T805):** possession on no stored fish row; the Dodd / Lois / Khartoum wild limit absent from
   the live release; the Fraser stretch on the card has no part in the new numbering (no dates); the Bowron park
   possession limit comes and goes with lake trout.
5. Eight cards taller than one screen (Vedder 723, Lois and Khartoum 742, Kitimat and its Oct 31 card 699, Skeena
   summer 673, Shuswap 665, Ross 645).

## On the designer's three questions (which choice misleads fewer anglers)
1. One line above the table when every fish closes the same day: keep, only when the closure covers the whole
   water; otherwise "closed to trout and char" with the reopening date.
2. A change that only gets looser stays undated: yes, but skip past it to the next tighter change.
3. Adams: the stricter 80 cm on the row, the one-day 60 cm rule in the sheet: yes.

## Not checked
A real phone; the page inside claude.ai; printed lines against page images (extracted page text and the app's stored
quotes were used); the T793 branch; how often fault 2 happens; angler-only and weekday rules; 2027 dates.

## After it (owner, 15:16, on round 10's picture)
"old look no outline because it looks like the groups are subgrouped inside the over keep 5 card then... one thing is
the words "only 1 of these 4 until oct 31, then release" should be changed to be "onlly 1 of these 4" being bigger it
is the actual rule right now then a smaller text for the change underneath. all current rules will have the bigger
text."; row spacing: "inbetween the 2". Round 11 = that, plus faults 1 and 2 above.
