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
