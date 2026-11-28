# T1140 — A water's plain link opens the rules of one part without saying which: the Squamish reads "Closed"

Status: PARTIAL — HIGH, on production. Part names merged in PR #265 (2026-11-26; L28) and the side-channel words in #312 (2026-11-27): no part is numbered anywhere; every part is named by what it is (below); the four open asks are answered or superseded (## Owner); left (Claude): landing A (the Map with the parts) is the next patch, pictures first; Work left 2-5.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1140 | A water's plain link opens the rules of one part without saying which: the Squamish reads "Closed" | PARTIAL — HIGH; part names merged (#265, #312) | 5d57ac8c2, 54e0c3e15, 82e6de1f4, 643b99fc3 | left (Claude): landing A next patch, pictures first; Work left 2-5 |

Source: owner 2026-11-25 14:55 (planning chat), with two pictures of the web app on production, verbatim: "big app
bug we will want to fix asap.. basically if the user searches something liek squamish river regs and clicks the
link https://canifishthis.ca/water/squamish-river then it opens to regulatiosn tab which is fine. but it doesnt say
which section it leads with and in the case of squamish it is the eco resevre on it.. and it isnt labelled... so
maybe from the basic links it should go to the map and the user can then select which section they want to view
regulations for? also this case doesnt even say it is in the ecological reserve sectionb..."

Owner 15:00: "it should happen as first thign on new org repo maytbe? and it will be the thing used for testing releases?
also if it is a single part then it could open regulations directly" — a water with one part keeps opening its
Regulations tab; only a multi-part water gets the new landing (A, the owner's pick, 15:14).

## Step 1 (2026-11-25 15:08, candidate r-71d09ad82e81): the list
`docs/app-integration/T1140-files/multi-part-waters.md`. A plain link, the search page and open-by-item reach
`targetFor(store, item, null)` (`app/packages/data/src/regs/inputs.ts`): the FIRST part, mouth first, with an answer.
478 waters have two or more parts; (a) lead closed while another part is open: 89 (the Squamish); (b) the unsafe way,
lead OPEN while another part is closed today: 156 (Capilano, Chilliwack, Fraser, Skeena, …). 26 parts had no name and
showed the bare water name, all side channels with no main stem.

## Built (lane L28): every part named, none numbered
- **No "Part n of m" anywhere** (`core/src/waterNav.ts` `partWhere`, `partRowAria`). The Regulations view waits for the
  water's parts (`useMapEntries` `settled`), never for live salmon. "Other parts of this water have different rules" stays
  under the part's name, ONE 24 px line (the class line's height, a recorded exception to 44) that opens the Map with its
  list open. Under the head the water's name is not repeated ("Inside Baynes Island Ecological Reserve"; `nameUnder`).
- **Named in the bundle stage, measured on the shapes** (`pipeline/deliver/bundle/side_channels.py`, before
  `topology.write`; no atlas or reach change; new bundle tables `part_inside`, `part_own_name`, `section_join`, read by
  deliver only; the parts keep their river order, numbered from the flow's own measures):
  - INSIDE: ≥ 95 % of every section (lines by length, polygons by area) in ONE cut area → "<water> inside <area>"
    (Squamish p1, Gordon p0: 100 %).
  - AT (owner 19:45): ≥ 95 % of its lines in one area while a polygon section straddles the edge → "<water> at <area>"
    (Okanagan River Oxbows p1: lines 706 m, 100 % inside Hayne's Lease; its lake polygon 137,540 of 430,815 m², 31.9 %).
  - NAMED ENDS (rule A, the ONE such part of a water whose channels have a named end): "Skeena River side channel from
    the Exchamsiks River confluence up to Skeena River Ecological Reserve boundary"; a run naming only its own mouth
    takes the named water its flow pointer reaches (`section_down`): "Theodosia River side channel into Olsen Lake".
  - JOIN DISTANCE: a channel the flow never walks to the main line joins it at the main line's point nearest its
    downstream end (by its flow pointer; else its end nearer the main line), along the main line from the mouth, on its
    OWN water's main line, within 50 m (the 15 channels measured all snap at 0 m; the nearest beyond is 231 m).
  - TOUCH (PROPOSAL, coordinator 19:45, for "a consistent way to handle this"): a channel whose downstream end touches a
    water body of its own river joins AT it — a named lake its main line runs through names the end ("into <Lake>"); its
    own polygon (a wide reach) or an unnamed lake its main line runs through is measured at the main line's point
    nearest the touch, whatever the distance.
  - OWN NAME (PROPOSAL, coordinator 19:55, under the owner's "do whatever makes our current state decent until this is
    fixed in hardening"): a part made only of added city lines (negative blk) with one own name, not the water's, is
    named by it and its municipality: "Noons Creek: Turner Creek (Port Moody)", "Seymour River: Tunnel Creek
    (Burnaby)"; under the water's name "Turner Creek (Port Moody)" (a watershed-code collision filed them there; the
    refile is the hardening release's). The name: `added_streams.json` `streams[].name` by the section's blk
    (`section_measure`), read at build time; the municipality: from the water's `name_source.label` ("City of Port Moody
    ESA streams layer"), whose full words stay the source. Bundle table `part_own_name`. "(city stream)" has no token.
  - NEAREST (PROPOSAL, coordinator 20:30): a channel beyond the snap limit touching no water body of its river is said
    at its main line's nearest point, "Side channel, N km from the mouth", never "joining" (in a several-channel label
    the range includes it). A part with no main line to measure still STOPS the build, named.
    `part.REVIEWED_INSIDE` pins the inside/at pairs both ways. The tokens (`near:inside:`, `near:at:`, `near:side:`) are what a 0.9.0 app composes as "<water> <words>".
- **What each of the 26 reads, with the snap distances:** `docs/app-integration/T1140-shots/before-after.md`.
- **Proof:** clean side deliver (bundle → release, with and without this code; atlas, reach and tiles the live ones);
  WHOLE_EXPORT_LINE; 28 of 28 guard mutants killed (`pipeline/tests/test_t1140_inside_area.py`).

## Why it is high
Someone arriving from a web search reads "Squamish River: Closed all year" for a river open outside a few hundred metres
of reserve, and on 156 waters the lead part is open while another is closed. It is the page search engines send people to.

## Work left
1. Landing A (the owner's pick): a plain link to a water with more than one part opens the Map tab with the parts
   listed; a link that names a part still opens that part. Next patch, pictures first.
2. The closure block names the area too ("Closed all year: Baynes Island Ecological Reserve"), not only "Federal / parks".
3. Later, the owner's thoughts (planning chat to ticket): simpler braids, especially at the mouth; cleaner section splits.
4. First patch (audit of #265, pipeline): `side_channels.join_measure` orients the main-line section it lands on by the
   NEAREST section below it by distance, not the one its flow pointer says is directly below; adjacent sections touch, so
   every real join measured today lands right, but the flow pointer is the sounder rule.
5. Left for hardening (the review of #265's last changes, 2026-11-25; one MED, three LOW):
   - MED: a NAME of several side channels can say "joining" for a channel that does not join (`part.py` `side_words`
     never reads `section_join.how`); no real name does today, but the range labels do (Manson, Squamish).
   - LOW: a keyed part missing from the map's places is reported once and the head drops its "other parts" line:
     nothing shows on screen (`core/src/regs/header.ts`).
   - LOW: no test runs the real v0.9.0 `spanName`; checked by hand, 0.9.0 reads "Noons Creek : Turner Creek (Port
     Moody)" with a space before the colon.
   - LOW: a city stream that also has an OSM or excluded-FWA name gets no own-name row (`side_channels.py`); the
     build stop still catches an empty name.

2026-11-26 test fix: test_t1140_inside_area kept its unnamed lakes in a module dict keyed by id(db), so a later db reusing a freed id inherited them (pre-push chunk saw 'join' for 'nearest'); now passed to `_write`, with a guard test.

## Acceptance
From a search result for the Squamish River nobody can read "closed" without also reading that it is the reserve reach,
and the open part is one tap or less away; no water's landing shows a single unnamed part; the search page agrees.
Severity HIGH. Size M. Owner: yes (pictures; the touch rule).

## Owner

- [x] 2026-11-25 — Where a multi-part water's link lands → "A: Map with the parts"; when → "web first right after repo move and make repo move happen asdap"; move now → "Yes, move now, re-cut 0.10.0 after" (2026-11-25 15:14).
- [x] 2026-11-25 — Say yes to the label's words ("Part N of M", "other parts have different rules") from pictures → "itshould have the real name. there should be no partt x of n nameas anywhewre in the app. that is a fault. instead it will say eco reserve here etc all types will have a name"; on the side-channel leads → "Hold 0.10.0 for the reserve name" (2026-11-25 ~16:55).
- [x] 2026-11-25 — The Map bar with the count gone → "The part's real name (Recommended)"; parts outside B.C. → "\"<water> outside B.C.\" (Recommended)"; the 15 rule-gap channels → "Fix the binding in 0.10.0"; the Strathcona parts → "outside strathcona park sounds bad maybe a different name could be worked in?" (2026-11-25 ~17:05).
- [x] 2026-11-25 — A name for the parts with no real-world name → "The data's own label (Recommended)" (2026-11-25 17:31).
- [x] 2026-11-25 — The doubled water name on the Regulations head → "Drop the repeat (Recommended)"; the other-parts line → "I want image"; the map ring label "Inside Baynes Island Ecological Reserve" → "Keep it (Recommended)"; Oxbows p1 and Skeena p4 reading a label → "Hold for real names" (2026-11-25 evening).
- [x] 2026-11-25 — The other-parts line, from the crop → "Keep but need to make sure it doesn't take up too much real state" (2026-11-25 ~18:55).
- [x] 2026-11-25 — Skeena p4, Oxbows p1 and the nameless parts → "Named by its ends (Recommended)"; Oxbows → "I don't see image but would be in the reserve right"; distances → "They should always have the cut names or distance it is impossible not to" (2026-11-25 ~19:00).
- [x] 2026-11-25 — The five STOPs and the words → Oxbows: "\"at Hayne's Lease Ecological Reserve\" (Recommended)"; Halfway p2: "We need a consistent way to handle this and maybe the stream should be simplified and have some of the braidi g removed? Especially at the mouth? Seems like our streams are too complicated and we could avoid this if small braids get simplified more. But either way there should be a consistent way to handle this? Like enter lake having different rules or similar? Thoughts?"; Noons p4 and Seymour p3: "We want them to be refiled properly so let's take the fwa version for now but add a gotcha to check on which one is the real stream line."; the words: "The clearer one but also maybe we could make the sections split more cleanly? Also simplification might help reduce the many side channels" (2026-11-25 ~19:45).
- [x] 2026-11-25 — Noons p4 / Seymour p3, now known as the cities' Turner Creek and Tunnel Creek → "just add all to hardening release"; for now: "do whatever makes our current state decent until this is fixed in hardening" (2026-11-25 ~19:55, through the coordinator).
- [x] 2026-11-25 — Say yes to the OWN-NAME words (a proposal): "Noons Creek: Turner Creek (Port Moody)"; under the head "Turner Creek (Port Moody)". Default: as built until the hardening refile. Pictures: T1140-shots/round3-noons-*. → superseded by T1246 (#379): Turner: "Make it its own water (Recommended)"; "All three now, in the re-cut" (2026-11-27)
- [x] 2026-11-25 — Say yes to NEAREST (a proposal): a side channel that meets its river nowhere is said at the main line's nearest point, "N km from the mouth", without "joining" (Halfway p2, Manson p0; snap distances in before-after.md). Default: as built. → superseded by #312's rulings: "No verb for a leaving channel (Recommended)"; "Fix the measure in this PR" (2026-11-26)
- [x] 2026-11-25 — Say yes to the TOUCH rule (a proposal, built for Halfway p2 and Manson p0): a side channel whose downstream end touches its own river's polygon or a named lake on its main line joins there ("into <Lake>", or measured at the main line's nearest point whatever the distance). Default: as built. Every named-lake lower end says "into <Lake>" only where a flow pointer leads into the lake (Theodosia into Olsen Lake, Stirling into Acme Lake, was "from Acme Lake"); with no pointer the direction is unknown and no lake is named (Robertson's channel LEAVES Boot Lagoon; Thelwood has none). Pictures: T1140-shots/round3-*. → superseded by #312: "Fix the measure in this PR" (2026-11-26): a channel is measured where it leaves its main line
- [x] 2026-11-25 — Look at the pictures before the naming merges (T1140-shots/before-after.md: Skeena p4, Oxbows p1, Theodosia, Halfway p2, a derived-distance part, the other-parts line crop), then say go. Nothing merges before you have seen them. → "Show me the final table first", then "Merge; names only (Recommended)" (2026-11-26); #312 merged 2026-11-27
