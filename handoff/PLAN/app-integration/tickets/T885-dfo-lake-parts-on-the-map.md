# T885 — every DFO place on the map: the 14 active lake parts (Babine creek-mouth circles, Mabel north, Osoyoos north basin) and the 17 dormant ones

Status: OPEN (main chat). After production.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T885 | DFO's lake parts on the map (Babine's 12 creek-mouth circles, Mabel north, Osoyoos north basin); said in words until then (owner) | OPEN | main chat | owner 2026-11-18 ~21:13 |

Owner 2026-11-18 ~21:13: "There are some more check DFO too"; then, on the 31 DFO places with no extent (dialog,
his own words): "We should have had no unmapped stream items???! For lakes the parts should say in words in the salmon
section for now? We do try to map them if possible tho".

## Found (r-fc137c4040fa, salmon `unmapped`)
- 17 of the 31 are DORMANT (d: 1; DFO no longer prints them): Pitt ×3, Seymour, Brunette, Inch Creek, Birkenhead, San
  Juan ×2, Sakinaw ×2, Quesnel Horsefly Bay ×2, Babine's Pinkut ×2 and its "rest", Osoyoos (one wording). They say
  nothing and paint nothing. No ACTIVE stream place is unmapped.
- 14 are ACTIVE, all lake parts: Babine Lake "within a 400 m radius of the mouth of" Twain, Tachek, Pendleton, Six Mile,
  Big Loon, Sockeye, Tsezakwa, Hazelwood, Five Mile, Pierre, Four Mile, Morrison Creeks; Mabel Lake "Northerly of a
  line drawn from a white triangular fishing boundary sign…"; Osoyoos Lake north of the Highway 3 bridge (North Basin).
- The other "Check DFO" source is DFO's own "To be determined" (Thompson chinook + sockeye, South Thompson sockeye).

## Fix
1. Now: check the salmon section says each active lake part in words on its lake's card (the "Fishing is different
   in some spots today" fold), named and with DFO's rule.
2. Map them as proposals (review app): a 400 m circle at each creek's mouth on Babine (the creek's FWA junction with the
   lake is known), Mabel's and Osoyoos' lines from DFO's sign / bridge descriptions. Lake parts were deferred
   (memory: lake splits deferred); a radius circle is an area on the lake, not a split of it.

Owner 2026-11-18 ~21:18, verbatim: "But all of these should not exist / Those 12 should still be mapped even if retired /
To be determined I think we could change to actually say to be determined / However they should still show green if any
salmon there are open only yellow if closed other then the to be determined". So: EVERY DFO place gets an extent, the
17 dormant ones too (the day DFO prints one again it must already be on the map), and the 14 active lake parts. The
"To be determined" wording and colour go to the app chat (its ticket); the colour rule he states is already the map's
precedence (`salmon.map_status`: open, then release, then check, then closed): a water is green when any salmon is
open there; the "check" colour only when none is open or release and one is "to be determined".

State 2026-11-22 (stopped for credits, lane L12, branch feat/T754-one-bank-places): item 2 (Babine circles) built with T812 (same branch). Item 1: the release lists each active circle on its lake chain (`u`) and in `unmapped` in words (checked in the payload, not on screen). Left: Mabel north + Osoyoos north basin lines (lake lines: need owner cuts / lake split, deferred) and the 17 dormant places (curation, owner marks) — not started.
