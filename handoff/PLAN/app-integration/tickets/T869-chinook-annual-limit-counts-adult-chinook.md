# T869 — the chinook annual limit counts ADULT chinook, and "adult" differs by river

Status: IN REVIEW — on production since 0.8.3 (data r-6258ffded676, feeds worker 0a3205f6; built c9dc6ba69, a6f79b38a; first on staging r-379e72fc9524); the Vedder follows DFO (owner 2026-11-19 ~06:40 "good"); owner: OWNER-TODO "Test 0.8.2 on staging (2026-11-19 morning)" (item 2).

Owner 2026-11-18 ~20:22 (via the app chat), verbatim: "For salmon  It is 10 max adult Chinook not over 50cm because some
rivers adult is defined differently". His screenshot: Coquitlam River, What you can keep, the salmon block's header:
"The total annual limit for chinook salmon over 50 cm is 10 fish from all fresh waters combined."

## What the sources say
- The header line is DFO's own sentence, carried verbatim (region pages, e.g. region2-eng.html: "over 50&nbsp;cm"), typed
  as an `annual` note by `pipeline/regs/dfo_salmon/header.py`. The live feed carries the notes and wins; the release's
  copy is the offline fallback (`pipeline/deliver/salmon.py`).
- DFO's same page also defines the adult per area: "An 'adult chinook' in Region 2 is defined as being over 50 cm
  except in the following areas where an 'adult chinook' is defined as being over 62 cm: …". `header.py` already types
  these as `adult` notes with their waters (scoped).
- The book, p.77 (PDF p.83), "Definition of Adult Chinook in Non-Tidal Waters": over 50 cm in most non-tidal waters;
  over 62 cm on the Fraser (CPR Bridge at Mission to the powerline ~1 km above the Agassiz-Rosedale Bridge, Sep 5-Dec 31),
  the Harrison below the boundary signs near its mouth (Sep 5-Dec 31), the Chilliwack/Vedder below Slesse Creek (Jul 1-Dec
  31), the Sumas below the Barrowtown pumping station (Jul 1-Dec 31); over 65 cm year round on the Bella Coola, Atnarko,
  Iknouk, Kemano, Khutzeymateen, Kildala, Kincolith, Kitimat, Kitlope, Kwinimass, Nass and Skeena. p.7: record your
  retention of ADULT chinook on your licence.

So on the Vedder in August, or on the Skeena, "chinook over 50 cm" counts fish that are not adults there.

## Fix (wording to be picked by the owner)
A. Reword the line to count adults and say what an adult is HERE, from the scoped `adult` note (the water's own length
   and dates): "The annual limit is 10 adult chinook from all fresh waters combined (here an adult is over 62 cm, Jul
   1-Dec 31; over 50 cm the rest of the year)."
B. Keep DFO's sentence as printed and add a line under it: "Adult chinook here: over 62 cm (Jul 1-Dec 31) — DFO,
   rule book p.77."
Either way: the chinook row's "adult chinook over 50 cm" size words follow the same scoped length. A test over the
live feed: no water bound by a 62/65 cm adult note says "over 50 cm" for adults. Ships in the feed (live) and in the
next data release (fallback).

## Owner's picks (2026-11-18)
- Wording: (A), reword. Picked in the main chat's dialog ~20:30 ("Reword the line (Recommended)") and in the app chat's
  dialog ("Reword"): "10 adult chinook a year from all fresh waters" and where adult differs "(here an adult is over
  62 cm, Jul 1–Dec 31)". Our words, not DFO's.
- Timing: AFTER production. The two chats got different answers (main chat ~20:30 "After launch", app chat "Yes,
  before prod"); asked again at 20:34, the owner picked "After production". Lands in the feeds worker + the next data
  release after the switch.

## Built (lane data/t869-t870-salmon, c9dc6ba69, 2026-11-19)
- Pipeline `regs/dfo_salmon/quota.py` `period_says` + worker `quota.ts` `periodSays`: the bucket's annual chinook tier says
  "10 adult chinook a year from all fresh waters" (DFO's sentence names "all fresh waters"; else "10 adult chinook a
  year"). Every other period limit (Region 8's monthly "4 chinook over 50 cm") keeps DFO's words — not in the pick.
- Core `salmon.ts` `adultHereOf`: where the water's adult chinook is another length than the limit's 50 cm (DFO's scoped
  `adult` sizes, every window; else the region's own), the line ends "(here an adult is over 62 cm, Sep 1–Dec 31)";
  Vedder / Capilano "(here an adult is over 62 cm)", Skeena (R6) and Bella Coola "(here an adult is over 65 cm)"; R6's
  Fraser watershed none. A dated scoped length in a region whose own adult differs adds "; over 65 cm the rest of the
  year". DFO's scoped notes carry no Jul 1 date for the Vedder (the book's p.77 does): the card says DFO's (none).
- The chinook row's sizes already follow the scoped length per day (`sizeEntry`): pinned.
- Tests: `pipeline/tests/test_salmon_t869_t870.py`, core `salmonSources.test.ts`, release conformance
  `conformance/src/regs/salmonAdult.release.test.ts` (every scoped / region adult length over the release under test,
  never "over 50 cm" there; mutation-checked), `t356_header.json` and the dfo vectors re-cut.
- Ships: the live feed when the feeds worker is deployed (bucket tiers), the release copy with the next data release.


## Timing changed 2026-11-19 02:41 (owner)
"After production" (11-18 20:34) is superseded by the owner's 02:41 ask to the retiring chats, verbatim: "i think all these items plus the map speed up idea and other loose ends being finished should all go into the release on staging while i sleep and i can test and release to prod tomorrow". T869 ships in tonight's staging release; he tests it before production.

## Open for the owner (2026-11-19 night review)
The Vedder: DFO's own scoped adult note carries no dates, so the card says "(here an adult is over 62 cm)" all year; the book (p.77) limits the 62 cm adult to Jul 1–Dec 31. As built the card follows DFO (the salmon authority). In the morning list.


## Owner 2026-11-19 ~06:40: the Vedder follows DFO
On "Built: follow DFO (the card says over 62 cm all year). Or use the book's dates?", verbatim: "good". Kept as built.
