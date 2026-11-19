# T869 — the chinook annual limit counts ADULT chinook, and "adult" differs by river

Status: OPEN (main chat; DFO salmon header). Waits on the owner's wording pick (below).

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
