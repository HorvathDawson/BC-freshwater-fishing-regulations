# Gotchas — things that look like a bug, a change, or a duplicate, and are not

Short entries, newest first. One per trap that has already cost someone an hour. A gotcha earns a
place here when the *evidence* is misleading: the data says one thing and means another. Things the
code now handles belong in the code's comments, not here.

---

## A row's closed season does not shorten the zone's (the Stein, the Nahatlatch)

**2026-10-05 · zone closures, all regions · `pipeline/deliver/bundle/rules.py`, export `gotchas.closures_combine`**

The Stein's row prints "**No Fishing** Jan 1-May 31"; Region 3 prints "No Fishing in any stream in
Region 3 from Jan 1-June 30". It reads as if the row's dates replace the zone's, so the Stein
opens June 1. They do not: **both hold, closed on the union** (Jan 1-June 30). The p.28 line that
lists "Nahatlatch River downstream of Nahatlatch Lake and Stein River: from Jan 1-May 31" looks
like an exception list and is not one — it sits under *Steelhead Management Changes*, a list of
steelhead closures (the Thompson heads it). Two lifts built on it were removed.

A row beats a zone closure only where the book PRINTS an exemption ("exempt from spring
closure", or the zone page names the water), or prints a dated catch and release, opening or
quota INSIDE the closure — then on exactly those dates, for exactly those fish (the Nicola below
its lake: trout catch and release Jan 1-Feb 28; whitefish stay closed). Every entry where the
two overlap is listed, generated, in the export's `guide.gotchas.closures_combine`. Before adding
a lift, find the printed words; a row's own season is never them.

---

## A dated bait ban DOES replace the zone's all-year one (Quatse, Somass, Sproat, Stamp)

**2026-10-05 · Region 1 bait ban · export `gotchas.dated_bait_ban_replaces_zone`**

The opposite of the entry above, and easy to "fix" by analogy. Region 1 prints "Bait ban: applies
to all streams of Region 1, all year, with some important exceptions. Check the tables." The
Quatse prints "Bait ban, May 1-Nov 30". Here the row's dates DO replace the zone's: bait is
allowed on the Quatse Dec 1-Apr 30 (`quatse_river.r4x` lifts the zone ban on those dates). A
closure never works this way; a bait ban with "check the tables" does. Only Region 1 prints this
shape today: Regions 6, 7A and 7B ban bait all year on streams too, but no row there prints a
dated ban on the same water (the one Region 5 Fraser section under Zone 7A's ban is lifted by the
Region 7 Fraser row's "EXEMPT from bait ban").

---

## The same closure appears twice, under two sign counts (Skeena @ Kispiox)

**2026-09-21 · DFO salmon, Region 6 · `pipeline/regs/dfo_salmon/`**

The Skeena carries **three** locators for the fishing-boundary-sign closure at the Kispiox
confluence:

- `6:skeena-river:all-waters-within-the-4-triangular` — *"all waters within the **4** triangular
  fishing boundary signs located at the confluence of the Kispiox River with the Skeena River"*
- `6:skeena-river:mainstem-waters-within-3-white-tri` — *"mainstem waters within **3** white
  triangular fishing boundary signs located at the confluence of the Kispiox River…"*
- `6:skeena-river:mainstem-waters-within-three-white` — *"mainstem waters within **three** white
  triangular fishing boundary signs located at the confluence with the Kispiox River"*

The duplicate grouper caught the numeral one (score 0.934) and missed the spelled-out one — the
gap between `3` and `three` is wider than its threshold. Both are now
`duplicate_confirmed` against the four-sign primary.

**They are one zone.** DFO re-worded it across years and every wording survives in the corpus, so
the sign count is not a distinguishing fact — do not go looking for two or three closures, and do
not bind them to different water. All three bind to the same pair of cuts
(`skeena_river__kispiox_sign_zone_lower` / `_upper`).

The sign count is the trap: a wording can change 4 -> 3 -> three without a single fish being
affected, and each change mints a locator that looks new.

Why it reads as several: the locator id is a fingerprint of `section|water|specific_area`, so re-worded
scope text mints a *new* locator rather than updating the old one. That is the design — the old one
goes dormant and revives if the wording returns — but it means **wording drift looks exactly like a
new regulation**. `whatchanged` diffs rules on structure for this reason; locators have no such
defence, and a curator is the only thing that spots it.

Check before assuming two: same water, same section, same rules, landmark in the same place. If all
four match, it is one zone — record it with `duplicate_of` + `duplicate_confirmed`, not by binding
the copies to different geometry.

> The 400 m either side of the confluence is an **assumption**, not a survey. DFO gives no distance;
> 400 m is the radius it uses for the comparable Region 6 mouth closures (Babine Lake, Pinkut Creek).
> `spatial_caveat` stays set until someone pins the real signs.

---

## A locator that matches everything but a trailing period

**2026-09-21 · DFO salmon · `pipeline/regs/dfo_salmon/`**

Four of the ten live Skeena locators matched their stored binding only after normalising a trailing
`.`. The page's scope text gains and loses sentence-ending punctuation between scrapes, and because
the fingerprint is taken over the raw text, one character retires a locator and mints its twin.

Same family as the entry above, smaller blast radius: it shows up as a locator that has "vanished"
alongside an identical one that has "appeared", on the same water, in the same run.
