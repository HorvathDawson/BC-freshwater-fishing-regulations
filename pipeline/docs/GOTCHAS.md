# Gotchas — things that look like a bug, a change, or a duplicate, and are not

Short entries, newest first. One per trap that has already cost someone an hour. A gotcha earns a
place here when the *evidence* is misleading: the data says one thing and means another. Things the
code now handles belong in the code's comments, not here.

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
