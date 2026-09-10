# Region 3 — Thompson-Nicola

Transcribed verbatim from the 2025-2027 synopsis Region 3 chapter. **Source text — do not edit
to fit the model.** Companion to `definitions.md` and `provincial-regulations.md`.

---

## General Regulations

**Spring closure:** No Fishing in any stream in Region 3 from Jan 1 – June 30 (see tables for
exceptions). See definition of "streams" on page 80.

**Single barbless hook:** must be used in all streams of Region 3, all year.

**Steelhead fishing:** Your basic licence must be validated with a Steelhead Conservation
Surcharge Stamp if you fish for steelhead anywhere in B.C. In addition, **a Steelhead Stamp is
mandatory when fishing most Classified Waters regardless of the species being angled for.** Please
see page 6 for details.

## Region 3 Daily Quotas

(See tables for exceptions)

```
Trout/char: 5, but not more than:
  • 4 from streams
  • 1 over 50 cm
  • 1 bull trout (Dolly Varden) or lake trout, none under 60 cm
And you must release:
  • ALL STEELHEAD
  • Bull trout (Dolly Varden) from streams, Aug 1-Oct 31
  • Lake trout from Oct 15-Jan 31

Bass:           0 quota, CLOSED TO FISHING
Burbot:         2
Crayfish:       25
Kokanee:        5 (none from streams)
Whitefish:      15 (all species combined)
White Sturgeon: CATCH AND RELEASE ONLY
Yellow Perch:   0 quota, CLOSED TO FISHING
```

## Possession Quotas

Possession quotas = 2 daily quotas (See tables for exceptions)

## Annual Quotas

```
Annual catch quota for Shuswap Lake (per licence year):
  Rainbow trout: 5 over 50 cm
  Char - Lake trout and Bull trout (Dolly Varden): 5 over 60 cm
```

## Daily & Annual Quotas for Salmon

Please refer to the NOTICE on page 77 for salmon regulations.

## Report Tagged Fish

> Please report tagged fish to the Fish and Wildlife Regional Office in Kamloops at
> 1-800-388-1606. Information should include tag number and colour, fish length and weight, and
> location of capture.

## Steelhead Management Changes

> In response to low abundance of steelhead in the Thompson-Nicola Region, steelhead fisheries in
> the following waters, and during the following times, are closed:

* **Thompson River:** downstream of signs at Kamloops Lake outlet to the confluence with Fraser
  River, Oct 1 – May 31 (see tables for exceptions)
* **Fraser River:** from Hwy 99 bridge at Lillooet to BC Hydro tail race outflow channel,
  Oct 1 – May 31; and from the confluence with Thompson River to CNR Bridge approximately 1 km
  downstream, Oct 1 – May 31
* **Nahatlatch River** downstream of Nahatlatch Lake **and Stein River:** from Jan 1 – May 31
* **Frances and Hannah lakes:** from Jan 1 – May 31
* **Seton River** downstream of Seton Lake: from Apr 1 – May 31

> **NOTICE TO ANGLERS** It is illegal to fish for bass or perch in the **Thompson-Nicola Region**.
> This measure is part of B.C.'s management approach to illegal fish introductions.

**ILLEGAL MOVEMENT OF LIVE FISH** — see important information on page 8.

---

## Checked against the curated entries

`data/curated/regulations/entries/zones/region-3.json` — 21 entries, 21 rules.

**Quotas are complete and correct**, including the two easiest to lose: the char sub-limit's
*"none under 60 cm"* and the two seasonal release windows (bull trout Aug 1 – Oct 31, lake trout
Oct 15 – Jan 31).

### Missing — in the synopsis, not in the entries

| # | rule | note |
|---|---|---|
| 1 | **Steelhead Conservation Surcharge Stamp**, and the Classified Waters clause | there is **no stamp, surcharge or classified-water entry in `region-3.json` at all**. The second half matters most: *"a Steelhead Stamp is mandatory when fishing most Classified Waters **regardless of the species being angled for**"* — that obliges an angler fishing for anything on a Classified Water. Region 6 has `z6:r6_steelhead_stamp`; Region 3 has nothing. |

### Wrong or over-broad

| # | issue | detail |
|---|---|---|
| 2 | **`z3:bass_perch_illegal` binds all of Region 3** | the notice says *"in the **Thompson-Nicola Region**"*, which is a sub-area, not the whole of Region 3. Over-reach in the restrictive direction. |
| 3 | **`z3:steelhead_annual_quota` is not in this chapter** | Region 3's Annual Quotas section lists **only** the Shuswap Lake quotas. The B.C.-wide 10-steelhead line is printed in the Region 1, 2 and 4 chapters, not this one. True province-wide, so harmless — but it is authored here from a chapter that does not contain it, and it belongs in `region-provincial.json`. |
| 4 | **`z3:kokanee_quota` is narrowed to lakes** (`feature_types: ["lake"]`) | the source says *"Kokanee: 5 (none from streams)"* — the 5 is the general quota, with a separate stream prohibition. Corpus-wide this splits **7 narrowed to lake** (z3, z5, z6 ×2, z7a, z7b, z8) against **3 with no filter** (z1, z2, z4). One sentence, two encodings; z1/z2/z4 are the minority. |

### Correction to an earlier finding

**`z3:bass_closed` and `z3:bass_perch_illegal` are NOT a curation duplicate.** The synopsis states
both, in two places: the quota table (*"Bass: 0 quota, CLOSED TO FISHING"*) and the NOTICE TO
ANGLERS. An earlier analysis listed them as "duplicates to merge" — that was wrong; the source
duplicates them deliberately, one as a quota and one as an enforcement notice. The same applies to
regions 4 and 5. What *is* true is that they must not both render as unrelated rules.

### Present, correctly, at water level

| # | rule | where |
|---|---|---|
| 5 | **Shuswap Lake annual quotas** | `r3:shuswap_lake…@3-26` — *"Rainbow trout daily quota = 1 (none under 50 cm), annual quota = 5"* and *"Char daily quota = 1 (none under 60 cm), annual quota = 5"*. Matches the chapter. |
| 6 | **All six Steelhead Management Changes closures** | `r3:thompson_river_downstream…`, `r3:nahatlatch_river@3-15` (×2), `r3:stein_river@3-16`, `r3:frances_lake@3-15`, `r3:hannah_lake@3-15`, `r3:seton_river…`, and two `r3:fraser_river@3-14` rules — every window matches the chapter. |

**On those six: the chapter's framing is context, not a narrowing.** The preamble says *"steelhead
fisheries … are closed"*, but each water rule's own verbatim reads **"No Fishing"** with no species
— a total closure. The water tables are authoritative and the entries follow them correctly.

What the preamble *does* supply is the **reason** — low steelhead abundance in the Thompson-Nicola.
`no_fishing.reason` exists and is populated on only 9 rules corpus-wide; these six are candidates,
and a closure a reader understands the reason for is one they are more likely to respect.

### Note

The provincial protected-species list is **not exhaustive** — Region 2 adds Green sturgeon. Regions
may extend it, so `zp:protected_species` must be treated as a floor, never as the complete set.
