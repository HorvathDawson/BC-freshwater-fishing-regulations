# Region 4 — Kootenay

Transcribed verbatim from the 2025-2027 synopsis Region 4 chapter. **Source text — do not edit
to fit the model.** Companion to `definitions.md` and `provincial-regulations.md`.

---

## General Regulations

**No Fishing:** in any stream in Region 4 from Apr 1 – June 14 (see tables for additional closed
times or exceptions).

**Trout/char release:** in streams from Nov 1 – Mar 31 (see tables for additional dates or
exceptions).

**Single barbless hook:** must be used in all streams of Region 4, all year.

**Classified Waters:** many East Kootenay Rivers and their tributaries are Classified Waters and
require a supplemental **Classified Waters Licence**, see page 7, map of waters on page 33, and
the Water-Specific Regulations tables (pages 35-40).

## Region 4 Daily Quotas

(See tables for exceptions)

```
Trout/char: 5, but not more than
  • 1 rainbow trout or cutthroat trout over 50 cm
  • 2 from streams
  • 1 bull trout (Dolly Varden) of any size

Bass:           0 quota, CLOSED TO FISHING (See tables for exceptions)
Burbot:         2
Crayfish:       25
Kokanee:        15 (none from streams), no more than 5 over 30 cm
Northern pike:  0 quota, CLOSED TO FISHING (See tables for exceptions)
Walleye:        0 quota, CLOSED TO FISHING (See tables for exceptions)
White Sturgeon: 0 quota, CLOSED TO FISHING (No exceptions)
Whitefish:      15 (all species combined)
Yellow perch:   0 quota, CLOSED TO FISHING (See tables for exceptions)
```

## Possession Quotas

Possession quotas = 2 daily quotas (See tables for exceptions)

## Annual Quotas

**Rainbow trout over 50 cm from the main body of Kootenay Lake: 20 per licence year.**

## Tributaries of Lakes

> When fishing the tributaries of the following lakes, check for special regulations in the tables
> under **both** the name of the tributary **and** the name of the lake (such as "Columbia Lake's
> tributaries"): Columbia, Connor, Duncan, Kinbasket, Kootenay, Lake Revelstoke, Little Slocan,
> Lower Arrow, Premier, Slocan, Trout, Upper Arrow, Waneta Reservoir, Whiteswan.

## Report your Lake Trout Catch

> Please report any lake trout you catch in the Kootenay Region to a Ministry office (or see page
> 75). Lake trout are not a native fish species in the Kootenays and could impact other native fish
> populations if they colonize. It is unlawful to transplant fish into any waters in B.C. (see page
> 8).

## Creston Valley Wildlife Management Area

> A permit is required for fishing on all waters within the Creston Valley Wildlife Management
> Area, including Six Mile, Leach, Kootenay River and Canal and Duck Lake.

> **NOTICE TO ANGLERS** It is illegal to fish for bass, perch, pike or walleye in the **Kootenay
> Region**, with the exception of certain waters, as listed in the Water-Specific Tables.

## Kootenay Lake Boundaries

* **Main Body** — the area **east** of a line between boundary signs on opposite shores near
  Balfour Point and Procter Lighthouse.
* **Upper West Arm** — the area **west** of that line, to McDonalds Landing (Six Mile).
* **Lower West Arm** — the area between McDonalds Landing (Six Mile) and Corra Linn Dam.

> **IMPORTANT** Kootenay Lake recovery may require **in-season regulation changes**. Check the
> website for in-season changes or closure dates for the 2025-2027 season.

---

## Checked against the curated entries

`data/curated/regulations/entries/zones/region-4.json` — 22 entries, 22 rules.

**Quotas are complete and correct**, including the three sub-limits (1 rainbow-or-cutthroat over
50 cm, 2 from streams, 1 bull trout of any size), the kokanee double limit (15, no more than 5
over 30 cm) and White Sturgeon's *"No exceptions"*.

### Missing — in the synopsis, not in the entries

| # | rule | note |
|---|---|---|
| 1 | **Classified Waters Licence** | **there is no classified, licensing or stamp entry in `region-4.json` at all** — and Region 4 carries **27 `Classified` entries, more than any other region.** The failure direction is an angler fishing a Classified Water without the supplemental licence. |
| 2 | **Annual quota: rainbow over 50 cm from the main body of Kootenay Lake, 20 per licence year** | see below — it exists at water level, but misfiled |
| 3 | **Tributaries of Lakes** — the 14-lake list | a curation instruction (*"check under both the tributary and the lake"*), and also a real statement that those 14 lakes have tributary-scoped rules. Nothing captures it. |
| 4 | **Kootenay Lake boundary definitions** (Main Body / Upper West Arm / Lower West Arm) | three named reaches the water entries already reference (`r4:kootenay_lake_main_body…`, `r4:kootenay_lake_upper_west_arm`). The *definitions* — Balfour Point/Procter Lighthouse, McDonalds Landing, Corra Linn Dam — are absent, so nothing can resolve the boundaries independently. |
| 5 | **In-season regulation changes notice** | Kootenay Lake recovery may change rules mid-season. Worth surfacing: it tells the reader the printed rule may be stale. |

### Wrong or misfiled

| # | issue | detail |
|---|---|---|
| 6 | **`z4:steelhead_annual_quota` is not in this chapter** | Region 4's Annual Quotas section contains **only** the Kootenay Lake rainbow line. Same defect as Region 3 — the B.C.-wide steelhead quota authored into a chapter that does not print it. Belongs in `region-provincial.json`. |
| 7 | **the Kootenay Lake annual quota is filed as `licensing`** | `r4:kootenay_lake_main_body…@4-19` reads *"Conservation Surcharge Stamp required over 50 cm; annual quota = 20"* with `kind=licensing`. It bundles a licensing requirement and a **harvest annual quota** in one rule, so the quota is invisible to anything looking at harvest rules. Split it. |
| 8 | **`z4:bass_perch_pike_walleye_illegal` binds all of Region 4** | the notice says *"in the Kootenay Region"* **"with the exception of certain waters, as listed in the Water-Specific Tables"** — the exception is not expressed. Same shape as `z3:bass_perch_illegal`. |

### Possible edition drift — worth one check

`z4:report_lake_trout` carries the verbatim *"Please report all lake trout caught from Region 4
waters **(except Koocanusa Reservoir)** to the Kootenay regional office at **(250) 489-8540**."*
The 2025-2027 text reads *"Please report any lake trout you catch in the Kootenay Region to a
Ministry office"* — **no Koocanusa exception and no phone number.** Either the entries were built
from an earlier edition, or the pasted text is abridged. If it is edition drift it will not be
confined to this rule, and the archive's vintage needs establishing before the reparse.

---

## The Classified Waters mechanism — use the symbol

The synopsis marks classified waters with a **CW symbol**, and that symbol is already curated:
`source.symbols` carries `"Classified"` on **72 entries**, and it survives into the bundle's
`entry.symbols` column, so the app has it too.

```
r1: 13 · r3: 1 · r4: 27 · r5: 9 · r6: 21 · r7: 1 · r8: 0 · r2: 0      (72 total)
```

**This is the right join, and it is two parts:**

* the **symbol** is the *fact* — this water is a Classified Water;
* the **zone rule** is the *consequence* — what that status obliges you to do, which differs by
  region (Region 4: a supplemental Classified Waters Licence; Region 3: a Steelhead Stamp
  *regardless of the species being angled for*).

So the licensing rule need not bind geographically at all. It attaches to *entries carrying the
symbol within this region* — an entry-attribute join, not a polygon.

**It also fixes two of the nine over-broad rules the zones README already flags** —
`z3:steelhead_surcharge.r2` and `z4:classified_waters_licence`, both recorded as *"binds the whole
region, should bind Classified Waters only"*. Those are the **licensing** ones, the worst
direction to be wrong in, because they tell an angler they need an endorsement on water where none
is required. The symbol resolves both exactly.

**Coverage gap this exposes:** zone licensing rules exist only for regions 2, 5, 6 (plus Region
4's Creston Valley WMA permit). Regions **1, 3, 4, 7 and 8 have none**, yet regions 1, 3, 4, 5, 6
and 7 all carry Classified entries.
