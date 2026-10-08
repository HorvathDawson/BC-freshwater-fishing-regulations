# Region 5 — Cariboo

Transcribed verbatim from the 2025-2027 synopsis Region 5 chapter. **Source text — do not edit to
fit the model.** Companion to `definitions.md`, `provincial-regulations.md` and `licensing.md`.

---

## General Regulations

**Spring closure:** No fishing in any stream in **Fraser River Watershed of Region 5 (including
the Thompson River Watershed)** from Apr 1 – June 30, **EXCEPT the mainstem of the Fraser River**
and other streams listed in the tables.

**Single barbless hook:** must be used in all streams of Region 5, all year.

**Size limit:** There is no minimum size in lakes (see tables for exceptions).

## Region 5 Daily Quotas

(See tables for exceptions)

```
Trout/char: 5, but not more than
  • 1 over 50 cm
  • 2 from streams
  • 1 Dolly Varden/bull trout
  • 2 lake trout
And you must release:
  • ALL STEELHEAD
  • Lake trout, Oct 1-Nov 30
  • Bull trout (Dolly Varden) from streams, Aug 1-Oct 31

Bass:      0 quota, CLOSED TO ALL FISHING
Burbot:    5
Kokanee:   5 (none from streams)
Whitefish: 15 (all species combined)

White Sturgeon:
  • CLOSED TO ALL FISHING in the Fraser River Watershed UPSTREAM of Williams Lake River.
  • CATCH AND RELEASE in the Fraser River Watershed DOWNSTREAM of and including
    Williams Lake River.
  • CLOSED TO ALL FISHING in the Fraser River downstream of and including Williams
    Lake River Sept 15-July 15.
```

## Possession Quotas

Possession quotas = 2 daily quotas (see tables for exceptions)

## Steelhead fishing

Your basic licence must be validated with a Steelhead Conservation Surcharge Stamp if you fish for
steelhead anywhere in B.C. In addition, a Steelhead Stamp is mandatory when fishing most Classified
Waters **regardless of the species being angled for**.

## Steelhead Management Changes

> In response to declining abundance of Fraser Basin steelhead, steelhead fisheries within the
> **Chilcotin River Watershed** may be closed.

## Dean River Classified Waters

> All anglers are required to buy a Classified Waters Licence to fish the classified portions of
> the Dean River. **There are no limits on the number of days which a Canadian resident may fish**
> the classified sections of the Dean River.
>
> A **Non-Resident Alien** is allowed only **one** Classified Waters Licence for the Dean River,
> and may only fish **one classified section** of the Dean River for a maximum of **8 consecutive
> days per year regardless of whether guided or unguided**. A **non-guided Non-Resident Alien**
> wishing to fish the **Class I - Main Section of the Dean River, from Crag Creek to signs 500 m
> upstream the canyon**, must enter an **annual limited entry draw** held each spring.

## Notice to Anglers

> The following waters ARE CLOSED TO ALL FISHING: **Chilcotin River downstream of Chilko River
> from October 1 through June 10.** Sport fishing openings will be announced in-season, if
> scientific information suggests that abundance is adequate to support a fishery.

> It is illegal to fish for **bass** in the **Cariboo Region**.

> **ICE FISHING HUTS: WARNING** Failure to remove ice fishing huts from lakes before spring
> breakup is an offence under the **Environmental Management Act**.

> **WARNING** Due to aeration projects, DANGEROUS THIN ICE and OPEN WATER may exist on **Dewar,
> Higgins, Irish, Simon and Skulow Lakes.**

---


## Box text read from the rendered page (RULES round, 2026-10-07)

Pelican alert (p.41):

> PELICAN ALERT American White Pelicans are an endangered species and protected under the B.C. Wildlife Act. B.C.'s only nesting colony (350 nesting pairs) is located in the Cariboo-Chilcotin. Pelicans return to the region each April/May to breed. After the young have fledged in August, they migrate south to overwinter in the westerm U.S. and Mexico. Pelicans forage for fish on lakes throughout the region and travel as far as 165 km from the nesting colony. They do not dive but feed from the surface in shallow water. When breeding pelicans are disturbed while foraging, their feeding and timely return to the nests is disrupted. This leaves the young without food and may reduce survival. Please do not approach pelicans. To report pelican sightings, please contact the Fish and Wildlife Regional Office in Williams Lake.

## Checked against the curated entries

`data/curated/regulations/entries/zones/region-5.json` — 20 entries, 20 rules.

All four trout/char sub-limits and all three release windows are present and correct.

### ⚠️ A live inversion — a closure rendered as a permission

`r5:fraser_river@5-2` rule `gnis:39325.r5`:

```
text    : **No Fishing** for sturgeon in the Fraser River Watershed upstream of
          Williams Lake River (sturgeon catch and release)
details : Sturgeon catch and release, upstream of Williams Lake River
kind    : harvest          ← not closure
```

**The verbatim says No Fishing upstream; the label the app shows says catch and release
upstream.** The chapter settles which is right:

* upstream of Williams Lake River → **CLOSED TO ALL FISHING**
* downstream of and including Williams Lake River → **CATCH AND RELEASE**

The parser attached the downstream reach's *"catch and release"* to the **upstream** reach and
flipped `kind` from `closure` to `harvest`. An angler is told they may fish for sturgeon on water
the province has closed. This is the same failure class as the `r4:kootenay_lake_upper_west_arm`
window inversion, and it is the worst direction to be wrong in.

### Missing — in the synopsis, not in the entries

| # | rule | note |
|---|---|---|
| 1 | **White sturgeon — all three rules** | `region-5.json` contains **no white sturgeon rule at all**, while `z1`, `z2`, `z3`, `z4`, `z6` and `z7a` each carry one. Region 5's chapter has three, and they are the most reach-dependent in the corpus (a watershed split at Williams Lake River, plus a seasonal closure on the lower reach). |
| 2 | **Size limit: no minimum size in lakes** | Region 2 has `z2:trout_lake_no_minimum_size`; Region 5 has nothing. Region 1's equivalent is also missing — see `region-1.md`. |
| 3 | **Steelhead Management Changes** — Chilcotin River Watershed steelhead fisheries may be closed | conditional/advisory, but it tells the reader a printed opening may not hold |
| 4 | **Dean River Classified Waters specifics** | the provincial `zp:classified_waters_licence` now carries the one-licence-per-year rule for Non-Resident Aliens. Region 5 adds three more facts: no day limit for **Canadian residents**; a Non-Resident Alien may fish **only one classified section**; and a **non-guided** Non-Resident Alien on the **Class I Main Section (Crag Creek to signs 500 m upstream the canyon)** must enter an **annual limited entry draw**. |

### Confirmed droppable — already at water level

You were right about these.

| rule | where it already is |
|---|---|
| **Chilcotin River closure**, Oct 1 – Jun 10 downstream of Chilko | `r5:chilcotin_river@5-12+5-13+5-14` carries **both** windows — Apr 1 – Jun 30 upstream of Chilko, Oct 1 – Jun 10 downstream. |
| **Thin ice warning** — Dewar, Higgins, Irish, Simon, Skulow | all five carry it: `r5:dewar_lake@5-2`, `r5:higgins_lake@5-1`, `r5:irish_lake@5-1`, `r5:simon_lake@5-2`, `r5:skulow_lake@5-2`. Note the wording already drifts — three say *"Warning: dangerous thin ice due to aeration"* and two say *"Dangerous thin ice due to aeration"*, which is the `hazard` type argument in miniature. |

`z5:ice_fishing_huts_notice` should **stay** — it is a regional statement of an offence under the
Environmental Management Act, not a per-lake warning.

### Inconsistency — measured corpus-wide

`z5:kokanee_quota` is narrowed to lakes (`feature_types: ["lake"]`). Across all zone files the
identical sentence — *"Kokanee: N (none from streams)"* — is encoded **7 ways narrowed to lake**
(z3, z5, z6 ×2, z7a, z7b, z8) and **3 ways with no filter** (z1, z2, z4). The narrowed form is the
majority; z1, z2 and z4 are the outliers.

### Note on species coding

The Fraser sturgeon rules use `SG`, not `WSG`. `SG` is one of the six collective codes still in the
corpus (`SLV` 204, `BS` 52, `WF` 16, `SA` 2, `SG` 2, `P` 1) — so a rule coded `WSG` and one coded
`SG` will not compare. See the catalogue §3 on comparison-time expansion.
