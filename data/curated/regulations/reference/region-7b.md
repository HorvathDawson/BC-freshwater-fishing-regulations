# Region 7B — Peace (Zone B)

Transcribed verbatim from the 2025-2027 synopsis Region 7 Zone B chapter. **Source text.**

---

## Notices

**PEACE RIVER** The Peace River is no longer navigable past the Site C construction site. Avoid
boat travel between 2 km upstream of the dam site (warning signs are on shore) and the downstream
construction bridge. See https://www.sitecproject.com/boating for details.

**ICE FISHING HUTS** should have the owner's contact information displayed in a prominent location
when left unoccupied. Failure to remove ice fishing huts from lakes before spring breakup is an offence under
the Environmental Management Act.

**THIN ICE AND OPEN WATER** Due to aeration projects, dangerous thin ice and open water may exist
on **Inga and Sundance lakes**. **It is prohibited to enter within the fenced area surrounding the
aerator.**

NOTICE TO ANGLERS BC Hydro is tracking the movement of fish throughout the Site C Reservoir, Peace
River and its tributaries using acoustic and radio telemetry to gather valuable data on the
potential effects of the Site C project on fish. Anglers who catch tagged fish are encouraged to
return them to the water if possible, or return the tag by e-mailing tagreturns@golder.com or
calling (250) 785-9281. If tags come from anywhere other than the Site C Reservoir, Peace River
and its tributaries, please call the regional Ministry office in Fort St. John at (250) 787-3415.

## General Zone B Regulations

**Single barbless hook:** must be used in all streams of Zone B, all year. See definition of
“streams” on page 80.

**Bait ban:** applies to all streams of Zone B, all year.

**Fin fish:** (as defined on page 8 under “Rules on Bait Usage”) may not be used as bait in any
waters of Zone B.

**Set lining:** is not permitted in Zone B.

## Zone B Daily Quotas

```
Trout/char: 5, but not more than
  • 1 over 50 cm      • 2 from streams      • 2 lake trout      • 1 bull trout
NOTE: Bull trout may only be retained from Oct 16-Aug 14. These fish may only be from the
      Liard River watershed (or other specified waters) and only 30-50 cm in length.
And you must release:
  • Rainbow trout of any size from streams, May 1-June 15
  • Lake trout under 30 cm
  • Lake trout of any size, Sept 15-Oct 31
  • Bull trout from the Liard River watershed Aug 15-Oct 15, and from the Peace River
    watershed all year

Arctic grayling: 2 (none under 30 cm and only 1 over 45 cm)
And you must release:
  • any size, May 1-June 15
  • all from Williston Lake and its tributaries

Burbot: 5      Goldeye: 10      Inconnu: 1
Kokanee: 10 (none from streams, except Peace River)
Northern pike: 3 (only 1 over 90 cm)
Walleye: 3 (only 1 over 70 cm) — release all from streams, Apr 1-May 15
Whitefish: 15 (all species combined)      Yellow perch: 5
```

## Possession Quotas

2 daily quotas for most species. **Exception: 1 daily quota for Arctic grayling, bull trout and
lake trout.**

> **NOTE:** Bull trout and Dolly Varden are two distinct species. Since only bull trout are found
> in the Peace Region, we have removed references to Dolly Varden here.

---

## Checked against the curated entries

`region-7b.json` — 30 entries.

Grayling, goldeye, inconnu, pike, walleye, whitefish, perch, kokanee and all three possession
exceptions are correct — including the two-part grayling limit (*none under 30 cm and only 1 over
45 cm*) and the walleye stream release.

### Missing — the entire bull trout treatment

| # | rule | note |
|---|---|---|
| 1 | **"1 bull trout"** — the daily sub-limit | **`region-7b.json` has no `bull_trout_limit` entry at all.** It carries `z7b:bull_trout_possession` and `z7b:bull_trout_dolly_varden_notice`, so bull trout is named twice — but the actual daily limit of 1 is absent. Region 7A has the exact parallel rule (`z7a:bull_trout_limit`) with its full qualifier. |
| 2 | **the bull trout retention qualifier** | *"may only be retained Oct 16 – Aug 14 … only from the Liard River watershed (or other specified waters) and only 30–50 cm in length"* — three conditions, none captured. Region 7A's equivalent **is** captured in full, which makes this a straight omission rather than a modelling limit. |
| 3 | **release bull trout from the Liard River watershed Aug 15 – Oct 15, and from the Peace River watershed all year** | the fourth release rule. `area:watershed:liard_river` **already exists** — this is the one watershed-scoped rule in the corpus that could be bound today. |

**Net effect: an angler in Zone B is shown no bull trout limit, no retention window, no size band
and no closure.** The possession rule tells them bull trout possession is 1 daily quota, of a daily
quota that is never stated.

### Missing — advisories

| # | rule |
|---|---|
| 4 | **Peace River not navigable past Site C** — a safety notice with a specific reach (2 km upstream of the dam site to the downstream construction bridge) |
| 5 | **"It is prohibited to enter within the fenced area surrounding the aerator"** on Inga and Sundance lakes — this is a **prohibition**, not a warning, and it is the only part of the thin-ice notice that is enforceable |

### Confirmed droppable — already at water level

**Thin ice on Inga and Sundance** — `r7:inga_lake@7-34` (*"Warning: thin ice due to aeration"*) and
`r7:sundance_lake@7-32` (*"Dangerous thin ice warning (aeration)"*). Note the wording differs
between them again. The **fenced-area prohibition** above is NOT covered by either.

**Release all grayling from Williston Lake and its tributaries** —
`r7:williston_lake_in_zone_b@7-31+7-36` carries *"Catch and release, including tributaries"* for
`GR`. Covered.
