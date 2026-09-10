# Region 1 — Vancouver Island (including Haida Gwaii)

Transcribed verbatim from the 2025-2027 synopsis Region 1 chapter. **Source text — do not edit
to fit the model.** Companion to `definitions.md` and `provincial-regulations.md`.

> **Haida Gwaii (MUs 6-12, 6-13) is administered from Region 1 as of this edition**, and its
> daily quotas are printed in this chapter. See `pipeline/docs/17-rule-catalogue.md` §9.

---

## General Regulations

**Summer closure:** No Fishing in any stream in Management Units 1-1 to 1-6 from July 15 – Aug 31
(see water specific regulations table for exceptions).

**Single barbless hook:** must be used in all streams of Region 1, all year.

**Bait ban:** applies to all streams of Region 1, all year, with some important exceptions. Check
the tables.

**Bait ban:** applies to all streams in Management Units 6-12 and 6-13, Nov 1 – Apr 30.

## Region 1 Daily Quotas — **(excluding Haida Gwaii)**

(See tables for exceptions)

```
Trout: 4, but not more than
  • 1 over 50 cm (2 hatchery steelhead over 50 cm allowed, see table for exceptions)
  • 2 from streams (must be hatchery)
And you must release:
  • All wild steelhead
  • All wild trout from streams
  • All char (includes Dolly Varden)

NOTE: There is no general minimum size limit for trout in lakes or hatchery origin
      trout in streams.

Bass:           unlimited (see water specific regulations table for exceptions and
                mercury advisory)
Crayfish:       25
Kokanee:        5 (none from streams)
White Sturgeon: catch and release only
Yellow perch:   unlimited
```

## Wild Trout Release — all streams regulation

> Anglers note there is a region wide regulation **(excluding Haida Gwaii)** requiring the release
> of all wild origin trout in streams. This regulation allows only hatchery origin trout in
> streams to be harvested. In Region 1, hatchery origin trout from streams can be distinguished
> from wild origin trout by the presence of a healed scar in place of the adipose fin. **Please
> note, this regulation does not apply to lakes.** For more information please contact regional
> fisheries staff at (250) 751-7220.

## Haida Gwaii Daily Quotas

(See tables for exceptions)

```
Trout/char: 5, but not more than
  • 1 over 50 cm
  • 3 Dolly Varden
  • 2 from streams
And you must release:
  • Trout/char under 30 cm from streams
  • All wild steelhead

Kokanee: 10 (none from streams)
```

## Possession Quotas

Possession quotas = 2 daily quotas

## Annual Quotas

Annual catch quota for all B.C.: 10 steelhead per licence year (only hatchery steelhead may be
retained in B.C.)

## Daily & Annual Quotas for Salmon

Please refer to the NOTICE on page 77 for Salmon Regulations.

## Mercury Advisory

> Mercury levels in larger Smallmouth Bass in lakes on Vancouver Island and the Gulf Islands may
> be above national guidelines. Mercury levels tend to increase with the size of the fish and
> larger Smallmouth Bass generally have higher levels of mercury. The general public, especially
> children and women of child bearing age, including pregnant and breastfeeding women, are
> recommended to limit their consumption of Smallmouth Bass.

## Cutthroat Trout Reward Tagging Program

> **Comox Lake, Cowichan Lake, Horne Lake and Oyster River:** $100 reward tags are being used to
> assess the cutthroat trout fishery. Refer to page 76 for instructions on what to do if you catch
> a fish with a reward tag.

---

## Checked against the curated entries

`data/curated/regulations/entries/zones/region-1.json` — 18 entries, 20 rules, plus the 9
`z6:hg_*` rules that belong in this chapter.

### Missing — in the synopsis, not in the entries

| # | rule | note |
|---|---|---|
| 1 | **"There is no general minimum size limit for trout in lakes or hatchery origin trout in streams"** | a Region 1 rule, distinct from the provincial `zp:no_general_minimum_size`, because it names *lakes* and *hatchery origin trout in streams* specifically |
| 2 | **Cutthroat Trout Reward Tagging Program** (Comox, Cowichan, Horne, Oyster River) | Region 6 has its equivalent (`z6:r6_tagging_program`); Region 1's is absent |

### Wrong — in the entries, contradicted by the synopsis

| # | issue | detail |
|---|---|---|
| 3 | **`z1:*` quotas must EXCLUDE Haida Gwaii** | the chapter header reads *"Region 1 Daily Quotas **(excluding Haida Gwaii)**"* and the Wild Trout Release note repeats the exclusion. Once the remap puts Haida Gwaii inside `area:region:1`, every `z1:` quota rule reaches it — and Haida Gwaii has its **own** quotas (trout/char 5 vs trout 4; kokanee 10 vs 5; char kept, not released). Binding both gives contradictory quotas on 25,772 sections. |
| 4 | **duplicate kokanee closure** | `z1:kokanee_quota.r2` and `z1:kokanee_streams_closed.r1` are the same rule, both authored from *"Kokanee: 5 (none from streams)"*, both binding Region 1 streams. The reader sees it twice. |
| 5 | **`z1:trout_daily_quota` says "all species combined"** | the synopsis says only *"Trout: 4"*. Region 1 releases all char separately, so "combined" is an interpolation — harmless here, but it is authored text asserting something the source does not say. |
| 6 | **`z1:single_barbless_hook_streams` names the Haida Gwaii MU group explicitly** | the 2025-2027 text says only *"all streams of Region 1"*. Once Haida Gwaii is inside Region 1 the second extent is redundant; keeping it hides whether the remap worked. Same for `z1:possession_quota` and `z1:steelhead_annual_quota`. |

### Ambiguous — needs a human eye on the printed page

| # | question |
|---|---|
| 7 | **Does the all-year Region 1 bait ban apply to Haida Gwaii?** The chapter lists a *separate* Haida Gwaii bait ban for Nov 1 – Apr 30. If the all-year ban covered Haida Gwaii the seasonal one would be redundant — which implies the all-year ban excludes it. But unlike the quotas, the bait ban line carries no "(excluding Haida Gwaii)". Currently `z1:bait_ban_streams` binds all of Region 1 and `z6:hg_bait_ban_streams` binds Haida Gwaii Nov–Apr, so Haida Gwaii gets both. |

### What this chapter proves about the model

**The cascade handles "(excluding Haida Gwaii)" without any subtraction operator.** Haida Gwaii's
rules sit at the MU-group tier and Region 1's at the region tier, with
`sections(hg) ⊊ sections(region 1)` — so the Haida Gwaii rule wins on Haida Gwaii and the Region
1 rule renders struck through beside it. That is the display the printed chapter implies, and an
`excludes` list would destroy it: an excluded rule is invisible, and the requirement is that a
displaced rule stays on screen with its reason.

**The bait ban is the proof.** Region 1 bans bait in all its streams all year; Haida Gwaii bans
it Nov 1 – Apr 30. The seasonal rule is Haida Gwaii's *complete* statement on bait, not an
addition to the all-year one — so a finer-tier rule must replace the coarser one **wholesale**,
carrying its own dates. Merging the windows would leave the all-year ban standing on Haida Gwaii
from May to October, which the two parallel bullets rule out. Only *species* produce a residual;
dates do not. Recorded in `pipeline/docs/17-rule-catalogue.md` §9.

A subtraction mechanism is still wanted, but only for carve-outs with **no replacement rule** —
`z7b:kokanee_closed_streams` ("except the Peace") and `z5:spring_stream_closure` (a watershed
intersection). Those are area-definition problems, not a general operator.
