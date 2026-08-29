# 17 — Regulation model: defaults, types, closures, species

What the bundle must carry so a client can answer *"can I fish this, for what I'm after, today"*.
Companion to `16-bundle-and-clients.md`, which covers delivery. This one is about the data.

---

## 1. Two kinds of regulation, and only one is attached to a water

`06-zone-regulations.md` already settled this and it still holds:

1. **Water-specific** — "Adams River: quota 2". Belongs to a section. Attached.
2. **Base / zone / provincial** — "Region 4 trout limit is 5", "no fishing in Ecological Reserves".
   These are rules for fishing *in this area*, not about one waterbody. **Resolved by location, never
   copied onto every section.**

Copying defaults onto sections would multiply 235 rules across 49,639 sections and make every
default edit a full rebuild. The overlay stays an overlay.

### What already exists

V1 built these and v2 has not reimplemented them — `archive/pipeline/enrichment/base_regulations.json`,
235 entries:

| source | count | targeting |
|---|---:|---|
| `zone` | 228 | `zone_ids` (region) + `feature_types` (stream/lake/manmade) |
| `provincial` | 3 | `admin_targets` — national parks, ecological reserves, with `buffer_m` |
| `land_access` | 2 | admin layers |
| `municipal` | 2 | admin layers |

**This file is an asset, not legacy.** It is hand-sourced from the synopsis preamble with page
citations in `notes`. Port the data; rebuild the assigner against MUs and the `area` registry items
(v2 already has 4 `area` items and an `area_catalog.gpkg`).

### The lift mechanism already exists too

A water-specific rule can **lift** a default: `exempts_from` is on 81 rules today, over a vocabulary of
seven (`spring_closure` 63, `bull_trout_release`, `bait_ban`, `trout_char_release`, `summer_closure`,
`single_barbless_hook`, `kokanee_stream_quota`).

So the resolution order for any section is:

```
applicable = base regs for (region, MU, feature_type, admin areas)
           - those any water-specific rule here exempts_from
           + the water-specific rules themselves
```

**This is the only correct way to answer "is it open".** A closure rule list alone cannot say —
"open" is often the *absence* of a default plus an exemption. `curation-review/API.md` makes the same
point about `exempts_from`, and the bundle has to encode it, not flatten it away.

---

## 2. Splitting by type

Already true in v2 — every rule carries a `restriction_type`:

| type | rules |
|---|---:|
| harvest | 807 |
| gear_restriction | 787 |
| closure | 690 |
| vessel_restriction | 391 |
| note | 263 |
| licensing | 100 |

So "show gear separately from harvest separately from closures" is a grouping, not new parsing.

**The one piece of work:** v1's base regs use a *different, free-text* vocabulary —
`Quota` 86, `Closed` 35, `Catch and Release` 32, `Notice` 29, `Possession Quota` 14,
`Gear Restriction` 12, `Annual Quota` 8, `Licence Requirement` 6, `Bait Restriction` 5, and five more.
Fourteen strings against v2's six enums.

They must be reconciled to **one vocabulary** before the bundle, or the client renders a "Gear
Restriction" section and a "gear_restriction" section side by side. Proposed mapping:

```
closure           <- Closed, Time Restriction
harvest           <- Quota, Possession Quota, Annual Quota, Catch and Release,
                     Quota Enforcement, Protected Species
gear_restriction  <- Gear Restriction, Bait Restriction, Bait Exception
licensing         <- Licence Requirement
note              <- Notice, Advisory
vessel_restriction (v2 only)
```

`Catch and Release` → `harvest` is the judgement call in there: it is a limit of zero, not a gear rule.
Worth confirming against how it should read in the UI.

---

## 3. The closure layer — and the blocker in front of it

The ask: colour the map by whether a water is closed, **accounting for dates**.

**This cannot be built from today's data.**

```
rules with dates : 584 of 3038  (19%)
date format      : free text  -->  "July 15-Aug 31"
```

Nineteen percent, and the 19% is prose. There is no way to compute "is this closed today" from
`"July 15-Aug 31"` without a parse step, and no way at all for the 81% with no dates — where the
absence means *either* year-round *or* the parser dropped it, and those are not distinguishable.

### What has to happen first

1. **A structured date model.** Something like
   `{from: {month, day}, to: {month, day}, years?: [...], annual: bool}`, with support for the shapes
   the synopsis actually uses — recurring annual windows, one-off in-season closures, "until further
   notice".
2. **A normaliser** over the 584 existing strings, with the residue surfaced for curation rather than
   silently dropped. Same discipline as the extents: an unparseable date is a curation item, not a
   guess.
3. **An explicit "year-round" marker** so no-dates means *year-round* rather than *unknown*.

### Then the layer

Per section, for a given date, a status computed from §1's resolution:

| status | meaning |
|---|---|
| `closed` | a closure applies on that date (water-specific or base, not exempted) |
| `restricted` | open, but gear/bait/quota constraints apply |
| `open` | no closure, defaults only |
| `unknown` | a rule applies whose dates could not be resolved |

**`unknown` must exist and must be visible.** Colouring an unparsed rule as "open" is the one failure
mode with real consequences for someone standing in a river.

Where it is computed is an open question: pre-computing a status per section per day is 49,639 × 365
and rebuilds on every data change; computing on the client from structured dates keeps the bundle
small and makes the date picker instant. **Client-side, from structured dates, is the recommendation.**

---

## 4. Filtering to the fish you're after

The ask: let someone see only what matters for their target species.

```
rules with NO species : 2221 of 3038  (73%)
top codes             : RB 444, CT 325, CCT 310, WCT 308, GB 308, GT 308, SLV 157, BT 74, LT 70
```

**73% of rules name no species, and that is not missing data.** A closure closes the water to
everyone. A bait ban binds whoever is fishing. A vessel restriction has nothing to do with fish.

So a naive "filter to Rainbow Trout" would hide the closure that stops the trip. The filter has to be
**species-aware, not species-blind**:

| type | behaviour under a species filter |
|---|---|
| `harvest` | filtered — this is where species actually discriminates |
| `closure` | **always shown**. A closed water is closed |
| `gear_restriction` | always shown — binds the angler, not the fish |
| `vessel_restriction`, `licensing` | always shown |
| `note` | shown, de-emphasised |

Framed for the reader as **"what applies to you, with your Rainbow Trout limits highlighted"**, not
"only Rainbow Trout rules". The filter changes emphasis and ordering; it never hides a prohibition.

Needed to support it: the species list per water (derivable from `rule.species` plus regional
defaults), and a species → common-name table with the group codes resolved (`CT`/`CCT`/`WCT` are
cutthroat variants; `GB`/`GT` group codes — `pipeline/…/species` and the review app's picker already
carry this).

---

## 5. What this adds to the bundle

On top of `16`'s tables:

| table | holds |
|---|---|
| `base_rule` | the 235 zone/provincial defaults, typed to the unified vocabulary |
| `base_rule_target` | zone/MU/feature-type/admin-area targeting |
| `rule_exemption` | which defaults a water-specific rule lifts (`exempts_from`) |
| `rule_date` | **structured** windows; `annual` flag; `unresolved` flag |
| `rule_species` | species per rule, group codes expanded |
| `species` | code → common name, group membership |

Everything here is small. It changes the bundle's shape, not its size.

## Open questions

1. **`Catch and Release` → `harvest`?** (§2) It is a zero limit, but it may read better beside gear.
2. **Who computes closure status** — pre-computed per section/day, or client-side from structured
   dates? Recommendation: client-side.
3. **How much date work is in scope now?** The structured model gates the closure layer entirely; the
   584 strings need a normaliser and a curation pass for the residue.
4. **Do defaults get their own curation surface?** The 235 base rules are hand-maintained and
   currently have no review UI at all.
