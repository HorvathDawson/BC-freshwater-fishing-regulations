# Parsing a water into the rule catalogue

You are converting one row of the BC Freshwater Fishing Synopsis into typed rules.

**You do not write labels.** You pick a TYPE and fill its CONDITIONS. The displayed line is
generated from those by `pipeline/regs/parsing/catalogue.py`. A label typed beside a number drifts
from it — that is the defect this format exists to remove.

Read `pipeline/docs/18-how-regulations-are-stored.md` first. The synopsis' own key to these tables
is `data/curated/regulations/reference/water-specific-tables.md` — it defines the terms below and
you must follow it, not your intuition about what the words mean.

---

## Absolute rules — violations fail validation

1. **`verbatim` must be an exact contiguous substring of `regs_verbatim`.** Never reword, never
   stitch two sentences, never add a connective. If the printed text is a bullet under a heading,
   quote the run that contains both.
2. **`regs_verbatim` is the printed passage, unedited.** If you find yourself improving it, stop.
3. **One restriction per rule.** *"Bait ban and single barbless hook"* is two rules.
4. **Never invent a number.** Every `take`, `over_cm`, `under_cm`, `max_kmh`, `max_power_kw` must
   appear in that rule's own `verbatim`.
5. **If you cannot bind a location, say so** — set `needs_review` with a reason that names what is
   missing. An honest flag beats a wrong guess.

---

## The catalogue is TWO TIERS: six families, fifteen types

The family is how the reader's screen is sectioned. The type is what you choose.

```
RETENTION          what you may keep
  retention_limit            how many, of what, when, how caught, at what size
  stop_fishing_after_quota   taking the quota ends your day on that water

GEAR AND METHOD    how you may fish
  bait_restriction           what may go in the water to attract fish
  tackle_restriction         what may be attached to the line
  method_rule                angling · set line · spear · trap · ice · net · snag

VESSEL             boats
  vessel_rule                        propulsion (an ordered scale) · speed · towing
  angling_from_vessel_prohibited     you may boat here but not fish from the boat
  navigation_duty                    an obligation toward other vessels

LICENSING          paperwork and permission
  document_required          you must hold document D
  access_permission          WHO may fish here

CONDUCT
  handling_rule              what you must do with the fish after catching it

INFORMATION
  hazard · advisory · program_membership · facility
```

Two rules **within one family** may still be unrelated — a bait ban and a hook rule are both
gear-and-method and never compete. The family groups the display; the **type plus its dimension**
decides what competes. Never reach across a family.

**Choosing between them:** two rules share a type only if one could ever override the other. A bait
ban and a hook rule never compete — you obey both — so they are different types. A closure and a
quota are one subject at two values, so they are the **same** type.

### Type by what the rule DOES, not by the words it uses

These were each filed two ways in the old corpus. The right column is the rule:

| statement | type | why |
|---|---|---|
| *"No ice fishing"* | `method_rule(ice_fishing, permitted=false)` | it prohibits a METHOD. It says nothing about what you may keep and competes with no quota. |
| *"No powered boats"* | `vessel_rule(aspect=propulsion)` | restricts the boat, not the tackle |
| *"No angling from boats"* | `angling_from_vessel_prohibited` | restricts ANGLING, not boating — a water can allow motoring and forbid fishing from the boat |
| *"Class I/II water"* | `document_required` | a licence classification; the water's `Classified` symbol carries the fact |
| *"Youth/disabled accompanied water"* | `program_membership` | an ACCESS provision — who may be brought along, not what licence is held |
| *"Angling prohibited for non-guided non-resident aliens on Saturdays"* | `access_permission` | it restricts **who** may fish, not what may be kept. Filed as a closure it collides with quotas and with its own sibling. |
| *"Exempt from the spring closure"* | **not a type** — `exempts` on the rule it lifts | an exemption takes the type of whatever it removes |
| *"Bass: 0 quota, closed to fishing"* | `retention_limit(take=0, may_target=false)` | a closure IS a limit. Filed apart from quotas the override never fires. |

**Quick test:** `retention_limit` limits what you keep · `bait_restriction` what goes in the water ·
`tackle_restriction` what is on the line · `method_rule` how you fish · `vessel_rule` the boat ·
`document_required` what you must hold · `access_permission` who may fish · `handling_rule` the fish
after capture · the note types impose nothing.

---

## The distinction that matters most

The synopsis defines these two, and they are not the same:

| printed | means | encode |
|---|---|---|
| **Catch and Release** | *"You may fish for the named species, but you must release any that you catch."* | `take=0, may_target=true` |
| **No fishing for** | *"You may not **deliberately fish** for the species named **even if your intention is to release**."* | `take=0, may_target=false` |

`take=0` alone is ambiguous and validation will refuse it. **`may_target` is per species** — *"No
fishing for kokanee"* on a stream leaves that stream open for trout.

A whole-water closure is `species=ALL_GAME_FISH, take=0, may_target=false`.

---

## Conditions you will reach for

```
species        REQUIRED on retention_limit. Use a group (TROUT, CHAR, TROUT_CHAR, WHITEFISH,
               BASS, ALL_GAME_FISH) when the page names a group; leaf codes when it names fish.
               species=[] is an ERROR. "all other species" is species_except.
take           int | null. null = a size limit whose COUNT comes from the region. NOT zero.
unlimited      separate from take
may_target     see above. Required whenever take = 0.
period         daily (default) | possession | annual (licence year, Apr 1 - Mar 31) | monthly
per_daily      a possession MULTIPLIER, not a count
within         the rule_id of the limit this one sits inside
over_cm        a CAP on big fish        under_cm  a FLOOR under which fish go back
band           true only for "none BETWEEN x and y"
combined       "all species combined"
water          stream | lake            origin  hatchery | wild

REQUIRED ON THREE TYPES, and the commonest reason an entry is rejected. Each says WHICH WAY
the rule runs, and none of them can be inferred from the words afterwards:

allowed        bait_restriction and tackle_restriction. true or false, ALWAYS.
               "bait ban" is allowed=false. "roe may be used" is allowed=true. A permission
               is a rule too, so the field is never optional and never `permitted`.
aspect+level   vessel_rule. aspect is propulsion | speed | towing.
               For propulsion, `level` is required and is an ORDERED scale, strictest first:
                 none        no vessels at all          ("No vessels")
                 unpowered   no motor                   ("No powered boats")
                 electric    electric motors only       ("Electric motor only")
                 power_capped  a kW limit, with max_power_kw ("7.5 kW / 10 hp")
               `permitted: false` is NOT a substitute — "No vessels" and "No powered boats"
               are both a refusal and they are different rules.
method+permitted  method_rule. Both, always.
method         angling | set_lining | spear_fishing | crayfish_trapping | ice_fishing | netting
windows        A LIST OF STRINGS, copied as printed: ["Oct 1-June 30"]. NOT objects —
               {"start": ..., "end": ...} is rejected. [] means ALL YEAR. Dates INCLUSIVE.
windows_are    excepts  ONLY when the dates say when the rule does NOT apply
weekdays · from_time/to_time · angler_class · when_open
extent_text    the reach in the page's own words, when no split can express it
exempts        what this rule LIFTS
obligation     must (default) | should — "anglers are encouraged" is should, not law
```

## Extents — WHERE the rule applies

**EVERY RULE CARRIES ITS OWN `extents`. On the rule, never only on the entry.**

This is the single most-missed instruction in the format. On the first full parse 1,957 of
2,397 rules came back with none — the reach went on the entry and the rules were left bare —
and a rule with no extents binds to NO WATER AT ALL. 73% of the corpus resolved to nothing.

An entry-level extent narrows the ROW ("FRASER RIVER (upstream of the CPR Bridge at Mission)").
It does not give a rule its reach. If a rule covers the whole of the water the row names, say
so explicitly:

```json
"extents": [{"op": "whole"}]
```

Most rules are exactly that. Write it out every time.

A list of extents is a UNION ("this reach plus that one"). The ops:

```
whole            the entire water. NO split ids.
upstream_of      exactly 1 split id
downstream_of    exactly 1 split id
between          exactly 2 split ids
within           an area, not a reach — area_id or area_kind
```

**Split ids come ONLY from that item's "Bindable boundaries" menu.** Never invent one, never
reuse an id you saw on another item. Ingest refuses an id the water cannot bind.

**A cut-point can have two names.** The menu shows them as:

```
- `gauge__08NM247`  — 08NM247 · Okanagan River Below Mcintyre Dam  [split]
     — also written `okanagan_river__mcintyre_dam`; BIND THE ID ABOVE
```

Those are ONE physical point. The page says *"below McIntyre Dam"*; the id that survived the build
is a gauge number. **Bind the id on the first line** — the canonical one. Binding the alias is
accepted and rewritten, but naming the canonical id directly is what you should do.

### `within_area` — limiting a reach to a polygon

`within_area` is a FIELD on an extent, not an op. It intersects whatever the extent already
selected with an area, and it is applied **after** the tributary walk — so it limits the
tributaries too.

Use it when the page bounds a rule **by a region or park line rather than by a point on the
water**. Those have no cut-point and cannot be expressed as a reach:

```json
"extents": [{"op": "whole", "within_area": "area:region:5"}]
```

*"That part of the Fraser River within Region 5"* is the whole Fraser intersected with Region 5 —
not `between` two splits, because the region boundary is not a cut-point on the river. Region 6
has **zero** Fraser mainstem sections, which is why a bounded reach could never have worked there.

Combine it freely with an op: `{"op": "upstream_of", "splits": ["x"], "within_area": "area:region:5"}`.

**When nothing fits.** Do not force a binding. Put the page's words in `extent_text`, record the
phrase in `unresolved_locators`, set `needs_review` with a `review_reason`. A rule bound to the
wrong point is far worse than one visibly sent to review.

### Size polarity — read the sentence, not the preposition

```
"not more than 1 over 50 cm"   you MAY keep one big one     take=1, over_cm=50, within=<parent>
"no trout over 50 cm"          you may keep NO big ones     take=0, may_target=true, over_cm=50
"1 bull trout over 60 cm"      the one you keep must BE big take=1, under_cm=60, within=<parent>
```

The word "over" appears in all three and maps to a different field in each. Ask **which fish go
back**: they are the ones outside the bound you store.

---

## Rules the page imposes on you

* **Bait bans and hook rules carry NO `species`.** *"During the period when bait is banned it is
  banned for all angling and for all species."* *"Trout/char catch and release, bait ban"* is two
  rules and the bait ban is on the **whole river** — the species belongs to the release clause.
* **But a bait or hook rule MAY be scoped to what you are fishing FOR** — a stream can carry a
  salmon bait ban and nothing else. That is **`when_targeting`**, not `species`. The difference:
  `species` would mean the ban protects those fish; `when_targeting` means the ban applies to that
  fishery. A salmon bait ban and a general bait ban coexist; neither displaces the other.
* **`electric_only` already means max 7.5 kW.** Set `max_power_kw` only if the water differs.
* **The MU column is a locator, never an extent.** *"Not all applicable M.U.'s may be listed."*
* **An asterisk in the first column** means every rule reaches tributaries → `includes_tributaries`
  on the ENTRY. **An asterisk after one regulation** means only that one does → on the RULE.
* **(CW)** is a symbol on the entry, not a rule. The licence obligation is provincial.

---

## Output

```json
{
  "entry_id": "r3:tranquille_lake@3-29",
  "name": "TRANQUILLE LAKE",
  "display_name": "Tranquille Lake",
  "region": "3",
  "regs_verbatim": "<the printed row, unedited>",
  "source_pages": [29],
  "symbols": ["Classified"],
  "matched": ["gnis:12345"],
  "extents": [{"op": "whole"}],
  "includes_tributaries": null,
  "rules": [
    {
      "rule_id": "tranquille_lake.r1",
      "type": "retention_limit",
      "verbatim": "Rainbow trout daily quota = 8",
      "species": ["RB"],
      "take": 8
    }
  ]
}
```

Validate before submitting:

```bash
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.validate_catalogue batch.json candidate.json
```

Exit 0 means every entry passed. Fix what it reports and run it again — do not submit a candidate
that has not come back clean.
