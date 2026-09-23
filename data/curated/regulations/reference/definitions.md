# Definitions — 2025-2027 BC Freshwater Fishing Regulations Synopsis

Transcribed verbatim from the synopsis "Definitions" page (p. 80). **This is source text.
Do not edit to fit the model; change the model.**

These definitions are normative for the rule catalogue (`pipeline/docs/17-rule-catalogue.md`).
Where a catalogue type or condition depends on one, it is cross-referenced in that doc.

---

**above**: when used in reference to a lake or stream means "upstream of".

**adipose fin**: see diagram on page 13.

**adult chinook salmon**: see page 77.

**anadromous**: swimming up rivers from the ocean to spawn (for example, steelhead).

**angling**: fishing with a hook and line, with or without a rod. **It does not include fishing
with a set line.**

**annual**: the licence year, beginning April 1 and ending on March 31.

**artificial fly**: a single-pointed hook that is dressed only with fur, feathers, hair,
textiles, tinsel and/or wire, and to which no external weight or external attracting device is
attached. Two or more hooks tied in tandem is not permitted. Where gear is restricted to
artificial flies, floats and sinkers **may** be attached to the line. Where areas are restricted
to "fly fish only" floats and sinkers **may not** be attached to the line or fly.

**bait**: see page 8.

**barbless hook**: a hook without a barb on any part of the hook, including both the point and
shank. Existing tackle may be modified by completely removing the barb, or by crimping the barb
down so that its point is flush against the shaft.

**below**: when used in reference to a lake or stream means "downstream of".

**chumming**: see page 8.

**Classified Waters**: see page 7.

**confluence**: a place where two streams meet.

**creek**: see streams.

**daily quota**: the maximum number of fish of a given species, group of species, **or size
class** that you may keep in one calendar day.

**day**: a legal fishing day runs from midnight one night to midnight the following night.

**fish**: means fin fish, shellfish and crustaceans (such as crayfish) in any life stage,
including eggs.

**fishing**: means fishing for, catching or attempting to catch fish **by any method**.

**fly fishing**: angling with a line to which only an artificial fly is attached (floats,
sinkers, or attracting devices may not be attached to the line when fishing is restricted to
"fly fishing only").

**hatchery trout**: in some waters, hatchery trout may be harvested but wild trout must be
released. In these waters, hatchery trout are marked before stocking by removal of their adipose
fin. Therefore, these hatchery trout must have a healed scar in place of the missing fin.

**kokanee**: a land-locked sockeye salmon.

**landed immigrant**: a permanent resident of Canada (as defined in federal statute).

**licence year**: the period beginning April 1 and ending March 31.

**Management Unit**: a subdivision of a region.

**max**: abbreviation for maximum. · **min**: abbreviation for minimum.

**non-resident**: means you are not a "resident", but (a) you are a Canadian citizen or landed
immigrant, OR (b) your primary residence is in Canada, AND you have resided in Canada for the
immediately preceding 12 months.

**non-resident alien**: means you are neither a "resident" nor a "non-resident".

**ordinary residence**: a residential dwelling where a person normally lives, with all
associated connotations including a permanent mailing address, telephone number, furnishings and
storage of automobile; the address on one's driver's licence and automobile registration, where
one is registered to vote. A motor home or vessel at a campsite or marina is not considered to
be an ordinary residence.

**possession quota**: the number of fish of any species that an angler may have in their
possession at any given time, **EXCEPT at place of ordinary residence**. In most instances, the
possession quota is two times the daily quota. See Tables for exceptions.

**resident**: means your primary residence is in British Columbia, AND (a) you are a Canadian
citizen or landed immigrant, AND have been physically present in B.C. for the greater portion of
each of 6 calendar months out of the immediately preceding 12 calendar months, OR (b) you are
NOT a Canadian citizen or landed immigrant, but have been physically present in B.C. for the
greater portion of each of the immediately preceding 12 calendar months.

**river**: see streams.

**set line**: a fishing line that is left unattended in the water.

**single hook**: a hook having only one point. (In contrast, a treble hook is a hook having three
points on a common shaft.) **Note: use of a treble hook is permitted unless "single hook" is
specified.**

**slough**: a stagnant channel or backwater.

**snagging (foul hooking)**: hooking a fish in any other part of its body other than the mouth.
Attempting to snag fish of any species is prohibited. Any fish willfully or accidently snagged
must be released immediately.

**spear fishing**: fishing with a spear or arrow that is propelled by a spring, an elastic band,
compressed air or a bow or by hand.

**sport fishing**: fishing for recreation and not for sale or barter. Sport fishing includes
**angling, spear fishing, set lining and crayfish trapping**.

**steelhead**: **a rainbow trout longer than 50 cm in waters where anadromous rainbow trout are
found.** Both hatchery and wild steelhead may be found in B.C. waters.

**streams**: flowing waters (rivers, sloughs and creeks). Note that standing water behind a
beaver dam on a stream is considered part of the stream. **A stream flowing through the drawdown
portion of a reservoir basin is still considered to be a stream, not part of the reservoir.**

**stream mouth**: the point at which the surface elevation of a stream and the water body into
which it flows are the same, except as posted by signs or markers, or otherwise defined.

**tributaries**: all streams that contribute to a larger stream or to a lake.

**trout/char**: **all regulations that apply to trout (as a group) also apply to char unless
char are specifically excluded.**

**watershed**: all the streams and lakes that drain the land into a named waterbody, including
the named waterbody itself.

**wild trout**: in some waters, hatchery trout may be harvested but wild trout must be released.
In these waters, wild trout will not be marked as hatchery fish and will have a normal adipose
fin, or will have an unhealed scar in place of that fin, if missing.

---

## Freshwater game fish — the closed list

This is the definition of "all game fish". A rule that applies to everything applies to **this
set**, not to the empty set. See `ALL_GAME_FISH` in the catalogue.

| TROUT | CHAR | WHITEFISH | BASS | OTHER |
|---|---|---|---|---|
| Rainbow Trout | Dolly Varden | Lake Whitefish | Largemouth Bass | Kokanee |
| Steelhead | Bull Trout \* | Mountain Whitefish | Smallmouth Bass | Arctic Grayling |
| Cutthroat Trout | Lake Trout | | | Burbot (Ling) |
| Brown Trout | Brook Trout | | | White Sturgeon |
| | | | | Black Crappie |
| | | | | Northern Pike |
| | | | | Yellow Perch |
| | | | | Walleye |
| | | | | Goldeye |
| | | | | Inconnu |
| | | | | Crayfish |

\* **Any bull trout that you catch and keep must be counted as part of your Dolly Varden quota.**

---

## What these definitions decide

Each of these is a modelling consequence, not a paraphrase.

| definition | consequence |
|---|---|
| **angling** excludes set lining | `no_angling` ≠ `no_fishing`. 30 corpus rules are angling-worded, 605 fishing-worded. A "No angling" rule leaves set lining lawful. |
| **fishing** = any method; **sport fishing** = angling ∪ spear ∪ set line ∪ crayfish trap | gives the closed `method` vocabulary |
| **trout/char** — trout rules apply to char unless char excluded | trout-group expansion **must include char**; this is why `TRT` and `SLV` are not independent |
| **steelhead** = rainbow trout > 50 cm **in waters where anadromous rainbow trout are found** | steelhead is a **conditional size class of rainbow**, not a sibling species. `ST ∩ RB ≠ ∅` — but only where the water holds anadromous rainbow, which this corpus cannot determine. See the caution in the catalogue §3. |
| **bull trout** counted in the Dolly Varden quota | `BT` and `DV` share a quota bucket |
| **artificial fly** allows floats/sinkers; **fly fishing** does not | confirms the two `fly_only.tackle` values must never merge |
| **single hook** = one point; treble permitted unless specified | the provincial baseline for `hook_restriction.count` |
| **barbless** = no barb on point **and** shank | |
| **daily quota** may be per **size class** | a size class is a quota subject, not only a filter |
| **possession quota** excepts place of ordinary residence | a condition on `possession_multiple` |
| **annual** = licence year Apr 1 – Mar 31 | annual windows are licence-year, not calendar |
| **above/below** = upstream_of / downstream_of | extent vocabulary |
| **streams** include sloughs, creeks, beaver-dam standing water, **and reservoir drawdown reaches** | a drawdown reach is a **stream**; see the `r4:upper_arrow_lake_drawdown_area` defect |
| **watershed** includes the named waterbody itself | needed for the Fraser/Thompson watershed areas |
| **resident / non-resident / non-resident alien** | the `residency` axis of `Who` (`catalogue.py`) |
