# Rule statement standards

One regulation must read the same way everywhere in the corpus. `details` is the line a curator and
the app read on its own, and it is what `split_bundled_gear` pattern-matches — so a rule written five
different ways is five different things to every consumer downstream.

Measured on the 2026-09-01 corpus (3,047 rules) before this standard existed:

| | |
|---|---|
| distinct quota grammars | **80** (`daily quota = 2` · `daily quota 2` · `quota 2` · `quota = 2`) |
| ways to write one motor limit | **3** (`Electric motor only, max 7.5 kW` · `- max 7.5 kW` · `(max 7.5 kW)`) |
| ways to write one engine limit | **3** (`- 7.5 kW (10 hp)` · `7.5 kW (10 hp)` · `, 7.5 kW (10 hp)`) |
| statements filed under **two different `restriction_type`s** | **6 statements, 128 rules** |

None of that variance carries meaning. All of it breaks grouping, search and normalisation.

---

## 1. The shape

    <Subject> <restriction> (<qualifier>)

* **Sentence case.** `No fishing`, never `No Fishing` or `NO FISHING`.
* **Qualifiers go in parentheses at the end** — never after a comma, never after a dash.
  `Trout daily quota = 2 (none under 30 cm)`, never `Trout daily quota = 2, none under 30 cm`.
* **Never drop the subject.** See `PARSE_PROMPT.md` → "`details` MUST KEEP ITS SUBJECT".
* **One restriction per rule.** See `PARSE_PROMPT.md` → "ONE RESTRICTION PER RULE".

## 2. Canonical forms

### harvest

    <Subject> daily quota = <N>                        Trout daily quota = 2
    <Subject> daily quota = <N> (<qualifier>)          Trout daily quota = 2 (none under 30 cm)
    <Subject> daily quota = unlimited                  Bass daily quota = unlimited
    <Subject> annual quota = <N>                       Rainbow trout annual quota = 5
    <Subject> possession quota = <N>                   Chinook possession quota = 4
    <Subject> catch and release                        Bull trout catch and release
    <Subject> catch and release (<qualifier>)          Trout/char catch and release (mainstem only)
    No <subject> over|under <N> cm                     No wild trout over 50 cm

`= ` is mandatory before a quota number. `release` alone is **not** a form — write `catch and release`.

### gear_restriction

    Bait ban · Single barbless hook · Single hook · Barbless hook · Artificial fly only
    Fly fishing only · Artificial lure only · No set lines · Max hook size <N> mm

### vessel_restriction

    No powered boats · No vessels · No towing · No angling from boats
    Electric motor only (max <N> kW)
    Engine power restriction (<N> kW / <N> hp)
    Speed restriction (<N> km/h)

### closure

    No fishing
    No fishing for <species>                           No fishing for bass
    No ice fishing

### licensing

    Class I water · Class II water · Steelhead Stamp mandatory

### note

Anything that is not a restriction: cross-references, facility info, access provisions
(`Youth/disabled accompanied water`), and **exemptions** (`Exempt from bait ban`,
`Exempt from spring closure`).

## 3. `restriction_type` is decided by what the rule DOES

Not by the words it uses. These six were filed both ways in the corpus; the right column is the rule:

| statement | type | why |
|---|---|---|
| `No ice fishing` | **closure** | it prohibits fishing, in a season. Not a gear rule. |
| `No powered boats` | **vessel_restriction** | restricts the vessel, not the tackle. |
| `Class I/II water` | **licensing** | a licence classification. |
| `Youth/disabled accompanied water` | **note** | an ACCESS provision — who may be brought along, not what licence is held. |
| `Angling prohibited for <group> on <days>` | **closure** | prohibits fishing. |
| `Exempt from <X>` | **note** | it removes a restriction rather than imposing one. |

Quick test: *closure* stops fishing · *harvest* limits what you keep · *gear_restriction* limits
tackle · *vessel_restriction* limits the boat · *licensing* decides who may fish · *note* imposes
nothing.
