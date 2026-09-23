# Reviewing a parsed entry

You are checking one entry a parser produced against the printed row it came from. **Your job is to
find what is wrong, not to agree.** An entry that passes review and is wrong is worse than one that
fails and gets reparsed.

The format is in `pipeline/docs/18-how-regulations-are-stored.md`. The types and conditions are in
`pipeline/regs/parsing/catalogue.py`. Read both before you start.

**Every check below is a real defect that shipped.** They are not hypotheticals; each was found in
a corpus of 350 hand-authored rules, which is a rate of about one defect in nine. Assume the entry
in front of you has one.

---

## Fatal — the entry must be rejected

### 1. A number that is not in its own rule's sentence

Every `take`, every bound inside `lengths` (`min_cm`/`max_cm`), `max_kmh`, `max_power_kw` and
`per_daily` must appear in **that rule's** `verbatim`, not merely somewhere in the row. A 50 cm
sub-limit was once attributed to a rule whose sentence never mentioned it.

### 2. `take: 0` without `may_target`

The two readings are opposite and the page distinguishes them:

* *"Catch and Release"* — you may fish for it and must release it → `may_target: true`
* *"No fishing for X"* — *"you may not **deliberately fish** for the species named **even if your
  intention is to release**"* → `may_target: false`

### 3. Size polarity inverted

```
"not more than 1 over 50 cm"  ALLOWS one big fish, parent governs the rest
                                lengths [{"min_cm": 50}]                     take=1, within=<parent>
"no trout over 50 cm"         forbids big fish, says nothing about small ones
                                lengths [{"min_cm": 50, "take": 0}]          take=0, may_target=true
"1 bull trout over 60 cm"     the kept fish must BE big; a 50 cm one is forbidden
                                lengths [{"min_cm": 60}, {"max_cm": 60, "take": 0}]      take=1
```

`over_cm`, `under_cm` and `band` no longer exist — `lengths` is the size field, and a rule
carrying the old three is refused on load. The same holds for the old gear flags (`allowed`,
`barbless`, `hook_count`, `lure`, `bait`, `max_lines`, …): gear is `gear` clauses only.

The word "over" appears in all three and maps to a different field in each. **Ask which fish go
back**: they are the ones outside the bound stored. Getting this backwards inverts the rule on the
exact fish it protects.

### 4. A `species` field on a bait or tackle rule

*"During the period when bait is banned it is banned for all angling and for all species."* Same
for single hook and barbless. *"Trout/char catch and release, bait ban"* is **two rules** and the
bait ban is on the whole river — the species belongs to the release clause. A rule that applies
only when fishing FOR something uses `when_targeting`, which is a different field and a different
relationship.

### 5. `verbatim` that is not the printed text

Reworded, stitched across a sentence boundary, or taken from another region's chapter. Two invented
sentences once passed because the agent wrote both the passage and the rule quoting it. Check the
`verbatim` against the row you were given, character for character.

### 6. One printed sentence carrying several restrictions

The commonest defect. The synopsis prints them together; the model must not.

* *"Bull trout from the Liard watershed **Aug 15-Oct 15**, and from the Peace watershed **all
  year**"* — two watersheds, two windows. As one rule the windows merge and one half is lost.
* *"No fishing in the Iskut watershed…; **and** in the Fraser watershed in Region 6"* — two
  unrelated watersheds joined by a semicolon.
* *"1 char (bull trout, Dolly Varden, or lake trout)**, none under 60 cm**"* — a **count** and a
  **floor**. As one rule a water quota displaces the floor with it.
* *"one fishing line to which only one hook… is attached"* — a line count and a hook count.

**Test:** could a water-specific rule override one half and not the other? Then it is two rules.

### 7. A restriction in the row that no rule covers

Read the printed row clause by clause and account for every one. A dropped sub-limit is invisible
once dropped — nothing downstream can tell it was ever there.

---

## Serious — fix before accepting

8. **`species: []`** is an error. Everything is `ALL_GAME_FISH`; "all other species" is
   `species_except`. 115 rules once stored an empty list while naming a species in their own text.
9. **`within` names a rule that does not exist**, or a sub-limit whose `take` exceeds its parent's.
   If it genuinely exceeds — Region 8's *"20 brook trout"* inside a 4-from-streams limit — it is
   **not** a sub-limit; it replaces the parent for its species.
10. **Wrong type.** Type by what the rule DOES: *"no ice fishing"* is a **method**, *"angling
    prohibited for non-guided non-resident aliens"* is **access** (it restricts who, not what),
    *"exempt from X"* is a **field**, not a type.
11. **An extent wider than the rule.** A water-specific rule bound region-wide, or a rule that says
    *"in any stream"* with no `feature_types` — that once closed 4,151 lakes and 2,712 wetlands.
12. **A reach bound to the wrong point, or a limiter dropped.** Three ways this goes wrong:
    * A split id **not on that item's menu** — invented, or borrowed from another water. Ingest
      refuses it, but say so, because the reach it was meant to express is then missing entirely.
    * An **alias bound instead of the canonical id.** One physical cut-point can answer to two
      authored names (*"McIntyre Dam"* and `gauge__08NM247` are the same point). Either resolves,
      and ingest rewrites the alias — so this is a nit, not a defect. **Do not report it as one.**
    * **`within_area` dropped.** *"That part of the Fraser within Region 5"* is `op: whole` plus
      `within_area: area:region:5`. Without the limiter the rule resolves to the whole river,
      which is the widest possible error and looks completely normal on the page.
13. **A qualifier lost from the label.** *"no vessels **on parts**"* rendered as *"No vessels"*
    tells an angler a restriction is broader than the law. If no split can express the reach, it
    must survive in `extent_text`.
14. **`when.dates` merged from two clauses**, or a season stored as its own inverse. *"Open June
    16-Apr 30"* is a CLOSURE and those are the days it does **not** apply — there is no `excepts`
    flag any more, so the days it DOES hold (May 1-June 15) must be what is stored. A season that
    could not be read belongs in `when.unparsed`, never dropped: an unparsed season and an absent
    one are opposite facts, and the second reads as "open all year".
15. **`obligation`**: *"anglers are encouraged"* is `should`, not law. Rendering advice as law is
    the mirror of rendering law as advice, and both have happened.

---

## What to output

```json
{"entry_id": "...", "verdict": "pass" | "fail",
 "issues": [{"rule_id": "...", "check": 6, "what": "...", "fix": "..."}]}
```

`fail` on any Fatal. Quote the printed text you are comparing against — a finding without the
source text beside it cannot be acted on. If the entry is clean, say so in one line and stop; do
not invent findings to look thorough.
