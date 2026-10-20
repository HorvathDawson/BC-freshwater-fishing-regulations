# Round 20i data: the UR4 user panel's data words (2026-10-19)

rows 25 → 26, gear 11 → 12, glossary 4 → 5. ANSWERS-SPEC ROUND20i. The source is the UR4 write-up in
~/.claude/projects/…/handoff/PLAN/USER-ROUNDS.md, "Round UR4", items D1–D7 and O3.

## Strings changed (old → new), for Fable
| where | old | new |
|---|---|---|
| tree kind `size_rule` (new) | — (the chip "30–50 cm (12–20 in)") | "keep 30–50 cm (12–20 in); release the rest". Where the top is the steelhead line on a steelhead water: "keep 30–50 cm (12–20 in); release smaller ones". Null for any size, for a kind kept at none, and for steelhead |
| worked keep-share line | "none from other streams today — up to 2 more from here or from lakes." (Glenn: "up to 3 more") | "none from other streams today — up to 2 more of any kind, from here or from lakes." |
| gear `summary.changes[].when` (Vedder May 15) | "Jun 1–Apr 30: one hook, lure or fly …" | "Jul 1–Apr 30: …" (the closed Jun 1–30 is left out) |
| gear hook words | "1 line with a single barbless hook (one hook, lure or fly). …" | "1 line with a single barbless hook, no trebles (one hook, lure or fly). …" |
| fly fishing only | "… No float, sinker or attracting device." | "… No float or strike indicator, no sinker or split shot, no lure, spinner or other attracting device." |
| glossary `fly_fishing.says` | "Only an artificial fly on the line — no bait, lure, float, sinker or attractor. …" | "Only an artificial fly on the line (one single-pointed hook) — no bait or lure, no float or strike indicator, no sinker or split shot, no spinner or other attracting device. …" |
| steelhead stop duty | "stop fishing here after 2 a day" | "stop fishing this river after 2 a day" ("this stream" where the water has no own row) |
| `elsewhere.says` | "Shared with every Region 2 stream you fish today." | "Your own limit, counted across every Region 2 stream you fish today." |
| `elsewhere.says` (lake) | "Shared with everywhere in Region 2 you fish today, lakes and streams alike." | "Your own limit, counted across every water in Region 2 you fish today, lakes and streams alike." |

**Why the stop duty says "this river" and not the Vedder's name:** a rows frame is per rule set and serves every water with the
same rules, so the data cannot carry one water's name. It gives the noun of the water's own row instead. The page may replace
"this river" with the water's name.

## Cites
- **Hook (p.80):** "single hook: a hook having only one point. (In contrast, a treble hook is a hook having three points on a
  common shaft.)" This supports "no trebles".
- **Fly fishing only (p.80):**
  - "fly fishing: angling with a line to which only an artificial fly is attached (floats, sinkers, or attracting devices may
    not be attached to the line when fishing is restricted to 'fly fishing only')".
  - "artificial fly: a single-pointed hook … Two or more hooks tied in tandem is not permitted."
  - The book does not use the words "strike indicator", "split shot" or "spinner". They are the gear an angler knows the book's
    float, sinker and lure/attracting device by. This is flagged as the one wording step beyond the book's own words.
- **Stop duty (p.21, Region 2):** "When you have caught and retained your daily quota of hatchery steelhead from any water, you
  must stop fishing that water for the remainder of that day."

## 6 Verified
- **(a) Steelhead stamp on a water closed to steelhead (Bella Coola): not wrong per the book, not changed.**
  - Licensing p.6: "Your basic licence must be validated with a Conservation Surcharge Stamp if you fish for steelhead anywhere
    in B.C. (whether you keep or release your catch)".
  - On a river with "No fishing for steelhead" that condition cannot hold there, so the line is irrelevant rather than false.
  - Licence frames are per licence key, not per rule set, so the data cannot drop it per water. The page can drop or mark it
    when the card's `not_kept` says steelhead are `closed`. This is owner item O3.
- **(b) Kokanee need no salmon stamp.**
  - Licensing p.6–7: "to keep a salmon of any legal size or species (other than kokanee) from non-tidal waters".
  - The export record `zp:salmon_stamp` already has `parts.doing` "to keep salmon (not kokanee)", and `FEDERAL_SALMON` has no KO.
  - The page should show `parts.doing`. No data change.
- **(c)** Changed as in the table above.

## Checks
- `pipeline/tests/test_answers_r20i.py`: 6 tests, all failing before the change and passing after.
- Pins moved in test_answers_{25, 28_card, 28_live, r10, r11, r12, r13, r18, r20b, r20g, r20h, tidal, ur1, ur2}.
- `model --check` rc 0. Live build rc 0. Card gate: 15,862 groups / 7,144 cards, 0 problems. Page v36 regenerated; it now
  also draws `size_rule`.
- Live spot checks:
  - Vedder May 15: "Jul 1–Apr 30: one hook, lure or fly …"
  - Glenn Jun 15: "up to 3 more of any kind, from here or from lakes."
- Fast suite, unfiltered, run to the end: **3447 passed, 6 skipped, 0 failed**.
