# T501 — "Where you can't fish" in short lines

Status: DONE — 657f6e244, 6dce77a24e6a, d14bdb180f55; owner 2026-11-09 on build 24: fine

Was: IN REVIEW — 657f6e244, in r-6dce77a24e6a + r-d14bdb180f55; owner: OWNER-TODO "Phone check with build 22" (the two "Where you can't fish" lines).
Earlier: IN REVIEW — 657f6e244; ships with the next release (owner reads the two lines). Owner 2026-11-08 (phone, 0.6.9).

Today: "No fishing: within 23 m (75 ft) downstream of the lower entrance to any fishway, canal, obstacle or leap ·
Rule book p.9 ↗" and "No fishing: within a 100 m (330 ft) radius of any government facility operated for counting,
passing or rearing fish (e.g., fishway, fish hatchery) unless otherwise designated · Rule book p.9 ↗". Owner wants
quick lines: "No fishing at x", "No fishing at y".

## Do
- A short line per place (e.g. "No fishing 23 m below a fishway, canal, obstacle or leap", "No fishing within 100 m
  of a fish counting, passing or rearing facility"); the full rule-book wording + page stays one tap away, so nothing
  of the rule is lost ("unless otherwise designated" included there).
- Short lines are owner-reviewed wording: list every one for the owner before shipping (everything added is a proposal).

## Done (657f6e244)
- Only two rules are "Where you can't fish" (no_fishing_buffers.r1/r2). Each already has curated short place words
  (`extent_text`); `cant_fish_words` uses them. The lines now read:
  - "No fishing: within 23 m (75 ft) downstream of any fishway, canal, obstacle or leap · Rule book p.9 ↗"
  - "No fishing: within 100 m (330 ft) of any government fish counting, passing or rearing facility · Rule book p.9 ↗"
- "unless otherwise designated" and "lower entrance" stay on the Rule book page the line links to.
- Test `test_t501_the_buffers_say_their_short_place_words`.
