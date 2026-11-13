# T700 — The Regulations header names its own part, never the first part of its picker choice

Status: DONE — 2b49a643b

Owner 2026-11-13, phone (0.7.7 build 29, release r-0a9930fb8826), Capilano River: Map tab "PART 3 OF 3 · Capilano
Lake → Source"; "Show regulations for this part" → Regulations header "Cable Pool Bridge footbridge → Capilano Lake
outlet". "This section got a different location text in regulation panel?"

## Found (release r-0a9930fb8826, read-only, through the app's own load path)
Capilano River (`gnis:5922`) has 4 release parts; the map lists 3 (part 0 is the tidal piece):

| part | map | ends (tokens) | key | picker choice |
|---|---|---|---|---|
| 1 | Part 1 of 3 | CN Railway Bridge (tidal boundary) → Cable Pool Bridge footbridge | 1139 | [1] (open) |
| 2 | Part 2 of 3 | Cable Pool Bridge footbridge → Capilano Lake outlet (`lake_outlet:wbk:329532436`) | 1140 | [2, 3] closed all year |
| 3 | Part 3 of 3 | Capilano Lake (`lake_inlet:wbk:329532436`) → Source | 1140 | [2, 3] closed all year |

- A NAMING defect, not a selection one: the Regulations page loaded part 3 (target `{part, 3}`, key 1140 — part 3's
  own); the header's label came from `partLabels`, which for a part in a picker choice named the choice's FIRST part
  (`partNameOf(inp, g.parts[0])`, a prototype `shortLabel` holdover). Parts 2 and 3 are one choice (same rule set,
  both closed all year) but two map entries, apart across Capilano Lake. Not a T517 combined stretch: the map does not
  join them; the header simply said part 2's name.
- The two ends differ because they are two parts: below the lake the part ends at the lake's OUTLET, above it the next
  part starts at the lake (its inlet token, said "Capilano Lake"). The lake itself is its own water, in neither part.
- Rules: never wrong. The page reads the selected part's own key in every case (guard below checks it); in this choice
  both parts share key 1140 anyway. Of the 304 mislabelled parts, 263 share the named part's key; the other 41 share
  its rule-set group but not its key (salmon chain / access differ) — their rules were still their own, only the
  location line was another part's.
- Every way in: a Map pick, a step, "Show regulations for this part" and a tab switch all open the frame's part; a tap
  opens the part holding the tapped section (`targetFor`); all reach the same header, so each showed it on part 3. A
  search opens the lowest keyed part (part 1 here, first in its own choice), so search alone did not show it.
- Province (r-0a9930fb8826, 1,589 listed parts of the waters of several parts): 304 parts on 206 waters said another
  part's name (Bull, Fort Nelson, Kanaka, Peace, Fraser p6, Campbell p9, Dean p4 …).

## Done
- `app/packages/core/src/regs/header.ts` `partLabels`: the short label (and the long one when the subtitle does not
  say the choice's text) is `partNameOf(inp, inp.partIx, names)` — the page's own part, as the map names it.
- Tests: `header.test.ts` (Capilano shape: one closed choice of two parts the map lists apart; failed before);
  `app/tools/stretch-names.test.ts` (T700) province-wide on the release: every part of every water of several, by a
  part open (Map pick / step / Show regulations / tab switch), by a tap on its first and last section, and by search:
  the header's name is the map's, before the group is read it is the part's own ends, the long label is the name or this
  part's own choice text, and the page reads the part's own key. Reverting the fix fails it (Capilano part 3 named).

## Not changed — a wording option for the owner
On a part whose picker choice is that part alone, the header's long label (shown when it fits two lines) is still the
book's wording of the part, kept per rule-set group by T517: Capilano part 1 reads "From the CN Railway Bridge (tidal
boundary) up to the Cable Pool Bridge footbridge" on Regulations, "CN Railway Bridge (tidal boundary) → Cable Pool
Bridge footbridge" on Map. Same place, two forms (659 parts on 445 waters). Options: (a) keep (book words where they
fit); (b) the map's "A → B" name on both screens always.
