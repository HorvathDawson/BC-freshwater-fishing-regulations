# T1146 — Side channels with no main stem that the river's own rule does not reach become nameless parts

Status: IN PROGRESS — 0.10.0 (the 0.10.0 chat): the mechanical part only, a side reach run proves the diff first

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1146 | Side channels with no main stem that the river's own rule does not reach become nameless parts | IN PROGRESS — 0.10.0 (the 0.10.0 chat): the mechanical part only, a side reach run proves the diff first | | the real binding diff to the owner before merge; C, D, E causes are not this ticket |

Source: the 0.10.0 chat's read-only investigation for T1140, 2026-11-25, on the candidate r-71d09ad82e81. Of 1,476 parts
over the 478 multi-part waters, 26 have no name and show the bare water name; all 26 are side channels with no
main stem. 15 of them exist only because the river's own or regional rule does not reach the channel (Seymour p3,
Skeena p4, Manson and 12 more); 6 because a Strathcona Park rule does not reach the channel (Burman, Bedwell,
Thelwood and 3 more; Thelwood lies inside Strathcona - Westmin Park). If the binding is wrong, the fix makes the
part take its river's rule and merge into its neighbour, and it needs no name. Related: T1140, the memory notes
`receiving-river-not-a-tributary`, `split-fix-changes-trib-walk`, `reach-only-rerun`.

Owner, 2026-11-25 (planning chat dialog), to "name them now and fix the binding next patch (recommended) / fix the
binding in 0.10.0": **"Fix the binding in 0.10.0"**.

Work:
1. FIRST, read-only: is this one mechanical cause in the reach walk (a side channel without a main stem does not
   take its river's water-level rule), or separate curation cases? Are the 6 Strathcona parts the same fault?
2. If mechanical: fix in code with a test that fails without it (mutation), rerun the reach step, and diff every
   rule binding against the candidate. Expected: only those channels gain their river's rule and merge away.
   Anything else stops the release and comes to the planning chat.
3. If it needs the owner's marks case by case: stop; the cases go to the owner. No agent writes a mark.
4. Parts that still have no real-world name afterwards (Rees p0, Comox p0, ...) are T1140's question to the owner.

Acceptance: the count of bare-name parts before and after, by cause; the binding diff attributed line by line;
the tiles and bundle of one vintage (rule: tiles and bundle share a vintage). Severity: high (a part with the
wrong rule set, the Squamish case's family). Model: Opus. Size: S if mechanical, otherwise unknown. Owner: the
marks if step 3.

## 2026-11-25 about 17:25: the finding (0.10.0 chat, read-only): not one cause; the mechanical part is small
- **A (mechanical):** the tributary walk only climbs upstream (`pipeline/atlas/graph/tributaries.py:1052`): a side
  channel that leaves its river and does not flow back in is never reached. **B (mechanical, same fix):** the
  stream-order guard (`:1247`) refuses a channel of the river's own water when FWA gives it a bigger order than
  the piece it joins. **C (curation):** canal and ditch pieces are the walk's barrier by design (Mahood p1,
  Stirling p1). **D (curation):** area and region membership by overlap, parks not cut (the five Strathcona parts
  and Campbell p6: T1149; Cottonwood p1 and West Road p3 are region straddles, as designed). **E (real
  differences):** Squamish, Gordon, Okanagan Oxbows (real names, T1140); Theodosia, Snehumption; Noons p4, Seymour
  p3 (added city streams the owner accepted as "leave out"); Robertson p1; Skeena p4.
- **A/B waters:** Forsyth p1, Koksilah p1, MacMillan p2, Shelagyote p0, Omineca p0; partly Halfway p2, Manson p0.
- **Measured reach of the fix** (all 15,244 side-channel groups, all 478 walked rules): rules change on 14 groups,
  19 sections, 7 waters, all among the 26 (Forsyth: elk_river_s_tributaries r1-r4; Koksilah: cowichan_river.r2;
  Halfway: peace_river r2, r3; MacMillan: skeena_river.r1; Manson, Omineca: williston_lake.r4; Shelagyote:
  babine_river r6, r7 and skeena_river.r1); one licensing change (the Babine designation on Shelagyote); DFO
  salmon: the Cowichan place on Koksilah's channels, and ONE change outside the 26: a 735 m Nechako channel gains
  `6:f:closure`. Reach, tiles and deliver rerun, about 33 minutes; no atlas.
- **After it:** 5 of the 26 nameless parts go, Halfway and Manson shrink, 21 stay (3 get real area names; 18 read
  the data's own label, the owner's pick in T1140).
- **Scope of this ticket:** A and B only, one Opus lane, mutation-proven tests, a side reach run compared with the
  list above before any PR; anything beyond the list stops it. No curated row, no mark, no park cut.
