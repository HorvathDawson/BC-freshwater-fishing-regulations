# T862 — Search finds a water by its other names; audit the name variations (stocking / bathymetry first)

Status: OPEN — future (owner 2026-11-18 17:30: "this is a future ticket i think"); after 0.8.1.

## The owner (2026-11-18 ~17:30, verbatim)

"tyhe search doesnt show this? (Waters with an official name plus a different name that search also finds: 1,078
(1,005 lakes, 73 streams). For example, stocking records call Sixteen Mile Lake "Baker Lake", and Tranquille Lake
"Lake #355". ) if searching one the other doesnt come up? this is a future ticket i think? it will also involve
auditing the name vairations especially stocking/bathy maps"

## What is known (main chat, 17:30)

- The other names DO ship: the bundle's `alias` / `name_source` tables carry them (Sixteen Mile Lake: "BAKER LAKE"
  stocking, "SIXTEEN MILE L.  (BAKER LAKE)" bathymetry; Tranquille Lake: "LAKE #355" stocking, "TRANQUILLE L."
  bathymetry), and release r-86d43303bee2's core files hold them ("LAKE #355" in core and core-3).
- So the gap is the search (app half): it does not find a water by these names, or ranks/normalises them away
  ("#", "L.", upper case, "(…)").
- Count (r-86d43303bee2): 1,078 waters (1,005 lakes, 73 streams) carry another name beyond spelling; 2,181 unnamed
  waters are named from found names. Sources: stocking ~770 / bathymetry ~300 / regulation ~110 / angler-info
  markers ~40 … (main chat's breakdown).

## Two parts

1. **Search (app chat):** typing any of a water's names finds it, and the result says which name matched ("Sixteen
   Mile Lake — also called Baker Lake"). Read the shipped aliases; normalise "L." → Lake, "#", case, brackets.
2. **Name audit (main chat):** the stocking and bathymetry names were matched to FWA lakes automatically; check them
   before search leans on them (a wrong alias sends an angler to the wrong lake). Start with names that are not the
   official name's spelling (~1,050 stocking + bathymetry), multi-lake titles ("CHICKEN RANCH L.  MENZIES L.  KANE
   VALLEY CHAIN"), numbered lakes ("LAKE #355"), and one alias on two waters. Proposals go to the owner (review app).
