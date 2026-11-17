# T823 — "Small next to its gauge": Python decides when a section's lead gauge reading needs a warning; the release ships it

Status: OPEN (owner 2026-11-16; data half of the app chat's spots cleanup).

Source: owner 2026-11-16 (relayed by the app chat, spots cleanup), on a 1.4 km² creek shown "Below normal" from a
gauge 676× bigger: "Could we just use our gauge network that colors stuff wtc to determine if it is significant
enough? It would drop that in theory. Then ofr small ones still give a warning if used."

## What
- A spot (and any card) takes its gauge from the section's lead in `section_gauge` (the network the Conditions map
  colours with; the drainage gate already applied). No lead = no estimate.
- Python decides, per section with a lead, whether the water is "small next to its gauge" and ships it in the
  conditions file (a field on `section_gauge`, e.g. `small` or the ratio band), with the sentence the app shows as
  the warning written by Python. The app computes no ratio and no threshold (owner 16:50: Python decides every word
  and number).
- The threshold is policy (memory: gauge representativeness is policy): propose it from the drainage areas
  (`gauge.area_km2`, the section's own drainage) with examples, for the owner's word before it ships.

## Owner, 2026-11-16 22:18
Cut-off "50× (Recommended)"; wording "Much smaller stream (Recommended)". The pending-owner marks on the branch are
now decided.

## Rework (22:19): the speaker, not section_gauge
The map colours from the paint panels (standing.ts speakerFor / buildStandingClasses; 249k sections), not section_gauge
(114k). Owner: "Allow, with a warning (Recommended)": the warning hangs on the gauge that SPEAKS for the section
(the panel), on the map and spots alike. edc097013's flag on section_gauge is replaced.

## Built on the speaker (lane/t823-small-water, 2026-11-16)
- New release file `flow-small` (`flow-small/1`, paint.py `encode_small`): one varint per paint section in sid order,
  the same rows as `flow-area`, = a bitmask over its panel's members by `ord`. Bit i is set where member i drains at
  least 50x the section's target area (`pipeline/gauges/consume/small_water.py`, `SMALL_WATER_POLICY = "x50"`). The
  areas are the ones the app already holds (`panel_member.area_km2` in paint, the target in flow-area). 21.4 kB gz.
- Conditions meta: `small_water_text` (the sentence) and `small_water_ratio` ("50"); the key names are in
  `gauge-policy.generated.ts`. Decided at release time from the bundle: no bundle rebuild needed, a re-cut ships it.
  edc097013's `section_gauge.small` is gone (no reader).
- Why a bit per member: the sheet's lead is chosen LIVE from the feed (`sheetLead` / `leadOver`: the heaviest
  reporting donor, flow first), so Python cannot name it ahead of time; it decides every member the app may lead with.
  `speakerFor` (panel, else lake station) needs no change: it is the release's own choice. A lake speaker has no bit.
- Live bundle (141,189 panel sections; the "249k" in standing.ts is r-7b6e802bf22b's count): every gauge warned 117,562,
  some 2,982, none 20,645; the owner's creek 63623 (+6) is warned for all three of its gauges (453x, 453x, 676x).
- App chat to do: read `flow-small` with `flow-area` (decode beside `decodeArea`), show the conditions meta text where
  the member it leads with (`leadOver`) has its bit set, on the sheet, the spot and wherever the map words a speaker;
  add `flow-small` to `contract.WEB_BUDGET["conditions_open"]` with its re-cut walk pin (left out so the pin test holds).
  `flow-small` is required in PACK_FILES: an app built with this pack refuses a published release without the file, so
  cut a release with it before that app ships.
