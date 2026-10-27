# T209 — vessel restrictions in the app

Status: DONE (ffe7e7e2)

Source: owner 2026-10-26. The catalogue holds `vessel_rule` rules (speed limits, no powered boats, electric motors
only …). The app should show them on the water page and the map sheet.

## What the catalogue holds (measured 2026-10-26, after `drop_on_parts`)
390 `vessel_rule` rules on 364 entries. 375 bind the whole water; 15 bind a stretch (`between` 9, `within` 3,
`downstream_of` 2, `upstream_of` 1). 25 still hold on a named undrawn part; 3 carry a `when`.

| kind (aspect / level) | rules | example (water: verbatim) | fields |
|---|---|---|---|
| propulsion / electric_only | 162 | Chicadee Lake: "Electric motor only - max 7.5 kW" | — (the 7.5 kW is in the words, not a field) |
| propulsion / unpowered | 97 | Antler Lake: "No powered boats" | — |
| propulsion / power_capped | 64 | Beaver Lake (R1): "engine power restriction - 7.5 kW (10 hp)" | `max_power_kw` |
| speed | 48 | Hatzic Lake: "speed restriction on parts (8 and 50 km/h)" (two rules, 8 and 50) | `max_kmh` (43 carry it) |
| towing | 11 | Beaver Lake (R1): "no towing on parts" | — |
| propulsion / none | 8 | Campbell River: "No vessels between Elk Falls and John Hart Dam Power Station" | — |

## Today
The app shows them only in the Methods tab's "Boats" cell (`core/regs/methods.ts boatsVM`, `ui-native/regs/Boats.tsx`):
each rule's export label in a plain list. Nothing on the water page header or the map sheet says "electric motors
only" or "8 km/h", and the 25 named-part rules are in the in-part caveat (`caveats.inPartOf`, "boat" wording).

## Work
- Water page: a compact boats line near the top ("Electric motors only (max 7.5 kW)", "No powered boats",
  "Speed limit 8 km/h", "No towing"), one chip per kind, from the bundle's facts (`aspect`, `level`, `max_kmh`,
  `max_power_kw`), never re-parsed from the words. Keep the Methods "Boats" cell as the full list with sources.
- Map sheet: the same one-line summary for the tapped water / stretch; a stretch-bound rule (the 15) shows only on
  its stretch.
- Two limits of one kind on the whole water (Prospect, Shawnigan, Hatzic: "8 and 60/65/50 km/h", the book does not
  say where each holds — owner 2026-10-26) read as "8–50 km/h on parts", never as the stricter one alone.
- electric_only: 7.5 kW is printed in the words of every one but carried in no field — decide (owner) whether the
  parser fills `max_power_kw`, or the label stays word-only.
- Done when: every one of the six kinds shows on the water page and the map sheet for one example water each
  (golden test), and a water with no vessel_rule shows no boats line.

## Outcome (2026-10-27, app built; no deliver rebuild needed)
- The bundle already carries the facts in `rule.fields` (`aspect`, `level`, `max_kmh`, `max_power_kw`); no contract change,
  no type regeneration.
- `core/regs/boats.ts` (`boatsLine`, `RegsPage.boats`): one chip per kind from the section's gear-frame vessel rules, never
  from the words - "Electric motors only", "No powered boats", "Engine limit 7.5 kW (10 hp)", "Speed limit 8 km/h", "No towing",
  "No vessels"; two speed limits of one kind read "Speed limits 8-50 km/h on parts"; an all-undrawn-part kind ends "(in part)";
  a dated kind carries its dates. The frame's lists are per section, so a stretch-bound rule shows only on its stretch.
  `ui-native regs/BoatsLine.tsx` draws the chips under the header (each opens its rule's sheet); none for a water with no
  vessel rule. The Methods "Boats" cell is unchanged. The map-tapped section's page is this page, so the map sheet has the same line.
- Tests: `boats.test.ts` - one example water per kind (golden-style), the Hatzic range, ordering, dates, no-line.
- OPEN for the owner: electric_only reads without its 7.5 kW (carried in no field) until the parser fills `max_power_kw`.
