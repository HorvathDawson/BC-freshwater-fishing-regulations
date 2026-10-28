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

## Verified on the live web build (2026-10-27, release r-8ce19c1f2104, no export needed)
- Hatzic Lake (`?regs=wbk:329291809`): "Speed limits 8–50 km/h on parts". Elk Lake R1 (`wbk:329676313`): "No vessels |
  No powered boats | Engine limit 7.5 kW (10 hp) | No towing", none marked "(in part)": the 33 on-parts rules
  (curation-review/on-parts.md) already hold on the whole water in this bundle. Chicadee: "Electric motors only";
  Heffley: "Speed limit 15 km/h | No towing"; Campbell River (gnis:39532) part 1: "No powered boats", other parts none.
- `boats.test.ts` 5/5; `test_drop_on_parts` + `test_t207_on_parts_is_a_note` 14/14. `pnpm check` red only on other
  lanes' work (map style diff + overzoom/tile-contract: tiles lane; PinBar vedder_1010: 9849276b; regs-text-scale
  vedder place cut: e5f0318a).
- Still OPEN (owner, OWNER-TODO): `max_power_kw` on the 162 electric_only rules; until then the chip reads without 7.5 kW.

## max_power_kw stored (owner 2026-10-28: "store the number")
- `pipeline/tools/electric_kw.py` (one-off, not in run_parse.sh): sets `max_power_kw` on every `vessel_rule` at
  `level: electric_only` from the kW its own words print; a rule printing none, or two, is listed, never defaulted.
  Dry run: 162 would be set, all 7.5, 0 skipped, 0 already set. Applied: 162 rules in 8 region files; the diff is the
  162 field lines only (verbatim untouched). The export label was already "Electric motor only (max 7.5 kW)" either way;
  the app chip still reads "Electric motors only". The prompt now asks the parser to store it (a re-parse keeps it).
- Tests: `pipeline/tests/test_electric_kw.py` (words → number, only empty electric_only rules, every catalogue rule
  holds the number its words print). Picked up by the next deliver rebuild.
- Still OPEN (owner): the 10 close-wording undrawn parts ("various locations", "swimming areas") in on-parts.md.
