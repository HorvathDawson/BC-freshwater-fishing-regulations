# T817 — A water with only level data (Harrison Lake) opens its Conditions on Level, and says so

Status: IN PROGRESS (fixed on branch `app-fix/t817-level-only`, not merged; left (Claude): the phone look on the next build, then the merge; app chat; refines T728 and the owner's 2026-11-10 "Flow leads, Level auxiliary")

Source: owner 2026-11-16 ~21:22, testing iPhone TestFlight build 36 (app chat; pictures in the chat). Verbatim: "also opening harrison lake then going to conditions should default to level only because there is no
other data (the map looks super weird and not visible because of it) then when you switch to level the empty flow
option disappers so if only level data exists it should show level data. just mention at top it is level data maybe...
have an agent look at this".

## What happens today
Harrison Lake's Conditions opens on Flow, which is empty (T728 made the card always open on Flow), and the Conditions
map on Flow draws nothing useful for it ("super weird and not visible"). After switching to Level, the Flow option
disappears, so the choice was never real.

## Wanted
A water whose station(s) read level only opens on Level, with a short line at the top that this is level data (no flow
is measured here). Flow still leads wherever flow exists (unchanged). The map on such a water shows the level reading
instead of an empty flow. The Flow / Level switch only offers what exists.

Added 2026-11-16 ~21:52 (owner, same build): "when you click level on chehalis then you can never go back to flow because
it moves to harrison level gauge".

## Fixed, 2026-11-16 (agent, branch `app-fix/t817-level-only`)
Cause, both reports: the card's Flow / Level switch offered only what the CHARTED gauge measures once a quantity was picked
(`cardQuantity`, `ui/src/answers.ts`), and T728 opened every water on Flow. Harrison Lake is spoken for by its own gauge
08MG012, which has a level record only: the card opened on an empty Flow, and after Level the switch had one choice (so it
was hidden). The Chehalis that day: its own gauge (08MG001, both quantities) not reporting, so the one lead, chosen in Flow
(`leadOver`), fell to the level ranking: Harrison River at Harrison Mills (08MG014, level only). The lead never changed with
the quantity (checked on the web: the same gauge under Flow and Level); Flow vanished after Level for the same reason.
The map: a gauged lake has no donors, so its route map held "you are here" alone, framed on a point in open water.

Fix, one decision in core: `conditionsOpening(measured, opening)` (`core/src/conditions.ts`) over what each gauge that
speaks for the reach measures (the bundle's `gauge_clim`, `useSpeakerMeasures` in `ui/src/panel/conditions.ts`; no
Python flag needed: a panel member's `years.discharge` agrees with it on every member of the release). Flow wherever any
of them measures flow (T728 holds); Level, with "Level only: no flow is measured here." at the top, where none does and one
measures level. It also returns the switch's choices, the WATER's quantities, the same before and after a pick
(`cardQuantity`'s `offered`). `WaterConditions` reads it (`WaterViews.tsx`). The lake's route map now pins its own gauge,
live, with where its reading stands ("Above normal"; core `ownGaugeMap`), and frames it with the place
(`donorView` `own`). The level-only lead note is not repeated under the top line. The card's first spinner holds until the water's
gauges are read (`ConditionsPanel` `holding`), so a level-only water never draws Flow first. `tools/water-frame-layout.test.ts`
now waits for the span switch (`chart-span`): with the water's choices known from the start, the Conditions card has two
radio groups at once, and its old "the one radiogroup" readiness probe threw.

Level-only waters in r-46e4355a7277: 77 (30 gauged lakes: Harrison, Okanagan, Shuswap, Cowichan, Upper and Lower Arrow,
Quesnel, ...; 47 waters whose panel reads level only, e.g. Kingcome River).

Tests: `core/src/levelOnly.test.ts` (flow+level, level only, flow only, neither; the lake pin's words),
`ui/src/cardQuantity.test.ts` (the switch the same before and after a pick), `ui/src/panel/ownGauge.test.ts` (the lake's
map), `ui-native/src/WaterConditions.levelOnly.test.tsx` (first view; Flow -> Level -> Flow keeps the gauge and the
switch, the Chehalis' shape and Harrison's; both mutations fail it), `conformance/src/levelOnly.release.test.ts` (live:
Harrison, Okanagan, Shuswap, Cowichan lakes and the Kingcome open on Level, Crest Lake and the Chilliwack on Flow).
Web pictures (phone width): before/after of Harrison Lake, after of the Chehalis on Level.
