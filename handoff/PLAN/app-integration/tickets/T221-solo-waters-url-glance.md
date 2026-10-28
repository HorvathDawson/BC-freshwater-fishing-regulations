# T221 — Solo waters open on their regulations, the URL follows the water, one salmon table, the at-a-glance fish table

Status: DONE — 73625e35. Item 5 (at-a-glance table) is a try: owner to keep or revert (one `glance` prop in WaterModeHost).

Source: owner feedback 2026-10-28 on the web app (Dala River).

## Items
1. **Solo water: Regulations, no Map.** A water of one part that goes on as / from no other water (`@app/core`
   `soloWater`: `parts.length === 1`, no `water_continue` either way) opens from a search result on its Regulations,
   not its Map (`waterOpener.searched` asks `regsSoloOf`). However it was opened, the Shell reads its group and sends
   `{type:"solo"}`: the frame drops its Map view (`frameViews`), Regulations in front if the Map was. The Vedder /
   Chilliwack (a continuation) keep the Map. A part of two stretches keeps it too (two map entries). The access
   notice's "on the map" link is hidden where there is no Map.
2. **Dala River salmon in two boxes — cause.** Data + presenter: DFO lists "Dala River — including tributaries" (closed
   Oct 1–Dec 31) but its place is not drawn (`P.unmapped`), so Dala's section binds Region 6's default chain. The
   presenter (`salmonCard`) returns the default chain's rows (forced to "check DFO", partial coverage) AND the undrawn
   place as `unmapped[]`, and `SalmonCard` drew each in its own block — two tables. Fix (UI): one table — the undrawn
   place's rows join the water's rows, each saying its place and "Location not yet mapped"; the partial line leads it.
   The curation gap (draw the Dala place) is unchanged.
3. **URL follows the route.** `@app/ui` `routeSearch` writes the water in front as the start link
   (`?regs=<item>&part=&view=`; date / angler / theme kept; browse and unnamed reaches say none — no section id in a
   URL, AGENTS 5). `systemBack.web.ts` `routeUrl` REPLACES the current entry (a microtask after the history push, and
   again after a browser back/forward lands), so the back stack is unchanged. `parseStartLink` takes `view`. Refresh
   reopens the same water / part / view; back from it is the map.
4. **Per-species size rule.** The accent rail is gone: the rule is a plain semibold ink meta line under its count
   line. The shared-cap size box (Dala's "only 1 over 50 cm") is unchanged.
5. **At-a-glance fish table (a try).** In the app (`WaterModeHost` → `WaterScreen glance`), the Keep tab's species list
   heads "Fish" and carries Salmon and each quota group as folded rows — picture, name, the day's answer
   (`groupGlance` "5 a day"; `salmonGlance` "check DFO" / "closed" …) and a bordered "Details ⌄" button; pressed, the
   full card opens in place ("Hide ⌃"). The Salmon card no longer draws above the Keep tab there. A screen mounted
   alone (tests, the regs tools) keeps every card in full.

6. **(owner add-on) A count line that reaches past its size box goes before it.** On the Dala River the stream's
   "1 brown trout, cutthroat trout, rainbow trout or steelhead in all" sat inside Region 6's "only 1 over 50 cm" box,
   which does not hold the rainbow. General rule (`@app/core` `liftCounts`, presentation only, the release's tree is
   unchanged): a count tier inside a size box is drawn as a line BEFORE the box when a kind outside the box carries its
   count (`also_counts` by the same rule, `tier` null — the T203 split). Its kinds stay in the box. The
   conformance store walk mirrors it. `../hard-regs-spot-check.md` starts the tester's list of hard cases.

## Shots (`../T221-shots/`)
`dala-{desktop,phone}-glance.png`, `-salmon-open.png` (one table), `-trout-open.png` / `-trout-open-2.png` (count line before the box, plain size rule);
`search-kildala-{desktop,phone}.png` (solo: search opens Regulations, no Map tab); `search-vedder-desktop.png`
(continuation: still the Map).

## Open for the owner
- Keep the at-a-glance table (item 5) or revert to the full cards? It is one prop (`glance`) in `WaterModeHost`.
