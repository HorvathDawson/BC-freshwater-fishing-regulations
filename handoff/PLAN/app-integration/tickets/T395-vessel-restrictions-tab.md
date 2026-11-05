# T395 — Vessel restrictions move to their own tab under Methods and gear

Status: DONE in code 2026-11-05 (65e8bd6d7; owner report 2026-11-05, 0.5.0 build 8, water gnis:21100 Nicomekl River, Regulations). Phone check with the next APK.

The vessel restrictions sit at the top of the water's regulations in an odd format. Owner: a new tab under Methods and
gear for vessel restrictions, and move them there (out of the top of the pane).

## Done in code 2026-11-05 — phone check with the next APK
Reading taken: "a new tab under Methods and gear" = a new SECTION in the Methods tab, not a fifth header tab (the strip
stays What you can keep · Gear · Methods · Licence). A fifth header tab would also need the scroll-spy (`tabInView`), the
anchors and the start-link tab list widened — say so if that is what was meant.
- The T209 boats chips (tinted boxes right under the sun card, the "odd format") are gone from the top of the page.
- The Methods tab now has a "Vessel restrictions" section (core `METHODS.VESSELS`) after Ways to fish: one row per kind
  ("No powered boats (in part)", "Speed limit 8 km/h", …), each opening its rule's sheet, then the existing Boats cell
  (the full rule list with its sources) moved into it from the Methods section.
- On a closed day the Methods tab keeps its sentence and the section under it. Where the Methods tab is not offered at
  all (a day nothing may be fished, T227 `gearTabsFor`) the vessel rules are not shown — as with the rest of Methods.
- Not moved: a boat-only "Boat rules in some spots" in-part caveat inside the Keep card (rules the map cannot draw yet)
  stays where it is; if that is what read oddly on Nicomekl, it is a follow-up.
Tests: methods-licence.render.test.tsx (section under Methods, row opens `rule:`, closed day, none → no section; the
golden mounts take the new heading before Boats).
Phone: Nicomekl River (gnis:21100) — nothing about boats at the top; Methods → Vessel restrictions.
