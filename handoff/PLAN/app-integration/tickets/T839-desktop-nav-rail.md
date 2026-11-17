# T839 — Web on a desktop: the Map / Conditions / Spots / More bar moves to a left navigation rail

Status: IN PROGRESS (built on branch app-fix/t839-desktop-rail; owner picked "Land it" 12:30)

Source: owner 2026-11-17 ~11:48, app chat (verbatim): "also desktop mode on webapp should maybe move the bottom
map.conditions. spots, more bar somewhere more standard for desktop. maybe on side or top please choose the most
standard spot".

## Choice (app chat, owner asked Claude to choose)
A vertical navigation rail on the LEFT edge at desktop widths. This is the standard pattern: Material 3 says a bottom
navigation bar becomes a navigation rail at medium and expanded widths, and map apps on desktop (Google Maps, AllTrails,
onX) put their tabs on the left side.
The top stays search + day (owner's T367 layout rule, 11-04). The legend and its control stay at the bottom of the map.
The phone layout is unchanged.

## Work
- `ui/src/route.ts` `bottomBarShown` and the web shell: at the desktop breakpoint, draw the four tabs as a left rail
  (icon over label, current tab filled). The bottom bar is hidden at that width; the map and side panels start right
  of the rail.
- Regs mode (it hides the bottom bar, owner 10-20): decide whether the rail stays or hides there, the same way the bar does.
- Shots: web desktop light + dark, every tab; phone width unchanged (before = after).
- Tests: the rail at desktop width, the bar at phone width, every press named and 44 × 44 (the T243 gate).

Size: M. Owner look: shots before landing.

## Built
- `ui/src/layout.ts` `tabsAsRail(layout)`: true at the existing `desktop` class (1280), so tablet and phone keep the bottom bar.
- `ui-native/src/NavRail.tsx`: the left rail from the same `TABS` list and handler as `TabBar` (icon over label, current tab
  filled ink with card-colour glyph and label, square corners, ink hairline, each tab 92 x 64 through `hitProps`, same coach refs).
- `Shell.tsx` and `shell/SpotHost.tsx`: at desktop the rail replaces the bar (root turns to a row, the screen to its right,
  no bottom-bar row, so `tabsH` is 0). It is drawn only where `bottomBarShown` says: a water's page (regs mode) and the
  add-a-spot flow have none, as the bar. The phone path keeps its tree (one extra full-size wrapper View).
- Note: the app has FIVE tabs (Runs between Conditions and Spots), not four; the rail draws all of them.
- Tests: `NavRail.shell.test.tsx` (rail at 1280; bar at 390 / 899 / 1279; none in regs mode); `a11y.shell.test.tsx` now tells
  the app its width, so the T243 desktop rows draw the rail, plus a rail-in-the-gate case.
- The walkthrough card (`GuideCard` `left`) starts right of the rail at desktop; a test holds it.
- Shots: web 1440 x 900 light and dark, each tab, plus phone 390.

2026-11-17 12:30, owner (app chat dialog, after the shots of Map light, Conditions dark and a phone): "Land it
(Recommended)". Built on `app-fix/t839-desktop-rail`: rail at the desktop class (1280 px and up, all five tabs, 92 wide,
current tab filled ink), tablet and phone keep the bar, hidden in regs mode and while adding a spot. Before landing: the
walkthrough card must not sit over the rail.

2026-11-17 13:06: Android benches 6/6 (install, walkthrough, spots, saved-spots, regs-open, crash-bed) on 71222a2bd.
