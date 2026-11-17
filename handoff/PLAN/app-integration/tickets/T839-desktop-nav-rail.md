# T839 — Web on a desktop: the Map / Conditions / Spots / More bar moves to a left navigation rail

Status: OPEN (app, web; app chat)

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

2026-11-17 12:30, owner (app chat dialog, after the shots of Map light, Conditions dark and a phone): "Land it
(Recommended)". Built on `app-fix/t839-desktop-rail`: rail at the desktop class (1280 px and up, all five tabs, 92 wide,
current tab filled ink), tablet and phone keep the bar, hidden in regs mode and while adding a spot. Before landing: the
walkthrough card must not sit over the rail.
