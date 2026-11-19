# T904 — Desktop Conditions: the map fills the window behind; no white bars

Status: IN PROGRESS (main chat; lane app/t904-conditions-desktop; pictures to the owner before it lands; staging web).

Owner 2026-11-19 ~06:40 (main chat), on a desktop staging shot of Conditions (Harrison River watershed: the "Flow vs.
past years" key and the day strip sit on a full-width white bar under the map, and a white strip runs down the right
edge), verbatim: "also could we make the conditions menu have map in backgroudn and remove big white bars on dekstop?"

## Work
- Desktop layout class only (phones and narrow web unchanged): on Conditions the map fills the whole window under the
  side bar, edge to edge; the key + watershed day strip become a card floating over the map at the bottom (square
  corners, map UI layout rule: top = search + day; the legend and its control in the bottom bar as a card), like the
  other modes' bottom card. No white bar at the bottom or the right.
- Same in light and dark; the card never covers the map's own controls or attribution.
- Render test at 1280 / 1279 px; pictures (1440 light + dark, before/after) to the owner before it lands.

## Owner 2026-11-19 ~07:15
Shown the before/after pictures; owner, verbatim: "yes we can add stuff to staging a okay as we work". So T904 goes to
the staging website as soon as its checks pass.
