# T902 — Desktop Runs: every fish chip shows, no sideways scroll

Status: IN PROGRESS (app chat; lane app/t902-runs-chips; in 0.8.2 web; pictures to the owner).

Owner 2026-11-19 ~01:16 (app chat), desktop web shot of the Runs map's bottom panel (All · Steelhead · Chinook · Chum ·
Coho · Pink · Sock… cut off at the panel's right edge, a sideways-scrolling row inside a centred column with empty space
both sides), verbatim: "in dekstop why do we need to scroll fish...".

Build: at the desktop layout every species chip is visible without scrolling: the row uses the panel's width and wraps
to a second row only when it must (or the panel widens to fit — whichever reads cleaner); phones keep the scroll row.
Chips keep their look and their order. Tests at 1280/1440 (no horizontal scroll container on desktop; all chips
rendered), 390 unchanged. Pictures before it lands.
