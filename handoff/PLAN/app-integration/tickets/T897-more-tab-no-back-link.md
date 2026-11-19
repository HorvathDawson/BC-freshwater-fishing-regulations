# T897 — The More tab has no "‹ <previous tab>" link

Status: IN PROGRESS (app chat; lane app/t897-more-no-back; in 0.8.2).

Owner 2026-11-19 ~01:04 (app chat), desktop web shot of the More tab headed "‹ Spots / More", verbatim: "why does more
link to spots?"

Why: T48 gave the More tab a ‹ back to the tab it was opened from (BrowseHost.tsx, `back={TABS.find(... browse.prev
...)}`), from when More was the hamburger's page. Now More is a tab like the others, and the tab bar (phone) or the rail
(desktop) is always there, so the link only names whichever tab was last open — it reads as if More belonged to Spots.
Build: the More tab draws no back link on any layout (title "More" alone); the pages opened FROM More (Help, About, …)
keep their ‹ More. Android's back button still steps back as the back layers say. Tests: the More tab's head has no back
press at 390 and 1440; a page opened from it still has ‹ More; the surfaces test (one way per action) stays green.
