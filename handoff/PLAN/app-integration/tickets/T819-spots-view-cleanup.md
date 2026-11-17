# T819 — Spots view cleanup: review and proposals for the owner

Status: OPEN (app; app chat. A fresh Opus worker reviews the spots area and brings pictures plus a proposed cleanup list;
nothing is built until the owner picks)

Source: owner 2026-11-16, in the app chat (~21:40), verbatim: "the spots view also needs a lot of work to be cleaned up maybe put a fresh agent on it?".
Out of scope here: T815 (photo viewer close under the iOS status bar) and T816 (iOS clipped text).

## Owner, 2026-11-16 ~21:52 (app chat dialog, after the review and its pictures)
Bundles picked (verbatim labels): "B: readable spot page (Recommended)", "A: fix wrong bits (Recommended)", "C: list +
adding", "D: fit & finish": all four, items 1 to 15.
Item 3 (a 1.4 km² creek given "Below normal" from a gauge 676× bigger), verbatim: "Could we just use our gauge network
that colors stuff wtc to determine if it is significant enough? It would drop that in theory. Then ofr small ones still
give a warning if used." Read: a spot takes its gauge from the SAME lead-gauge network the Conditions map colours with
(the release's section → lead gauge, the drainage gate already applied); where that network gives the water no lead, the
spot shows no estimate; where a lead exists but the water is small next to the gauge, the estimate shows with a plain
warning.

## Item 3 corrected, owner 2026-11-16 ~22:00 (app chat dialog)
The map colours the 1.4 km² creek "Below normal" from its only paint panel (08MH001), so "the network that colours the
map" keeps the estimate. Asked whether a gauge that much bigger may speak for a small water at all, on the map and in
spots, he picked (verbatim label) "Allow, with a warning (Recommended)". So a spot keeps the sheet and map lead
(leadOver), and the warning comes from Python (T823, cut-off shown to him with examples).
