# T816 — iOS: text cut off (the hour on "When were you there?", the weather icons on the spot card)

Status: OPEN (app, iOS; app chat)

Source: owner 2026-11-16 ~21:22, testing iPhone TestFlight build 36 (app chat; pictures in the chat). Verbatim: "text is cut off weird". His pictures: on "When were you there?" (saving a spot, Lynn Creek) the hour
"18:00" shows with its top cut off; on the spot card's WEATHER line the two weather icons before "wind 9 km/h" are cut
to fragments.

## Look at
Inferred, not checked: T741 (in build 36) moved to per-weight font files; a font whose line box is shorter than its
glyphs clips on iOS (lineHeight smaller than the font's ascent + descent, or a fixed height). The weather icons may be a
glyph font or an icon set drawn in a text box of a fixed height. Reproduce on the iOS simulator, fix by the font metrics
or the box, not by per-screen nudges; Android and web unchanged.
