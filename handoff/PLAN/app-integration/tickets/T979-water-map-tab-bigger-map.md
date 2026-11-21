# T979 — iPhone: the water page's Map tab could show a much bigger map (too much white space)

Status: IN PROGRESS. Built by lane 085-ui (phone only, owner pick 1 + trim white space): the report link is now the foot of the Map bar (no row under it), the exit slot is held only on a water that has an exit, the bar's bottom padding trimmed. Web 390x844, Squamish River: map 326 -> 384 px (+58); tablet/desktop split unchanged. After shot t979-after.png (light only; dark and a device not checked). left (Claude): merge + combined review.

Source: owner's in-app report bbf794a9ba18466c, 2026-11-20 16:38, iPhone iOS 18.5, app 0.8.3 (build 40), screen "water ·
map", Squamish River (gnis:25671). Verbatim: "We could make the map a lot bigger here. Right now there is a lot of white
space".

## Work
- Reproduce it on the iPhone simulator (Squamish River, Map tab) and on Android. Take before shots.
- Let the map fill the free height of the tab, with no empty band.
- Take after shots in light and dark mode.

## Acceptance
On iPhone and Android, the Map tab's map fills the tab with no empty band. The before and after shots are in this ticket.

Depends: —. Size: S. Owner: look at the after shots.
