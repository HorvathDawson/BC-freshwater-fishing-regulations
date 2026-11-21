# T979 — iPhone: the water page's Map tab could show a much bigger map (too much white space)

Status: IN PROGRESS. Lane 085-ui measured it on the web build at 390x844 (Squamish River, Map tab): the map is 326 px of 844. The bar under it is 324 px and holds, by design, rows kept empty so nothing moves (T731/T875): the exit slot 44 px (empty at a part that is not the mouth), the name's two-line floor 46 px (one line used), then the report row 52 px below. The ~90 px of white is those held rows. No code change: taking them back trades against the owner's T875 'the steps stay put' rule, so it needs an owner pick (e.g. drop the exit slot where the water has no exit, or put the report link in the bar's foot). left (Claude): owner pick, then build.

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
