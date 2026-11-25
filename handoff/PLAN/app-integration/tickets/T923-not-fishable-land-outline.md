# T923 — Not-fishable land (reserves, private land) is hard to see, on satellite and in light mode

Status: DONE — 0ca408307, e5b0b4709, 79e7f4432, 7d4eca0d8, 42aead45f, merge d77d883bd; thicker satellite outline, one look in every theme (owner 2026-11-20 "Looks good, land it"); smoke green, pushed.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T923 | Not-fishable land (reserves, private) hard to see on satellite and light mode: darker same-hue outline (tester via owner) | DONE | 0ca408307, e5b0b4709, 79e7f4432, 7d4eca0d8, 42aead45f, merge d77d883bd | tester 2026-11-19 via owner |

Source: a tester's feedback relayed by the owner 2026-11-19 (`T922-shots/tester-2026-11-19-3.png`). Tester, verbatim: "the
color of the not disable land such as tribal land. It's hard to see when on satellite mode. ... Light mode is even harder
to see". Owner: "I purposely made indigenous and private land a mild color because it was making the map super busy
otherwise. It gets easier to see when you zoom in." Tester: "Maybe keep the color but make a different border color or at
least make the border a bolder color of the same one that fills the area" / "the Same color as the fill but just a tad
darker". Owner: "Could try that ... See what it looks like".

## Work
Keep the fill; give the restricted-land areas an outline in the same hue, a little darker than the fill, from z11, as the edges always started
(today the border only shows when zoomed in). Light, dark and satellite. Edit `app/packages/map/style/layers.source.json`
only (AGENTS 28), tokens by name. Pictures (a reserve on the Squamish / Cheekye, map + satellite, light + dark, two zooms)
before and after, to the owner.
## Acceptance
Owner picks from the pictures; style build + colour checks pass.
Depends: none. Size: S. Owner: yes (look). Default: outline = fill hue, ~20 % darker, 1 px low zoom → 1.5 px high zoom.
## Owner 2026-11-20 ~01:40 (main chat): satellite only, Indigenous land only
After round-1 pictures (map darker outline, satellite barely changed). Verbatim: "Maybe for satellite we could have it
purposely stand out a little more ?" then "make indigenous easy to see only in satellite mode it will change". So: the
map (light/dark) and private land stay as they were; only the reserve outline changes, only on satellite, standing out.
Then, verbatim: "Can be on private too". So on satellite only, private land gets the same stand-out outline (its own hue).

## Done (2026-11-20, lane/t923)
Owner round 2: "make indigenous easy to see only in satellite mode it will change", then "Can be on private too", then "Looks good, land it".
The drawn map (light, dark) and private land are exactly as before. Over SATELLITE ONLY the reserve and private-parcel outlines stand
out: own darker hue (`color.indigenous.edge.imagery` #2E1500, `color.parcel.edge.imagery` #2A2A1C) at full strength, 2.5 px far out
to 3.5 px close in, with a thin light casing under it (`color.imagery.casing` #FFD9B0, fixed in every theme: the photo does not
follow the theme). The edges start at z11, as they always did. Built as `outline.satellite` in `layers.source.json`, a
`<layer>__casing` companion line layer (build-style), and `landOutline` on the existing ground switch (`on-imagery.ts`, also
applied to the first frame in `runtime-style.ts`). Pictures: `T923-shots/round2-*`.

Round 3 (owner 2026-11-20, dialog after the dark-mode comparison): "Second, but thicker" — the one pale casing in every theme, the outline itself wider:
2.5 px far out to 3.5 px close in, the casing 1 px wider each side (4.5 / 5.5). Then (06:46): "Thicker should be the one for light or dark on satellite.
Satellite should maybe just be its own colors? Best for satellite?" Answer: yes. Over satellite the reserve and private-land outline is ONE look in
every theme (light, dark, cvd): the `*.edge.imagery` and `imagery.casing` tokens are identical in all three, the widths are theme-free, and
a test (`native-style.test.ts`) holds the satellite paint identical across themes. The drawn map is unchanged.
