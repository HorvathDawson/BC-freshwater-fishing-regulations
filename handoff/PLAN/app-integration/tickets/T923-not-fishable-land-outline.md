# T923 — Not-fishable land (reserves, private land) is hard to see, on satellite and in light mode

Status: OPEN.

Source: a tester's feedback relayed by the owner 2026-11-19 (`T922-shots/tester-2026-11-19-3.png`). Tester, verbatim: "the
color of the not disable land such as tribal land. It's hard to see when on satellite mode. ... Light mode is even harder
to see". Owner: "I purposely made indigenous and private land a mild color because it was making the map super busy
otherwise. It gets easier to see when you zoom in." Tester: "Maybe keep the color but make a different border color or at
least make the border a bolder color of the same one that fills the area" / "the Same color as the fill but just a tad
darker". Owner: "Could try that ... See what it looks like".

## Work
Keep the fill; give the restricted-land areas an outline in the same hue, a little darker than the fill, at every zoom
(today the border only shows when zoomed in). Light, dark and satellite. Edit `app/packages/map/style/layers.source.json`
only (AGENTS 28), tokens by name. Pictures (a reserve on the Squamish / Cheekye, map + satellite, light + dark, two zooms)
before and after, to the owner.
## Acceptance
Owner picks from the pictures; style build + colour checks pass.
Depends: none. Size: S. Owner: yes (look). Default: outline = fill hue, ~20 % darker, 1 px low zoom → 1.5 px high zoom.
