# A neat zoomed-out map: the practice behind look A (T1200, built T1208-T1211)

Sources read 2026-11-26: Ordnance Survey, [Text on maps](https://docs.os.uk/more-than-maps/geographic-data-visualisation/guide-to-cartography/text-on-maps);
[Typography (cartography)](https://en.wikipedia.org/wiki/Typography_(cartography)) (Imhof through it); the radical law of
selection in [arXiv 1102.1561](https://arxiv.org/pdf/1102.1561); [OSM boundary relations](https://wiki.openstreetmap.org/wiki/Relation%3Aboundary);
the ArcGIS [Legend](https://developers.arcgis.com/javascript/latest/api-reference/esri-widgets-Legend.html); Material 2
[chips](https://m2.material.io/components/chips); Apple's [pickers](https://developer.apple.com/design/human-interface-guidelines/pickers);
the [Mapbox](https://docs.mapbox.com/style-spec/reference/layers/) and [MapLibre](https://maplibre.org/maplibre-style-spec/layers/) style specs.

| practice | the sources | built as |
|---|---|---|
| Water names blue, italic, biggest first; rivers named along a smoothed course | OS; Imhof | T1210: a label layer in our tiles |
| Towns by rank, then spread; a name above-right, else the next corner | OS; radical law | T1209: three tiers, `text-variable-anchor` |
| Admin boundaries dashed, quieter than roads, a band inside | OSM; OS | T1208: `dash.region`, `line-offset` band |
| A phone's legend collapses to one control; a sheet opens it | ArcGIS legend, Material chips | T1211: the floating Map key |
| A compact day control that opens a calendar | Apple pickers | not built (open with the owner) |

Both engines draw every built layer the same way (T1154): `line-dasharray` (constant), `line-offset`, `concat`,
`text-variable-anchor` with `text-radial-offset` (Mapbox GL JS 0.54+), `symbol-sort-key` (0.53+), GeoJSON sources with
inline data, `zoom` in filters (both read it at whole zooms). One difference met: a zoom `step` or `interpolate` over a
feature-state colour threw in MapLibre GL JS 5.24 (`interpolationFactor is not a function`); look A does not use one.

What the data allowed (measured): the basemap carries 5 BC towns in its z4 tiles, 11 at z5, 30 at z6, with OSM
placeholder populations below that, so the tiers are our own list (`app/packages/map/src/towns.json`, a reason each).
The atlas tiles carry only the 10 largest lakes at z4 and river sections too short to hold a name, so lake and river
names zoomed out need the T1210 tile layer.
