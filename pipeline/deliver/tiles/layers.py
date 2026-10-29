"""The tile layer inventory — the single place that says what ships and what it carries.

Three rules, and all three are corrections of v1:

1. **One purpose per layer.** v1 fused things that want different styling and different
   simplification into one layer, so a style change to one broke the other. `park` and
   `eco_reserve` are drawn differently and closed differently; they are two layers.

2. **Attributes are declared, not inherited.** Only the names below reach a tile. A source
   column that is not listed is dropped, so the tile cannot quietly grow a field nobody
   decided to ship. Every attribute here has a reader in the app or the style; one without
   is removed (`alt`, `mus`, `areas`, `item`, 2026-10-03).

3. **No regulation data, ever.** Tiles carry geometry, identity and administrative
   geography. What a rule *says* lives in the bundle and joins on `id`/`item`. A colour
   comes from feature-state, so switching view or date refetches no tiles.

   Which polygons a water lies inside (parks, reserves, MUs) is geography too, but it is
   the bundle's (`area:` items, `item_section`): the tile carried it as `mus` / `areas`
   and nothing read them.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import ladder


@dataclass(frozen=True)
class LayerSpec:
    name: str
    """snake_case, singular, says exactly one thing."""
    geometry: str
    """line | polygon | point"""
    attrs: tuple[str, ...]
    """The complete property list. Anything not here is dropped at export."""
    why: str
    minzoom: int = 4
    maxzoom: int = ladder.MAX_ZOOM
    ladder: str = "none"
    """magnitude | area | contour | none — which function in ladder.py stamps minzoom."""
    simplify: bool = True
    drop_densest: bool = False
    """Let tippecanoe thin features to keep a tile under budget. NEVER for water:
    a stream that vanishes because a tile was crowded is a stream a person cannot tap."""
    decorative: bool = False
    """This layer is DRAWN AND NOTHING ELSE — not tapped, not highlighted, not searched.

    It says the layer has NO IDENTITY, which is not the same as no attributes. Every other
    layer's first attribute is its feature id and two tests hold it to that; saying
    `decorative=True` is how a layer opts out of identity ON PURPOSE, so that dropping any
    other layer's id stays the failure it should be. A decorative layer may still carry
    what the STYLE needs to draw it — a width driver, say — just nothing that names it.

    Setting this obliges the app to match: the layer must be out of the tap query, out of
    `highlightable`, and out of `featureIds`. `app/tools/tile-contract.test.ts` checks it.
    """
    flows: str | None = None
    """Which way a LINE layer's geometry runs relative to the water: "mouth_to_source", or None
    where the direction means nothing. The app draws flow arrows from it (`stream__arrows`), so
    it is a claim about every feature, not a habit: FWA digitises blue lines mouth first
    (`graph.cutting.line_coords_2d`: coords[0] = mouth), the added streams are oriented mouth
    first by ground elevation (`waters.added_streams.build_dataset._orient_mouth_first`), a
    sectionizer cut is a `substring` of its chain and keeps it, and tippecanoe never reverses
    a line. Measured on the live atlas 2026-10-21: 342/347 junctions mouth-first on the
    Chilliwack/Vedder (z13), 539/542 on the lower Fraser (z9), 62/62 on the Burnaby/Port Moody
    municipal streams (z14) — every exception a side channel meeting its own river at its
    upstream end. An export that reversed a line would turn its arrows upstream."""
    mvt_id: bool = False
    """The feature id (`attrs[0]`, the section handle) is written as the tile feature's own MVT
    id, and ONLY there: the tile carries no `section_id` property (T202, owner 2026-10-27 — the
    copy was 18 MB of the archive). The layer FILE keeps it (the bundle's section access reads it).

    MapLibre Native ignores the style's `promoteId` (N1 spike), so on a device a feature's
    id is empty unless the tile carries one, and `setFeatureState` addresses nothing. GL JS
    reads the same MVT id as `feature.id` for a source-layer it does not promote, so the web
    style no longer promotes the sections either. MVT ids are unsigned integers, so only the
    section layers can do this; the area layers' ids are strings and keep property-based
    styling. `tippe.py` stamps it per feature and cuts the property (`tile_attrs`).
    """
    own_pass: bool = False
    """Built in a tippecanoe pass of its own, z9 to this layer's `maxzoom`, so that maxzoom is
    the pass's TOP zoom and keeps the finer top-zoom tolerance (`tippe.OWN_PASS`). For a layer
    that stops short of the archive and is overzoomed by the app from its last zoom (`mu`)."""


WATER: tuple[LayerSpec, ...] = (
    LayerSpec(
        name="stream", geometry="line", ladder="magnitude", mvt_id=True,
        flows="mouth_to_source",
        attrs=("section_id", "name", "ord"),
        why="Every flowing reach, and the route through a polygon that is a STREAM's own (a "
            "river's wide reach, a slough: the registry's kind) under that polygon's section id, "
            "so the river has no hole at the zooms its polygon is not drawn at. `section_id` is "
            "the feature id the app sets state on. `name` is the REGISTRY owner's name (the "
            "water a tap opens) and is DRAWN — the along-the-line label at z11+. `ord` is "
            "Strahler order and sets line weight; it is read by the style, not by app code. "
            "(`alt`, `mus` and `areas` were carried and read by nothing — search is the "
            "bundle's — so they are gone, 2026-10-03; containment is the bundle's `area:` items.)",
    ),
    LayerSpec(
        name="lake", geometry="polygon", ladder="area", mvt_id=True,
        attrs=("section_id", "name", "water", "area_m2"),
        why="Standing water big enough to fish — and a stream's polygon (a river's wide reach, "
            "a slough's). Same identity fields as stream. `water` is THE WATER KIND, the "
            "registry's `item.kind` (stream | lake): the style draws a `stream` polygon in "
            "stream colours (`lake_flowing`), so a slough never reads as a lake. `area_m2` "
            "earns its place twice: it sets the outline width (sqrt, so one big lake does "
            "not swamp every small one) and it is the label's collision sort key, which is "
            "what stops a cluster of ponds crowding out the lake somebody came for.",
    ),
    LayerSpec(
        name="under_lake", geometry="line", ladder="area", minzoom=10,
        attrs=(), decorative=True,
        why="Where a river's route runs THROUGH a lake — the stitched under-lake fid run. "
            "It is not a stream you can fish and it is not the lake's shape; it is the "
            "thread the topology follows, and drawing it grey and dotted is the only thing "
            "that makes a chain of lakes read as one river. Its own layer precisely so it "
            "can never be styled as though it were open water.\n\n"
            "IT CARRIES NO IDENTITY. It is decoration: a person tapping the dotted thread\n"
            "through a lake means the LAKE, so it is not in the tap order, it is not\n"
            "highlightable, and its only colour mode is static. Dropping `section_id`,\n"
            "`item` and `name` took 3.9% of the archive that was buying a feature id for\n"
            "something nobody can select.\n\n"
            "SO IT CARRIES NOTHING AT ALL. It was briefly given `ord`, to draw it at the\n"
            "weight of the river it continues; that is wrong, and v1 had it right. The\n"
            "route is a CONSTRUCTION LINE — it says the river continues, and nothing about\n"
            "how big it is. Drawn at the mainstem's own weight it stops reading as a note\n"
            "and starts reading as more river, which is precisely the confusion this layer\n"
            "exists in order to avoid. A small constant width, dotted and faint, and the\n"
            "lake's own outline carries the size.",
    ),
    LayerSpec(
        name="wetland", geometry="polygon", ladder="area", minzoom=9, mvt_id=True,
        attrs=("section_id", "name", "water"),
        why="Marsh and swamp. `water` is the registry's kind (stream | wetland: a slough drawn as "
            "a wetland is a stream, `wetland_flowing`). Drawn, occasionally regulated, never searched for by name — "
            "and `name` is here for the tap sheet, not a label: no wetland symbol layer "
            "exists. `area_m2` went with it; wetland has no width spec and no label to "
            "sort, so it was 2.6% of the archive answering nothing.",
    ),
    LayerSpec(
        name="not_fishing_water", geometry="polygon", ladder="area", minzoom=9, decorative=True,
        attrs=(),
        why="NOT FISHING WATER (T152): sewage settling basins, mine tailing ponds and dumps — FWA "
            "polygons that hold water and that no rule speaks of (`pipeline.common.fishing_water`, "
            "stamped by the atlas as `StreamNode.not_fishing`). They were drawn in `lake` and read as "
            "lakes with the region's rules; now no rule binds them, so they are no section anyone can "
            "tap. Their own layer so the style may draw them as a muted non-water fill (the owner's "
            "call, OWNER-TODO T152) without ever styling them as water. Decorative: no identity.",
    ),
    LayerSpec(
        name="contour", geometry="line", ladder="contour", minzoom=11,
        attrs=("item", "depth_m", "index"),
        why="Digitised lake bathymetry (WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP). The scanned "
            "sheets stay remote PDFs; these are the lakes where depth works offline.",
    ),
)

ADMIN: tuple[LayerSpec, ...] = (
    LayerSpec(
        name="park", geometry="polygon", ladder="area", minzoom=6,
        attrs=("area_id", "name", "kind"),
        why="National and provincial parks, protected and recreation areas. `kind` "
            "separates the authority, because a national park is closed by a different "
            "regulation than a provincial one.",
    ),
    LayerSpec(
        name="eco_reserve", geometry="polygon", ladder="area", minzoom=7,
        attrs=("area_id", "name"),
        why="Ecological reserves. Its own layer because it is a blanket closure and is "
            "drawn to look like one — never the same style as a park you may fish in.",
    ),
    LayerSpec(
        name="sign_zone", geometry="polygon", ladder="area", minzoom=9,
        # NO `kind`. Every other admin layer carries one because it separates two things a
        # reader must not confuse — national from provincial park, closed land from permit
        # land. A sign zone has no second kind, so the field would be a column of nulls, and
        # a declared attribute nothing writes is the gap the contract exists to catch.
        attrs=("area_id", "name", "cuts"),
        why="Sign-bounded closures — the area a regulation encloses with fishing boundary "
            "signs, drawn by hand because no fetched layer has them.\n\n"
            "ITS OWN LAYER BECAUSE IT IS NOT A PLACE, IT IS A RULE. A park is geography that "
            "happens to carry regulations; this polygon has no existence apart from the "
            "closure that describes it, and it is metres across where a park is kilometres. "
            "Drawn as a park it would read as somewhere to go rather than somewhere you may "
            "not fish.\n\n"
            "`cuts` NAMES THE WATER, and without it the polygon is a shape with no way back "
            "to the regulation: these rings sit ON a river, the rule is written about that "
            "river, and a reader who taps the zone has to arrive at the Skeena rather than at "
            "an anonymous quadrilateral. It is the registry item id, so the app can go "
            "straight to the water the closure belongs to.",
    ),
    LayerSpec(
        name="wma", geometry="polygon", ladder="area", minzoom=7,
        attrs=("area_id", "name"),
        why="Wildlife management areas. Creston Valley carries its own quotas, and it is the ONLY one drawn "
            "(T202, owner 2026-10-27: `tile_names` on the def in areas.json). The other 34 are still cut and "
            "stamped; nothing regulates them, so their 0.2 MB of tiles answered nothing.",
    ),
    LayerSpec(
        name="indigenous_land", geometry="polygon", ladder="area", minzoom=8,
        attrs=("area_id", "name", "name_indigenous", "group"),
        why="Reserves and treaty lands. An access advisory, never a fishing closure, and "
            "the layer is named and styled so it cannot be mistaken for one.",
    ),
    LayerSpec(
        name="no_access", geometry="polygon", ladder="area", minzoom=9,
        attrs=("area_id", "name", "kind"),
        why="Land the public may not enter, and land they may enter only with permission. "
            "Water inside is unfishable for a reason that has nothing to do with fish.\n\n"
            "`kind` SEPARATES THE TWO, and it has to. 'You may not go here' and 'you may go "
            "here with a permit' are different advice, and drawing them alike gives the "
            "wrong one in both directions. It also un-drops four areas: the source marks "
            "them restriction_level='restricted' rather than 'closed', and the area def "
            "took only 'closed' — so the UBC Malcolm Knapp Research Forest and Ten Mile "
            "Point Ecological Reserve, both real permit-access water, were on nobody's map.",
    ),
    LayerSpec(
        name="tidal_water", geometry="polygon", ladder="none", minzoom=9,
        attrs=("area_id", "name"),
        why="TIDAL WATER (T166, owner 2026-10-28): the DFO tidal-waters polygon (`tidal_boundary` in the build gpkg), "
            "plus the footprint of every stream piece taken off the map below an accepted tidal boundary where the "
            "polygon does not already cover it (`tiles/tidal.py`). The freshwater regulations stop there (RULINGS K7): "
            "the stream line below a tidal boundary is NOT drawn (`undrawn.json` `tidal`), and this area is what a tap "
            "there finds — the app explains it (\"Tidal water — DFO rules\"). `area_id` is `tidal` on every feature "
            "(one meaning, one sheet); `name` the label. No regulation data: the words are the app's.",
    ),
    LayerSpec(
        name="watershed", geometry="polygon", ladder="area", minzoom=5, decorative=True,
        attrs=(),
        why="Named basins a regulation targets — the Liard is 142,173 km2 and 15.7% of "
            "the atlas. Never cut: a drainage divide is not a line water crosses.\n\n"
            "DRAWN AS A WASH AND NOTHING ELSE (T202, owner 2026-10-27): a darker green fill under "
            "everything, so a reader sees where the watershed rule applies. Decorative: no id and no "
            "name in the tile — the area's identity is the bundle's `area:` item, and nothing taps or "
            "labels the wash, so `area_id` and `name` in every z13 tile across 142,000 km2 bought nothing.",
    ),
    LayerSpec(
        name="parcel", geometry="polygon", ladder="none", minzoom=11, decorative=True,
        attrs=(),
        why="PRIVATE LAND, AND ONLY PRIVATE LAND. One polygon: the union of every privately "
            "titled parcel in British Columbia. The source is 1,290,764 individual lots and "
            "the dissolved form is the only one that can ship.\n\n"
            "ONLY PRIVATE, because only private answers a question. The other eight "
            "ownership classes are Crown, agency, federal, municipal and untitled — which "
            "is most of the province, and 'this is Crown land' is the DEFAULT STATE of "
            "British Columbia, not news. Drawing all nine cost 308 MB of layer file for "
            "eight polygons saying 'as you were'. The question a person has at a gate is "
            "whether the ground in front of them is somebody's.\n\n"
            "IT IS NOT AN AREA AND IT IS NOT IN areas.json, for the same reason `mu` is "
            "not: nothing regulates fishing by who holds title. Putting it there would "
            "stamp `in_areas` on most of the province and grow every water feature's "
            "membership list for a fact no rule reads.\n\n"
            "NO ATTRIBUTES AT ALL. It is one polygon and its meaning is its existence — "
            "there is nothing to say about a feature when every feature is the same thing. "
            "No feature id either: you tap the WATER, never the lot. z11 and up (z12 until the "
            "refinement pass, owner 2026-10-22: private land \"should show slightly more zoomed "
            "out\"), because a title boundary is meaningless at the scale of a region; z11 is a "
            "valley-side on a phone, where the question at a gate starts to be asked.",
    ),
    LayerSpec(
        name="region", geometry="polygon", ladder="none", minzoom=4, maxzoom=11,
        # `region_id` ONLY (T202): the label is the id ("7A"), and `name` had no reader.
        simplify=True, attrs=("region_id",),
        why="The eight management REGIONS, each the union of its management units. It is "
            "the same fabric as `mu` dissolved one level up, and it is a separate layer "
            "rather than an attribute for a reason measured earlier: `region` and "
            "`region_name` on every MU feature cost 3.0% of the archive, repeating a "
            "region number into every tile a unit touches. Eight polygons carry the same "
            "fact once.\n\n"
            "IT IS WHAT THE MAP SHOWS AT A DISTANCE, AND ONLY THAT. Zoomed out, 225 unit "
            "boundaries are hatching; the regions are the shape a person navigates by, and "
            "the regulations are organised by them. The units take over from z7 and the "
            "region STOPS at z11 — measured, it cost 38.1 MiB drawn to z14, 27.7 of that "
            "at z14 alone, which is more than all 225 units together. A dissolved region "
            "traces every inlet of the coast it contains, so its boundary is the most "
            "detailed line in the archive and the least looked at up close.\n\n"
            "NOTHING IS LOST BY STOPPING. Every region boundary IS a unit boundary — a "
            "region is a union of units — so the dotted line is still drawn there, and "
            "which region a unit belongs to is 225 rows that belong in the bundle, not a "
            "fact worth re-stating in every tile of the province.",
    ),
    LayerSpec(
        name="mu", geometry="polygon", ladder="none", minzoom=7, maxzoom=12, own_pass=True,
        simplify=True, attrs=("mu_id",),
        why="Wildlife management units, 225 of them. Administrative geography: every "
            "square metre of BC is in one whether or not it is regulated, which is why "
            "this may sit in a tile while a regulated-area membership list may not.\n\n"
            "z7 AND UP, not z4. Below that the units are illegible and `region` is what is "
            "drawn instead — the same handover v1 made. It also stops paying for 225 "
            "polygon boundaries in every low-zoom tile, where nothing could read them.\n\n"
            "TO z12, OVERZOOMED (T202, owner 2026-10-27). The app reads the units through their own "
            "source stopped at z12 (`sources.mus`) and MapLibre magnifies the z12 tile to z18; built in "
            "its own pass (`own_pass`), z12 is that pass's top zoom, so the edge is at most ~8 m off "
            "where z13 had ~4 m. -7 MB: inside the atlas a z13 tile holds streams too, so mu's z13 "
            "squares never deduped. To reverse, see `tippe.OWN_PASS`.",
    ),
)

# NOT BUILT. Places are already in the basemap — Protomaps draws four label layers from
# its own `places` — and drawing ours on top produced a second set of markers for the same
# towns. We still need the gazetteer, but only for SEARCH: name, coordinates, and `rank`
# so a fuzzy match can order "Prince George" above a hamlet of forty. That lives in the
# bundle's `place` table, which is a text index and cannot be a tile.
_RETIRED_LABELS: tuple[LayerSpec, ...] = (
    LayerSpec(
        name="place", geometry="point", ladder="none", minzoom=5, drop_densest=True,
        attrs=("name", "kind", "rank"),
        why="Retired — see above.",
    ),
)

LABELS: tuple[LayerSpec, ...] = ()

# NOT BUILT ANY MORE — kept as a record of a decision, not as a layer.
#
# The world-minus-BC polygon cost about 1 GB (a shape that large lands in every tile at
# every zoom), vanished above the layer's maxzoom, and still left bare canvas under it.
# The basemap is already clipped to the province, so the cutout is free: a background
# colour under the whole map shows through exactly where BC is not. See
# `app/packages/map/src/runtime-style.ts`.
_RETIRED_MASK: tuple[LayerSpec, ...] = (
    LayerSpec(
        name="outside", geometry="polygon", ladder="none", minzoom=4, simplify=True,
        attrs=("side",),
        why="Everything that is NOT British Columbia — the world rectangle with the "
            "province punched out of it. Drawn as a wash over the basemap so a reader can "
            "see at a glance where our answers stop.\n"
            "This is a correctness feature, not decoration. The tiles cover a bounding box "
            "whose corners are Alberta, Alaska, Washington and the Pacific, and a river "
            "that crosses the border keeps being drawn on the far side with no regulation "
            "attached — which reads as 'no restrictions here' rather than 'not our "
            "jurisdiction'. Those are opposite meanings.",
    ),
)

#: The zoom the basin field hands over to the rivers.
#:
#: Below it a river is a hairline and its colour is a guess three pixels wide; above it the
#: rivers are wide enough to carry the colour themselves and a field of catchments over the
#: top of them is noise. One number, named once, because the tile build, the style and the
#: app's tap handling all have to agree about it or the map has a zoom where nothing is
#: clickable or two things answer at once.
#:
#: z8 (T130, owner 2026-10-22: "should be able to zoom out quite a bit more first"): measured
#: with shots at z6-z10, the coloured rivers still read at z8 (magnitude >= 100, ~12k
#: features, connected), while at z7 (magnitude >= 250) they break into hairline fragments
#: on blank land. The SAME number as the app style's field handover (the `basin` layer's
#: maxzoom in app/packages/map/style/layers.source.json, `FIELD_HANDOVER_Z`), held equal by
#: app/tools/handover.test.ts. Archives built at 9 still serve: the style reads basins to z7
#: only, so their z8 basin tiles are simply never asked for.
BASIN_HANDOVER_Z = 8

HYDROLOGY: tuple[LayerSpec, ...] = (
    LayerSpec(
        name="basin", geometry="polygon", ladder="none",
        minzoom=4, maxzoom=BASIN_HANDOVER_Z - 1, simplify=True,
        attrs=("basin_id",),
        why="THE PROVINCE AS A FIELD, for the zooms where a river is a hairline.\n\n"
            "At z5 the question is not what this creek is doing but whether the country you "
            "are driving to is wet or dry, and a percentile is a claim about a CATCHMENT — "
            "a shape, not a line and not a radius. These are the Province's own named "
            "watersheds, the leaves only (under 500 km2), which is the level at which they "
            "stop nesting and become a mosaic: 11,580 polygons cover 2.68M km2 of a "
            "945,000 km2 province because a named watershed contains its tributaries'.\n\n"
            "SIMPLIFIED TO 100 m, AS ONE COVERAGE (refinement pass, 2026-10-22): one pixel at "
            "z10, where the field then handed over (z8 since T130, so it is finer than it needs), and every shared border "
            "simplified once so neighbours never gap or overlap (tiles/basins.py). It was 500 m, "
            "one polygon at a time: 3,552 slivers up to ~700 m and a sawtooth edge at z9.\n\n"
            "`basin_id` AND NOTHING ELSE. The reading is not in the tile — it changes every "
            "half hour and the tile changes once a build — so the id is the join and the "
            "colour arrives as feature-state, exactly as a stream's does. Which station "
            "speaks for which basin IS precomputed, in the bundle's `basin_station`, "
            "because it changes with the gauge network rather than with the weather.",
    ),
)

#: THE THINGS ON THE WATER (T173): marks a reader switches on, never part of the answer the water's colour gives.
FEATURES: tuple[LayerSpec, ...] = (
    LayerSpec(
        name="obstacle", geometry="point", ladder="none", minzoom=10,
        attrs=("obstacle_id", "type", "name", "water", "height_m", "source", "year", "barrier", "habitat", "road", "date"),
        why="FISH OBSTACLES (T173): falls, cascades, canyons, velocity barriers, culverts and dams — the Province's "
            "Obstacles to Fish Passage (FISS), its dam register (WRIS) and its stream-crossing assessments (PSCIS; "
            "owner treats it as OGL-BC, Q165), merged and deduped by `tiles/obstacles.py`. A switch in the Layers "
            "sheet, off by default; a tap names what it is.\n\n"
            "`obstacle_id` is the source's own id (`fiss:<point id>`, `wris:<dam file number>`, `pscis:<stream "
            "crossing id>`), a string, so no MVT id. `type` is one of `obstacles.OBSTACLE_TYPES`; `name` a dam's; "
            "`water` the stream it is on; `height_m` where measured; `source` fiss | wris | pscis; `year` surveyed "
            "(FISS), commissioned (WRIS) or assessed (PSCIS). PSCIS culverts only: `barrier` (barrier | potential | "
            "unknown), `habitat` (high | medium | low), `road`, `date` (assessed, ISO). Every one is read by the tap "
            "sheet; none by the style.\n\n"
            "z10 AND UP, one zoom for every mark: 27,267 marks (12,192 PSCIS culverts), +5.7 MB on the 168 MB atlas "
            "(T173 measured, each obstacle tile appended to the live atlas tile and gzipped: z10 +0.81, z11 +1.10, "
            "z12 +1.58, z13 +2.19 MB; FISS + WRIS alone were +3.0). Below z10 a mark has no stream under it that a "
            "reader can see.",
    ),
)

ALL: tuple[LayerSpec, ...] = WATER + ADMIN + LABELS + HYDROLOGY + FEATURES
BY_NAME = {s.name: s for s in ALL}

assert len({s.name for s in ALL}) == len(ALL), "layer names must be unique"


def tile_attrs(spec: LayerSpec) -> tuple[str, ...]:
    """The properties a TILE feature of this layer may carry: `attrs`, less the section id on an MVT-id layer (it is
    the feature's own id there, `LayerSpec.mvt_id`). `attrs` stays the layer FILE's list, which keeps it."""
    return spec.attrs[1:] if spec.mvt_id else spec.attrs


def contract() -> dict:
    """The tile contract, as data — the file both sides of the seam read.

    The app's map style names a `sourceLayer` and a `featureIdProperty` for every layer it
    draws. Those are strings, and nothing checked they matched what the pipeline emits: the
    style said `sections`/`section_id` while the tiles carried `stream`/`id`, which draws
    NOTHING and errors NOWHERE. It sat in the repo the whole time.

    So the pipeline writes this and `app/tools/tile-contract.test.ts` fails if the style
    names a layer or a property that does not exist here. Drift stops being possible.
    """
    return {
        "$comment": "GENERATED by `python -m pipeline.deliver.tiles --write-contract`. "
                    "Do not edit; edit pipeline/deliver/tiles/layers.py and regenerate.",
        "layers": {
            s.name: {
                "geometry": s.geometry,
                # what the TILE carries — an MVT-id layer's `featureId` is the feature's own id, not a property
                "attrs": list(tile_attrs(s)),
                # None for a decorative layer, which has no identity to promote — even
                # when it carries attributes, because those are for the style, not for
                # naming the feature. The app's contract test reads this: a style layer may
                # only name a featureIdProperty where the tile actually has one.
                "featureId": None if s.decorative else s.attrs[0],
                "decorative": s.decorative,
                # true: `featureId` is the MVT feature id and NOT a property (T202) — both renderers
                # read it as `feature.id`, and the style must not promote it.
                "mvtId": s.mvt_id,
                "minzoom": s.minzoom,
                "maxzoom": s.maxzoom,
                "ladder": s.ladder,
                # Which way a line runs — the flow arrows read it (see `LayerSpec.flows`).
                "flows": s.flows,
            }
            for s in ALL
        },
        # THE LADDER ITSELF, not just its name.
        #
        # The app draws one thing the tiles do not: a dot per gauge, on a GeoJSON source
        # that has no zoom ladder of its own. Left alone it drew every station in the
        # province at every zoom, so a dot for a creek gauge sat over a country where its
        # creek had thinned out four zooms ago — a reading with no water under it.
        #
        # Publishing the stops here rather than restating them in TypeScript is what stops
        # the two from drifting: `app/tools/tile-contract.test.ts` fails if they differ.
        "magnitudeLadder": [list(pair) for pair in ladder.MAGNITUDE_LADDER],
    }
