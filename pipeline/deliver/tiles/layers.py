"""The tile layer inventory — the single place that says what ships and what it carries.

Three rules, and all three are corrections of v1:

1. **One purpose per layer.** v1 fused things that want different styling and different
   simplification into one layer, so a style change to one broke the other. `park` and
   `eco_reserve` are drawn differently and closed differently; they are two layers.

2. **Attributes are declared, not inherited.** Only the names below reach a tile. A source
   column that is not listed is dropped, so the tile cannot quietly grow a field nobody
   decided to ship. Every attribute here has a use in the app.

3. **No regulation data, ever.** Tiles carry geometry, identity and administrative
   geography. What a rule *says* lives in the bundle and joins on `id`/`item`. A colour
   comes from feature-state, so switching view or date refetches no tiles.

   The one that looks like an exception is not: `areas` and `mus` say which polygons a
   water lies inside. That is true whether or not anybody regulates them — and because we
   carry every park, reserve and indigenous land rather than only the regulated ones, the
   membership list is geography, not a leaked rule.
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
    maxzoom: int = 14
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


WATER: tuple[LayerSpec, ...] = (
    LayerSpec(
        name="stream", geometry="line", ladder="magnitude", maxzoom=14,
        attrs=("section_id", "name", "alt", "ord", "mus", "areas"),
        why="Every flowing reach. `section_id` is the feature id the app sets state on. "
            "`name` is DRAWN — the along-the-line label at z11+ — and `alt` is the search "
            "haystack: lowercased, deduped, '|'-separated. `ord` is Strahler order and "
            "sets line weight; it is read by the style, not by app code.",
    ),
    LayerSpec(
        name="lake", geometry="polygon", ladder="area",
        attrs=("section_id", "name", "alt", "area_m2", "mus", "areas"),
        why="Standing water big enough to fish. Same identity fields as stream. `area_m2` "
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
        name="wetland", geometry="polygon", ladder="area", minzoom=9,
        attrs=("section_id", "name"),
        why="Marsh and swamp. Drawn, occasionally regulated, never searched for by name — "
            "and `name` is here for the tap sheet, not a label: no wetland symbol layer "
            "exists. `area_m2` went with it; wetland has no width spec and no label to "
            "sort, so it was 2.6% of the archive answering nothing.",
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
        name="wma", geometry="polygon", ladder="area", minzoom=7,
        attrs=("area_id", "name"),
        why="Wildlife management areas. Creston Valley carries its own quotas.",
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
        name="watershed", geometry="polygon", ladder="area", minzoom=5,
        attrs=("area_id", "name"),
        why="Named basins a regulation targets — the Liard is 142,173 km2 and 15.7% of "
            "the atlas. Never cut: a drainage divide is not a line water crosses.",
    ),
    LayerSpec(
        name="parcel", geometry="polygon", ladder="none", minzoom=12, decorative=True,
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
            "No feature id either: you tap the WATER, never the lot. z12 and up, because a "
            "title boundary is meaningless at the scale of a region.",
    ),
    LayerSpec(
        name="region", geometry="polygon", ladder="none", minzoom=4, maxzoom=11,
        simplify=True, attrs=("region_id", "name"),
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
        name="mu", geometry="polygon", ladder="none", minzoom=7, simplify=True,
        attrs=("mu_id",),
        why="Wildlife management units, 225 of them. Administrative geography: every "
            "square metre of BC is in one whether or not it is regulated, which is why "
            "this may sit in a tile while a regulated-area membership list may not.\n\n"
            "z7 AND UP, not z4. Below that the units are illegible and `region` is what is "
            "drawn instead — the same handover v1 made. It also stops paying for 225 "
            "polygon boundaries in every low-zoom tile, where nothing could read them.",
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

ALL: tuple[LayerSpec, ...] = WATER + ADMIN + LABELS
BY_NAME = {s.name: s for s in ALL}

assert len({s.name for s in ALL}) == len(ALL), "layer names must be unique"


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
                "attrs": list(s.attrs),
                # None for a decorative layer, which has no identity to promote — even
                # when it carries attributes, because those are for the style, not for
                # naming the feature. The app's contract test reads this: a style layer may
                # only name a featureIdProperty where the tile actually has one.
                "featureId": None if s.decorative else s.attrs[0],
                "decorative": s.decorative,
                "minzoom": s.minzoom,
                "maxzoom": s.maxzoom,
                "ladder": s.ladder,
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
