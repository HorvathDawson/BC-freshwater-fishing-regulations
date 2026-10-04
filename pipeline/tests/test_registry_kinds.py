"""An `area:` item must never be mistaken for a water.

WHY THIS HAS ITS OWN FILE. The registry stores two kinds of thing and `section_ids` means a
different relation for each — identity for a water, containment for an area. Nothing in the
type system says so, so it has been written wrong three times independently:

    bundle `_gauges`        123 of 220 lake_gauge rows named a park
    bundle `_place_water`   35,133 of 352,666 rows offered a park as "water near this town"
    tiles  `_identity`      every feature inside a park took the park's id and slug

All three were `setdefault`/first-wins loops, and `area:` sorts first while outnumbering
waters twelve to one — so the park won every time, and none of them errored.

These tests pin the shared helper AND assert that each of the three readers uses it, because
the bug is not "the helper is wrong", it is "somebody wrote the loop again without it".
"""

from __future__ import annotations

import inspect

from pipeline.common.registry_kinds import AREA_KINDS, is_water, waters


class TestTheHelper:
    def test_an_area_is_not_a_water(self):
        assert not is_water({"id": "area:park:garibaldi", "kind": "area"})

    def test_every_water_kind_passes(self):
        for kind in ("stream", "lake", "wetland"):
            assert is_water({"id": "blk:1", "kind": kind}), kind

    def test_it_reads_kind_not_the_id_scheme(self):
        # `kind` is the fact; the id is a naming scheme and a scheme can be renamed. They
        # agree exactly today (1,473 of 1,473) — this asserts which one is load-bearing.
        assert not is_water({"id": "renamed:whatever", "kind": "area"})
        assert is_water({"id": "area_lookalike:1", "kind": "stream"})

    def test_a_missing_kind_falls_back_to_the_id_rather_than_guessing_water(self):
        # Guessing "water" would let a park through, which is the failure this prevents.
        assert not is_water({"id": "area:park:x"})
        assert is_water({"id": "blk:1"})

    def test_it_accepts_an_object_as_well_as_a_dict(self):
        # The registry is read both ways; a helper only half the callers can use is no help.
        class Item:
            id, kind = "area:park:x", "area"
        assert not is_water(Item())

    def test_an_enum_kind_is_unwrapped(self):
        class Kind:
            value = "area"

        class Item:
            id, kind = "area:park:x", Kind()
        assert not is_water(Item())

    def test_waters_preserves_order(self):
        items = [{"id": "area:a", "kind": "area"}, {"id": "b", "kind": "lake"},
                 {"id": "c", "kind": "stream"}]
        assert [i["id"] for i in waters(items)] == ["b", "c"]

    def test_area_is_the_only_non_water_kind(self):
        # If a new administrative kind is added, it must be added to AREA_KINDS too — this
        # fails loudly rather than letting it silently count as a water.
        assert AREA_KINDS == {"area"}


class TestEveryReaderUsesIt:
    """The three sites that got it wrong. Reading the source is the point: the bug was
    always a hand-rolled loop, never a wrong helper."""

    def _src(self, module, fn):
        import importlib
        return inspect.getsource(getattr(importlib.import_module(module), fn))

    def test_the_bundle_gauge_owner_map_filters(self):
        assert "is_water(" in self._src("pipeline.deliver.bundle.build", "_gauges")

    def test_the_bundle_place_water_owner_map_filters(self):
        assert "is_water(" in self._src("pipeline.deliver.bundle.build", "_place_water")

    def test_the_bundle_item_table_filters(self):
        assert "is_water(" in self._src("pipeline.deliver.bundle.build", "_items")

    def test_the_tile_export_reads_no_registry(self):
        # The one that draws the map used to build an item map from the registry (`_identity`,
        # filtered to waters) for `item`/`alt` attributes nothing read. Identity and search are
        # the bundle's (`item_section`, `alias`): the tile export opens no registry at all now
        # (2026-10-03), so there is no reader here to hold to `waters(`.
        import inspect
        import pipeline.deliver.tiles.export as tiles_export
        src = inspect.getsource(tiles_export)
        assert "_identity" not in src and "registry.json" not in src and "registry_kinds" not in src
