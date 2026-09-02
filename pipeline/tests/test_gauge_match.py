"""One match per build — the thing two consumers must not disagree about."""

from __future__ import annotations

import json

from pipeline.hydro import match as M


class TestOneMatchPerBuild:
    def test_reads_a_cache_rather_than_rematching(self, tmp_path):
        # THE POINT OF THE CACHE IS NOT SPEED. `pipeline.bundle` and
        # `pipeline.hydro.splits` both need to know which water each station sits on, and
        # they used to ask separately — two call sites that could drift on the alias file,
        # the radius, or which stations were passed, silently. Whichever runs first warms
        # this; the second reads the same bytes.
        (tmp_path / M.MATCH_CACHE).write_text(json.dumps([{
            "station": "08MF005", "node_id": "356364114:157377", "status": "matched",
            "resolved_by": "name+radius", "distance_m": 146.2, "reason": "",
        }]))
        got = M.match_for_build(tmp_path)
        assert [m.station for m in got] == ["08MF005"]
        assert got[0].node_id == "356364114:157377"

    def test_a_build_with_no_graph_returns_nothing_rather_than_raising(self, tmp_path):
        # A caller with no build has to be able to skip the gauge tables, not crash.
        assert M.match_for_build(tmp_path) == []

    def test_the_cache_lives_beside_the_build_not_in_data(self):
        # Node ids are PER BUILD: a match cached against one graph names sections another
        # graph does not have. Keeping it in `data/` would make it look shareable.
        assert "/" not in M.MATCH_CACHE
        assert M.MATCH_CACHE.endswith(".json")
