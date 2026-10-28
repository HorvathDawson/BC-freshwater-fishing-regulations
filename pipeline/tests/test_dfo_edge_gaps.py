"""T228: the DFO place edges the bundle could not cut at go on the review's DFO queue (`edge_gaps.fold`)."""
from pipeline.regs.dfo_salmon.edge_gaps import fold


def test_fold_adds_edge_items_and_replaces_the_previous_ones():
    doc = {"suggestions": {"a": {"location_id": "a", "state": "unbound"},
                           "old": {"location_id": "old", "state": "edge_gap", "edge_gaps": [{"water": "X"}]}}}
    rows = [{"location_id": "a", "items": ["gnis:1"], "state": "edge_gap", "edge_gap": {"water": "Atnarko River"}},
            {"location_id": "5b:region:section", "items": ["gnis:3273"], "state": "edge_gap",
             "edge_gap": {"water": "Klinaklini River"}}]
    got = fold(doc, rows)
    assert got["suggestions"]["a"]["state"] == "unbound"                     # an existing row keeps its suggestion
    assert got["suggestions"]["a"]["edge_gaps"] == [{"water": "Atnarko River"}]
    assert "old" not in got["suggestions"]                                  # the previous edge rows are replaced
    assert got["suggestions"]["5b:region:section"]["edge_gaps"] == [{"water": "Klinaklini River"}]
    assert fold(got, rows) == got and got["edge_gaps"] == 2                  # idempotent
