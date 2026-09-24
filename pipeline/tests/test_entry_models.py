"""Contract tests for the op + split binding models that survive in entry_models.py."""

import pytest
from pydantic import ValidationError

from pipeline.regs.parsing.entry_models import Extent, Op


def test_extent_arity():
    assert Extent(op=Op.WHOLE).splits == []
    assert len(Extent(op=Op.UPSTREAM_OF, splits=["falls"]).splits) == 1
    assert len(Extent(op=Op.BETWEEN, splits=["a", "b"]).splits) == 2
    assert Extent(op=Op.WITHIN, area_id="GARIBALDI PARK").area_id == "GARIBALDI PARK"
    assert Extent(op=Op.WITHIN, area_id="area:park:wells_gray", feature_types=["lake", "wetland"]).feature_types == ["lake", "wetland"]
    with pytest.raises(ValidationError):
        Extent(op=Op.WITHIN, area_id="x", feature_types=["fish"])       # invalid feature kind
    # feature_types on ANY op: the builder applies it to the rule's reach after the walk
    # ("lakes of the Fraser watershed"), so the model no longer refuses the shape it reads.
    assert Extent(op=Op.WHOLE, item_id="gnis:1", feature_types=["lake"]).feature_types == ["lake"]
    assert Extent(op=Op.UPSTREAM_OF, splits=["a"], feature_types=["stream"]).feature_types
    with pytest.raises(ValidationError):
        Extent(op=Op.WHOLE, feature_types=["fish"])                  # still a real kind
    for bad in (
        dict(op=Op.UPSTREAM_OF, splits=[]),          # needs 1
        dict(op=Op.UPSTREAM_OF, splits=["a", "b"]),  # too many
        dict(op=Op.BETWEEN, splits=["a"]),           # needs 2
        dict(op=Op.WHOLE, splits=["a"]),             # takes none
        dict(op=Op.WITHIN),                          # needs area or splits
    ):
        with pytest.raises(ValidationError):
            Extent(**bad)


