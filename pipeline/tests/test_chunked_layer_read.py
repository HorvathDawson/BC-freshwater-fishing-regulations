"""Chunked layer reads must be indistinguishable from reading the layer whole.

`FWADataAccessor.iter_layer` exists because reading the 4.9M-row `streams` layer in one call holds
the Arrow table AND the pandas frame at once — ~33 GB, close enough to a 48 GB machine's ceiling that
the kernel can take the process. Chunking bounds peak memory by chunk size instead.

That trade is only acceptable if the seams are invisible, and the risks are specific:

  * a row skipped or duplicated where two slices meet;
  * an exact-multiple row count reading one chunk too few, or too many;
  * a column whose dtype infers differently in a chunk where it happens to be all-null, since
    `_normalize_columns` now runs per chunk rather than once over the whole frame.

These pin all three against the real gpkg, on small layers so they stay cheap.
"""

from __future__ import annotations

import os

import pytest

from data.data_extractor import FWADataAccessor
from pipeline.common.curated import CURATED, SOURCE

_DATA = str(SOURCE / "bc_fisheries_data.gpkg")
_needs_data = pytest.mark.skipif(not os.path.exists(_DATA), reason="needs data/bc_fisheries_data.gpkg")


def _frames_equal(whole, chunks) -> tuple[bool, str]:
    import pandas as pd
    joined = pd.concat(chunks, ignore_index=True) if chunks else whole.iloc[0:0]
    if len(joined) != len(whole):
        return False, f"row count {len(joined)} != {len(whole)}"
    if list(joined.columns) != list(whole.columns):
        return False, f"columns {list(joined.columns)} != {list(whole.columns)}"
    for col in whole.columns:
        if col == "geometry":
            a = [g.wkb if g is not None else None for g in whole[col]]
            b = [g.wkb if g is not None else None for g in joined[col]]
        else:
            # NaN != NaN, so compare NaN-aware or every null column "differs"
            a = [None if pd.isna(v) is True else v for v in whole[col]]
            b = [None if pd.isna(v) is True else v for v in joined[col]]
        if a != b:
            bad = next(i for i, (x, y) in enumerate(zip(a, b)) if x != y)
            return False, f"column {col!r} differs at row {bad}: {a[bad]!r} != {b[bad]!r}"
    return True, ""


@_needs_data
@pytest.mark.parametrize("layer,chunk", [
    ("wmu", 50),        # 225 rows: several seams
    ("wmu", 225),       # EXACTLY the row count: the off-by-one boundary
    ("wmu", 1000),      # bigger than the layer: one chunk, no seam
    ("manmade", 500),   # a second layer, different column mix
])
def test_chunked_read_matches_whole_layer(layer, chunk):
    fwa = FWADataAccessor(_DATA)
    whole = fwa.get_layer(layer)
    chunks = list(fwa.iter_layer(layer, chunk_size=chunk))
    ok, why = _frames_equal(whole, chunks)
    assert ok, f"{layer} @ chunk={chunk}: {why}"


@_needs_data
def test_a_bbox_read_is_a_single_chunk():
    """A bbox read is already small, so it must not be sliced — slicing a spatial filter would
    interact with the filter's own row ordering."""
    fwa = FWADataAccessor(_DATA)
    bbox = (1280000, 465000, 1300000, 485000)
    chunks = list(fwa.iter_layer("streams", bbox=bbox, chunk_size=10))
    assert len(chunks) == 1, "a bbox read must stay one chunk regardless of chunk_size"
    assert len(chunks[0]) == len(fwa.get_layer("streams", bbox=bbox))


@_needs_data
def test_dtypes_survive_an_all_null_chunk():
    """`_normalize_columns` runs per chunk now. If a column is entirely null inside one chunk it can
    infer a different dtype there, and the concatenated result would disagree with a whole read."""
    fwa = FWADataAccessor(_DATA)
    whole = fwa.get_layer("wmu")
    chunks = list(fwa.iter_layer("wmu", chunk_size=17))    # odd size: nulls land mid-chunk
    import pandas as pd
    joined = pd.concat(chunks, ignore_index=True)
    assert {c: str(t) for c, t in joined.dtypes.items()} == {c: str(t) for c, t in whole.dtypes.items()}
