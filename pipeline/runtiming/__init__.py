"""Run timing: when each salmon and steelhead run enters fresh water, and on which reach.

THE THREE-WAY SPLIT IS BY WHAT EACH HALF NEEDS, exactly as `pipeline/gauges` splits, and
for the same reason — so "do not re-run the join on every build" is a fact about the import
graph rather than something to remember.

    fetch/      needs HTTP and nothing else. Three pages and a tile cache, saved verbatim
                to data/source/runtiming/. Runs about once a year, because that is how
                often the Pacific Salmon Foundation revises the averages.

    generate/   needs the atlas: 2.6 GB of stream geometry. Runs by hand after a fetch,
                on a machine that can hold it. Writes the frozen index.

    consume/    needs only the frozen index. Runs on every build. Cannot reach `generate`.

`runs.py` sits at the top rather than in a subpackage because it belongs to both halves:
the record `generate` writes and `consume` reads, plus the run-label derivation that turns
"Skeena Coastal Winters" into a winter run.

WHAT MAKES THIS DIFFERENT FROM GAUGES. A gauge answers with one reading. A reach answers
with a LIST of runs — 4.45 species on average, and on the Skeena mainstem two steelhead
runs six months apart. Nothing here may collapse that list to one curve; see
`generate/profiles.py` for where the map is allowed to, and why the sheet is not.
"""

from pipeline.runtiming.runs import Run, derive_form, derive_label, read_runs, write_runs

__all__ = ["Run", "derive_form", "derive_label", "read_runs", "write_runs"]

# `generate.index` and `generate.profiles` are DELIBERATELY not re-exported: importing them
# pulls in shapely's STRtree, mapbox_vector_tile and the expectation of a completed tile
# build. Reach for them explicitly when you mean to rebuild the index — a few times a year.
