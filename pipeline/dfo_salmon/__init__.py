"""dfo_salmon — DFO recreational *salmon* limits/openings/closures for BC Regions 1–8.

Separate authority from the provincial Synopsis this repo is built on: salmon in
non-tidal water is federal (DFO), everything else is provincial. The two rule sets
must never be merged — see this package's README.md.

Submodules are imported directly (`from pipeline.dfo_salmon.fetch import fetch_all`);
this file stays empty so `python -m pipeline.dfo_salmon.fetch` does not double-import.
"""
