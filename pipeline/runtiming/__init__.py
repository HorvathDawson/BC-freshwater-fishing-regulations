"""Run timing: when each salmon and steelhead run is in fresh water, and on which reach — FED AS ITS OWN STREAM
(T162 parse, T199 feed; `README.md` is the map).

    fetch/      HTTP only: the PSF pages, the CU tiles and NuSEDS, saved under data/source/runtiming/ (owner-run,
                about once a year).
    parse.py    seconds, no atlas: the fetched pages -> data/source/runtiming/timing.json (tracked source data).
    feed/       base -> build -> publish: the run-timing feed the app's Map tab reads from R2 `runtiming/`
                (`.github/workflows/feeds-runtiming.yml`; the data contract is `feed/__init__.py`).
    generate/extract.py   `runs_from_page`, what `parse` reads the PSF page with.

`runs.py` is the run record and the label derivation ("Skeena Coastal Winters" -> a winter run). A reach answers with a
LIST of runs (the Skeena carries two steelhead runs six months apart); nothing here collapses it to one curve.
(T436) The 2026-09-08 frozen-index path (`generate/{index,passage,profiles,indicators,crosswalk}`, `consume/bundle`)
had no importer and was deleted; git keeps it.
"""

from pipeline.runtiming.runs import Run, derive_form, derive_label, read_runs, write_runs

__all__ = ["Run", "derive_form", "derive_label", "read_runs", "write_runs"]
