"""The answers layer against the consumer page v35, field by field (pipeline/deliver/answers/reference).

The golden side is what the page itself shows, produced by `reference/regenerate.sh` (node runs the
page's own script over the current export) into a directory OUTSIDE the repo; point `ANSWERS_GOLDEN`
at its `golden/`. The pipeline side is the answers layer's projection onto `compare.py`'s meeting
point: either files in `ANSWERS_DIR` (`<stream>.jsonl[.gz]` of `{"key", "value"}`) or a module
`pipeline.deliver.answers.page_view` with `view(keys: dict[stream, list[key]]) -> dict[stream, dict]`.
Until both exist the comparison SKIPS; the comparator's own tests always run.
"""
from __future__ import annotations

import hashlib
import importlib
import os
from pathlib import Path

import pytest

from pipeline.deliver.answers.reference.compare import (STREAMS, answers_view, compare, diff_values,
                                                        golden_view, summary)

REF = Path(__file__).resolve().parents[1] / "deliver" / "answers" / "reference"
#: page_v35.js is the page's <script>, byte for byte; a change here means it is no longer the page
PAGE_SHA256 = "d6855ef640287127f0350de322cba10138fc0edb3a6c6ce6c3d633af546411f2"


def test_page_script_is_verbatim():
    assert hashlib.sha256((REF / "page_v35.js").read_bytes()).hexdigest() == PAGE_SHA256


def test_diff_values_reports_each_field():
    page = {"status": "keep", "daily": 2, "roles": {"a": ["governs", None]}, "lines": [{"t": "rel", "b": 30}]}
    same = {"status": "keep", "daily": 2, "roles": {"a": ["governs", None]}, "lines": [{"t": "rel", "b": 30}]}
    assert diff_values(page, same) == []
    got = {"status": "keep", "daily": 4, "roles": {"a": ["also", None], "b": ["moot", "a"]}, "lines": []}
    d = diff_values(page, got)
    paths = {p for p, _, _ in d}
    assert ("daily",) in paths and ("roles", "a", 0) in paths and ("roles", "b") in paths
    assert ("lines", "len") in paths
    assert (("daily",), 2, 4) in d


def test_compare_counts_missing_extra_and_differing_keys():
    want = {"answer": {("w", "p", "07-01", "RB", "wild"): {"daily": 1}, ("w", "p", "07-01", "CT", "wild"): {"daily": 2}}}
    got = {"answer": {("w", "p", "07-01", "RB", "wild"): {"daily": 3}, ("w", "p", "07-01", "EB", "wild"): {"daily": 2}}}
    r = compare(want, got, ["answer"])["answer"]
    assert (r["n_only_page"], r["n_only_pipeline"], r["n_differ"]) == (1, 1, 1)
    assert "answer" in summary({"answer": r})


def _golden() -> Path | None:
    g = os.environ.get("ANSWERS_GOLDEN")
    return Path(g) if g and Path(g, "manifest.json").exists() else None


def _pipeline_view(want: dict) -> dict | None:
    d = os.environ.get("ANSWERS_DIR")
    if d and Path(d).is_dir():
        return answers_view(Path(d))
    try:
        mod = importlib.import_module("pipeline.deliver.answers.page_view")
    except ImportError:
        return None
    return mod.view({s: list(want[s]) for s in STREAMS})


@pytest.fixture(scope="module")
def views():
    g = _golden()
    if g is None:
        pytest.skip("no golden outputs: run reference/regenerate.sh OUT and set ANSWERS_GOLDEN=OUT/golden")
    want = golden_view(g)
    got = _pipeline_view(want)
    if got is None:
        pytest.skip("the answers layer has no page view yet (ANSWERS_DIR or pipeline.deliver.answers.page_view)")
    return want, got


@pytest.mark.parametrize("stream", STREAMS)
def test_answers_match_the_page(views, stream):
    want, got = views
    rep = compare(want, got, [stream])
    r = rep[stream]
    assert (r["n_only_page"], r["n_only_pipeline"], r["n_differ"]) == (0, 0, 0), summary(rep)
