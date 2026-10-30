"""T288 — the glossary carries every definition the synopsis prints on its "Definitions" page (PRINTED p.80).

The page's term list is read off the repo PDF (`pipeline.common.synopsis_definitions`, deterministic — no LLM), never
typed here. Three pins: the curated transcription the bundle copies says the PDF's words; every printed term is a
glossary term (its own `def_*` in the book's words, or a written term that explains it under its name, `COVERED`);
and the mutation — a definition missing from the book's text is missing from the glossary, so the check is not
tautological.
"""
from __future__ import annotations

import inspect
import re

import pytest

from pipeline.common.synopsis_definitions import definitions
from pipeline.common.synopsis_pages import default_pdf
from pipeline.deliver.answers import glossary as G
from pipeline.deliver.bundle.glossary_source import rows_from_book
from pipeline.tests.conftest import need

# no silent skips (conftest): the repo synopsis PDF is source data — a run without it FAILS naming it
pytestmark = pytest.mark.needs_source


@pytest.fixture(autouse=True)
def _the_pdf(request):
    need(request, "source", default_pdf(), "the repo synopsis PDF (data/source; see pipeline.common.synopsis_pages)")


def _q(s: str) -> str:
    """Typographic quotes folded: the transcription types straight ones."""
    return s.replace("“", '"').replace("”", '"').replace("’", "'")


def _covered(rows) -> set:
    ids = {t["id"] for t in G.t_definitions(G.Book(rows))}
    return {t for t, _ in definitions() if t.lower() in G.COVERED or f"def_{G._slug(t)}" in ids}


def test_the_page_reads_its_45_terms():
    terms = [t for t, _ in definitions()]
    assert len(terms) == 45
    assert terms[0] == "above" and terms[-1] == "wild trout"
    for t in ("kokanee", "min", "snagging (foul hooking)", "trout/char", "Management Unit", "non-resident alien"):
        assert t in terms
    d = dict(definitions())
    assert d["max"] == "abbreviation for maximum."
    assert d["snagging (foul hooking)"].endswith("must be released immediately.")   # runs on into the next column


def test_the_transcription_says_the_pages_words():
    book = {t: x for k, t, x in rows_from_book() if k == "definition"}
    for term, text in definitions():
        assert _q(book.get(term.lower(), "")) == _q(text), term


def test_every_printed_term_is_a_glossary_term():
    rows = rows_from_book()
    printed = {t for t, _ in definitions()}
    assert _covered(rows) == printed
    src = inspect.getsource(G)
    for word, tid in G.COVERED.items():
        assert word in {t.lower() for t in printed}, word
        assert re.search(rf'_term\(\s*"{tid}"', src), tid
    terms = {t["id"]: t for t in G.t_definitions(G.Book(rows))}
    d = dict(definitions())
    k = terms["def_kokanee"]
    assert k["term"] == "Kokanee" and _q(k["says"]) == _q(d["kokanee"]) and k["quote"] == k["says"]
    assert k["pages"] == [80] and k["source"] == "definitions p.80"
    assert terms["def_adipose_fin"]["pages"] == [13, 80]


def test_a_missing_definition_is_a_missing_term():
    """MUTATION: drop kokanee from the book's text and the completeness check fails."""
    rows = [r for r in rows_from_book() if r[:2] != ("definition", "kokanee")]
    assert "kokanee" not in _covered(rows)
