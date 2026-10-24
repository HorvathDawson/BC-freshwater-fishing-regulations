"""T61 2.8 (audit F20): ONE name key, `pipeline.common.names.key`.

The table is whole values: every case states the full key, never a fragment (AGENTS 33). The mutation pins below
each break ONE step of `key` and prove a case in the table fails, so no step can be dropped silently. The callers are
pinned too: every module that used to carry its own normaliser now answers with `key` (or names the other question it
asks — `compact_key`, `drop_notes`, the app's `search_key`).
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import pytest

from pipeline.common import names
from pipeline.common.names import compact_key, drop_notes, fold, key
from pipeline.common.search_key import search_key

ROOT = Path(__file__).resolve().parents[2]

TABLE = [
    # the audit's case: one key for every spelling of St. Mary's (it had four)
    ("St. Mary's River", "st marys river"),
    ("ST. MARY'S RIVER", "st marys river"),
    ("St Marys River", "st marys river"),
    ("St. Mary’s River", "st marys river"),
    ("St. Mary‘s River", "st marys river"),
    ("St. Maryʼs River", "st marys river"),
    ("St. Mary`s River", "st marys river"),
    # St./Saint and Mt./Mount are NOT folded (measured: no registry pair)
    ("Saint Mary's River", "saint marys river"),
    ("Mt. Milligan Lake", "mt milligan lake"),
    # accents
    ("Barrière Lake", "barriere lake"),
    ("Pouce Coupé River", "pouce coupe river"),
    ("Tŝilhqox Biny", "tsilhqox biny"),
    ("Taaltsux̱éi Héen", "taaltsuxei heen"),
    ("T’ooch’ Héeni", "tooch heeni"),
    ("Sts'a'í:les", "stsai les"),
    # hyphens, dashes and other punctuation are separators
    ("Pend-d'Oreille River", "pend doreille river"),
    ("Pend D'Oreille River", "pend doreille river"),
    ("Kootenay Lake — Main Body", "kootenay lake main body"),
    ("WOLF LAKE *", "wolf lake"),
    ("FOWL CREEK LAKE NO.2", "fowl creek lake no 2"),
    ("Champion Lake #1", "champion lake 1"),
    ("snake_case", "snake case"),
    # parentheticals are kept as words (dropping them is the matcher's `drop_notes`)
    ("Kwotlenemo (Fountain) Lake", "kwotlenemo fountain lake"),
    # whitespace and case
    ("  Chilliwack \t RIVER\n", "chilliwack river"),
    ("﻿Mabel Lake", "mabel lake"),
    # the kind word stays, and abbreviations are not expanded
    ("Yakoun Lake", "yakoun lake"),
    ("Unnamed Boulder Cr", "unnamed boulder cr"),
    # letters that do not decompose stay letters; compatibility forms fold
    ("Łąka Lake", "łaka lake"),
    ("ﬁsh Lake", "fish lake"),
    ("Ｅｌｋ Lake", "elk lake"),
    ("Straße Creek", "strasse creek"),
    ("", ""),
    (None, ""),
]


@pytest.mark.parametrize("text,want", TABLE, ids=[repr(t) for t, _ in TABLE])
def test_the_table(text, want):
    assert key(text) == want


def test_compact_key_and_drop_notes():
    assert compact_key("Logger's Lane") == "loggerslane"
    assert compact_key("Mc Kay Creek") == compact_key("McKay Creek") == "mckaycreek"
    assert compact_key("Brûlé Creek") == "brulecreek"
    assert drop_notes("Kwotlenemo (Fountain) Lake") == "Kwotlenemo   Lake"
    assert key(drop_notes("Kwotlenemo (Fountain) Lake")) == "kwotlenemo lake"
    assert fold("Barrière Lake") == "Barriere Lake"


def _broken(**over):
    """`key` with one step replaced."""
    def k(text):
        s = over.get("fold", fold)(text)
        s = over.get("case", str.casefold)(s)
        s = over.get("apostrophe", lambda t: names._APOSTROPHES.sub("", t))(s)
        s = over.get("separate", lambda t: names._SEPARATORS.sub(" ", t))(s)
        return over.get("strip", str.strip)(s)
    return k


MUTATIONS = {
    "no accent fold": dict(fold=lambda t: t or ""),
    "NFD instead of NFKD": dict(fold=lambda t: "".join(c for c in unicodedata.normalize("NFD", t or "")
                                                       if not unicodedata.combining(c))),
    "no case fold": dict(case=lambda t: t),
    "lower instead of casefold": dict(case=str.lower),
    "apostrophe becomes a space": dict(apostrophe=lambda t: re.sub("['’‘ʼ`]", " ", t)),
    "only ASCII apostrophe deleted": dict(apostrophe=lambda t: t.replace("'", "")),
    "punctuation kept": dict(separate=lambda t: re.sub(r"\s+", " ", t)),
    "ASCII-only letters": dict(separate=lambda t: re.sub(r"[^a-z0-9]+", " ", t)),
    "underscore kept": dict(separate=lambda t: re.sub(r"[^\w]+", " ", t)),
    "not trimmed": dict(strip=lambda t: t),
}


@pytest.mark.parametrize("name", sorted(MUTATIONS))
def test_every_step_is_pinned(name):
    """Each mutation of `key` fails at least one whole-value case of the table."""
    broken = _broken(**MUTATIONS[name])
    assert any(broken(t) != w for t, w in TABLE if t is not None), name
    # and the unbroken assembly IS key (so the mutations mutate the real thing)
    assert all(_broken()(t) == w for t, w in TABLE if t is not None)


def test_the_search_key_is_finer():
    """Equal app search keys always give equal name keys: `key` never splits what search joins."""
    cases = json.loads((ROOT / "pipeline/common/search_key_cases.json").read_text())["cases"]
    texts = [t for t, _ in cases] + [t for t, _ in TABLE if t is not None]
    for a in texts:
        assert key(search_key(a)) == key(a), a


# ------------------------------------------------------------------ the callers ask `key`

CALLERS = {
    "pipeline/regs/matching/matcher.py": "from pipeline.common.names import drop_notes, key",
    "pipeline/atlas/registry/build.py": "from pipeline.common.names import key",
    "pipeline/atlas/registry/flowing.py": "from pipeline.common.names import key",
    "pipeline/atlas/reach/steelhead.py": "from pipeline.common.names import key",
    "pipeline/atlas/reach/build.py": "from pipeline.common.names import key as name_key",
    "pipeline/atlas/graph/tributaries.py": "from pipeline.common.names import key as name_key",
    "pipeline/deliver/tiles/names.py": "from pipeline.common.names import key",
    "pipeline/deliver/bundle/spans.py": "from pipeline.common.names import key as name_key",
    "pipeline/deliver/bundle/place_names.py": "from pipeline.common.names import key as name_key",
    "pipeline/regs/dfo_salmon/match.py": "from pipeline.common.names import drop_notes, key",
    "pipeline/regs/dfo_salmon/splitwork.py": "from pipeline.common.names import key as name_key",
    "pipeline/atlas/waters/added_streams/fwa_match.py": "from pipeline.common.names import key",
    "pipeline/atlas/waters/added_streams/build_dataset.py": "from pipeline.common.names import compact_key",
    "pipeline/atlas/waters/added_streams/validate.py": "from pipeline.common.names import compact_key",
    "pipeline/atlas/waters/added_streams/dem.py": "from pipeline.common.names import compact_key",
}


@pytest.mark.parametrize("path", sorted(CALLERS))
def test_the_caller_imports_the_one_key(path):
    assert CALLERS[path] in (ROOT / path).read_text(encoding="utf-8"), path


def test_the_callers_answer_with_key():
    """Mutation: put any old normaliser body back (casefold-only, apostrophe-to-space, lower) and this fails."""
    from pipeline.atlas.reach import steelhead
    from pipeline.atlas.registry import build, flowing
    from pipeline.atlas.waters.added_streams import build_dataset, fwa_match, validate
    from pipeline.deliver.tiles.names import normalise
    from pipeline.regs.dfo_salmon import match as dfo_match, splitwork
    from pipeline.regs.matching import matcher

    for s in ("St. Mary's River", "Pend-d'Oreille River", "Barrière Lake", "WOLF LAKE *"):
        assert (build._norm_name(s) == flowing._norm(s) == steelhead._norm(s) == normalise(s)
                == fwa_match._norm(s) == splitwork._norm(s) == matcher._norm_keep(s) == key(s)), s
        assert build_dataset._canon_name(s) == validate._canon(s) == compact_key(s), s
    # the matcher and the DFO lookup drop the notes first, and agree
    assert matcher._norm("Kwotlenemo (Fountain) Lake") == dfo_match._index_key("Kwotlenemo (Fountain) Lake") \
        == "kwotlenemo lake"


def test_the_dfo_lookup_finds_what_the_index_holds():
    """The bug the unification fixed: the DFO suggestions asked the matcher's name index with another key
    (`locations.normalize`), so 364 registry names were unreachable. Every name keys the index's way now."""
    from pipeline.regs.dfo_salmon import match as dfo_match
    from pipeline.regs.matching import matcher

    for s in ("BLACK'S LAKE", "Brûlé Creek", "B. & F. Creek", "Alexis Lake(formerly)", "Joe's Lake"):
        assert dfo_match._index_key(s) == matcher._norm(s), s
