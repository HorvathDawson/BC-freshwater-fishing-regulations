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
