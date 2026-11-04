"""Owner 2026-11-04: one glossary term per provincial Conservation Surcharge Stamp (`glossary.t_surcharge_stamps`), so
each stamp's ⓘ on the Licence tab explains that stamp alone; the steelhead stamp keeps `steelhead_stamp` (`t_stamp`)
and `conservation_surcharge` stays the general term for running text.

Pinned against the live export: every stamp in its licences has a term with its condition (its own requirement
record's `parts.doing`, the record the Licence tab's "Only if you keep …" line is made from) and its fee; and the
mutations — a stamp without a fee record, or without its requirement, stops the build; a new stamp gets a term by
itself."""
from __future__ import annotations

import copy
from functools import lru_cache

import pytest

from pipeline.deliver.answers import glossary as G
from pipeline.deliver.answers.common import AnswersError, load_export
from pipeline.deliver.bundle.glossary_source import rows_from_book
from pipeline.deliver.export.codec import expand
from pipeline.tests.conftest import EXPORT_HINT, need
from pipeline.tests.test_answers_v22 import EXPORT_DIR

pytestmark = pytest.mark.needs_bundle

#: the four stamps besides steelhead, and the condition each is needed for (the requirement records' own words)
STAMPS = {"kootenay_rainbow_stamp": "to keep rainbow trout over 50 cm",
          "salmon_stamp": "to keep salmon (not kokanee)",
          "shuswap_char_stamp": "to keep lake trout or Dolly Varden/bull trout over 60 cm",
          "shuswap_rainbow_stamp": "to keep rainbow trout over 50 cm"}


@lru_cache(maxsize=1)
def _doc() -> dict:
    need(None, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    return expand(*load_export(EXPORT_DIR))


def _terms(doc: dict) -> dict:
    return {t["id"]: t for t in G.build(doc, G.Book(rows_from_book()))["terms"]}


def test_every_stamp_has_its_own_term():
    doc, t = _doc(), _terms(_doc())
    stamps = {k for k, v in doc["licences"].items() if "conservation surcharge" in v["name"].lower() and v.get("provincial")}
    assert stamps == set(STAMPS) | {"steelhead_stamp"}            # the five real stamps
    assert "stamp_steelhead_stamp" not in t and "steelhead_stamp" in t
    for key, doing in STAMPS.items():
        term = t[G.stamp_term_id(key)]
        f = doc["licensing"][f"zp:licence_fees#{key}"]["fields"]["fees_cad"]
        assert term["term"] == doc["licences"][key]["name"]
        assert f"You need it only {doing}." in term["says"]
        assert f"${f['resident']:,.2f} a year for B.C. residents, ${f['non_resident']:,.2f} for everyone else." in term["says"]
        assert "record" in term["says"] and 5 in term["pages"] and 7 in term["pages"]
        assert term["quote"].startswith("Your basic licence must be validated with a Conservation Surcharge Stamp")
    assert t["stamp_salmon_stamp"]["says"] == (
        "An add-on to your basic licence that pays for salmon conservation. You need it only to keep salmon (not "
        "kokanee). You must record every adult chinook you keep on your licence right away. $17.14 a year for B.C. "
        "residents, $34.29 for everyone else.")
    assert "up to five" in t["conservation_surcharge"]["says"]
    assert "salmon" not in t["conservation_surcharge"]["says"]     # the general term no longer lists them


def test_a_stamp_without_a_fee_stops_the_build():
    doc = copy.deepcopy(_doc())
    del doc["licensing"]["zp:licence_fees#salmon_stamp"]
    with pytest.raises(AnswersError, match="salmon_stamp has 0 fee records"):
        _terms(doc)


def test_a_stamp_without_its_condition_stops_the_build():
    doc = copy.deepcopy(_doc())
    del doc["licensing"]["zp:shuswap_char_stamp#shuswap_char_stamp"]
    with pytest.raises(AnswersError, match="shuswap_char_stamp has 0 province-wide conditions"):
        _terms(doc)


def test_a_new_stamp_gets_its_own_term():
    doc = copy.deepcopy(_doc())
    doc["licences"]["sturgeon_stamp"] = {"name": "Conservation Surcharge Stamp for white sturgeon", "provincial": True}
    req = copy.deepcopy(doc["licensing"]["zp:salmon_stamp#salmon_stamp"])
    req["fields"]["satisfied_by"] = [{"hold": ["sturgeon_stamp"]}]
    req["parts"]["doing"] = "to keep white sturgeon"
    doc["licensing"]["zp:salmon_stamp#sturgeon_stamp"] = req
    fee = copy.deepcopy(doc["licensing"]["zp:licence_fees#salmon_stamp"])
    fee["fields"]["document"] = "sturgeon_stamp"
    doc["licensing"]["zp:licence_fees#sturgeon_stamp"] = fee
    t = _terms(doc)["stamp_sturgeon_stamp"]
    assert t["term"] == "Conservation Surcharge Stamp for white sturgeon"
    assert "pays for white sturgeon conservation. You need it only to keep white sturgeon." in t["says"]
