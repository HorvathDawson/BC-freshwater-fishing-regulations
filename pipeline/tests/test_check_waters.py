"""T324: `check_curated --waters` — the fast check of the hand-edited waters files.

The real files pass; each MUTATION below (one bad hand edit) must fail with an error naming the file, so the check
cannot pass by reading nothing (memory `checks-seeded-from-input-launder`)."""
from __future__ import annotations

import json
import shutil
from types import SimpleNamespace

import pytest

from pipeline.tests import under_test
from pipeline.tools.audit import check_curated as C

FILES = ("name_variants", "areas", "added_areas", "added_lakes", "added_streams", "continuations", "unmapped_waters")


@pytest.fixture
def waters(tmp_path):
    """A stand-in `CURATED.waters`: copies of the real files the test may break, plus the other declared keys."""
    ns = {}
    for name in FILES:
        src = getattr(under_test.curated().waters, name)
        dst = tmp_path / src.name
        shutil.copyfile(src, dst)
        ns[name] = dst
    return SimpleNamespace(**ns)


def _edit(path, fn):
    doc = json.loads(path.read_text(encoding="utf-8"))
    fn(doc)
    path.write_text(json.dumps(doc), encoding="utf-8")


def test_the_real_files_pass_without_a_registry(waters, tmp_path):
    err, warn, notes = C.check_waters(waters, registry=tmp_path / "no-registry.json")
    assert err == [], err
    assert any("registry checks skipped" in n for n in notes)       # said, never silent


@pytest.mark.parametrize("name, mutate, says", [
    ("name_variants", lambda d: d[0].update(target={}), "names no water"),
    ("name_variants", lambda d: d[1].update(names=[]), "non-empty list"),
    ("name_variants", lambda d: d[2]["names"][0].update(name=" "), "no `name`"),
    ("areas", lambda d: d["areas"].append(dict(d["areas"][0])), "duplicate id"),
    ("areas", lambda d: d["areas"][-1].update(file="no_such_key_or_path"), "no declared curated file"),
    ("areas", lambda d: d["areas"][0].update(file="added_areas"), "exactly one of `layer`"),
    ("added_areas", lambda d: d["features"][0]["geometry"].update(
        coordinates=[[[p[1], p[0]] for p in d["features"][0]["geometry"]["coordinates"][0]]]), "outside BC"),
    ("added_lakes", lambda d: d["features"][1]["properties"].update(id=d["features"][0]["properties"]["id"]),
     "duplicate id"),
    ("added_streams", lambda d: d["name_variants"][0].update(target_blk="-1"), "is no added stream"),
    ("continuations", lambda d: d["refused"].append(dict(d["reviewed"][0])), "already in `reviewed`"),
    ("continuations", lambda d: d["reviewed"][0].update(to=d["reviewed"][0]["from"]), "continues as itself"),
    ("continuations", lambda d: d["reviewed"][0].update(to="Bella Coola River"), "is no item id"),
    ("unmapped_waters", lambda d: d["ungazetted"][0].update(location=[49.0, -122.4]), "inside BC"),
    ("unmapped_waters", lambda d: d["ungazetted"][0].update(id="marsh_pond"), "must start with 'ungaz:'"),
])
def test_one_bad_edit_fails(waters, tmp_path, name, mutate, says):
    _edit(getattr(waters, name), mutate)
    err, _, _ = C.check_waters(waters, registry=tmp_path / "no-registry.json")
    assert any(says in e for e in err), err


def test_invalid_json_is_an_error_not_a_crash(waters, tmp_path):
    waters.continuations.write_text("{ not json", encoding="utf-8")
    err, _, _ = C.check_waters(waters, registry=tmp_path / "no-registry.json")
    assert any("not valid JSON" in e for e in err)


def test_a_name_variant_the_registry_lacks_is_a_warning(waters, tmp_path):
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"items": [{"id": "wbk:1", "ref_ids": ["wbk:1"]}]}), encoding="utf-8")
    err, warn, _ = C.check_waters(waters, registry=reg)
    assert any("not in the current atlas registry" in w for w in warn)
    assert not any("name_variants" in e for e in err)
