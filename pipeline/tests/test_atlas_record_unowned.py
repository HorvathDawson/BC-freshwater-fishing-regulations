"""(T1187) The atlas record names a module no distribution owns by its NAME and a portable place, never the absolute path
it was loaded from, and the check finds its file now by name: the same atlas read from another checkout or venv (the
same bytes at another path) is not behind; a changed byte still is. Every test builds its own two install paths."""
from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.atlas import inputs as I
from pipeline.common import stepcache as SC

MOD = "t1187lone"


def _install(site: Path, body: str = "A = 1\n") -> Path:
    site.mkdir(parents=True, exist_ok=True)
    f = site / f"{MOD}.py"
    f.write_text(body)
    importlib.invalidate_caches()
    return f


def _meta(atlas: Path, packages: dict, monkeypatch) -> None:
    """An atlas meta whose code record holds `packages` (beside PyYAML and one config value of a tmp config.yaml)."""
    from pipeline import _trace
    (atlas.parent / "config.yaml").write_text("a: 1\n")
    monkeypatch.setattr(SC, "config_path", lambda: atlas.parent / "config.yaml")
    atlas.mkdir()
    I.write_meta(atlas, {"input_hashes": {"k": "v"}, I.CODE: {
        "modules": {}, "reads": {}, "packages": {"pyyaml": SC.package_versions(["pyyaml"])["pyyaml"], **packages},
        "config": {"a": _trace.config_digest(1)}, **SC.stage_env()}})


def test_an_old_record_s_absolute_path_is_found_by_name_in_another_venv(tmp_path, monkeypatch):
    """The ticket's case: a record holding `module:x=/old/venv/x.py`; the venv is rebuilt elsewhere with the same bytes."""
    old = _install(tmp_path / "old-venv" / "site")
    entry = SC.unowned_entry(MOD, str(old))
    _meta(tmp_path / "atlas", {entry: SC.file_identity(str(old))}, monkeypatch)
    old.unlink()                                                    # the old venv is gone
    _install(tmp_path / "new-venv" / "site")
    monkeypatch.syspath_prepend(str(tmp_path / "new-venv" / "site"))
    assert I.code_behind(tmp_path / "atlas") == []
    _install(tmp_path / "new-venv" / "site", "A = 2\n")             # one byte changed: behind, by name
    assert I.code_behind(tmp_path / "atlas") == [
        f"module {MOD} ({old}) (code: packages): its bytes changed since the atlas was built (no distribution owns it)"]


def test_a_module_found_nowhere_now_is_behind(tmp_path, monkeypatch):
    _meta(tmp_path / "atlas", {SC.unowned_entry(MOD, "<prefix>/site/t1187lone.py"): "sha256:0123456789abcdef"},
          monkeypatch)
    assert I.code_behind(tmp_path / "atlas") == [
        f"module {MOD} (<prefix>/site/t1187lone.py) (code: packages): its bytes changed since the atlas was built "
        "(no distribution owns it)"]
    assert SC.unowned_identity_now(MOD, "") == "missing"


@pytest.fixture
def recorded(tmp_path, monkeypatch):
    """An atlas record written by `record_code` from a trace that imported `t1187lone` from venv A (under sys.prefix)."""
    venv_a = tmp_path.resolve() / "venv-a"
    site_a = venv_a / "lib" / "site"
    mod = _install(site_a)
    monkeypatch.setattr(sys, "prefix", str(venv_a))
    monkeypatch.syspath_prepend(str(site_a))
    from importlib import metadata
    monkeypatch.setattr(metadata, "packages_distributions", lambda: {})      # owned by nothing, as in a real venv
    root = tmp_path.resolve() / "repo"
    (root / "pipeline").mkdir(parents=True)
    helper = root / "pipeline" / "helper.py"
    helper.write_text("X = 1\n")
    (root / "config.yaml").write_text("a: 1\n")
    for m in (I, SC):
        monkeypatch.setattr(m, "REPO_ROOT", root)
    monkeypatch.setattr(SC, "config_path", lambda: root / "config.yaml")
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    from pipeline import _trace
    tr = tmp_path / "trace"
    tr.mkdir()
    (tr / "1.trace").write_text(f"M\t{helper}\t{SC.sha_file(helper)}\nP\t{MOD}\t{mod}\nC\ta\t{_trace.config_digest(1)}\n")
    atlas = tmp_path / "atlas"
    atlas.mkdir()
    I.write_meta(atlas, {"input_hashes": {"k": "v"}})
    code = I.record_code(atlas, tr)
    return tmp_path, atlas, code


def test_the_record_holds_no_absolute_path(recorded):
    tmp, atlas, code = recorded
    assert SC.unowned_entry(MOD, f"<prefix>/lib/site/{MOD}.py") in code["packages"]
    text = (atlas / I.META).read_text()
    assert str(tmp) not in text and str(tmp.resolve()) not in text
    assert not any(json.dumps(k).startswith('"/') for k in code["packages"])


def test_the_same_bytes_in_another_install_path_are_current(recorded, monkeypatch):
    tmp, atlas, _code = recorded
    assert I.behind(atlas, {"k": "v"}) == []
    venv_b = tmp.resolve() / "elsewhere" / "venv-b"
    _install(venv_b / "lib" / "site")
    (tmp / "venv-a" / "lib" / "site" / f"{MOD}.py").unlink()        # venv A is gone; B has the same bytes
    monkeypatch.setattr(sys, "prefix", str(venv_b))
    monkeypatch.syspath_prepend(str(venv_b / "lib" / "site"))
    importlib.invalidate_caches()
    assert I.behind(atlas, {"k": "v"}) == []
    _install(venv_b / "lib" / "site", "A = 2\n")
    assert I.behind(atlas, {"k": "v"}) == [
        f"module {MOD} (<prefix>/lib/site/{MOD}.py) (code: packages): its bytes changed since the atlas was built "
        "(no distribution owns it)"]


def test_an_entry_script_is_found_by_its_recorded_place(tmp_path, monkeypatch):
    """`__main__` names no file by name (every process has its own): its portable place finds it in the other checkout."""
    root = tmp_path.resolve() / "checkout"
    (root / "tools").mkdir(parents=True)
    (root / "tools" / "entry.py").write_text("print(1)\n")
    monkeypatch.setattr(SC, "REPO_ROOT", root)
    where = SC.portable_where(str(root / "tools" / "entry.py"))
    assert where == "<repo>/tools/entry.py"
    was = SC.file_identity(str(root / "tools" / "entry.py"))
    assert SC.unowned_identity_now("__main__", where) == was
    other = tmp_path.resolve() / "other"
    subprocess.run(["cp", "-R", str(root), str(other)], check=True)
    monkeypatch.setattr(SC, "REPO_ROOT", other)
    assert SC.unowned_identity_now("__main__", where) == was
    (other / "tools" / "entry.py").write_text("print(2)\n")
    assert SC.unowned_identity_now("__main__", where) != was
    assert SC.portable_where(str(tmp_path / "outside.py")) == "<elsewhere>/outside.py"
