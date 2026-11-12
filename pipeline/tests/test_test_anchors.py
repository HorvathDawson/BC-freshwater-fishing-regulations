"""T667 work item 4: the test-anchor scanner (`pipeline/tests/anchors.py`, the rule is its header) finds one planted
offender of each kind, leaves a test on its own data alone, and FAILS on every offender (owner D4: no baseline, no list
of excused files; switched on 2026-11-11 when the offender list reached zero). `--report` only counts."""
from __future__ import annotations

import ast

from pipeline.tests import anchors as A

LIVE = "import pytest\npytestmark = pytest.mark.needs_bundle\n"


def kinds(source: str) -> list[str]:
    return [k for _, k, _ in A.scan_source(source)]


def test_one_planted_offender_of_each_kind_is_found():
    planted = {
        "generated": "from pipeline.common.curated import GENERATED\nB = GENERATED.bundle / 'bundle.sqlite'\n",
        "curated": "from pipeline.common.curated import CURATED\ncuts = CURATED.waters.cuts\n",
        "env": "import os\nB = os.environ.get('UI_EXPORT_BUNDLE')\n",
        "live-const": "from pipeline.deliver.bundle import read as R\nB = R.BUNDLE\n",
        "path": "from pathlib import Path\nREPO = Path('.')\np = REPO / 'data/curated/waters/cuts.json'\n",
        "pointer": "from pipeline.tests.conftest import release_root\nr = release_root()\n",
        "release-id": LIVE + "RID = 'r-303b4007a70a'\n",
        "digest": LIVE + "HANDLES = 'd5242c0c9f1e77aa'\n",
        "raw-section": LIVE + "def test_a(row):\n    assert row['sid'] == 316756\n",
    }
    assert set(planted) == set(A.KINDS)
    for kind, src in planted.items():
        assert kind in kinds(src), kind
    # each read is found by its own rule, not as a side effect of another
    assert kinds(planted["env"]) == ["env"] and kinds(planted["pointer"]) == ["pointer"]
    assert kinds(planted["path"]) == ["path"] and kinds(planted["live-const"]) == ["live-const"]


def test_the_other_spellings_of_each_read_are_found():
    assert kinds("import os\nb = os.environ['ATLAS_BUILD']\n") == ["env"]
    assert kinds("import os\nb = os.getenv('RELEASE_DIR')\n") == ["env"]
    assert kinds("from pipeline.deliver.export.contract import BUNDLE\n") == ["live-const"]
    assert kinds("from pathlib import Path\np = Path(__file__).resolve().parents[2] / 'data/release'\n") == ["path"]
    assert kinds("import glob\nfs = glob.glob('data/curated/regulations/synopsis/region-*.json')\n") == ["path"]
    assert kinds(LIVE + "Q = 'SELECT 1 FROM section WHERE sid IN (1235781, 1235782)'\n") == ["raw-section"]
    assert kinds(LIVE + "def test_a(f):\n    f(sid=1235781)\n    sids = [1235781, 1235782]\n") == ["raw-section"] * 2
    assert kinds(LIVE + "TILES = 'd768d6e135e62b99.c9f9604f12bf'\n") == ["digest"]


def test_a_test_on_its_own_data_is_no_offender():
    own = (
        '"""Reads GENERATED.bundle and data/release/LATEST — in a docstring, which is not code."""\n'
        "# REPO / 'data/curated/x.json' and r-303b4007a70a in a comment\n"
        "def test_a(tmp_path, monkeypatch):\n"
        "    (tmp_path / 'data/release').mkdir(parents=True)\n"            # its own tree
        "    monkeypatch.setenv('UI_EXPORT_BUNDLE', str(tmp_path))\n"        # setting is not reading
        "    row = {'sid': 99_999, 'release': 'r-303b4007a70a', 'handles': 'd5242c0c9f1e77aa'}\n"   # own data
        "    assert row['sid'] == 99_999\n"
        "    doc = {'before': 'data/generated/release/baseline/x'}\n")      # a path as data, in a dict
    assert A.scan_source(own) == []


def test_a_pin_counts_only_in_a_file_that_follows_live_data():
    pins = "RID = 'r-303b4007a70a'\ndef test_a(row):\n    assert row['sid'] == 316756\n"
    assert kinds(pins) == []
    assert kinds(LIVE + pins) == ["release-id", "raw-section"]
    assert kinds("from pipeline.tests import under_test\n" + pins) == ["release-id", "raw-section"]
    # invented ids and digests are never pins; a section named by its id string is a name
    assert kinds(LIVE + "A = 'r-000000000001'\nB = 'r-aaaaaaaaaaaa'\nC = '0123456789abcdef'\nD = '356075101:0'\n") == []


def test_a_sandboxed_curated_tree_is_not_the_live_one():
    src = "from pipeline.common.curated import CURATED\ndef test_a():\n    return CURATED.waters.cuts\n"
    assert kinds(src) == ["curated"]
    assert kinds("from pipeline.tests.conftest import curated_root\n" + src) == []


def test_the_helper_is_the_allowed_way():
    ok = (LIVE + "from pipeline.tests import under_test\n"
          "def test_a(request):\n"
          "    b = under_test.bundle(request)\n"
          "    h = under_test.sections(request).stream(356075101, 0)\n"
          "    cuts = under_test.curated().waters.cuts\n")
    assert A.scan_source(ok) == []


def test_an_emitters_committed_contract_file_is_no_live_constant():
    """`E.OUT` of a `pipeline.tools.emit` module is the committed generated file under app/ (anchors header)."""
    ok = ("from pipeline.tools.emit import gauge_policy as E\n"
          "def test_a():\n"
          "    from pipeline.tools.emit import pack as emit_pack\n"
          "    assert E.OUT.read_text() == E.render() and emit_pack.OUT.exists()\n")
    assert A.scan_source(ok) == []
    assert A.scan_source("import pipeline.tools.emit.pack as P\nfrom pipeline.tools.emit.pack import OUT\nx = P.OUT\n") == []
    # NEAR MISSES, each still a live-const: another module's OUT (the review file dfo-suggestions.json, the export),
    # an emitter's name also bound to something else, BUNDLE on an emitter, a look-alike package, the dotted spelling
    for src in (
        "from pipeline.regs.dfo_salmon import edge_gaps as E\nx = E.OUT\n",
        "from pipeline.deliver.export import contract as E\nx = E.OUT\n",
        "from pipeline.tools.emit import pack as E\nfrom pipeline.deliver.export import contract as E\nx = E.OUT\n",
        "from pipeline.tools.emit import pack as E\nE = object()\nx = E.OUT\n",
        "from pipeline.tools.emit import pack as E\ndef test_a(E):\n    return E.OUT\n",
        "from pipeline.tools.emit import pack as E\nx = E.BUNDLE\n",
        "from pipeline.tools.emit.pack import BUNDLE\n",
        "from pipeline.tools.emitters import pack as E\nx = E.OUT\n",
        "from pipeline.tools.audit import tidal_triage as E\nx = E.OUT\n",
        "import pipeline.deliver.export.contract\nx = pipeline.deliver.export.contract.OUT\n",
        "from pipeline.tools import emit as E\nx = E.OUT\n",
    ):
        assert kinds(src) == ["live-const"], src


def test_every_emitters_out_is_a_committed_file_under_app():
    """THE PREMISE of the exemption above, held on the real package, read as the scanner reads (the source, not the
    running module): every emitter's `OUT = <repo root> / "app" / … / "<name>.generated.<ext>"`, a file that exists. An
    emitter whose OUT pointed into data/ would make `E.OUT` a live-data read the scanner lets through: it fails here."""
    outs = {}
    for f in sorted((A.ROOT / A.EMITTERS.replace(".", "/")).glob("*.py")):
        for n in ast.parse(f.read_text(encoding="utf-8")).body:
            if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "OUT" for t in n.targets):
                parts, e = [], n.value
                while isinstance(e, ast.BinOp) and isinstance(e.op, ast.Div):
                    assert isinstance(e.right, ast.Constant) and isinstance(e.right.value, str), (f.name, ast.unparse(n))
                    parts.insert(0, e.right.value)
                    e = e.left
                assert isinstance(e, ast.Name) and e.id in A.REPO_NAMES, (f.name, ast.unparse(n))
                outs[f.stem] = "/".join(parts)
    assert {"gauge_policy", "pack"} <= set(outs)
    for name, out in outs.items():
        assert out.startswith("app/") and ".generated." in out and (A.ROOT / out).is_file(), (name, out)


def test_the_books_own_sha_is_no_build_digest():
    sha = "08f58537261ffe51c513193013ba165754285719dec8d0332beaef1cbab8ecfa"
    assert kinds(LIVE + f"def test_a(m):\n    assert m['pdf_sha256'] == '{sha}'\n") == []
    assert kinds(LIVE + f"def test_a(m):\n    assert '{sha}' == m['pdf_sha256']\n") == []
    # NEAR MISSES, each still a digest: another key, a build's 16 digits under the book's key, the sha assigned,
    # passed, or tested with anything but ==
    for src in (
        f"def test_a(m):\n    assert m['sha256'] == '{sha}'\n",
        f"def test_a(m):\n    assert m['raw_data_sha256'] == '{sha}'\n",
        "def test_a(m):\n    assert m['pdf_sha256'] == 'd5242c0c9f1e77aa'\n",
        f"PDF_SHA256 = '{sha}'\n",
        f"def test_a(f):\n    f(pdf_sha256='{sha}')\n",
        f"def test_a(m):\n    assert m['pdf_sha256'] != '{sha}'\n",
        f"def test_a(m):\n    assert m['handles'] == m['pdf_sha256'] == '{sha}'\n",
    ):
        assert kinds(LIVE + src) == ["digest"], src


def test_an_invented_tiles_id_is_two_counting_runs():
    assert not A.is_digest("0123456789abcdef.0123456789ab") and not A.is_digest("fedcba9876543210.0123456789ab")
    assert kinds(LIVE + "T = '0123456789abcdef.0123456789ab'\n") == []
    # one real half is a real id; so is any other tiles id
    assert A.is_digest("d768d6e135e62b99.0123456789ab") and A.is_digest("0123456789abcdef.c9f9604f12bf")
    assert A.is_digest("d768d6e135e62b99.c9f9604f12bf")


def test_the_guard_fails_on_an_offender_and_says_what_to_write_instead(tmp_path, capsys):
    (tmp_path / "pytest.ini").write_text("[pytest]\ntestpaths = t\n")
    (tmp_path / "t").mkdir()
    (tmp_path / "t" / "test_bad.py").write_text("from pipeline.common.curated import GENERATED\nB = GENERATED.bundle\n")
    (tmp_path / "t" / "test_ok.py").write_text("def test_a():\n    assert 1\n")
    found = A.scan(tmp_path)
    assert list(found) == ["t/test_bad.py"] and A.totals(found)["generated"] == (1, 1)
    assert "1 of 2 test files offend" in A.report(found, 2)
    assert A.strict([]) and A.strict(["--strict"]) and A.strict(["--json"]) and not A.strict(["--report"])
    assert A.main(["--report"], tmp_path) == 0                       # reading only
    capsys.readouterr()
    assert A.main([], tmp_path) == 1 and A.main(["--strict"], tmp_path) == 1 and A.main(["--json"], tmp_path) == 1
    err = capsys.readouterr().err
    assert "t/test_bad.py:2  generated: GENERATED" in err
    assert f"  generated: {A.INSTEAD['generated']}" in err and "under_test.bundle(request)" in err
    assert "no list of excused files" in err and "  curated:" not in err      # advice for the kinds hit, only
    assert set(A.INSTEAD) == set(A.KINDS)
    (tmp_path / "t" / "test_bad.py").write_text("def test_b():\n    assert 1\n")
    assert A.main([], tmp_path) == 0


def test_the_live_tree_has_no_offender():
    """THE GUARD, in the suite: one test file of this repo off its anchor fails here, with every hit and what to write
    instead. No baseline and no list of excused files (T667 D4)."""
    found = A.scan()
    assert not found, "\n" + A.failure(found)
