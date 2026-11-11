"""T667 work item 4: the test-anchor scanner (`pipeline/tests/anchors.py`, the rule is its header) finds one planted
offender of each kind, leaves a test on its own data alone, and has two modes: the report (exit 0, now) and the failing
one (`--strict` / TEST_ANCHORS_STRICT=1, switched on when the offender list is empty — owner D4: no baseline)."""
from __future__ import annotations

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


def test_report_mode_passes_and_strict_mode_fails_on_the_same_tree(tmp_path, capsys, monkeypatch):
    (tmp_path / "pytest.ini").write_text("[pytest]\ntestpaths = t\n")
    (tmp_path / "t").mkdir()
    (tmp_path / "t" / "test_bad.py").write_text("from pipeline.common.curated import GENERATED\nB = GENERATED.bundle\n")
    (tmp_path / "t" / "test_ok.py").write_text("def test_a():\n    assert 1\n")
    found = A.scan(tmp_path)
    assert list(found) == ["t/test_bad.py"] and A.totals(found)["generated"] == (1, 1)
    assert "1 of 2 test files offend" in A.report(found, 2)
    assert not A.strict({}, []) and not A.strict({A.STRICT_ENV: "0"}, [])
    assert A.strict({A.STRICT_ENV: "1"}, []) and A.strict({}, ["--strict"])
    monkeypatch.delenv(A.STRICT_ENV, raising=False)
    assert A.main([], tmp_path) == 0
    assert A.main(["--strict"], tmp_path) == 1
    assert "t/test_bad.py:2  generated: GENERATED" in capsys.readouterr().out
    (tmp_path / "t" / "test_bad.py").write_text("def test_b():\n    assert 1\n")
    assert A.main(["--strict"], tmp_path) == 0


def test_the_live_tree_report(capsys):
    """REPORT MODE on this repo: prints the counts and passes. With TEST_ANCHORS_STRICT=1 (flipped on when the list is
    empty, T667 D4) it fails naming every offending file."""
    files = A.test_files()
    found = A.scan(files=files)
    print(A.report(found, len(files)))
    if A.strict():
        assert not found, "test files off their anchor (pipeline/tests/anchors.py header): " + ", ".join(sorted(found))
