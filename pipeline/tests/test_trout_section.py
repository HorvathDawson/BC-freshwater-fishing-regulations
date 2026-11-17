"""THE TROUT SECTION IN THE RELEASE (trout table plan, T810 ticket I; contract 2.1-2.3, 2.7): `Section("trout", 1)`
behind `--trout-table` (default OFF), its codec (`encode.TroutCodec`), the store's `cell.trout` and four interned tables,
the regs shards that carry them, and the zdicts on `water-shared`.

Anchors, no live data:
  (a) THE REGISTRY: every hand list of region table names (the shards', the store's families and blob columns, the
      model's optional frames, the answers' optional sections) is held to the release contract
      (`contract._REGION_TABLES`, `contract.OPTIONAL_SECTION_TABLES`); a table added to one list only fails.
  (b) FLAG OFF: the committed mini release (built without the flag) holds no trout version, column, table or zdict; its
      answers file decodes and re-encodes to its own bytes with the trout code present (the shipped `spec` and
      `schemas` hold no trout text unless the section is there) — so a release without the flag is byte for byte the
      release before the section existed (`mini_release --check`: only the card gate's code stamp moves with the code).
  (c) FLAG ON: a side mini release built with `--trout-table` (this module builds one): all or nothing (the manifest
      and the layout name trout 1, every regs file carries the column and the four tables, `water-shared` their zdicts);
      the store put back together from it proves its answers bytes; every non-NULL cell decodes to F's `TroutFrame`, is
      exactly what the builder makes of the decoded rows / display / licence frames, is NULL exactly per Q-D3, says
      every trout and char fish exactly once, and each region file alone reaches every trout row its cells name.
  (d) the codec round trip and the whole-key compose's no-day-cut check, with its mutation.
"""
from __future__ import annotations

import copy
import gzip
import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.model import FRAMES, OPTIONAL_FRAMES, json_schemas, validate_section
from pipeline.deliver.answers.trout import table as T
from pipeline.deliver.release import contract as C
from pipeline.deliver.release import shards as SH
from pipeline.deliver.store import build as SB
from pipeline.deliver.store.common import BLOB_COLUMNS, unpack
from pipeline.tests import under_test
from pipeline.tests.test_trout_table import check_key, mini  # noqa: F401  (module fixture)

TC_TABLES = ("trout_frame", "trout_table", "trout_dates", "trout_sheet")


# ------------------------------------------------------------------------------------------------------ (a) the registry

def registry_problems(contract=None, shards=None, store=None, model=None, sections=None) -> list:
    """Every hand list of region table names that differs from the release contract's (each argument replaces one side,
    for the mutations)."""
    req = list(C._REGION_TABLES) if contract is None else contract[0]
    opt = C.OPTIONAL_SECTION_TABLES if contract is None else contract[1]
    top, shard_tables, otop, oshard = (SH.TOP, SH.SHARD_TABLES, SH.OPTIONAL_TOP, SH.OPTIONAL_SHARD_TABLES) \
        if shards is None else shards
    secs, osecs, fams = (SB.SECTIONS, SB.OPTIONAL_SECTIONS, SB.FAMILIES) if store is None else store
    oframes = OPTIONAL_FRAMES if model is None else model
    oanswers = {s.name for s in A.SECTIONS if s.optional} if sections is None else sections
    cell = C._REGION_TABLES["cell"] if contract is None else contract[2]
    out = []
    if list(top) != cell[2:] or tuple(secs) != tuple(cell[2:]):
        out.append(f"the required sections: cell {cell[2:]}, shards {list(top)}, store {list(secs)}")
    if set(shard_tables) != set(req) - {"meta"}:
        out.append(f"shards.SHARD_TABLES {sorted(set(shard_tables) ^ (set(req) - {'meta'}))}")
    if set(otop) != set(opt) or set(osecs) != set(opt) or set(oframes) != set(opt) or set(oanswers) != set(opt):
        out.append(f"the optional sections: contract {sorted(opt)}, shards {sorted(otop)}, store {sorted(osecs)}, "
                   f"model {sorted(oframes)}, answers {sorted(oanswers)}")
    for n, spec in opt.items():
        tables = set(spec["tables"])
        if [n] != spec["cell"] or otop.get(n) not in tables:
            out.append(f"{n}: its cell column / top frame")
        if set(oshard.get(n, ())) != tables:
            out.append(f"{n}: shards hold {sorted(oshard.get(n, ()))}, the contract {sorted(tables)}")
        if {t for (s, _k), t in fams.items() if s == n} != tables:
            out.append(f"{n}: the store's families {sorted(t for (s, _k), t in fams.items() if s == n)}")
        if not tables <= set(BLOB_COLUMNS):
            out.append(f"{n}: blob columns lack {sorted(tables - set(BLOB_COLUMNS))}")
    return out


def test_every_hand_list_of_region_tables_is_the_contracts():
    assert registry_problems() == []
    assert C.region_tables(["trout"])["cell"][-1] == "trout"
    assert set(C.region_tables(["trout"])) == set(C._REGION_TABLES) | set(TC_TABLES)
    assert C.region_tables() == C._REGION_TABLES


@pytest.mark.parametrize("where", ["contract", "shards", "store", "model", "answers"])
def test_mutation_a_table_or_section_added_to_one_list_only_fails(where):
    opt = copy.deepcopy(C.OPTIONAL_SECTION_TABLES)
    if where == "contract":
        opt["trout"]["tables"]["trout_extra"] = ["id", "j"]
        got = registry_problems(contract=(list(C._REGION_TABLES), opt, C._REGION_TABLES["cell"]))
    elif where == "shards":
        got = registry_problems(shards=(SH.TOP, SH.SHARD_TABLES + ("bait_frame",), SH.OPTIONAL_TOP,
                                        SH.OPTIONAL_SHARD_TABLES))
    elif where == "store":
        got = registry_problems(store=(SB.SECTIONS, SB.OPTIONAL_SECTIONS, {**SB.FAMILIES, ("trout", "x"): "trout_x"}))
    elif where == "model":
        got = registry_problems(model=OPTIONAL_FRAMES | {"bait"})
    else:
        got = registry_problems(sections={"trout", "bait"})
    assert got


def test_the_store_refuses_an_unknown_section():
    con = sqlite3.connect(":memory:")
    con.executescript(SB.SCHEMA.read_text())
    with pytest.raises(SB.StoreError, match="not optional sections"):
        SB.optional_schema(con, ["bait"])


# --------------------------------------------------------------------------------------------- (b) flag off: unchanged

def _release(d: Path):
    from pipeline.deliver.release.manifest import latest
    return latest(d) if not (d / "manifest.json").exists() else d


def _files(rel: Path, tmp: Path):
    m = json.loads((rel / "manifest.json").read_text())

    def open_(name):
        p = tmp / f"{rel.name}-{name}.sqlite"
        if not p.exists():
            p.write_bytes(gzip.decompress((rel.parent / m["files"][name]["url"]).read_bytes()))
        return sqlite3.connect(p)
    return m, open_


def test_the_committed_mini_release_has_no_trout_section(tmp_path):
    rel = _release(under_test.mini())
    m, open_ = _files(rel, tmp_path)
    assert "trout" not in (m["regulations"]["versions"] or {})
    for r in SH.REGIONS:
        con = open_(f"regs-{r}")
        assert SH.cell_sections(con) == list(SH.TOP)
        assert not {t for (t,) in con.execute("SELECT name FROM sqlite_master")} & set(TC_TABLES)
    zd = {t for (t,) in open_("water-shared").execute("SELECT tbl FROM zdict")}
    assert not zd & set(TC_TABLES)


def test_flag_off_the_answers_are_the_release_before_the_section(tmp_path):
    """The committed mini release was cut without the flag: its answers file (put back from the store, proved against
    its answers_sha256) decodes and re-encodes — with the trout section, codec, spec text and schemas all in the code —
    to the SAME value (the encoder's static-table key order is the codec's, so the comparison is by value; the release's
    bytes are `mini_release --check`'s), and its shipped `spec` and `schemas` name no trout. The flag off adds nothing."""
    from pipeline.deliver.store.release import province_store
    st = province_store(_release(under_test.mini()))
    raw = st.answers_bytes()                              # proved against the store's answers_sha256
    data = {"rule_ids": st.export_subset()["rule_ids"]}
    wire = json.loads(raw)
    assert "trout" not in wire["sections"] and "trout" not in wire["spec"]["sections"]
    assert not any(k.startswith("trout") for k in wire["schemas"])
    model = E.decode(wire, data)
    assert "trout" not in model.versions
    assert E.encode(model, data) == wire


def test_an_optional_section_is_built_only_when_named():
    assert [s.name for s in A.SECTIONS if s.optional] == ["trout"]
    with pytest.raises(A.AnswersError, match="not optional sections"):
        A.build("x", Path("."), optional=("rows",))


def test_the_spec_and_schemas_ship_trout_only_with_the_section():
    assert "trout" not in E.spec_for(["rows"])["sections"] and E.spec_for(["rows", "trout"]) is E.SPEC
    assert set(json_schemas(["rows"])) == set(json_schemas()) - {"trout", "trout.wire"}
    assert {"trout", "trout.wire"} <= set(json_schemas(["trout"]))


def test_a_null_cell_is_valid_only_for_the_trout_section():
    assert validate_section("trout", [None, None]) == 0
    with pytest.raises(Exception):
        validate_section("gear", [None])


# --------------------------------------------------------------------------------------------- (c) flag on: a side release

@pytest.fixture(scope="module")
def side(tmp_path_factory):
    """The mini chain built WITH `--trout-table` (a side release; never the committed copy)."""
    from pipeline.tools import mini_release as M
    root = tmp_path_factory.mktemp("trout-side")
    M.build(root / "b", trout_table=True)
    from pipeline.deliver.release.manifest import latest
    return latest(root / "b" / "release")


def test_the_side_release_carries_the_section_all_or_nothing(side, tmp_path):
    from pipeline.tests.test_release import contract_problems
    assert contract_problems(side, tmp_path, every_file=False) == []
    m, open_ = _files(side, tmp_path)
    assert m["regulations"]["versions"]["trout"] == 1
    layout = json.loads(dict(open_("water").execute("SELECT k, v FROM meta"))["layout"])
    assert layout["versions"]["trout"] == 1
    for r in SH.REGIONS:
        con = open_(f"regs-{r}")
        assert SH.cell_sections(con) == [*SH.TOP, "trout"]
    zd = {t for (t,) in open_("water-shared").execute("SELECT tbl FROM zdict")}
    assert set(TC_TABLES) <= zd


def test_mutation_a_release_missing_one_trout_table_fails_the_contract(side, tmp_path):
    from pipeline.tests.test_release import contract_problems
    import hashlib
    import shutil
    bad = tmp_path / "bad" / side.name
    shutil.copytree(side, bad)
    m = json.loads((bad / "manifest.json").read_text())
    f = m["files"]["regs-2"]
    con = sqlite3.connect(":memory:")
    con.deserialize(gzip.decompress((side.parent / f["url"]).read_bytes()))
    con.execute("DROP TABLE trout_dates")
    raw = con.serialize()
    (bad.parent / f["url"]).parent.mkdir(parents=True, exist_ok=True)
    (bad.parent / f["url"]).write_bytes(gzip.compress(raw))
    f["sha256"] = hashlib.sha256(raw).hexdigest()
    (bad / "manifest.json").write_text(json.dumps(m))
    (tmp_path / "t").mkdir()
    assert any(p.startswith("regs-2") for p in contract_problems(bad, tmp_path / "t", every_file=False))


@pytest.fixture(scope="module")
def decoded(side):
    """The side release put back together: its answers model (decoded), the rule table, and the decoded frames."""
    from pipeline.deliver.store.release import province_store
    st = province_store(side)
    raw = st.answers_bytes()
    sub = st.export_subset()
    data = {"rule_ids": sub["rule_ids"]}
    model = E.decode(json.loads(raw), data)
    rules = T.Rules({i: T.rule_of(sub["rules"][rid]) for i, rid in enumerate(sub["rule_ids"])})
    st.close()
    return model, rules


def test_every_trout_cell_decodes_to_the_model_and_is_the_builders(decoded):
    model, rules = decoded
    n = 0
    nulls = 0
    for k, starts in enumerate(model.segments):
        rows, disp, lic = (model.sections[s][k] for s in ("rows", "display", "licence"))
        cells = model.sections["trout"][k]
        assert len(cells) == len(starts)
        built = T.compose_key(rows, disp, starts, model.moments[k], rules, licences=lic)
        want = [None if f is None else json.loads(json.dumps(f, ensure_ascii=False)) for f in built]
        assert cells == want, k
        for v in cells:
            if v is None:
                nulls += 1
            else:
                FRAMES["trout"].validate_json(json.dumps(v))
                n += 1
        # NULL exactly per Q-D3, every trout and char fish said exactly once, covers whole rows
        assert check_key(rows, disp, built) == [], k
    assert n >= 15 and nulls >= 5, (n, nulls)                     # not vacuous: both kinds of cell


def test_each_region_file_alone_reaches_every_trout_row_its_cells_name(side, tmp_path):
    """The shard rule: a regs file holds every trout frame, table, date set and sheet its cells name (an app reads one
    region's file and `water-shared`'s zdicts, never the province)."""
    m, open_ = _files(side, tmp_path)
    zd = dict(open_("water-shared").execute("SELECT tbl, d FROM zdict"))
    checked = 0
    for r in SH.REGIONS:
        con = open_(f"regs-{r}")
        have = {t: {i for (i,) in con.execute(f"SELECT id FROM {t}")} for t in TC_TABLES}
        val = lambda t, i, con=con: json.loads(unpack(con.execute(f"SELECT j FROM {t} WHERE id = ?", (i,)).fetchone()[0],  # noqa: E731
                                                      zd.get(t)))
        for (f,) in con.execute("SELECT trout FROM cell WHERE trout IS NOT NULL"):
            assert f in have["trout_frame"]
            t, d, _covers = val("trout_frame", f)
            assert t in have["trout_table"] and (d is None or d in have["trout_dates"])
            for b in val("trout_table", t)["boxes"]:
                for row in b["rows"]:
                    assert row["sheet"] in have["trout_sheet"]
            checked += 1
    assert checked >= 15, checked


# ----------------------------------------------------------------------------------- (d) the codec and the whole-key check

def test_the_codec_round_trips_and_interns(mini):  # noqa: F811
    vals = []
    for k, starts, moms, fs, ds in mini.keys:
        vals += T.compose_key(fs, ds, starts, moms, mini.rules, licences=mini.lics[k])
    vals = [None if v is None else json.loads(json.dumps(v)) for v in vals]
    codec = E.CODECS["trout"]
    tables, refs = codec.encode(vals, {}, {})
    assert [r is None for r in refs] == [v is None for v in vals]
    sec = json.loads(json.dumps(tables))
    assert [codec.decode_frame(sec, r, [], []) for r in refs] == vals
    distinct = {json.dumps(v, sort_keys=True) for v in vals if v is not None}
    assert len(sec["frames"]) == len(distinct)                    # interned: one frame per distinct value
    n_rows = sum(len(b["rows"]) for v in vals if v is not None for b in v["table"]["boxes"])
    assert len(sec["sheets"]) < n_rows                            # a sheet that reads the same is stored once


def _key_with_two_equal_segments(mini):  # noqa: F811
    for k, starts, moms, fs, ds in mini.keys:
        got = T.compose_key(fs, ds, starts, moms, mini.rules, licences=mini.lics[k])
        for j, x in enumerate(got):
            if x is not None:
                return k, fs[j], ds[j], mini.lics[k][j]
    pytest.fail("the mini release holds no table")


def test_a_whole_key_compose_that_adds_a_day_cut_stops_the_build(mini, monkeypatch):  # noqa: F811
    """Contract 2.2: the trout section adds no day cut — two neighbouring segments whose rows, display and licence values
    are equal get the same table. A builder that answered them differently stops the build; the mutation pin lets it."""
    k, f, d, lic = _key_with_two_equal_segments(mini)

    class Ctx:
        cache = {"trout_rules": mini.rules}
    vals = {"rows": [f, copy.deepcopy(f)], "display": [d, d], "licence": [lic, lic]}
    assert len(A._trout_compose_key(vals, [1, 100], None, Ctx())) == 2
    real = T.compose_key

    def drifting(*a, **kw):
        out = real(*a, **kw)
        out[1] = {**out[1], "covers": tuple(out[1]["covers"]) + (99,)}
        return out
    monkeypatch.setattr(T, "compose_key", drifting)
    with pytest.raises(A.AnswersError, match="day cut"):
        A._trout_compose_key(vals, [1, 100], None, Ctx())
    monkeypatch.setattr(A, "A_TROUT_ADDS_NO_DAY_CUT", False)
    A._trout_compose_key(vals, [1, 100], None, Ctx())
