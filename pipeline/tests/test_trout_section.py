"""THE TROUT SECTION IN THE RELEASE (trout table plan, T810 tickets I and L; contract 2.1-2.3, 2.7): `Section("trout", 1)`,
REQUIRED since the cutover (ticket L: `--trout-table` is gone), its codec (`encode.TroutCodec`), the store's `cell.trout`
and four interned tables, the regs shards that carry them (format `regs-region/2`), and the zdicts on `water-shared`.

Anchors, no live data:
  (a) THE REGISTRY: every hand list of region table names (the shards', the store's families and blob columns, the
      model's frames, the answers' sections) is held to the release contract (`contract._REGION_TABLES`, the optional
      lists all empty); a table added to one list only fails.
  (b) THE COMMITTED MINI RELEASE (the cutover form): the manifest and the layout name trout 1, every regs file carries
      the column and the four tables and is `regs-region/2`, `water-shared` their zdicts; the store refuses answers
      without the section.
  (c) THE MINI CHAIN (`mini_chain`, the same stages, built here): every non-NULL cell decodes to F's `TroutFrame`, is
      exactly what the builder makes of the rows / display / licence frames it read (before the cut), is NULL exactly
      per Q-D3, says every trout and char fish exactly once, and each region file alone reaches every trout row its cells
      name.
  (d) the codec round trip and the whole-key compose's no-day-cut check, with its mutation.
  (e) AN OLDER RELEASE STILL OPENS: a release cut before the cutover (the committed regs fixture, cut from
      r-46e4355a7277: rows 33, no trout section, `regs-region/1`) is reassembled by this code as it was built — the
      trout section, `licence_alien` and `section_tidal` are ADDITIVE (`store.release.ADDITIVE_TABLES`,
      `store.build.ADDED_SECTIONS`) — and holds its store digest.
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
from pipeline.deliver.answers.model import FRAMES, NULLABLE_FRAMES, OPTIONAL_FRAMES, json_schemas, validate_section
from pipeline.deliver.answers.trout import table as T
from pipeline.deliver.release import contract as C
from pipeline.deliver.release import shards as SH
from pipeline.deliver.store import build as SB
from pipeline.deliver.store.common import BLOB_COLUMNS, unpack
from pipeline.tests import mini_chain, under_test
from pipeline.tests.test_trout_table import check_key, mini  # noqa: F401  (module fixture)

TC_TABLES = ("trout_frame", "trout_table", "trout_dates", "trout_sheet")
REPO = Path(__file__).resolve().parents[2]
#: a release from before the cutover, kept as its own fixture (T810 cutover): the regs fixture as it was cut from the live
#: release r-46e4355a7277 (rows 33, no trout section, `regs-region/1`), before the cutover re-cut the app's copy
REGS_FIXTURE = REPO / "pipeline" / "tests" / "fixtures" / "pre-cutover-regs-release"


# ------------------------------------------------------------------------------------------------------ (a) the registry

def registry_problems(contract=None, shards=None, store=None, model=None, sections=None) -> list:
    """Every hand list of region table names that differs from the release contract's (each argument replaces one side,
    for the mutations). Since the cutover the trout section is REQUIRED: in the contract's `cell` and tables; the store
    and the shards hold it as an ADDED section (a release cut before it has none, and is read as built); every optional
    list (the contract's, the model's, the answers') is empty."""
    req = list(C._REGION_TABLES) if contract is None else contract[0]
    opt = C.OPTIONAL_SECTION_TABLES if contract is None else contract[1]
    cell = C._REGION_TABLES["cell"] if contract is None else contract[2]
    top, shard_tables, atop, ashard = (SH.TOP, SH.SHARD_TABLES, SH.ADDED_TOP, SH.ADDED_SHARD_TABLES) \
        if shards is None else shards
    secs, asecs, fams = (SB.SECTIONS, SB.ADDED_SECTIONS, SB.FAMILIES) if store is None else store
    oframes = OPTIONAL_FRAMES if model is None else model
    answers = [s.name for s in A.SECTIONS] if sections is None else sections
    oanswers = {s.name for s in A.SECTIONS if s.optional}
    out = []
    want = cell[2:]
    if [*top, *atop] != want or [*secs, *asecs] != want or [n for n in answers if n != "ladder"] != \
            [n for n in want if n != "ladder"] or "ladder" not in answers:
        out.append(f"the required sections: cell {want}, shards {[*top, *atop]}, store {[*secs, *asecs]}, "
                   f"answers {answers}")
    held = set(shard_tables) | {t for ts in ashard.values() for t in ts}
    if held != set(req) - {"meta"}:
        out.append(f"the shards' tables {sorted(held ^ (set(req) - {'meta'}))}")
    if opt or oframes or oanswers:
        out.append(f"the optional sections: contract {sorted(opt)}, model {sorted(oframes)}, answers {sorted(oanswers)}")
    for n in asecs:
        tables = set(ashard.get(n, ()))
        if atop.get(n) not in tables:
            out.append(f"{n}: its top frame {atop.get(n)} is not one of its tables {sorted(tables)}")
        if {t for (s, _k), t in fams.items() if s == n} != tables:
            out.append(f"{n}: the store's families {sorted(t for (s, _k), t in fams.items() if s == n)}")
        if not tables <= set(BLOB_COLUMNS):
            out.append(f"{n}: blob columns lack {sorted(tables - set(BLOB_COLUMNS))}")
    return out


def test_every_hand_list_of_region_tables_is_the_contracts():
    assert registry_problems() == []
    assert C._REGION_TABLES["cell"][-1] == "trout"
    assert set(TC_TABLES) <= set(C._REGION_TABLES)
    assert C.region_tables() == C._REGION_TABLES and C.OPTIONAL_SECTION_TABLES == {}
    assert {f["format"] for n, f in C.FILES.items() if n.startswith("regs-") and n != "regs-sid"} == {"regs-region/2"}


@pytest.mark.parametrize("where", ["contract", "shards", "store", "model", "answers", "cell"])
def test_mutation_a_table_or_section_added_to_one_list_only_fails(where):
    if where == "contract":
        got = registry_problems(contract=(list(C._REGION_TABLES) + ["trout_extra"], C.OPTIONAL_SECTION_TABLES,
                                          C._REGION_TABLES["cell"]))
    elif where == "shards":
        got = registry_problems(shards=(SH.TOP, SH.SHARD_TABLES + ("bait_frame",), SH.ADDED_TOP,
                                        SH.ADDED_SHARD_TABLES))
    elif where == "store":
        got = registry_problems(store=(SB.SECTIONS, SB.ADDED_SECTIONS, {**SB.FAMILIES, ("trout", "x"): "trout_x"}))
    elif where == "model":
        got = registry_problems(model=frozenset({"bait"}))
    elif where == "answers":
        got = registry_problems(sections=[s.name for s in A.SECTIONS if s.name != "trout"])
    else:                                   # trout back out of the contract's cell (the release before the cutover)
        got = registry_problems(contract=(list(C._REGION_TABLES), C.OPTIONAL_SECTION_TABLES,
                                          C._REGION_TABLES["cell"][:-1]))
    assert got


def test_the_store_refuses_an_unknown_section():
    con = sqlite3.connect(":memory:")
    con.executescript(SB.SCHEMA.read_text())
    with pytest.raises(SB.StoreError, match="not added sections"):
        SB.added_schema(con, ["bait"])


# ------------------------------------------------------------------------------- (b) the committed mini release: required

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


def test_the_committed_mini_release_carries_the_section_all_or_nothing(tmp_path):
    from pipeline.tests.test_release import contract_problems
    rel = _release(under_test.mini())
    assert contract_problems(rel, tmp_path, every_file=False) == []
    m, open_ = _files(rel, tmp_path)
    assert m["regulations"]["versions"]["trout"] == 5 and m["regulations"]["versions"]["rows"] == 34
    layout = json.loads(dict(open_("water").execute("SELECT k, v FROM meta"))["layout"])
    assert layout["versions"]["trout"] == 5
    for r in SH.REGIONS:
        assert m["files"][f"regs-{r}"]["format"] == "regs-region/2"
        con = open_(f"regs-{r}")
        assert SH.cell_sections(con) == [*SH.TOP, "trout"]
        assert set(TC_TABLES) <= {t for (t,) in con.execute("SELECT name FROM sqlite_master")}
    zd = {t for (t,) in open_("water-shared").execute("SELECT tbl FROM zdict")}
    assert set(TC_TABLES) <= zd


def test_every_file_is_built_with_the_section():
    """No optional section is left: the build makes every section, the trout table among them (no flag), and a name
    passed as optional is refused."""
    assert [s.name for s in A.SECTIONS if s.optional] == []
    assert "trout" in [s.name for s in A.SECTIONS]
    with pytest.raises(A.AnswersError, match="not optional sections"):
        A.build("x", Path("."), optional=("trout",))


def test_answers_without_the_section_are_not_packaged(tmp_path_factory, tmp_path):
    """Answers from before the cutover (no trout section) are refused at packaging (`card_check.stamp_problems`, which
    the store reads first): the chain's own answers with the section taken out, the stamp rewritten for those bytes."""
    import hashlib
    from pipeline.deliver.answers.card_check import STAMP_NAME, stamp_problems
    d = mini_chain.deliver(tmp_path_factory)
    wire = json.loads((d / "ui-rules-answers.json").read_bytes())
    assert stamp_problems(d / "ui-rules-answers.json", (d / "ui-rules-answers.json").read_bytes()) == []
    del wire["sections"]["trout"]
    raw = json.dumps(wire).encode()
    st = json.loads((d / STAMP_NAME).read_text())
    st["card_check"]["answers_sha256"] = hashlib.sha256(raw).hexdigest()
    st["card_check"].pop("trout_check", None)
    (tmp_path / STAMP_NAME).write_text(json.dumps(st))
    got = stamp_problems(tmp_path / "ui-rules-answers.json", raw)
    assert any("holds no trout section" in p for p in got), got


def test_the_spec_and_schemas_always_ship_trout():
    assert E.spec_for(["rows"]) is E.SPEC and "trout" in E.SPEC["sections"]
    assert {"trout", "trout.wire"} <= set(json_schemas(["rows"])) == set(json_schemas())


def test_a_null_cell_is_valid_only_for_the_trout_section():
    assert NULLABLE_FRAMES == {"trout"}
    assert validate_section("trout", [None, None]) == 0
    with pytest.raises(Exception):
        validate_section("gear", [None])


# --------------------------------------------------------------------------------------------- (c) the mini chain

@pytest.fixture(scope="module")
def side(tmp_path_factory):
    """The mini chain's release (`mini_chain`: every release carries the section since the cutover)."""
    return mini_chain.release(tmp_path_factory)


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
def decoded(side, tmp_path_factory):
    """The chain's release put back together (its answers model, decoded) and the same answers built again with the
    cutover pass off (`mini_chain.trout_inputs`: the rows the builder read), with the builder's rule table."""
    from pipeline.deliver.store.release import province_store
    st = province_store(side)
    raw = st.answers_bytes()
    sub = st.export_subset()
    data = {"rule_ids": sub["rule_ids"]}
    model = E.decode(json.loads(raw), data)
    rules = T.Rules({i: T.rule_of(sub["rules"][rid]) for i, rid in enumerate(sub["rule_ids"])})
    st.close()
    return model, mini_chain.trout_inputs(tmp_path_factory), rules


def test_every_trout_cell_decodes_to_the_model_and_is_the_builders(decoded):
    """Each shipped cell is the builder's table over the rows it read (before the cut: `mini_chain.trout_inputs`), NULL
    exactly per Q-D3, every fish said once. (That the shipped rows are those rows cut over is
    `test_t810_cutover.test_every_row_the_table_does_not_cover_is_the_cards_row`.)"""
    model, pre, rules = decoded
    n = 0
    nulls = 0
    assert model.segments == pre.segments
    for k, starts in enumerate(model.segments):
        rows, disp, lic = (pre.sections[s][k] for s in ("rows", "display", "licence"))
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


# ------------------------------------------------------------------------------------- (e) an older release still opens

def _older_release() -> Path:
    got = sorted(p for p in REGS_FIXTURE.glob("r-*") if (p / "manifest.json").exists())
    if len(got) != 1:
        pytest.fail(f"the regs fixture: {len(got)} releases under {REGS_FIXTURE} (one expected) — "
                    "a copy of a regs fixture from before the cutover", pytrace=False)
    m = json.loads((got[0] / "manifest.json").read_text())
    if "trout" in (m["regulations"].get("versions") or {}):
        pytest.fail(f"the regs fixture {got[0].name} was re-cut from a release with the trout section: this test needs a "
                    "release from before the cutover (keep a copy of one as its own fixture)", pytrace=False)
    return got[0]


def test_a_release_cut_before_the_cutover_is_reassembled_as_it_was_built():
    """The previous live release still opens (ticket L): its store is put back with no trout column or table (an ADDED
    section it lacks), and the ADDITIVE tables (`section_tidal`, `licence_alien`) only where its files hold them."""
    from pipeline.deliver.store.release import ADDITIVE_TABLES, province_store
    assert {"section_tidal", "licence_alien"} <= ADDITIVE_TABLES
    rel = _older_release()
    m = json.loads((rel / "manifest.json").read_text())
    assert {f["format"] for n, f in m["files"].items() if n.startswith("regs-") and n != "regs-sid"} == {"regs-region/1"}
    st = province_store(rel, verify=False)                  # a fixture is a cut: no whole-store digest to hold
    try:
        con = st.con
        assert SH.cell_sections(con) == list(SH.TOP)
        names = {t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert not names & set(TC_TABLES)
        assert json.loads(st.meta["layout"])["versions"]["rows"] == 33
    finally:
        st.close()


def test_mutation_an_added_section_made_for_a_release_without_it_breaks_the_reassembly(monkeypatch):
    """Without the ADDED rule (the trout section made whether the release names it or not) the older release's cell
    rows cannot be put back: the reassembly fails, as it did for `licence_alien` before it was additive."""
    from pipeline.deliver.store import release as R
    real = SB.added_schema
    monkeypatch.setattr(SB, "added_schema", lambda con, names: real(con, SB.ADDED_SECTIONS))
    with pytest.raises(sqlite3.OperationalError):
        R.province_store(_older_release(), verify=False)


@pytest.mark.needs_bundle
def test_the_live_release_still_opens_and_holds_its_store_digest():
    """The release LATEST names (before the cutover release is promoted: r-46e4355a7277, rows 33) reassembles under this
    code and holds its store digest (`store_meta`): the trout section is ADDED, `licence_alien` and `section_tidal` are
    ADDITIVE. After the cutover release is promoted this proves the same of it."""
    from pipeline.deliver.store.release import province_store
    st = province_store(under_test.release_at())
    try:
        assert st.answers_bytes()
    finally:
        st.close()


# ------------------------------------------------------------------------------------------- the shards read rows 34

def test_the_shard_cut_reads_a_rows_34_frame():
    """A1's finding (T810 L): `shards.frame_refs` read a rows frame as exactly five slots, so F0's sixth slot
    (`page_possession`, rule indexes of the water file's rule table, no shard row) broke the shard cut. It reads both
    shapes, and the sixth slot reaches no shard row."""
    fish = {"RB": [3, 4, "hatchery"], "LT": [None, 5, "wild"]}
    five = [["RB", "LT"], fish, [0, 2], None, [1, 1]]
    page = {"lead": {"says": "Possession: 2 days’ quota.", "value": "2 days’ quota", "src": 7}, "also": []}
    want = {"rows_decided": [3, 4, 5], "rows_row": [0, 2]}
    assert SH.frame_refs("rows_frame", five) == want
    assert SH.frame_refs("rows_frame", five + [page]) == want
    assert SH.frame_refs("rows_frame", five + [None]) == want


def test_the_committed_mini_releases_rows_frames_are_rows_34_in_every_region_file(tmp_path):
    """Every regs file of the committed mini release holds its rows frames with the sixth slot, and every row and
    decided answer a frame names is in the same file (the shard cut over rows 34)."""
    rel = _release(under_test.mini())
    m, open_ = _files(rel, tmp_path)
    zd = dict(open_("water-shared").execute("SELECT tbl, d FROM zdict"))
    seen = 0
    for r in SH.REGIONS:
        con = open_(f"regs-{r}")
        have = {t: {i for (i,) in con.execute(f"SELECT id FROM {t}")} for t in ("rows_row", "rows_decided")}
        for (b,) in con.execute("SELECT j FROM rows_frame"):
            fr = json.loads(unpack(b, zd.get("rows_frame")))
            assert len(fr) == 6
            for t, ids in SH.frame_refs("rows_frame", fr).items():
                assert set(ids) <= have[t], (r, t)
            seen += 1
    assert seen > 0
