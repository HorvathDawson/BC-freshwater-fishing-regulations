"""(T1030) THE PER-WATER DIGEST LIST and `manifest.changed_waters`: what a water's digest covers, the count against the
release a build replaces (absent, never 0, when it cannot be counted), and that neither moves the release id."""
from __future__ import annotations

import copy
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from pipeline.deliver.release import rebase as RB
from pipeline.deliver.release import water_digests as WD
from pipeline.tools.audit import answers_diff as AD


# --------------------------------------------------------------------------------------------- a decoded release, small

class FakeRel:
    """The parts of `answers_diff.Release` the list reads: two shown waters, three answer keys, two salmon chains."""

    def __init__(self):
        self.frames = {(n, 0): {"v": 0, "t": "words"} for n in AD.SECTION_NAMES}
        self.frames.update({(n, 1): {"v": 1, "t": "words"} for n in AD.SECTION_NAMES})
        self.rule_ids, self.fish, self.lic_ids = ["r.a", "r.b"], [], ["L0", "L1"]
        self.wire = {
            "keys": [[0, 0, 0, "possible", 1, [], [], "stream", 0, 0, None],
                     [0, 0, 0, "possible", 1, [], [], "stream", 0, 1, None],
                     [0, 0, 1, "present", 1, [], [], "lake", 0, 0, None]],
            "segments": [[1], [1, 100]],
            "parts": {"w:a": [0, 1], "w:b": [2]},
            "sections": {"display": {"waters": {
                "w:a": {"parts": [self._part("mouth", "fork"), self._part("fork", "source")], "unresolved_licensing": []},
                "w:b": {"parts": [self._part("lake", "lake")], "unresolved_licensing": [1]}}}}}
        # the cells: key -> one frame ref per section per segment
        self.at = {0: [0], 1: [0, 1], 2: [1]}
        self.sections = {"ladder": {"at": [self.at[0], self.at[1], self.at[2]]}}
        self.salmon = {"chains": [self._chain("open"), self._chain("closed")],
                       "verdicts": [{"s": "open", "t": "Open"}, {"s": "closed", "t": "Closed"}], "gears": [["barbless"]]}
        self.chain_of, self.default_chain = {11: 0, 12: 1}, 0
        self.versions = {"display": 20, "rows": 34}
        self.manifest = {"files": {"salmon": {"format": "salmon/4"}}}
        self.sections_of = {("w:a", 0): [11], ("w:a", 1): [12], ("w:b", 0): [13]}

    @staticmethod
    def _part(lo, hi):
        return {"order": 0, "label": f"From {lo} to {hi}", "lo": lo, "hi": hi, "runs": "x", "place": None, "km": 1.5,
                "closed_all_year": False, "paper_licence": [0]}

    def _chain(self, kind):
        v = 0 if kind == "open" else 1
        return {"sp": [[0, v]] * 5, "a": [[0, 0]] * 5, "l": ["2:place"], "g": [0, 0], "w": "words", "r": ["2"],
                "m": [0], "n": [0], "c": [], "s": []}

    def rid(self, i):
        return self.rule_ids[i] if isinstance(i, int) and 0 <= i < len(self.rule_ids) else i

    def lid(self, i):
        return self.lic_ids[i] if isinstance(i, int) and 0 <= i < len(self.lic_ids) else i

    def key_meta(self, k):
        return self.wire["segments"][self.wire["keys"][k][9]], None

    def refs(self, k, s):
        r = self.at[k][s]
        return (r,) * len(AD.SECTION_NAMES) + (None,)


@pytest.fixture
def fake(monkeypatch):
    rel = FakeRel()
    monkeypatch.setattr(AD, "net_value", lambda r, name, ref, resolve: copy.deepcopy(r.frames[(name, ref)]))
    return rel


def lst(rel):
    return WD.digests_of(rel, rel.sections_of)


def test_the_list_is_keyed_on_item_ids_and_deterministic(fake):
    a, b = lst(fake), lst(FakeRel())
    assert sorted(a["waters"]) == ["w:a", "w:b"]
    assert a == b and WD.encode(a) == WD.encode(b)                          # no clock, sorted (AGENTS 19)
    assert gzip.decompress(WD.encode(a)) == WD.raw_bytes(a)


def test_a_change_to_what_you_may_do_changes_only_that_water(fake):
    before = lst(fake)
    fake.frames[("rows", 1)] = {"v": 2, "t": "words"}                       # w:a's second part and w:b read frame 1
    after = lst(fake)
    assert WD.changed(before, after) == ["w:a", "w:b"]
    fake2 = FakeRel()
    fake2.wire["keys"][2][7] = "stream"                                     # a key fact: w:b only
    assert WD.changed(before, lst(fake2)) == ["w:b"]


def test_wording_alone_is_not_a_change(fake):
    before = lst(fake)
    for k in list(fake.frames):
        fake.frames[k] = {**fake.frames[k], "t": "other words"}
    for w in fake.wire["sections"]["display"]["waters"].values():
        for p in w["parts"]:
            p["label"], p["runs"] = "Renamed", "Renamed"
    fake.salmon["verdicts"][0]["t"] = "Open, said again"
    fake.salmon["chains"][0]["w"] = "other words"
    assert WD.changed(before, lst(fake)) == []


def test_geometry_alone_is_not_a_change_but_where_parts_split_is(fake):
    before = lst(fake)
    fake.wire["sections"]["display"]["waters"]["w:a"]["parts"][0]["km"] = 9.9
    fake.sections_of[("w:a", 0)] = [11, 14]                                  # a re-cut: one more section, same chain
    assert WD.changed(before, lst(fake)) == []
    fake.wire["sections"]["display"]["waters"]["w:a"]["parts"][0]["hi"] = "bridge"
    assert WD.changed(before, lst(fake)) == ["w:a"]


def test_the_salmon_on_a_waters_sections_counts(fake):
    before = lst(fake)
    fake.chain_of[13] = 1                                                    # w:b's section now reads the closed chain
    assert WD.changed(before, lst(fake)) == ["w:b"]
    fake2 = FakeRel()
    fake2.salmon["chains"][1]["l"] = ["2:another-place"]                     # the place answering w:a's upper part
    assert WD.changed(before, lst(fake2)) == ["w:a"]


def test_rule_and_licensing_refs_are_read_as_ids_not_indexes(fake):
    before = lst(fake)
    fake.rule_ids = ["r.z", "r.b"]                                          # rule 0 (both paper licences) is another
    assert WD.changed(before, lst(fake)) == ["w:a", "w:b"]


def test_one_segment_split_in_two_with_the_same_answer_is_not_a_change(fake):
    before = lst(fake)
    fake.wire["segments"].append([1, 50])
    fake.wire["keys"][0][9] = 2
    fake.at[0] = [0, 0]
    fake.sections["ladder"]["at"][0] = [0, 0]
    assert WD.changed(before, lst(fake)) == []


def test_a_water_added_or_gone_counts():
    a = {"format": WD.FORMAT, "basis": {"x": 1}, "waters": {"w:a": "1", "w:b": "2"}}
    b = {"format": WD.FORMAT, "basis": {"x": 1}, "waters": {"w:a": "1", "w:c": "3"}}
    assert WD.changed(a, b) == ["w:b", "w:c"]
    assert WD.count(a, b, say=lambda s: None) == 2
    assert WD.count(a, a, say=lambda s: None) == 0


def test_no_list_before_or_another_basis_is_unknown_never_zero():
    a = {"format": WD.FORMAT, "basis": {"x": 1}, "waters": {"w:a": "1"}}
    said = []
    assert WD.count(None, a, say=said.append, why="the previous release r-x wrote no list") is None
    assert "unknown" in said[-1] and "r-x wrote no list" in said[-1]
    assert WD.count({**a, "basis": {"x": 2}}, a, say=said.append) is None
    assert "another basis" in said[-1]


# ------------------------------------------------------------------------------------------------- the build's half

def _release(root: Path, rid: str, *, digests=None, count=None) -> Path:
    """A release folder as `build.ship` leaves it: a manifest whose id `rebase.release_id` recomputes."""
    d = root / rid
    d.mkdir(parents=True)
    m = {"format": "app-manifest/2", "data_release": rid, "files": {}, "basemap": None, "synopsis": None,
         "tiles": {"compatible": ["h.*"], "current": None, "assets": None, "mask": None},
         "regulations": {"valid_until": None, "versions": None}}
    if digests is not None:
        raw = WD.raw_bytes(digests)
        (d / WD.FILENAME).write_bytes(gzip.compress(raw, mtime=0))
        m[WD.MANIFEST_KEY] = {"url": f"{rid}/{WD.FILENAME}", "sha256": hashlib.sha256(raw).hexdigest(),
                              "bytes": len(raw), "format": WD.FORMAT}
    if count is not None:
        m[WD.COUNT_KEY] = count
    (d / "manifest.json").write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")
    return d


def _list(**waters):
    return {"format": WD.FORMAT, "basis": {"scheme": WD.SCHEME}, "waters": waters}


def test_attach_names_the_list_and_counts_against_the_previous_one(tmp_path, monkeypatch):
    prev = _release(tmp_path / "live", "r-prev", digests=_list(**{"w:a": "1", "w:b": "2"}))
    new = _release(tmp_path / "stage", "r-new")
    monkeypatch.setattr(WD, "water_digests", lambda d: _list(**{"w:a": "1", "w:b": "9", "w:c": "3"}))
    said = []
    assert WD.attach(new, WD.previous_state(prev), say=said.append) == 2
    m = json.loads((new / "manifest.json").read_text())
    assert m[WD.COUNT_KEY] == 2 and isinstance(m[WD.COUNT_KEY], int)
    assert m[WD.FROM_KEY] == "r-prev"                                         # the count names the release it is against
    assert WD.release_list(new) == _list(**{"w:a": "1", "w:b": "9", "w:c": "3"})   # named, held to its sha256
    assert said == ["changed_waters: 2 of 3 waters differ from r-prev"]


def test_no_previous_or_one_without_a_list_states_no_count(tmp_path, monkeypatch):
    monkeypatch.setattr(WD, "water_digests", lambda d: _list(**{"w:a": "1"}))
    old = _release(tmp_path / "live", "r-old")                                 # built before T1030
    for prev, why in ((None, "no previous release was named"), (old, "r-old wrote no list"),
                      (tmp_path / "live" / "r-pruned", "r-pruned is not on disk")):
        new = _release(tmp_path / f"stage-{why[:6]}", "r-new", count=7)        # even a stale count goes
        said = []
        assert WD.attach(new, WD.previous_state(prev), say=said.append) is None
        m = json.loads((new / "manifest.json").read_text())
        assert WD.COUNT_KEY not in m and WD.FROM_KEY not in m
        assert why in said[0] and "states no count" in said[0]


def test_a_rebuild_of_the_release_it_replaces_keeps_that_releases_count(tmp_path, monkeypatch):
    monkeypatch.setattr(WD, "water_digests", lambda d: _list(**{"w:a": "1"}))
    prev = _release(tmp_path / "live", "r-same", digests=_list(**{"w:a": "1"}), count=4)
    m = json.loads((prev / "manifest.json").read_text())
    (prev / "manifest.json").write_text(json.dumps({**m, WD.FROM_KEY: "r-before"}))
    new = _release(tmp_path / "stage", "r-same")
    assert WD.attach(new, WD.previous_state(prev), say=lambda s: None) == 4
    assert json.loads((new / "manifest.json").read_text())[WD.FROM_KEY] == "r-before"


def test_a_named_list_that_is_missing_or_altered_is_refused(tmp_path):
    prev = _release(tmp_path, "r-prev", digests=_list(**{"w:a": "1"}))
    (prev / WD.FILENAME).write_bytes(gzip.compress(WD.raw_bytes(_list(**{"w:a": "2"}))))
    with pytest.raises(WD.DigestError, match="sha256"):
        WD.previous_state(prev)
    (prev / WD.FILENAME).unlink()
    with pytest.raises(WD.DigestError, match="missing"):
        WD.previous_state(prev)


def test_neither_the_list_nor_the_count_is_part_of_the_release_id(tmp_path, monkeypatch):
    """The id is the files + what `said_of` reads: the count depends on the predecessor, so the same content cut after
    two different releases must be ONE id (and the list is a function of the files, which the id already covers)."""
    d = _release(tmp_path, "r-x")
    m = json.loads((d / "manifest.json").read_text())
    rid = RB.release_id(m)
    monkeypatch.setattr(WD, "water_digests", lambda d: _list(**{"w:a": "1"}))
    for n, prev in ((1, _list(**{"w:a": "2"})), (0, _list(**{"w:a": "1"}))):
        WD.attach(d, {"name": "r-p", "id": "r-p", "list": prev, "count": None, "why": None}, say=lambda s: None)
        m2 = json.loads((d / "manifest.json").read_text())
        assert m2[WD.COUNT_KEY] == n and m2[WD.FROM_KEY] == "r-p" and WD.MANIFEST_KEY in m2
        assert RB.release_id(m2) == rid


def test_a_rebased_release_carries_the_list_and_changed_nothing(tmp_path):
    src = _release(tmp_path, "r-src", digests=_list(**{"w:a": "1"}), count=5)
    m = json.loads((src / "manifest.json").read_text())
    rid = RB.release_id(m)
    m["data_release"] = rid
    (src / "manifest.json").write_text(json.dumps(m))
    src = src.rename(tmp_path / rid)
    out = RB.rebase(src, {"id": "basemap-x", "url": "basemap/x.pmtiles", "bytes": 1, "sha256": "0"})
    m2 = json.loads((out / "manifest.json").read_text())
    assert (m2[WD.COUNT_KEY], m2[WD.FROM_KEY]) == (0, rid)                     # counted against its source, truthfully
    assert WD.release_list(out) == _list(**{"w:a": "1"})


def test_the_cli_never_writes_into_a_release_folder(tmp_path):
    d = _release(tmp_path, "r-x")
    with pytest.raises(SystemExit):
        WD.main([str(d), "--out", str(d / "mine.json.gz")])
    with pytest.raises(SystemExit):
        WD.main([str(d)])


class _Trout:
    def __init__(self, src):
        self.src = src

    def decode_frame(self, sec, ref, rule_ids, fish):
        return {"rows": [{"name": "Rainbow trout", "src": list(self.src), "sheet": {"src": list(self.src)}}],
                "from": 5, "to": 9}


def test_a_release_whose_rule_list_is_renumbered_changes_no_water(fake):
    """0.8.4 -> 0.8.5 added rules: every rule index moved. Read as indexes, 19,765 of 19,766 waters "changed"; read as
    rule ids (the paper-licence rules, the trout cell's `src`), only the waters whose rules did."""
    fake.refs = lambda k, s: (fake.at[k][s],) * len(AD.SECTION_NAMES) + (0,)
    fake.sections["trout"] = {}
    fake.codecs = {"trout": _Trout([0, 1])}
    before = lst(fake)
    fake.rule_ids = ["r.new", "r.a", "r.b"]                                  # one rule more, in front
    fake.codecs = {"trout": _Trout([1, 2])}
    for w in fake.wire["sections"]["display"]["waters"].values():
        for p in w["parts"]:
            p["paper_licence"] = [1]
    assert WD.changed(before, lst(fake)) == []
    fake.codecs = {"trout": _Trout([1, 0])}                                  # the trout rows now cite another rule
    assert WD.changed(before, lst(fake)) == ["w:a", "w:b"]


def test_deliver_all_names_the_release_latest_names_as_the_previous_one(tmp_path):
    from pipeline.deliver.__main__ import previous_args
    assert previous_args(tmp_path) == []                                     # no pointer: none (the step says so)
    (tmp_path / "LATEST").write_text("r-live\n")
    assert previous_args(tmp_path) == ["--previous", str(tmp_path / "r-live")]


def test_closed_all_year_counts(fake):
    before = lst(fake)
    fake.wire["sections"]["display"]["waters"]["w:b"]["parts"][0]["closed_all_year"] = True
    assert WD.changed(before, lst(fake)) == ["w:b"]


def _licence(fake, *, req="L0", doc="stamp", species=("CH",), act="keep", says="Carry a salmon stamp"):
    fake.frames[("licence", 1)] = {
        "profiles": [{"requirements": [{"id": req, "when": {"act": act, "species": list(species), "lengths": None},
                                         "t": says}]}],
        "documents": [{"id": doc, "when": {"act": act, "species": list(species), "on": None}, "words": [says]}]}


def test_a_licence_requirement_asked_for_another_fish_or_act_counts_its_words_do_not(fake):
    """(T1030 review) `when` is wording in the changed-answers list's TEXT_KEYS, but the licence frame's `when` is rule
    data {act, species, lengths, on}: a stamp asked for another fish under the same id is a change."""
    _licence(fake)
    before = lst(fake)
    for kw in ({"species": ("CO",)}, {"act": "fish"}, {"req": "L1"}, {"doc": "other-stamp"}):
        _licence(fake, **kw)
        assert WD.changed(before, lst(fake)) == ["w:a", "w:b"], kw            # both read licence frame 1
    _licence(fake, says="Carry your salmon stamp")                            # words only
    assert WD.changed(before, lst(fake)) == []


def test_law_drops_a_word_named_key_only_where_it_holds_words():
    keys = frozenset({"when", "t"})
    assert WD.law({"when": "Apr 1 to Jun 14", "t": ["a", "b"], "x": 1}, keys) == {"x": 1}
    assert WD.law({"when": {"act": "keep"}, "t": [{"r": 1}]}, keys) == {"when": {"act": "keep"}, "t": [{"r": 1}]}


def test_the_built_mini_release_names_its_list_and_states_no_count(tmp_path_factory):
    """The real release step (the mini chain) attaches the list: every water the release shows, held to its sha256; no
    previous release was named, so no count and no base."""
    from pipeline.deliver.release.manifest import latest
    from pipeline.tests import mini_chain
    d = latest(mini_chain.built(tmp_path_factory) / "release")
    m = json.loads((d / "manifest.json").read_text())
    lst_ = WD.release_list(d)
    assert lst_ is not None and m[WD.MANIFEST_KEY]["waters"] == len(lst_["waters"]) > 0
    assert WD.COUNT_KEY not in m and WD.FROM_KEY not in m
    assert sorted(lst_["waters"]) == sorted(WD.water_digests(d)["waters"])


def test_the_manifest_names_the_list_by_its_content_never_by_its_gzip_bytes(tmp_path, monkeypatch):
    """(T1030, CI 45405205568) CI's Linux zlib writes the same JSON in other gzip bytes than macOS: the manifest's block
    must say only what follows the content (raw length, raw sha256), so a release built on either machine states the same
    manifest. Two zlib "builds" (levels 9 and 1) give one manifest."""
    big = {f"w:{i}": hashlib.sha256(str(i).encode()).hexdigest()[:16] for i in range(500)}
    monkeypatch.setattr(WD, "water_digests", lambda d: _list(**big))
    real = gzip.compress
    seen = []
    for level in (9, 1):
        monkeypatch.setattr(WD.gzip, "compress", lambda b, compresslevel=9, mtime=0, _l=level: real(b, _l, mtime=0))
        new = _release(tmp_path / f"z{level}", "r-new")
        WD.attach(new, WD.previous_state(None), say=lambda s: None)
        seen.append(((new / "manifest.json").read_bytes(), (new / WD.FILENAME).read_bytes()))
    assert len(seen[0][1]) != len(seen[1][1])                                 # the gzip bytes do differ, in length too
    assert seen[0][0] == seen[1][0]                                           # the manifest does not


def test_the_mini_release_check_reads_the_list_by_content_under_another_release_id(tmp_path):
    """(T1030, CI 45410159283) CI cuts the same rows into other SQLite bytes, so another release id: the check must read
    the list's block as it reads each file's — by content, its url (which names the id) left out — and must still see a
    list whose content moved."""
    import shutil
    from pipeline.tools import mini_release as MR
    src = MR.COMMITTED
    rid = (src / "LATEST").read_text().strip()
    root = tmp_path / "other"
    shutil.copytree(src, root)
    (root / rid).rename(root / "r-000000000000")
    m = json.loads((root / "r-000000000000" / "manifest.json").read_text())
    m["data_release"] = "r-000000000000"
    for e in [*m["files"].values(), m[WD.MANIFEST_KEY]]:
        e["url"] = e["url"].replace(rid, "r-000000000000")
    if m.get("synopsis"):
        m["synopsis"]["url"] = m["synopsis"]["url"].replace(rid, "r-000000000000")
    (root / "r-000000000000" / "manifest.json").write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")
    (root / "LATEST").write_text("r-000000000000\n")
    assert MR.release_content(root) == MR.release_content(src)
    p = root / "r-000000000000" / WD.FILENAME
    lst = json.loads(gzip.decompress(p.read_bytes()))
    lst["waters"][sorted(lst["waters"])[0]] = "0" * 16
    p.write_bytes(gzip.compress(WD.raw_bytes(lst), mtime=0))
    got = MR.release_content(root)
    assert got["water_digests"] != MR.release_content(src)["water_digests"]
