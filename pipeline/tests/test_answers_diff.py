"""T809: the changed-answers list between two built releases (`pipeline/tools/audit/answers_diff.py`).

Anchor (b): the committed mini release (`under_test.mini()`), copied and ALTERED THROUGH THE REAL ENCODERS — the regs
store's own tables (reassembled by `province_store`, written back into the release's files, the store digest and the
manifest renewed) and the salmon files (`salmon.encode_files`) — so the tool reads a release exactly as a built one.
Each planted difference is reported once, in its group, on its days; an identical or re-encoded pair is empty.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver import salmon as SAL
from pipeline.deliver.store.common import store_digest, unpack
from pipeline.deliver.store.release import province_store
from pipeline.tests import under_test
from pipeline.tools.audit import answers_diff as AD


# ------------------------------------------------------------------------------------------------------- helpers

def _copy(tmp_path: Path, name: str) -> Path:
    src = under_test.mini()
    dst = tmp_path / name / src.name
    shutil.copytree(src, dst)
    return dst


def _rewrite(rel: Path, name: str, raw: bytes) -> None:
    """Write one release file's new bytes (gzip) and renew its manifest entry."""
    man = json.loads((rel / "manifest.json").read_text())
    f = man["files"][name]
    p = rel.parent / f["url"]
    gz = gzip.compress(raw, mtime=0)
    p.write_bytes(gz)
    f.update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw), gz=len(gz))
    (rel / "manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True))


class Store:
    """The release's regs store, reassembled; edits go back into every release file holding the row."""

    def __init__(self, rel: Path):
        self.rel = rel
        self.st = province_store(rel)
        self.con = self.st.con
        self.zd = dict(self.con.execute("SELECT tbl, d FROM zdict"))

    def get(self, tbl: str, i: int):
        (b,) = self.con.execute(f"SELECT j FROM {tbl} WHERE id = ?", (i,)).fetchone()
        return json.loads(unpack(b, self.zd.get(tbl)))

    def add(self, tbl: str, v) -> int:
        (n,) = self.con.execute(f"SELECT COALESCE(MAX(id), -1) + 1 FROM {tbl}").fetchone()
        self.con.execute(f"INSERT INTO {tbl} (id, j) VALUES (?, ?)", (n, json.dumps(v).encode()))
        return n

    def put(self, tbl: str, i: int, v) -> None:
        self.con.execute(f"UPDATE {tbl} SET j = ? WHERE id = ?", (json.dumps(v).encode(), i))

    def cell(self, akey: int, s: int, col: str, v: int = None):
        if v is None:
            return self.con.execute(f"SELECT {col} FROM cell WHERE akey = ? AND s = ?", (akey, s)).fetchone()[0]
        self.con.execute(f"UPDATE cell SET {col} = ? WHERE akey = ? AND s = ?", (v, akey, s))

    def save(self) -> None:
        """Every store table back into each release file that holds it (rows by primary key), the store digest
        renewed in the water file's `store_meta`, the manifest renewed."""
        con = self.con
        man = json.loads((self.rel / "manifest.json").read_text())
        names = [n for n in man["files"] if n == "water" or n.startswith("regs-")]
        digest = store_digest(con)
        for name in names:
            f = man["files"][name]
            raw = gzip.decompress((self.rel.parent / f["url"]).read_bytes())
            p = self.rel.parent / (name + ".edit.sqlite")
            p.write_bytes(raw)
            fc = sqlite3.connect(str(p))
            have = {t for (t,) in fc.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            for t in sorted(have - {"meta"}):
                if not con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (t,)).fetchone():
                    continue
                cols = [c[1] for c in fc.execute(f"PRAGMA table_info({t})")]
                sel = ", ".join(cols)
                fc.execute(f"DELETE FROM {t}")             # a region file then holds every region's rows: harmless
                fc.executemany(f"INSERT INTO {t} ({sel}) VALUES ({', '.join('?' * len(cols))})",
                               con.execute(f"SELECT {sel} FROM {t}"))
            if name == "water":
                meta = dict(fc.execute("SELECT k, v FROM meta"))
                sm = json.loads(meta["store_meta"])
                sm["store_digest"] = digest
                fc.execute("UPDATE meta SET v = ? WHERE k = 'store_meta'", (json.dumps(sm),))
            fc.commit()
            fc.close()
            data = p.read_bytes()
            p.unlink()
            _rewrite(self.rel, name, data)


def _salmon(rel: Path):
    man = json.loads((rel / "manifest.json").read_text())
    files = {n: gzip.decompress((rel.parent / f["url"]).read_bytes()) for n, f in man["files"].items()
             if n.startswith("salmon")}
    return SAL.decode_files(files)


def _salmon_save(rel: Path, handles, payload, secs) -> None:
    payload = {k: v for k, v in payload.items() if k != "shards"}
    files = SAL.encode_files(payload, secs, handles, lambda sid: ["1"])
    for n, b in files.items():
        _rewrite(rel, n, b)


def _diff(a: Path, b: Path) -> dict:
    return AD.run(a, b, log=lambda *x, **k: None)


def _plain_key(st: Store):
    """A key with three plain segments (no moments) and its akey's sections."""
    keys = st.st.answers()["keys"]
    segs = [json.loads(t) for _, t in st.con.execute("SELECT seg, starts FROM seglist ORDER BY seg")]
    for k, key in enumerate(keys):
        if key[10] is None and len(segs[key[9]]) == 3:
            return k + 1, segs[key[9]]
    raise AssertionError("the mini release has no key with three plain segments")


def _keep_fish(st: Store, akey: int, s: int):
    """(rows frame id, frame, a fish whose hatchery and wild answer is one keep row) at (akey, s)."""
    fid = st.cell(akey, s, "rows")
    fr = st.get("rows_frame", fid)
    for sp in fr[0]:
        h, w, _ = fr[1][sp]
        if h == w and st.get("rows_decided", h)["status"] == "keep":
            return fid, fr, sp
    raise AssertionError("no keep fish")


def _plant_decided(rel: Path, edit, origin: str = "both", seg: int = 1):
    """Point one fish of one key's segment `seg` at an edited copy of its decided answer. Returns (akey, starts, sp)."""
    st = Store(rel)
    akey, starts = _plain_key(st)
    fid, fr, sp = _keep_fish(st, akey, seg)
    h, w, lead = fr[1][sp]
    d = st.get("rows_decided", h)
    nd = st.add("rows_decided", edit(dict(d)))
    fr[1][sp] = [nd if origin in ("both", "hatchery") else h, nd if origin in ("both", "wild") else w, lead]
    st.cell(akey, seg, "rows", st.add("rows_frame", fr))
    st.save()
    return akey, starts, sp


def _only(r: dict, kind: str) -> dict:
    gs = [g for g in r["groups"] if g["kind"] == kind]
    assert len(gs) == 1, (kind, [(g["kind"], g["block"], g["subjects"]) for g in r["groups"]])
    return gs[0]


def _mask(starts, seg):
    hi = starts[seg + 1] - 1 if seg + 1 < len(starts) else AD.DAYS
    return AD.mask_of(starts[seg], hi)


@pytest.fixture()
def pair(tmp_path):
    return _copy(tmp_path, "a"), _copy(tmp_path, "b")


# ------------------------------------------------------------------------------------------------------- empty

def test_an_identical_pair_is_empty(pair, tmp_path):
    a, b = pair
    r = _diff(a, b)
    assert r["groups"] == [] and r["release_wide"] == []
    assert AD.text_report(r).startswith("No answer changed")
    assert AD.main([str(a), str(b), "--out", str(tmp_path / "o")]) == 0
    assert (tmp_path / "o" / "owner.md").read_text().endswith("No answer changed.\n")


def test_a_re_encoded_regs_store_with_the_same_answers_is_empty(pair):
    """Every rows frame moved to a new id (and the cells pointed at it): other bytes, same answers."""
    a, b = pair
    st = Store(b)
    for (fid, ) in st.con.execute("SELECT id FROM rows_frame ORDER BY id DESC").fetchall():
        nid = st.add("rows_frame", st.get("rows_frame", fid))
        st.con.execute("UPDATE cell SET rows = ? WHERE rows = ?", (nid, fid))
    st.save()
    assert (b / "manifest.json").read_text() != (a / "manifest.json").read_text()
    r = _diff(a, b)
    assert r["groups"] == [] and r["release_wide"] == []


def test_a_re_encoded_salmon_file_with_the_same_answers_is_empty(pair):
    """A copy of the default chain under a new index, two sections on it: other bytes, same answers."""
    a, b = pair
    h, p, secs = _salmon(b)
    p["chains"].append(dict(p["chains"][0]))
    _salmon_save(b, h, p, sorted([*secs, (10, len(p["chains"]) - 1), (11, len(p["chains"]) - 1)]))
    r = _diff(a, b)
    assert r["groups"] == [] and r["release_wide"] == []


def test_an_unreadable_release_stops_with_a_clear_error(pair, tmp_path, capsys):
    a, _ = pair
    assert AD.main([str(a), str(tmp_path / "nowhere"), "--out", str(tmp_path / "o")]) == 2
    assert "no release at" in capsys.readouterr().err
    assert not (tmp_path / "o").exists()


# ------------------------------------------------------------------------------------------------------- regs kinds

def test_a_fish_closed_on_one_segment_is_one_closes_group_on_exactly_those_days(pair):
    a, b = pair
    akey, starts, sp = _plant_decided(b, lambda d: {**d, "status": "closed", "daily": None})
    r = _diff(a, b)
    g = _only(r, "closes")
    assert len(r["groups"]) == 1 and g["errs"] == "closed" and g["block"] == "fish"
    want = AD.days_words(_mask(starts, 1))
    assert g["subjects"] == {AD.fish_words(sp): want}
    assert g["sections"] == len([h for h, k in province_store(b).section_akeys().items() if k == akey])
    # MUTATION: a day inside the segment that is a start in neither release is in the change
    mid = starts[1] + 10
    assert mid not in starts
    assert any(lo <= mid <= hi for lo, hi in g["subject_days"][AD.fish_words(sp)])
    # the whole sentence, as the owner reads it
    head = [l for l in AD.owner_page(r).splitlines() if l.startswith("- **")][0]
    assert head.startswith(f"- **Fish: {AD.fish_words(sp)} now closed, {want}.** Errs toward closed. On ")


def test_a_fish_opened_is_opens_and_errs_toward_open(pair):
    a, b = pair
    _plant_decided(a, lambda d: {**d, "status": "closed", "daily": None})     # A closed, B as built: opens
    r = _diff(a, b)
    g = _only(r, "opens")
    assert g["errs"] == "open" and len(r["groups"]) == 1
    head = [l for l in AD.owner_page(r).splitlines() if l.startswith("- **")][0]
    assert " now open, " in head and head.split("**")[2].startswith(" Errs toward open.")


@pytest.mark.parametrize("delta,kind,errs", [(-1, "limit_lower", "closed"), (+3, "limit_higher", "open")])
def test_a_daily_number_change_is_a_limit_change_in_its_direction(pair, delta, kind, errs):
    a, b = pair
    seen = {}

    def edit(d):
        seen["before"] = d["daily"]
        return {**d, "daily": d["daily"] + delta}
    _plant_decided(b, edit)
    r = _diff(a, b)
    g = _only(r, kind)
    assert g["errs"] == errs and len(r["groups"]) == 1
    assert g["before"] == [f"keep {seen['before']} a day"] and g["after"] == [f"keep {seen['before'] + delta} a day"]


def test_a_new_release_band_is_a_stricter_size_rule_and_its_removal_looser(pair):
    a, b = pair
    band = {"t": "rel", "r": 0, "a": 0, "b": 30}
    _plant_decided(b, lambda d: {**d, "lines": [*d["lines"], band]})
    g = _only(_diff(a, b), "size_stricter")
    assert g["errs"] == "closed" and g["after"] == ["release under 30 cm"]
    g = _only(_diff(b, a), "size_looser")
    assert g["errs"] == "open" and g["before"] == ["release under 30 cm"]


def test_one_origin_closing_alone_is_wild_hatchery(pair):
    a, b = pair
    _, _, sp = _plant_decided(b, lambda d: {**d, "status": "release", "daily": None}, origin="wild")
    r = _diff(a, b)
    g = _only(r, "wild_hatchery")
    assert len(r["groups"]) == 1 and g["errs"] == "closed"
    assert list(g["subjects"]) == [f"{AD.fish_words(sp)} (wild)"]


def test_a_possession_rule_added_is_possession(pair):
    a, b = pair
    _plant_decided(b, lambda d: {**d, "roles": [*d["roles"], [0, "possession", None]]})
    r = _diff(a, b)
    assert _only(r, "possession")["source_after"] is not None and len(r["groups"]) == 1


def test_a_gear_element_banned_is_gear_toward_closed(pair):
    a, b = pair
    st = Store(a)                                            # A allows what B (as built) bans
    akey, starts = _plain_key(st)
    gid = st.cell(akey, 1, "gear")
    g = st.get("gear_frame", gid)
    e = next(k for k, v in sorted(g["elements"].items()) if v["verdict"] == "ban")
    g["elements"][e] = {**g["elements"][e], "verdict": "allow"}
    st.cell(akey, 1, "gear", st.add("gear_frame", g))
    st.save()
    r = _diff(a, b)
    grp = _only(r, "gear")
    assert grp["errs"] == "closed" and list(grp["subjects"]) == [e] and len(r["groups"]) == 1
    assert grp["subjects"][e] == AD.days_words(_mask(starts, 1))


def test_a_licence_document_added_is_licence_toward_closed(pair):
    a, b = pair
    st = Store(b)
    akey, _ = _plain_key(st)
    lid = st.cell(akey, 1, "licence")
    h, d, *u = st.get("licence_frame", lid)
    docs = st.get("licence_doc", d)
    ans = st.get("licence_answer", docs[0])
    ans = {**ans, "none_needed": False,
           "documents": [*ans.get("documents", []), {"doc": "test_stamp", "when": {"act": "fishing"}, "base": False}]}
    docs = [st.add("licence_answer", ans), *docs[1:]]
    st.cell(akey, 1, "licence", st.add("licence_frame", [h, st.add("licence_doc", docs), *u]))
    st.save()
    r = _diff(a, b)
    g = _only(r, "licence")
    assert g["errs"] == "closed" and len(r["groups"]) == 1 and len(g["subjects"]) == 1


def test_a_whole_water_closure_is_one_closes_group_on_the_closure_block(pair):
    a, b = pair
    st = Store(b)
    akey, starts = _plain_key(st)
    did = st.cell(akey, 1, "display")
    f = st.get("display_frame", did)
    st.cell(akey, 1, "display", st.add("display_frame", {**f, "status": "closed", "closing": [[0, ["RB"]]]}))
    st.save()
    r = _diff(a, b)
    g = _only(r, "closes")
    assert g["block"] == "closure" and g["errs"] == "closed" and g["subjects"] == {"": AD.days_words(_mask(starts, 1))}


def _reword(v):
    """`v` with its first `says` text changed (None when it has none)."""
    if isinstance(v, dict):
        if isinstance(v.get("says"), str):
            return {**v, "says": v["says"] + " (reworded)"}
        for k, x in v.items():
            y = _reword(x)
            if y is not None:
                return {**v, k: y}
    if isinstance(v, list):
        for i, x in enumerate(v):
            y = _reword(x)
            if y is not None:
                return [*v[:i], y, *v[i + 1:]]
    return None


def test_a_words_only_change_is_wording_and_changes_nothing_you_may_do(pair):
    a, b = pair
    st = Store(b)
    akey, _ = _plain_key(st)
    fid = st.cell(akey, 1, "rows")
    fr = st.get("rows_frame", fid)
    i, row = next((i, st.get("rows_row", x)) for i, x in enumerate(fr[2]) if _reword(st.get("rows_row", x)))
    fr[2] = [*fr[2][:i], st.add("rows_row", _reword(row)), *fr[2][i + 1:]]
    st.cell(akey, 1, "rows", st.add("rows_frame", fr))
    st.save()
    r = _diff(a, b)
    g = _only(r, "wording")
    assert g["errs"] == "none" and g["block"] == "net:rows" and g["detail"][0].endswith("says")


def test_a_change_on_one_part_of_a_multi_part_water_names_that_part_only(pair):
    """MUTATION: a water with two keyed parts; only the second's answer changes."""
    a, b = pair
    st = Store(b)
    ans = st.st.answers()
    item, keys = next((w, ks) for w, ks in sorted(ans["parts"].items())
                      if len({k for k in ks if k is not None}) >= 2)
    pix = max(i for i, k in enumerate(keys) if k is not None)
    akey = keys[pix] + 1
    n = st.con.execute("SELECT COUNT(*) FROM cell WHERE akey = ?", (akey,)).fetchone()[0]
    for s in range(n):
        fr = st.get("rows_frame", st.cell(akey, s, "rows"))
        for sp in fr[0]:
            h, w, lead = fr[1][sp]
            fr[1][sp] = [st.add("rows_decided", {**st.get("rows_decided", h), "status": "closed", "daily": None}),
                         st.add("rows_decided", {**st.get("rows_decided", w), "status": "closed", "daily": None}),
                         lead]
        st.cell(akey, s, "rows", st.add("rows_frame", fr))
    st.save()
    r = _diff(a, b)
    assert r["groups"], "the part's change was not found"
    for g in r["groups"]:
        assert g["example"]["water_id"] == item and g["example"]["part_ix"] == pix
        assert [w["id"] for w in g["waters"]] == [item]
    other = [k for i, k in enumerate(keys) if k is not None and i != pix]
    secs = province_store(b).section_akeys()
    assert all(g["sections"] <= sum(1 for k in secs.values() if k == akey) for g in r["groups"])
    assert not any(k + 1 == akey for k in other)


def test_a_new_segment_start_inside_a_segment_is_found_on_its_days(pair):
    """MUTATION: B cuts a segment in two (a start A does not have) and answers its second half otherwise."""
    a, b = pair
    st = Store(b)
    akey, starts = _plain_key(st)
    cut = starts[1] + 7
    (seg,) = st.con.execute("SELECT seg FROM akey WHERE akey = ?", (akey,)).fetchone()
    new = sorted([*starts, cut])
    (sid,) = st.con.execute("SELECT MAX(seg) + 1 FROM seglist").fetchone()
    st.con.execute("INSERT INTO seglist (seg, starts) VALUES (?, ?)", (sid, json.dumps(new)))
    rows = st.con.execute("SELECT s, ladder, rows, gear, licence, display FROM cell WHERE akey = ? ORDER BY s",
                          (akey,)).fetchall()
    st.con.execute("DELETE FROM cell WHERE akey = ?", (akey,))
    out = [rows[0], rows[1], rows[1], rows[2]]
    for s, r_ in enumerate(out):
        st.con.execute("INSERT INTO cell (akey, s, ladder, rows, gear, licence, display) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (akey, s, *r_[1:]))
    st.con.execute("UPDATE akey SET seg = ?, nseg = 4 WHERE akey = ?", (sid, akey))
    st.save()
    assert _diff(a, b)["groups"] == []                     # same answers on every day: nothing, however cut
    fid, fr, sp = _keep_fish(Store(b), akey, 2)
    st = Store(b)
    h, w, lead = fr[1][sp]
    nd = st.add("rows_decided", {**st.get("rows_decided", h), "status": "closed", "daily": None})
    fr[1][sp] = [nd, nd, lead]
    st.cell(akey, 2, "rows", st.add("rows_frame", fr))
    st.save()
    g = _only(_diff(a, b), "closes")
    assert g["subject_days"][AD.fish_words(sp)] == [[cut, starts[2] - 1]]


def test_a_section_losing_its_answer_is_a_removed_part(pair):
    a, b = pair
    st = Store(b)
    (blk, buf) = st.con.execute("SELECT block, b FROM section_akey ORDER BY block").fetchone()
    secs = st.st.section_akeys()
    h = min(secs)
    i = h & 4095
    buf = bytearray(buf)
    buf[2 * i:2 * i + 2] = b"\x00\x00"
    st.con.execute("UPDATE section_akey SET b = ? WHERE block = ?", (bytes(buf), blk))
    st.save()
    r = _diff(a, b)
    g = _only(r, "removed_part")
    assert g["sections"] == 1


# ------------------------------------------------------------------------------------------------------- salmon

def _salmon_plant(rel: Path, status: str, lo: int, hi: int, sids=(10, 11)):
    """Coho on day indices lo..hi gets a new verdict on a copy of the default chain, on sections `sids`."""
    h, p, secs = _salmon(rel)
    ch = json.loads(json.dumps(p["chains"][p.get("default", 0)]))
    v = {"s": status, "t": f"Test {status} for coho"}
    p["verdicts"].append(v)
    vi = len(p["verdicts"]) - 1
    k = AD.SALMON_SPECIES.index("CO")
    base = ch["sp"][k][1]
    ch["sp"][k] = [0, base, lo, vi, hi + 1, base]
    p["chains"].append(ch)
    c = len(p["chains"]) - 1
    _salmon_save(rel, h, p, sorted([*[x for x in secs if x[0] not in sids], *[(s, c) for s in sids]]))


def test_a_salmon_only_opening_is_found_with_its_days_and_nothing_else(pair):
    a, b = pair
    _salmon_plant(b, "open", 99, 129)
    r = _diff(a, b)
    g = _only(r, "opens")
    assert len(r["groups"]) == 1 and g["domain"] == "salmon" and g["errs"] == "open" and g["sections"] == 2
    assert g["subject_days"] == {"coho": [[100, 130]]}
    head = [l for l in AD.owner_page(r).splitlines() if l.startswith("- **")][0]
    assert head.startswith(f"- **Salmon: coho now open, {AD.days_words(AD.mask_of(100, 130))}.** Errs toward open.")


def test_a_salmon_closing_is_closes_toward_closed(pair):
    a, b = pair
    _salmon_plant(a, "open", 9, 19)                         # A open, B as built (closed): closes
    g = _only(_diff(a, b), "closes")
    assert g["domain"] == "salmon" and g["errs"] == "closed" and g["subject_days"] == {"coho": [[10, 20]]}


# ------------------------------------------------------------------------------------------------------- words

def test_days_words_join_the_new_year_and_say_all_year():
    assert AD.days_words(AD.ALL_DAYS) == "all year"
    assert AD.days_words(AD.mask_of(1, 91) | AD.mask_of(306, 366)) == "Nov 1 to Mar 31"
    assert AD.days_words(AD.mask_of(1, 167)) == "Jan 1 to Jun 15"          # leap calendar: Feb 29 is 60
    assert AD.days_words(AD.mask_of(32, 32) | AD.mask_of(245, 250)) == "Feb 1, Sep 1 to Sep 6"


def test_across_atlases_the_handle_tables_match_every_section_by_its_id():
    """(2026-11-17) Two atlases number their sections differently. Without the handle tables an unnamed lake is in no
    water and no line, so it was listed as removed and new (~700,000 sections on the first real cross-atlas pair);
    with them every section is matched by its id, a section new to B falls back to the A section holding its midpoint,
    and only a really new lake is new."""
    from types import SimpleNamespace as NS
    from pipeline.tools.audit import answers_diff as D
    FF = 0xFFFFFFFF
    # A: 1 lake:7 (unnamed), 2 100:0, 3 lake:9 (Babine, one part)
    A = NS(handles="a", ids=["lake:7", "100:0", "lake:9"], akey={1: 1, 2: 1, 3: 1}, chain_of={},
           water={3: ("wbk:9", 0)}, lines=lambda: {2: (100, 0, FF)})
    # B: a new section inserted before them shifts every handle; 100 is cut at 50; lake:9 split into lake:-1, lake:-2
    B = NS(handles="b", ids=["lake:-1", "lake:-2", "lake:7", "100:0", "100:50"],
           akey={i: 1 for i in range(1, 6)}, chain_of={}, water={1: ("wbk:-1", 0), 2: ("wbk:-2", 0)},
           lines=lambda: {4: (100, 0, 50), 5: (100, 50, FF)})
    for x in (A, B):
        x.universe = (lambda r: lambda: set(r.akey))(x)
    pairs, how = D.match_sections(A, B)
    assert how.startswith("matched by section id")
    assert sorted(pairs, key=str) == sorted([(1, 3), (2, 4), (2, 5), (None, 1), (None, 2), (3, None)], key=str)
