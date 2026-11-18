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
                # the store's meta as edited (`layout`, `export_subset_sha256`), its digest renewed
                sm.update({k: v for k, v in con.execute("SELECT k, v FROM meta") if k != "store_digest"})
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


# ------------------------------------------------------------------------------------------------------- T809 leftovers
# (2026-11-17) rows 34 (the trout cutover), possession and gear-count directions, shared limits and the card's rows as
# kinds of their own, conduct rule refs resolved. Each planted change has a mutation beside it that must show.

def _set_rule_fields(st: Store, rix: int, fields: dict) -> None:
    """Rule `rix`'s structured fields (the export subset) replaced, the export digest renewed."""
    from pipeline.deliver.store.common import canonical, sha
    (b,) = st.con.execute("SELECT j FROM rule WHERE rix = ?", (rix,)).fetchone()
    r = json.loads(unpack(b, st.zd.get("rule")))
    st.con.execute("UPDATE rule SET j = ? WHERE rix = ?", (json.dumps({**r, "fields": fields}).encode(), rix))
    st.con.execute("UPDATE meta SET v = ? WHERE k = 'export_subset_sha256'",
                   (sha(canonical(st.st.export_subset(check=False))),))


def _both(pair, edit) -> None:
    for rel in pair:
        st = Store(rel)
        edit(st)
        st.save()


def _heads(r: dict) -> list:
    return [line for line in AD.owner_page(r).splitlines() if line.startswith("- **")]


def _cells(st: Store):
    return st.con.execute("SELECT akey, s, rows FROM cell ORDER BY akey, s").fetchall()


def _plant_row(rel: Path, pred, edit):
    """One cell's rows frame with ONE row (the first `pred` holds for, in cell order) replaced by `edit` of a copy.
    Returns (akey, s, row)."""
    st = Store(rel)
    for akey, s, fid in _cells(st):
        fr = st.get("rows_frame", fid)
        for i, x in enumerate(fr[2]):
            row = st.get("rows_row", x)
            if pred(row):
                fr[2] = [*fr[2][:i], st.add("rows_row", edit(dict(row))), *fr[2][i + 1:]]
                st.cell(akey, s, "rows", st.add("rows_frame", fr))
                st.save()
                return akey, s, row
    raise AssertionError("no row of the mini release holds")


def _trout_pool(row) -> bool:
    return AD.is_trout_row(row) and row["kind"] == "keep" and row.get("group") is not None


# --- possession: a direction, from the numbers of the rules it names

POS_RULE = 0       # a rule of the mini release given possession numbers in both releases (`_set_rule_fields`)


def _possession_pair(pair, fields) -> None:
    _both(pair, lambda st: _set_rule_fields(st, POS_RULE, {"period": "possession", **fields}))


@pytest.mark.parametrize("fields,kind,errs", [
    ({"take": 3}, "possession", "closed"),                                   # 3 under the region's 2 x 5 = 10
    ({"take": 30}, "source_only", "none"),                                   # above 10: the same 10, another rule
    ({"take": 1, "lengths": [{"min_cm": 50}]}, "possession", "closed"),      # 1 over 50 cm, 10 under: stricter
])
def test_a_possession_cap_added_reads_its_direction(pair, fields, kind, errs):
    a, b = pair
    _possession_pair(pair, fields)
    _, _, sp = _plant_decided(b, lambda d: {**d, "roles": [*d["roles"], [POS_RULE, "possession_cap", None]]})
    r = _diff(a, b)
    g = _only(r, kind)
    assert len(r["groups"]) == 1 and g["errs"] == errs
    back = _only(_diff(b, a), kind)                                          # MUTATION: the other way round
    assert back["errs"] == {"closed": "open", "open": "closed"}.get(errs, errs)


def test_a_possession_cap_swapped_for_a_looser_one_in_some_lengths_only_is_unclear(pair):
    """MUTATION of the direction: 10 everywhere -> no number under 50 cm and 1 over: stricter AND looser."""
    a, b = pair
    _possession_pair(pair, {"take": 1, "lengths": [{"min_cm": 50}]})
    _plant_decided(b, lambda d: {**d, "roles": [[POS_RULE, "possession_cap", None] if ro[1] == "possession" else ro
                                                for ro in d["roles"]]})
    g = _only(_diff(a, b), "possession")
    assert g["errs"] == "unclear"
    assert g["before"][0].endswith("(10 in possession)")
    assert g["after"][0].endswith("(in possession: no limit at 0-49 cm, 1 at 50-300 cm)")


def test_a_possession_rule_with_no_number_coming_is_unclear_not_the_same(pair):
    a, b = pair
    _possession_pair(pair, {})                                               # no take, no per_daily
    _plant_decided(b, lambda d: {**d, "roles": [*d["roles"], [POS_RULE, "possession", None]]})
    assert _only(_diff(a, b), "possession")["errs"] == "unclear"


def test_a_daily_change_moves_the_possession_number_but_only_the_limit_is_listed(pair):
    """5 a day -> 4 with 2 days' possession (10 -> 8): the fish's limit says it. MUTATION: without the skip in
    `_diff_frames` a possession group (10 -> 8) is listed too."""
    a, b = pair
    _plant_decided(b, lambda d: {**d, "daily": d["daily"] - 1})
    r = _diff(a, b)
    assert [g["kind"] for g in r["groups"]] == ["limit_lower"]


def test_a_fish_no_longer_asked_about_lists_no_possession_change(pair):
    """MUTATION of the skip: the fish leaves the card (`spp` and `fish`); its possession going is its own change."""
    a, b = pair
    st = Store(b)
    akey, _ = _plain_key(st)
    fid, fr, sp = _keep_fish(st, akey, 1)
    fr[0] = [x for x in fr[0] if x != sp]
    fr[1] = {k: v for k, v in fr[1].items() if k != sp}
    st.cell(akey, 1, "rows", st.add("rows_frame", fr))
    st.save()
    kinds = {g["kind"] for g in _diff(a, b)["groups"]}
    assert "other" in kinds and "possession" not in kinds


def test_the_possession_sentence_is_whole(pair):
    a, b = pair
    _possession_pair(pair, {"take": 3})
    _, starts, sp = _plant_decided(b, lambda d: {**d, "roles": [*d["roles"], [POS_RULE, "possession_cap", None]]})
    (head,) = _heads(_diff(a, b))
    assert head.startswith(f"- **Possession: {AD.fish_words(sp)} possession rule changed, "
                           f"{AD.days_words(_mask(starts, 1))}.** Errs toward closed. On ")


# --- gear counts: a direction, from the deciding rule's number, else from the words

def _points_rule(st: Store, entry: dict) -> None:
    rix = 18                          # z1 single barbless hook: gear [barb, points_per_hook max 1] (the gear frames' by)
    f = st.st.export_subset(check=False)["rules"][st.st.export_subset(check=False)["rule_ids"][rix]]["fields"]
    _set_rule_fields(st, rix, {**f, "gear": [f["gear"][0], {**entry, "slot": "points_per_hook"}]})


@pytest.mark.parametrize("a_entry,b_entry,errs", [
    ({"max": 1.0}, {"max": 2.0}, "open"),
    ({"max": 2.0}, {"max": 1.0}, "closed"),
    ({"min": 30.0}, {"min": 40.0}, "closed"),          # a smallest hook gap raised: stricter
    ({"min": 40.0}, {"min": 30.0}, "open"),
    ({"max": 1.0}, {"min": 1.0}, "unclear"),
])
def test_a_counted_gear_number_reads_its_direction(pair, a_entry, b_entry, errs):
    a, b = pair
    for rel, e in ((a, a_entry), (b, b_entry)):
        st = Store(rel)
        _points_rule(st, e)
        st.save()
    r = _diff(a, b)
    g = _only(r, "gear")
    assert g["errs"] == errs and list(g["subjects"]) == ["count:points_per_hook"] and len(r["groups"]) == 1


@pytest.mark.parametrize("a_says,b_says,errs", [
    ("1 line (rod)", "1 line (rod); 2 while alone in a boat on a lake", "open"),
    ("1 line (rod); no limit from a boat", "1 line (rod); 2 while alone in a boat", "closed"),
    ("2 lines (rods)", "1 line (rod)", "closed"),
    ("1 line (rod) from shore; no limit from a boat", "2 lines (rods); 1 while alone in a boat", "unclear"),
    ("1 line (rod); 2 while alone in a boat", "1 line (rod); 2 while alone in a boat on a lake", "unclear"),
])
def test_counted_gear_words_read_their_direction_when_the_number_is_the_same(pair, a_says, b_says, errs):
    a, b = pair
    for rel, says in ((a, a_says), (b, b_says)):
        st = Store(rel)
        akey, _ = _plain_key(st)
        g = st.get("gear_frame", st.cell(akey, 1, "gear"))
        c = sorted(g["counts"])[0]
        g["counts"][c] = {**g["counts"][c], "says": says}
        st.cell(akey, 1, "gear", st.add("gear_frame", g))
        st.save()
    assert _only(_diff(a, b), "gear")["errs"] == errs


def test_the_same_count_from_another_rule_is_source_only(pair):
    a, b = pair
    st = Store(b)
    akey, _ = _plain_key(st)
    g = st.get("gear_frame", st.cell(akey, 1, "gear"))
    c = sorted(g["counts"])[0]
    by = g["counts"][c]["by"]
    assert by[0] in (18, 27) and not g["counts"][c].get("says")
    # z1 single barbless hook's entry 1 and zp barbless streams r3's entry 0: both points_per_hook max 1
    g["counts"][c] = {**g["counts"][c], "by": [27, 0] if by[0] == 18 else [18, 1]}
    st.cell(akey, 1, "gear", st.add("gear_frame", g))
    st.save()
    assert _only(_diff(a, b), "source_only")["block"] == "gear"


# --- shared limits and the card's rows: kinds of their own

@pytest.mark.parametrize("edit,errs", [
    (lambda r: {**r, "daily": r["daily"] - 1}, "closed"),
    (lambda r: {**r, "daily": r["daily"] + 1}, "open"),
    (lambda r: {**r, "daily": r["daily"] - 1, "group": {**r["group"], "water_max": 99}}, "unclear"),
])
def test_a_pooled_rows_number_is_a_shared_limit_in_its_direction(pair, edit, errs):
    a, b = pair
    _, _, row = _plant_row(b, _trout_pool, edit)
    r = _diff(a, b)
    g = _only(r, "shared_limit")
    assert g["errs"] == errs and len(r["groups"]) == 1 and g["block"] == "shared"
    from pipeline.regs.parsing.catalogue_parts.labels import species_words
    assert list(g["subjects"]) == [species_words(sorted(row["all_members"]))]


def test_a_shared_limit_lists_only_the_limits_that_moved(pair):
    a, b = pair
    _, _, row = _plant_row(b, _trout_pool, lambda r: {**r, "daily": r["daily"] - 1})
    from pipeline.regs.parsing.catalogue_parts.labels import species_words
    g = _only(_diff(a, b), "shared_limit")
    fish = species_words(sorted(set(row["all_members"])))
    assert g["before"] == [f"{row['daily']} a day of {fish} together"]
    assert g["after"] == [f"{row['daily'] - 1} a day of {fish} together"]


def test_a_one_kind_rows_number_is_no_shared_limit(pair):
    """MUTATION of the several-fish rule: a one-fish row's number is that fish's own (its daily answer says it)."""
    a, b = pair
    _plant_row(b, lambda r: r["kind"] == "keep" and len(set(r["all_members"])) == 1,
               lambda r: {**r, "daily": r["daily"] - 1})
    kinds = {g["kind"] for g in _diff(a, b)["groups"]}
    assert kinds and "shared_limit" not in kinds


def test_a_gear_ban_on_an_element_no_rule_spoke_of_is_stricter(pair):
    a, b = pair
    st = Store(a)                                            # A: the element unspoken; B (as built) bans it
    akey, _ = _plain_key(st)
    g = st.get("gear_frame", st.cell(akey, 1, "gear"))
    e = next(k for k, v in sorted(g["elements"].items()) if v["verdict"] == "ban")
    g["elements"] = {k: v for k, v in g["elements"].items() if k != e}
    st.cell(akey, 1, "gear", st.add("gear_frame", g))
    st.save()
    assert _only(_diff(a, b), "gear")["errs"] == "closed"
    assert _only(_diff(b, a), "gear")["errs"] == "open"


def test_a_shared_count_added_is_a_stricter_shared_limit(pair):
    a, b = pair

    def edit(r):
        two = sorted(r["all_members"])[:2]
        rd = r.get("real_daily") or {}
        return {**r, "real_daily": {**rd, "shared_count": [*(rd.get("shared_count") or []),
                                                            {"take": 1, "members": two}]}}
    _plant_row(b, lambda r: _trout_pool(r) and len(r["all_members"]) >= 2, edit)
    g = _only(_diff(a, b), "shared_limit")
    assert g["errs"] == "closed" and any("together" in w for w in g["after"])


def test_a_shared_limit_that_moves_with_a_member_fishs_own_answer_is_not_listed_twice(pair):
    """MUTATION of the skip: the row's number AND a member fish's daily drop: only the fish's limit is listed."""
    a, b = pair
    akey, s, row = _plant_row(b, _trout_pool, lambda r: {**r, "daily": r["daily"] - 1})
    st = Store(b)
    fr = st.get("rows_frame", st.cell(akey, s, "rows"))
    sp = next(x for x in row["members"] if x in fr[1] and fr[1][x][0] is not None
              and st.get("rows_decided", fr[1][x][0])["status"] == "keep")
    h, w, lead = fr[1][sp]
    d = st.get("rows_decided", h)
    nd = st.add("rows_decided", {**d, "daily": d["daily"] - 1})
    fr[1][sp] = [nd, nd if w == h else w, lead]
    st.cell(akey, s, "rows", st.add("rows_frame", fr))
    st.save()
    kinds = {g["kind"] for g in _diff(a, b)["groups"]}
    assert "shared_limit" not in kinds and "limit_lower" in kinds


def test_rows_reordered_on_the_card_are_card_rows_and_change_nothing_you_may_do(pair):
    a, b = pair
    st = Store(b)
    akey, s, fid = next(c for c in _cells(st) if len(st.get("rows_frame", c[2])[2]) >= 2)
    fr = st.get("rows_frame", fid)
    fr[2] = [fr[2][1], fr[2][0], *fr[2][2:]]
    fr[4] = [fr[4][1], fr[4][0], *fr[4][2:]]
    st.cell(akey, s, "rows", st.add("rows_frame", fr))
    st.save()
    r = _diff(a, b)
    g = _only(r, "card_rows")
    assert len(r["groups"]) == 1 and g["errs"] == "none" and g["block"] == "net:rows"
    assert g["detail"][0].startswith("rows reordered")
    assert r["counts"]["sections_answer_changed"] == 0
    (head,) = _heads(r)
    assert head.startswith("- **Fish card: the card shows it otherwise, ") and "No change to what you may do." in head


def test_a_difference_inside_a_decided_answer_no_facet_reads_stays_other(pair):
    """MUTATION of `card_only`: a season-limit line added to a fish's decided answer is not the card's layout."""
    a, b = pair
    _plant_decided(b, lambda d: {**d, "lines": [*d["lines"], {"t": "annual", "r": 0}]})
    g = _only(_diff(a, b), "other")
    assert g["block"] == "net:rows" and g["errs"] == "unclear"


def test_a_row_split_says_so(pair):
    a, b = pair

    def split(st):
        akey, s, fid = next(c for c in _cells(st) if any(
            len(st.get("rows_row", x)["all_members"]) >= 2 and st.get("rows_row", x)["kind"] == "release"
            for x in st.get("rows_frame", c[2])[2]))
        fr = st.get("rows_frame", fid)
        i = next(i for i, x in enumerate(fr[2]) if len(st.get("rows_row", fr[2][i])["all_members"]) >= 2
                 and st.get("rows_row", fr[2][i])["kind"] == "release")
        row = st.get("rows_row", fr[2][i])
        m = row["all_members"]
        one = st.add("rows_row", {**row, "members": [x for x in row["members"] if x == m[0]], "all_members": m[:1]})
        rest = st.add("rows_row", {**row, "members": [x for x in row["members"] if x != m[0]], "all_members": m[1:]})
        fr[2] = [*fr[2][:i], one, rest, *fr[2][i + 1:]]
        fr[4] = [*fr[4][:i], fr[4][i], fr[4][i], *fr[4][i + 1:]]
        st.cell(akey, s, "rows", st.add("rows_frame", fr))
    st = Store(b)
    split(st)
    st.save()
    g = _only(_diff(a, b), "card_rows")
    assert g["detail"][0].startswith("rows split or joined: ")


# --- rows 34: the trout cutover (spec ticket L; contract Q-D9 and 2.11), planted on the mini release

def _rows34(rel: Path) -> None:
    """`rel` as the cutover writes it: rows 34 in the store's layout and the manifest, `page_possession` the sixth slot
    of every frame (a lead where an answer names a possession rule, else null), and `group` / `elsewhere` dropped from
    every trout row (every fish a trout or char). Nothing else moves."""
    st = Store(rel)
    for (i,) in st.con.execute("SELECT id FROM rows_row ORDER BY id").fetchall():
        r = st.get("rows_row", i)
        if AD.is_trout_row(r):
            st.put("rows_row", i, {k: v for k, v in r.items() if k not in ("group", "elsewhere")})
    for (i,) in st.con.execute("SELECT id FROM rows_frame ORDER BY id").fetchall():
        fr = st.get("rows_frame", i)
        assert len(fr) == 5
        pos = next((ro[0] for sp in sorted(fr[1]) for d in fr[1][sp][:2] if d is not None
                    for ro in st.get("rows_decided", d)["roles"] if ro[1] == "possession"), None)
        st.put("rows_frame", i, [*fr, None if pos is None else {
            "lead": {"says": "Possession: 2 days' quota.", "value": "2 days' quota", "src": pos}, "also": []}])
    (lay,) = st.con.execute("SELECT v FROM meta WHERE k = 'layout'").fetchone()
    lay = json.loads(lay)
    lay["versions"]["rows"] = type(lay["versions"]["rows"])(AD.CUTOVER_ROWS)
    st.con.execute("UPDATE meta SET v = ? WHERE k = 'layout'", (json.dumps(lay, separators=(",", ":")),))
    st.save()
    man = json.loads((rel / "manifest.json").read_text())
    man["regulations"]["versions"]["rows"] = AD.CUTOVER_ROWS
    (rel / "manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True))


def test_the_cutover_is_keyed_off_f0s_rows_version():
    from pipeline.deliver.answers.model.rows import PAGE_POSSESSION_SINCE
    assert AD.CUTOVER_ROWS == PAGE_POSSESSION_SINCE == 34 and AD.ROWS34_TROUT_DROPS == ("group", "elsewhere")


def test_across_the_cutover_a_dropped_trout_group_and_the_page_possession_are_not_changes(pair):
    a, b = pair
    st = Store(a)
    trout = [i for (i,) in st.con.execute("SELECT id FROM rows_row") if _trout_pool(st.get("rows_row", i))]
    assert trout, "the mini release has no trout row with a group: the plant proves nothing"
    _rows34(b)
    for x, y in ((a, b), (b, a)):
        r = _diff(x, y)
        assert r["groups"] == [] and r["release_wide"] == [], [(g["kind"], g["detail"]) for g in r["groups"]]
        assert r["rows_cutover"].startswith("across the trout cutover (rows ")
    assert {AD.Release(b).rows_version, AD.Release(a).rows_version} == {33, 34}


def test_across_the_cutover_mutation_without_the_drop_the_trout_groups_show(pair, monkeypatch):
    a, b = pair
    _rows34(b)
    monkeypatch.setattr(AD, "ROWS34_TROUT_DROPS", ())
    r = _diff(a, b)
    # the group's numbers read as shared limits gone, its words as the card's rows
    assert r["groups"] and {g["kind"] for g in r["groups"]} <= {"card_rows", "shared_limit"}


def test_across_the_cutover_a_non_trout_row_losing_its_kept_elsewhere_line_is_a_change(pair):
    a, b = pair
    _rows34(b)
    st = Store(b)
    non = [(i, st.get("rows_row", i)) for (i,) in st.con.execute("SELECT id FROM rows_row ORDER BY id")]
    non = [(i, r) for i, r in non if r["kind"] == "keep" and not AD.is_trout_row(r) and r.get("elsewhere")]
    assert non, "the mini release has no non-trout keep row"
    i, r = non[0]
    st.put("rows_row", i, {k: v for k, v in r.items() if k != "elsewhere"})
    st.save()
    r = _diff(a, b)
    assert r["groups"] and {g["kind"] for g in r["groups"]} == {"card_rows"}
    assert all("elsewhere" in g["detail"][0] for g in r["groups"])


def test_across_the_cutover_a_trout_fish_closing_is_still_found(pair):
    a, b = pair
    _rows34(b)
    _, _, sp = _plant_decided(b, lambda d: {**d, "status": "closed", "daily": None})
    assert _only(_diff(a, b), "closes")["errs"] == "closed"


def test_two_rows_34_releases_compare_their_page_possession(pair, tmp_path):
    """On the same side of the cutover nothing is left out: a lead's words that differ are a card change."""
    a, b = pair
    _rows34(a)
    _rows34(b)
    assert _diff(a, b)["groups"] == []
    st = Store(b)
    fid = next(i for (i,) in st.con.execute("SELECT id FROM rows_frame ORDER BY id")
               if st.get("rows_frame", i)[5] is not None)
    fr = st.get("rows_frame", fid)
    fr[5] = {**fr[5], "lead": {**fr[5]["lead"], "value": "2 days’ quota (changed)"}}
    st.put("rows_frame", fid, fr)
    st.save()
    r = _diff(a, b)
    assert r["groups"] and {g["kind"] for g in r["groups"]} == {"card_rows"} and r["rows_cutover"] is None
    assert all("page_possession" in g["detail"][0] for g in r["groups"])


def test_a_rows_34_release_with_a_five_slot_frame_stops_the_run(pair, tmp_path, capsys):
    a, b = pair
    _rows34(b)
    st = Store(b)
    fid = st.cell(*_plain_key(st)[:1], 1, "rows")
    st.cell(_plain_key(st)[0], 1, "rows", st.add("rows_frame", st.get("rows_frame", fid)[:5]))
    st.save()
    assert AD.main([str(a), str(b), "--out", str(tmp_path / "o")]) == 2
    assert "slots, not 6" in capsys.readouterr().err


# --- rule refs: every rule of a conduct line, not only the first of a pair

def test_conduct_lines_name_every_rule_by_its_id():
    v = {"conduct": {"keep": [["do_not_waste_catch", [3, 4]]]}, "counts": {"x": {"by": [3, 1]}}}
    ids = {3: "e::r3", 4: "e::r4"}
    out = AD.resolve(v, AD.RULE_KEYS["gear"], frozenset(), ids.get, None, (), AD.ALL_REF_KEYS["gear"],
                     AD.PAIR_KEYS["gear"])
    assert out["conduct"]["keep"] == [["do_not_waste_catch", ["e::r3", "e::r4"]]]
    assert out["counts"]["x"]["by"] == ["e::r3", 1]                    # a [rule, n] pair keeps its n
    # MUTATIONS: without them, the conduct lines stay indices (a rule list that differs makes every gear frame
    # differ) and a pair's entry index is read as rule 1
    bad = AD.resolve(v, AD.RULE_KEYS["gear"], frozenset(), ids.get, None)
    assert bad["conduct"]["keep"] == v["conduct"]["keep"] and bad["counts"]["x"]["by"] == ["e::r3", None]


def test_a_cap_whose_size_band_moves_is_the_same_cap():
    """A group cap is keyed by its fish (and origin) per place: `RB.0.h` -> `RB.25.h` is the rainbow's size band moving
    (its size lines say it), not a cap gone and another come. MUTATION: keyed by the kind ids, the two rows' limits
    differ."""
    def row(band):
        return {"all_members": ["CT", "RB"], "kind": "keep", "daily": 5, "group": {
            "kinds": ["CT", "RB"], "total": 5, "subcaps": [],
            "caps": [{"take": 1, "kinds": {"here": ["CT.50", f"RB.{band}.h"], "stream": [], "lake": []},
                      "says": "Only 1 over 50 cm", "if_kept_here": False}]}}
    a, b = AD.shared_limits(None, row(0)), AD.shared_limits(None, row(25))
    assert a == b and any(k.startswith("cap:CT,RB.h|") for k in a)
    assert AD._kind_fish("RB.25.h") == "RB.h" and AD._kind_fish("DV.0") == "DV"
