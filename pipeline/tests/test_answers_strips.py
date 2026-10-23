"""THE YEAR STRIP, STORED ONCE (rows 30, the release size gate 2026-10-21). No data: CI's no-data tier.

Stamping the strip INTO each row made rows that differ only by their strip distinct rows: the live set went from 2512
rows to 3794 (every one ~5 KB of row JSON) for 254 distinct strips, and the release total crossed its 14,000,000 B gzip
gate. The wire now holds each DISTINCT strip once (`sections.rows.strips`), the rows WITHOUT their strip
(`sections.rows.rows`), and each frame says, row by row, which strip its row reads (the frame's fifth slot). The
decoded model is unchanged: every row carries `year`, held to 365 days of runs by the model.
"""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.deliver.answers import model as M
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.encode import CODECS
from pipeline.deliver.release.shards import frame_refs
from pipeline.deliver.store.build import FAMILIES, SECTION_STATIC

R = CODECS["rows"]
KEEP = [["keep", 365]]
SPLIT = [["keep", 150], ["closed", 30], ["keep", 185]]
DEC = {"status": "keep"}


def row(kind, year, **x):
    return {"kind": kind, "members": ["RB"], **x, "year": year}


def frame(rows, spp=("RB",)):
    return {"spp": list(spp), "fish": {S: {"hatchery": DEC, "wild": None, "lead": "hatchery"} for S in spp}, "rows": rows,
            "steelhead_line": None}


def roundtrip(values):
    tables, refs = R.encode(values, {}, {})
    sec = json.loads(json.dumps(tables))                     # the wire is JSON
    return sec, refs, [R.decode_frame(sec, ref, [], []) for ref in refs]


def test_a_frame_decodes_to_its_rows_each_with_its_own_strip():
    vals = [frame([row("keep", KEEP), row("closed", SPLIT)]), frame([row("keep", SPLIT)])]
    sec, refs, back = roundtrip(vals)
    assert back == vals
    # the decoded row's fields keep their order, the strip last (as the build stamps it)
    assert [list(r) for f in back for r in f["rows"]] == [["kind", "members", "year"]] * 3


def test_identical_strips_share_one_id_and_each_distinct_strip_is_held_once():
    vals = [frame([row("keep", SPLIT), row("closed", SPLIT)]), frame([row("release", SPLIT), row("keep", KEEP)])]
    sec, _refs, _ = roundtrip(vals)
    assert sec["strips"] == [SPLIT, KEEP]
    assert [f[4] for f in sec["frames"]] == [[0, 0], [0, 1]]


def test_a_row_that_differs_only_by_its_strip_is_stored_once():
    # the size fault: the same row with two strips was two rows
    vals = [frame([row("keep", KEEP, daily=2)]), frame([row("keep", SPLIT, daily=2)])]
    sec, _refs, back = roundtrip(vals)
    assert sec["rows"] == [{"kind": "keep", "members": ["RB"], "daily": 2}]
    assert [f[2] for f in sec["frames"]] == [[0], [0]]
    assert [f[4] for f in sec["frames"]] == [[0], [1]]
    assert back == vals


def test_no_stored_row_carries_a_strip():
    vals = [frame([row("keep", KEEP), row("closed", SPLIT)])]
    sec, _refs, _ = roundtrip(vals)
    assert all("year" not in r for r in sec["rows"])
    assert list(sec) == ["decided", "rows", "strips", "frames"]


def test_the_frame_layout_is_spp_fish_rows_line_strips():
    sec, _refs, _ = roundtrip([frame([row("keep", KEEP)])])
    assert sec["frames"] == [[["RB"], {"RB": [0, None, "hatchery"]}, [0], None, [0]]]      # rows 33: + the lead


def test_frames_equal_but_for_their_strips_stay_two_frames():
    vals = [frame([row("keep", KEEP)]), frame([row("keep", SPLIT)])]
    _sec, refs, back = roundtrip(vals)
    assert refs == [0, 1] and back == vals


def test_a_row_with_no_strip_is_refused():
    with pytest.raises(AnswersError) as e:
        R.encode([frame([{"kind": "keep", "members": []}])], {}, {})
    assert str(e.value) == "answers: a rows row has no year strip (year.stamp_years runs before encoding)"


def test_a_frame_whose_strip_refs_do_not_match_its_rows_is_refused():
    sec, _refs, _ = roundtrip([frame([row("keep", KEEP), row("closed", KEEP)])])
    bad = copy.deepcopy(sec)
    bad["frames"][0][4] = [0]
    with pytest.raises(AnswersError) as e:
        R.decode_frame(bad, 0, [], [])
    assert str(e.value) == "answers: rows frame 0 has 2 rows and 1 strips"
    bad["frames"][0][4] = [0, 1]
    with pytest.raises(AnswersError) as e:
        R.decode_frame(bad, 0, [], [])
    assert str(e.value) == "answers: rows frame 0 names strip 1, which is not in `strips` (1)"


def test_the_model_check_still_holds_every_decoded_strip_to_365_days():
    full = {"kind": "release", "pool": None, "win": 3, "members": ["RB"], "all_members": ["RB"], "daily": None,
            "narrow": None, "everyone": [], "groups": [], "prot": None, "wins": None, "lift_notes": [], "scope": None}
    vals = [{"spp": ["RB"], "fish": {"RB": {"hatchery": None, "wild": None, "lead": None}}, "steelhead_line": None,
             "rows": [{**full, "year": SPLIT}]}]
    sec, refs, back = roundtrip(vals)
    M.validate_section("rows", back)
    sec["strips"][0] = [["release", 364]]                    # a damaged strip, shared: every row reading it fails
    with pytest.raises(M.ShapeError):
        M.validate_section("rows", [R.decode_frame(sec, refs[0], [], [])])
    sec["strips"][0] = [["release", 300], ["release", 65]]
    with pytest.raises(M.ShapeError):
        M.validate_section("rows", [R.decode_frame(sec, refs[0], [], [])])


def test_the_store_holds_the_strips_once_as_a_static_and_the_shard_reads_the_new_frame():
    # one copy in `water` (a static is a common table), never per region shard
    assert ("rows", "strips") in SECTION_STATIC and ("rows", "strips") not in FAMILIES
    # rows 33 (T161): a fish is [h, w, lead] — the lead word is no row reference
    assert frame_refs("rows_frame", [["RB", "CT"], {"RB": [3, None, "hatchery"], "CT": [None, 4, "wild"]}, [7, 8], None,
                                     [0, 0]]) == {"rows_decided": [3, 4], "rows_row": [7, 8]}
