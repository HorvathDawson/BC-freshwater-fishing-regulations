"""T125 item 8, answers 12 (re-derived 2026-10-28, Q119): the fish-list wording is ONE function pair in
answers/common.py — `each_name` (each fish by its own name; ROUND15 1 / RULINGS P19: a size group never says
a group's name) and `lc_names` (a closed group by its name, else `each_name`). The intentional split is
those two, named; `tree.kind_names` (the card tree's shared "trout" noun) builds on them. `card._names`,
`groups._names` and six inline `join([lc(sp_name(..))])` copies were the second paths; gear's `_or`/`_and`
were a second `join`."""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ANSWERS = REPO / "pipeline" / "deliver" / "answers"
#: an inline fish list: join over each code's lower-cased name
INLINE = re.compile(r"join\(\[\s*(?:common\.)?lc\(\s*(?:common\.)?sp_name\(")
#: an inline "a, b and c" joiner
JOINER = re.compile(r'"\, "\.join\(\w+\[:-1\]\)\s*\+')


def scan(text: str) -> list[str]:
    return [m.group(0) for m in (*INLINE.finditer(text), *JOINER.finditer(text))]


def test_no_second_fish_list_wording():
    bad = {str(p.relative_to(REPO)): scan(p.read_text(encoding="utf-8"))
           for p in sorted(ANSWERS.rglob("*.py")) if p.name != "common.py"}
    assert {k: v for k, v in bad.items() if v} == {}


def test_the_scan_bites():
    assert scan("return common.join([common.lc(common.sp_name(c)) for c in codes], ' or ')")
    assert scan("x = join([lc(sp_name(s)) for s in ex], ' or ')")
    assert scan('return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " or " + words[-1]')
    assert not scan("return common.each_name(codes, ' or ')")


def test_the_pair_says_what_it_did():
    from pipeline.deliver.answers import common
    assert common.each_name(["RB", "CT"], " or ") == "rainbow trout or cutthroat trout"
    n = [common.lc(common.sp_name(c)) for c in ("RB", "CT", "DV")]
    assert common.each_name(["RB", "CT", "DV"]) == f"{n[0]}, {n[1]} and {n[2]}"
    assert common.lc_names(["RB"]) == common.each_name(["RB"])
