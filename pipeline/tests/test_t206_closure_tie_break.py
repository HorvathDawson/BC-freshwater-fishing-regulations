"""T206 (owner 2026-10-26): a tie between closures is never broken by entry key.

Region 4's invasive-species notice (bass, pike, walleye, perch) and the quota box's "0, CLOSED" lines tie
on rank. Key order let `z4:daily_quotas` win once `z4:species_quotas` was renamed, splitting the one
folded "Largemouth bass, northern pike, …" row into five. `closure_order` breaks a tie between entries by
standing-before-dated, then by how many fish a closure names, whatever the entries are called; two rules
of one entry keep the book's rule order."""
from types import SimpleNamespace

from pipeline.deliver.answers.rows.evaluate import _stable_sort, closure_order


def _r(entry, rule, rank, species, when=None):
    return SimpleNamespace(key=f"{entry}::{rule}", entry_id=entry, rule_id=rule, rank=rank, species=species,
                           f={"when": when} if when else {})


NOTICE = ["LMB", "SMB", "YP", "NP", "WP"]
SPRING = {"dates": [{"from_month": 4, "from_day": 1, "to_month": 6, "to_day": 14}]}


def _win(xs):
    return _stable_sort(xs, key=closure_order)[0]


def test_the_wider_closure_governs_whatever_the_keys():
    for box in ("z4:daily_quotas", "z4:species_quotas", "z4:aaa", "z4:zzz"):
        notice = _r("z4:invasive_species_notice", "invasive_species_notice.r1", 1, NOTICE)
        line = _r(box, "species_quotas.r1", 1, ["LMB", "SMB"])
        for order in ([notice, line], [line, notice]):
            assert _win(order) is notice, box


def test_rank_still_comes_first():
    near = _r("r4:w", "w.r1", 0, ["LMB"])
    zone = _r("z4:invasive_species_notice", "invasive_species_notice.r1", 1, NOTICE)
    assert _win([zone, near]) is near


def test_a_standing_closure_before_a_dated_one_between_entries():
    """z3: the bass/perch notice (year-round) over the spring stream closure (all game fish, Apr 1-June 14)."""
    bass = _r("z3:zzz_notice", "n.r1", 1, ["LMB", "SMB", "YP"])
    spring = _r("z3:aaa_spring", "s.r1", 1, ["LMB"] * 22, when=SPRING)
    for order in ([bass, spring], [spring, bass]):
        assert _win(order) is bass


def test_one_entrys_rules_keep_the_books_order():
    """Fraser r2 (7 fish, dated) stays ahead of its own r5 (22 fish, year-round), as before."""
    r2 = _r("r3:fraser_river@3-14", "fraser_river.r2", 0, ["A"] * 7, when=SPRING)
    r5 = _r("r3:fraser_river@3-14", "fraser_river.r5", 0, ["A"] * 22)
    assert _win([r2, r5]) is r2


def test_a_rename_never_moves_the_winner():
    """Mutation: with the old rank-only key, renaming the box flips the winner."""
    def winner(box, key):
        notice = _r("z4:invasive_species_notice", "r1", 1, NOTICE)
        line = _r(box, "r7", 1, ["NP"])
        return _stable_sort(sorted([notice, line], key=lambda r: r.key), key=key)[0].key
    old = lambda r: r.rank  # noqa: E731
    assert winner("z4:daily_quotas", old) != winner("z4:species_quotas", old)
    assert winner("z4:daily_quotas", closure_order) == winner("z4:species_quotas", closure_order)
