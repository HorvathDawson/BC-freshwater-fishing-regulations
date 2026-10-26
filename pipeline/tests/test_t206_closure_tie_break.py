"""T206 (owner 2026-10-26): a tie between closures is never broken by entry key.

Region 4's invasive-species notice (bass, pike, walleye, perch) and the quota box's "0, CLOSED" lines tie
on rank. Key order let `z4:daily_quotas` win once `z4:species_quotas` was renamed, splitting the one
folded "Largemouth bass, northern pike, …" row into five. `closure_order` breaks the tie by how many fish
a closure names, whatever the entries are called."""
from types import SimpleNamespace

from pipeline.deliver.answers.rows.evaluate import _stable_sort, closure_order


def _r(key, rank, species):
    return SimpleNamespace(key=key, rank=rank, species=species)


NOTICE = ["LMB", "SMB", "YP", "NP", "WP"]


def test_the_wider_closure_governs_whatever_the_keys():
    for box_key in ("z4:daily_quotas::species_quotas.r1", "z4:species_quotas::species_quotas.r1",
                    "z4:aaa::r1", "z4:zzz::r1"):
        notice = _r("z4:invasive_species_notice::invasive_species_notice.r1", 1, NOTICE)
        box = _r(box_key, 1, ["LMB", "SMB"])
        for order in ([notice, box], [box, notice]):
            assert _stable_sort(order, key=closure_order)[0] is notice, box_key


def test_rank_still_comes_first():
    near = _r("w::r1", 0, ["LMB"])
    zone = _r("z4:invasive_species_notice::r1", 1, NOTICE)
    assert _stable_sort([zone, near], key=closure_order)[0] is near


def test_a_rename_never_moves_the_winner():
    """Mutation: with the old rank-only key, renaming the box flips the winner."""
    def winner(box_key, key):
        notice = _r("z4:invasive_species_notice::r1", 1, NOTICE)
        box = _r(box_key, 1, ["NP"])
        return _stable_sort(sorted([notice, box], key=lambda r: r.key), key=key)[0].key
    old = lambda r: r.rank  # noqa: E731
    assert winner("z4:daily_quotas::r7", old) != winner("z4:species_quotas::r7", old)
    assert winner("z4:daily_quotas::r7", closure_order) == winner("z4:species_quotas::r7", closure_order)
