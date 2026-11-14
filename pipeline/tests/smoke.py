"""THE SMOKE SET, DERIVED BY RULE (T667 D6, owner 2026-11-11; replaces the hand list `smoke.txt`, 2026-11-14).

`pytest -m smoke` is Tier 1 (`scripts/test-smoke.sh`, per commit). Which tests it runs is decided here, at collection,
per test file, with no list kept anywhere:

  1. the tests the file marks `@pytest.mark.smoke` itself (a whole-repo GUARD that must run beside its file's quick
     test, or a quicker test than the first one) — when the file marks any, these are its whole smoke set;
  2. otherwise its FIRST test in source order that carries no `needs_*` or `slow` marker (the no-data tier);
  3. a file whose every test carries `needs_*` / `slow` has none (it cannot run without data).

So a new test file joins the smoke set with no edit anywhere else. A test marked `smoke` that also carries `needs_*` or
`slow` is refused (`refused`): the smoke tier runs without data, and the marker would hide that it cannot.
The repo-root `conftest.py` applies `pick` to the collected items; `test_smoke_coverage.py` pins the rule by mutation.
"""
from __future__ import annotations

from typing import Iterable, Protocol


class Item(Protocol):
    nodeid: str

    def iter_markers(self, name: str | None = None) -> Iterable: ...


def needs_data(item: Item) -> bool:
    """The item carries a `needs_*` or `slow` marker (its own, its class's, its module's or a parameter's)."""
    return any(m.name == "slow" or m.name.startswith("needs_") for m in item.iter_markers())


def marked_smoke(item: Item) -> bool:
    return any(True for _ in item.iter_markers("smoke"))


def refused(items: Iterable[Item]) -> list[str]:
    """Node ids marked `smoke` in source that also need data or are slow."""
    return [i.nodeid for i in items if marked_smoke(i) and needs_data(i)]


def pick(items: Iterable[Item]) -> list[Item]:
    """The smoke items, in collection order. Collection order is source order within a file."""
    by_file: dict[str, list[Item]] = {}
    for i in items:
        by_file.setdefault(i.nodeid.split("::")[0], []).append(i)
    out: list[Item] = []
    for its in by_file.values():
        own = [i for i in its if marked_smoke(i)]
        if own:
            out += own
            continue
        first = next((i for i in its if not needs_data(i)), None)
        if first is not None:
            out.append(first)
    return out
