"""THE ONE ENTRY WALKER — which extents and which item references a catalogue entry holds (T112).

A catalogue entry (a dict as stored in `region-*.json`) keeps extents in FIVE places:

    extents[]                                   the entry's own reach          role SCOPE
    rules[].extents[]                           a rule's reach                 role RULE
    rules[].tributary_excludes[]                a rule's carve-out             role RULE_EXCLUDE
    licensing[].extents[]                       a record's place               role LICENSING
    licensing[].tributary_excludes[]            a designation's carve-out      role LICENSING_EXCLUDE

Before this module every checker and remapper listed those places itself, and each list was
different: `validate_catalogue.canonicalise_splits` skipped the rules' carve-outs (62 curator-written
split references never validated), `remap_boundaries` skipped every licensing extent (about 30
licensing split references went dead after a registry rebuild with no warning), the review app's
split check skipped both carve-out lists, and only `_referenced_item_ids` / `warnings_of` walked all
five. Now every one of them walks THIS — a sixth place added to the model is added here once.

`iter_item_refs` is the same decision one level down: every registry item id the entry names
(`matched`, and each extent's `item_id`, `item_ids`, `outside_items`).

A PATH is the entry's own JSON keys (`("rules", 2, "tributary_excludes", 0)`), so the review app
can address a refusal to the field, and `describe(path)` is the one way a message names the place.

Reading these lists by key anywhere else in a checker or remapper is refused by
`pipeline/tests/test_entry_walker.py`. (The reach builder, the bundle and the models READ a rule's
`tributary_excludes` for what it MEANS — that is not a walk over the entry and stays where it is.)
"""

from __future__ import annotations

from typing import Iterator

SCOPE = "scope"
RULE = "rule"
RULE_EXCLUDE = "rule_exclude"
LICENSING = "licensing"
LICENSING_EXCLUDE = "licensing_exclude"

#: Every role, in walk order.
ROLES = (SCOPE, RULE, RULE_EXCLUDE, LICENSING, LICENSING_EXCLUDE)

#: Where each role's list lives: (record group, key). The entry's own reach has no group.
_WHERE = {
    SCOPE: (None, "extents"),
    RULE: ("rules", "extents"),
    RULE_EXCLUDE: ("rules", "tributary_excludes"),
    LICENSING: ("licensing", "extents"),
    LICENSING_EXCLUDE: ("licensing", "tributary_excludes"),
}

#: The keys of an extent that name a registry item.
EXTENT_ITEM_KEYS = ("item_id", "item_ids", "outside_items")


def iter_extent_lists(entry: dict) -> Iterator[tuple[tuple, list, str]]:
    """`(path, extents list, role)` for every extent LIST the entry holds — one per record per key,
    in file order (scope; then each rule's extents and carve-outs; then each licensing record's).
    A record that is not a dict, or a list that is absent, is skipped; an empty list is yielded."""
    if not isinstance(entry, dict):
        return
    yield ("extents",), _as_list(entry.get("extents")), SCOPE
    for group in ("rules", "licensing"):
        roles = [r for r in ROLES if _WHERE[r][0] == group]
        for i, rec in enumerate(_as_list(entry.get(group))):
            if not isinstance(rec, dict):
                continue
            for role in roles:
                key = _WHERE[role][1]
                yield (group, i, key), _as_list(rec.get(key)), role


def iter_extents(entry: dict) -> Iterator[tuple[tuple, dict, str]]:
    """`(path, extent, role)` for EVERY extent dict in the entry, in file order. `path` ends at the
    extent (`("licensing", 0, "extents", 1)`); a non-dict extent is skipped (the model refuses it)."""
    for base, exts, role in iter_extent_lists(entry):
        for j, ex in enumerate(exts):
            if isinstance(ex, dict):
                yield base + (j,), ex, role


def iter_item_refs(entry: dict) -> Iterator[tuple[tuple, str]]:
    """`(path, item_id)` for every registry item id the entry names: each of `matched`, then each
    extent's `item_id`, `item_ids[]` and `outside_items[]`. `path` ends at the id
    (`("rules", 0, "extents", 0, "item_ids", 1)`)."""
    if not isinstance(entry, dict):
        return
    for k, iid in enumerate(_as_list(entry.get("matched"))):
        if isinstance(iid, str) and iid:
            yield ("matched", k), iid
    for path, ex, _role in iter_extents(entry):
        if isinstance(ex.get("item_id"), str) and ex["item_id"]:
            yield path + ("item_id",), ex["item_id"]
        for key in EXTENT_ITEM_KEYS[1:]:
            for k, iid in enumerate(_as_list(ex.get(key))):
                if isinstance(iid, str) and iid:
                    yield path + (key, k), iid


def ref_key(path) -> str:
    """Which key an `iter_item_refs` path names its id under: `matched`, `item_id`, `item_ids` or
    `outside_items`. A check that is about what a record BINDS reads the first three; an
    `outside_items` id is SUBTRACTED (a lake cut into parts may be named there: taking out
    "Kootenay Lake" takes out its parts and its ghost, `reach.extent.whole_water_sections`)."""
    return path[0] if path[0] == "matched" else (path[-1] if path[-1] == "item_id" else path[-2])


def scoped_items(extent: dict) -> list[str]:
    """The items an extent is SCOPED to — its `item_id`, else its `item_ids` (never `outside_items`,
    which it subtracts). Empty = the entry's own water."""
    if extent.get("item_id"):
        return [extent["item_id"]]
    return [i for i in _as_list(extent.get("item_ids")) if isinstance(i, str) and i]


def describe(path) -> str:
    """The place `path` names, in words a message uses: "entry scope", "rule 3", "rule 3
    tributary_excludes", "licensing 2", "licensing 2 tributary_excludes", "matched"."""
    p = list(path)
    if not p:
        return "entry"
    if p[0] in ("extents", "matched"):
        return "entry scope" if p[0] == "extents" else "matched"
    if p[0] in ("rules", "licensing") and len(p) >= 2 and isinstance(p[1], int):
        what = "rule" if p[0] == "rules" else "licensing"
        out = f"{what} {p[1] + 1}"
        if len(p) >= 3 and p[2] == "tributary_excludes":
            out += " tributary_excludes"
        return out
    return ".".join(str(x) for x in p)


def _as_list(v) -> list:
    return v if isinstance(v, list) else []
