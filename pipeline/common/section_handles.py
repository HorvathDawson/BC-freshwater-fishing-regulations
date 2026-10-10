"""The section handle table: ONE owner for the integer that names a section.

WHY THIS EXISTS. A section is identified by a string like `354087681:0` or `lake:-1174`. The
bundle referred to sections by that string in five tables and two indexes — 30.0 MB, 69% of
the bundle — and the tile carried the same string as the feature id the app sets state on.
An integer handle costs 3 bytes instead of ~12 and saves 12.5 MB (measured).

THE HANDLE IS NOT DERIVABLE FROM THE STRING. That was tried: `blk:ord` packs reversibly, but
`ord` reaches 1,933,351 and there is a second `lake:` namespace running to 708 million, so the
packed form needs 53 bits — eight bytes, barely better than the text it replaces. A dense
handle needs 21. So there has to be a MAPPING, and the moment there is a mapping the only
question that matters is who owns it.

ONE OWNER, ONE FILE, TWO READERS. The atlas build writes `section_handles.txt` — every node
id in the graph, sorted, one per line, and the handle IS the line number. The tile export and
the bundle both read that file; neither computes an order of its own. Had they each sorted
their own set the handles would agree only while the two sets did, and the failure would be
silent and worse than a crash: a valid handle pointing at a DIFFERENT SECTION, so the app
would show real regulations for the wrong piece of river.

That is what `digest` is for. It goes into the bundle's `meta` and into the tile sidecar, and
the app refuses to colour anything if the two disagree — see AGENTS rule 5 and the note in
schema.sql on `section_id` never leaving the bundle. A handle leaves it even less: it is not
durable across a rebuild, it is not meaningful without the table that made it, and it must
never appear in a URL, a saved pin, or a feed.
"""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Mapping

#: The file the atlas writes into its build directory.
FILENAME = "section_handles.txt"


def sort_key(node_id: str) -> tuple:
    """Blue line, then measure AS A NUMBER — the water's own order, mouth to source.

    NOT lexicographic, and that is the whole point. A section id is `{blue_line}:{measure}`,
    and sorted as text `...:122095` comes before `...:9942`. The app used to order a river's
    sections that way and drew its stretches interleaved while saying they were in order; the
    fix was an ORDER BY that split and cast the id in SQL. A handle cannot be split in SQL,
    so the meaning has to live in the ORDER ITSELF: assign handles in this order and `ORDER BY
    sid` is both correct and free, because it is the primary key.

    The `lake:` namespace sorts after the numeric blue lines, together, in numeric order.
    """
    head, _, tail = node_id.rpartition(":")
    try:
        line: tuple = (0, int(head), "")
    except ValueError:
        line = (1, 0, head)          # a named namespace, e.g. `lake`
    try:
        measure = int(tail)
    except ValueError:
        measure = 0                  # unparseable: stable, and grouped with its own line
    return (line, measure, node_id)


def assign(node_ids: Iterable[str]) -> list[str]:
    """The canonical order, and the ONLY place it is decided.

    Depends on the SET of ids and nothing else — not insertion order, not the iteration order
    of a dict, not which module happened to load the graph first. Two builds over the same
    atlas produce the same table; two builds over different atlases do not, and the digest is
    what says so.
    """
    return sorted(set(node_ids), key=sort_key)


def write(node_ids: Iterable[str], build_dir: Path) -> str:
    """Write the table into a build directory; returns its digest."""
    ordered = assign(node_ids)
    path = Path(build_dir) / FILENAME
    path.write_text("\n".join(ordered) + "\n", encoding="utf-8")
    return digest(path)


@lru_cache(maxsize=4)
def _load(build_dir: str) -> tuple[tuple[str, ...], Mapping[str, int]]:
    """Cached because the bundler needs the table in six places and it is ~24 MB of text.

    The returned mapping is SHARED between callers — read it, never mutate it.
    """
    ids, by_id = _read_uncached(Path(build_dir))
    return tuple(ids), by_id


def read(build_dir: Path) -> tuple[tuple[str, ...], Mapping[str, int]]:
    """`(ids_by_handle, handle_by_id)` for a build directory."""
    return _load(str(build_dir))


def _read_uncached(build_dir: Path) -> tuple[list[str], dict[str, int]]:
    """Raises rather than returning an empty mapping when the file is missing: a reader that
    silently continued would write a bundle whose handles mean nothing.
    """
    path = Path(build_dir) / FILENAME
    if not path.exists():
        raise SystemExit(
            f"no {FILENAME} in {build_dir} — this build predates the handle table, or the "
            f"atlas run did not finish. The atlas build writes it; rebuild the atlas.")
    ids = path.read_text(encoding="utf-8").split("\n")
    if ids and ids[-1] == "":
        ids.pop()
    # ONE-BASED, AND 0 IS RESERVED FOR "no section". Not a quirk — the app tests a section
    # for presence with `if (!section)` in ten places, and a zero handle is falsy, so a
    # zero-based table would have made the FIRST SECTION IN THE PROVINCE read as absent
    # everywhere. TypeScript cannot catch that: the type is right and the value is a lie.
    # Reserving 0 makes every one of those checks correct by construction rather than by
    # remembering, and it has a second virtue — the handle is the LINE NUMBER, so
    # `sed -n "${h}p" section_handles.txt` names the section a handle refers to.
    return ids, {s: i + 1 for i, s in enumerate(ids)}


def id_at(ids, handle: int) -> str:
    """The section a handle names. Handles are 1-based; 0 is "no section" and has no id."""
    if handle < 1 or handle > len(ids):
        raise KeyError(f"handle {handle} is outside the table (1..{len(ids)})")
    return ids[handle - 1]


def digest(path: Path) -> str:
    """Short content digest of the table — the vintage the tile and the bundle must share."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16]


def digest_for(build_dir: Path) -> str:
    return digest(Path(build_dir) / FILENAME)


def registry_digest_for(build_dir: Path) -> str:
    """THE REGISTRY'S CONTENT, as a digest (P2): SHA-256 (first 16 hex) over every item of the
    atlas's `registry.json` — id, name, kind, sections, aliases, `part_of`, everything — in id
    order, as canonical JSON. The handle digest pairs section NUMBERS; the tiles and the bundle
    also read the registry's names and kinds, and `pipeline.atlas.sidecars` can rewrite the
    registry without touching a handle. The reach run and the tile sidecar stamp this; the bundle
    refuses a reach run made against another registry and records its own for the vintage check."""
    import json
    items = json.loads((Path(build_dir) / "registry.json").read_text(encoding="utf-8"))["items"]
    items = sorted(items, key=lambda i: str(i.get("id")))
    return hashlib.sha256(json.dumps(items, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
