"""LICENCE RECORDS IN DATA FILES (owner 2026-10-28): the repo is public, so a committed file holding OpenStreetMap-derived
data says so IN the file, and is offered under the ODbL. JSON has no comments, so the record is a top-level `_licence`
key (`odbl(...)`); a file that used to be a bare list becomes `{"_licence": ..., "<key>": [...]}` and its readers take
the list through `entries`. Every file it applies to is listed in `data/LICENSE-ODbL.md`.

A record and not a fact any build reads: no loader may let `_licence` change an output (tests pin that).
Not legal advice — the owner's decision, recorded.
"""
from __future__ import annotations

from typing import Any

ODBL_URL = "https://opendatacommons.org/licenses/odbl/1-0/"
OSM_COPYRIGHT = "https://www.openstreetmap.org/copyright"
NOTICE = "data/LICENSE-ODbL.md"


def odbl(applies_to: str) -> dict:
    """The `_licence` record for OpenStreetMap-derived data; `applies_to` names which entries / fields it covers."""
    return {"spdx": "ODbL-1.0", "attribution": "© OpenStreetMap contributors", "applies_to": applies_to,
            "url": ODBL_URL, "source": OSM_COPYRIGHT, "notice": NOTICE}


def entries(doc: Any, key: str) -> list:
    """The list a licensed file holds under `key` (`{"_licence": ..., key: [...]}`). A bare list (the shape before the
    record, and a test's fixture) passes through. A dict without `key` raises: never an empty list."""
    if isinstance(doc, list):
        return doc
    if isinstance(doc, dict) and isinstance(doc.get(key), list):
        return doc[key]
    raise ValueError(f"expected a list, or a dict with a {key!r} list (a licensed file), got "
                     f"{type(doc).__name__}{' keys ' + str(sorted(doc)[:6]) if isinstance(doc, dict) else ''}")
