"""Every checked-in entry file must round-trip through the Entry model.

`save_entry` reads the WHOLE region file through the model before writing it back, so a single
malformed row anywhere in a region breaks saving for EVERY entry in that region — surfacing as a
bare 500 from /api/entries/{id}/confirm, naming an entry that is itself fine.

That is exactly what happened: a backfill wrote `audit_log` rows as
`{"at":…, "by":…, "what":…}` dicts into a field typed `List[str]`, by editing the JSON directly
instead of going through the model. 47 entries, and it took out two whole regions.
"""

import json
from pathlib import Path

import pytest

from pipeline.parsing.entry_models import Entry

ENTRIES = sorted((Path(__file__).resolve().parents[1] / "parsing" / "entries").glob("region-*.json"))


@pytest.mark.parametrize("path", ENTRIES, ids=lambda p: p.name)
def test_every_entry_validates(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    bad = []
    for e in data.get("entries", []):
        try:
            Entry(**e)
        except Exception as ex:                       # noqa: BLE001 — collect, don't stop at the first
            bad.append(f"{e.get('entry_id')}: {ex}")
    assert not bad, f"{len(bad)} invalid entr(ies) in {path.name}:\n" + "\n".join(bad[:5])


@pytest.mark.parametrize("path", ENTRIES, ids=lambda p: p.name)
def test_audit_log_rows_are_strings(path):
    """Called out separately because the generic failure above is noisy, and this is the shape that
    actually broke: any non-str row here disables saving for the whole region."""
    data = json.loads(path.read_text(encoding="utf-8"))
    bad = [(e.get("entry_id"), row) for e in data.get("entries", [])
           for row in (e.get("audit_log") or []) if not isinstance(row, str)]
    assert not bad, f"non-string audit_log row(s) in {path.name}: {bad[:3]}"
