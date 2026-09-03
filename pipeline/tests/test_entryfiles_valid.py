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
from pipeline.curated import CURATED, SOURCE

ENTRIES = sorted(CURATED.regulations.entries.synopsis.glob("region-*.json"))


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


@pytest.mark.parametrize("path", ENTRIES, ids=lambda p: p.name)
def test_entry_id_agrees_with_identity(path):
    """`entry_id` is `r{region}:{slug(name)}@{mus}`, so the id and the identity are two spellings of
    one fact. They must not drift apart.

    They did. `identity.mus` was being filled from the matched registry ITEM's MU union instead of
    the synopsis ROW's MU heading, so 287 entries claimed MUs their row never named — the West Road
    ("Blackwater") River's region-5 row, printed under 5-13, read `mus: [5-12, 5-13, 6-1, 7-8,
    7-10]` while its id said `@5-13`. Anything joining entries to the book on (region, mus) then
    matches the wrong rows, which is how the pre-reparse locked set was compared.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    bad = []
    for e in data.get("entries", []):
        eid = e.get("entry_id") or ""
        ident = e.get("identity") or {}
        if "@" in eid and eid.rsplit("@", 1)[1] != "+".join(sorted(ident.get("mus") or [])):
            bad.append((eid, ident.get("mus")))
        if not eid.startswith(f"r{ident.get('region')}:"):
            bad.append((eid, f"region={ident.get('region')!r}"))
    assert not bad, f"{len(bad)} id/identity disagreement(s) in {path.name}: {bad[:5]}"


@pytest.mark.parametrize("path", ENTRIES, ids=lambda p: p.name)
def test_every_rule_is_written_in_the_corpus_standard(path):
    """One regulation must read the same way everywhere — `pipeline/parsing/prompts/RULE_STANDARDS.md`.

    Before the standard existed the corpus held 80 distinct quota grammars (`daily quota = 2` /
    `daily quota 2` / `quota = 2` / `quota 2`), three spellings of one motor limit, and 6 statements
    (128 rules) filed under two different `restriction_type`s. None of that variance carried meaning
    and all of it broke grouping, search, and `split_bundled_gear`'s pattern matching.

    `normalize_details` is the executable half of the standard, so "off-standard" is defined as
    "that module would rewrite it".
    """
    from pipeline.parsing.normalize_details import normalize_details, normalize_type

    data = json.loads(path.read_text(encoding="utf-8"))
    bad = []
    for e in data.get("entries", []):
        for r in e.get("rules") or []:
            d = (r.get("details") or "").strip()
            want, _ = normalize_details(d)
            if want != d:
                bad.append(f"{e.get('entry_id')} [{r.get('rule_id')}] {d!r} -> {want!r}")
            wt, why = normalize_type(want, r.get("restriction_type") or "")
            if wt:
                bad.append(f"{e.get('entry_id')} [{r.get('rule_id')}] {d!r} "
                           f"is {r.get('restriction_type')}, should be {wt} ({why})")
    assert not bad, f"{len(bad)} off-standard rule(s) in {path.name}:\n" + "\n".join(bad[:5])
