"""THE LICENCE FEES ARE PINNED TO THE PRINTED TABLE, COLUMN BY COLUMN (T628 item 1; T553 left it unverified).

Printed p.5 is a table: a label and its three prices sit in different columns, so the reading-order check of
`test_base_rules_in_print.py` can only say that a number is somewhere on the page (`synopsis_print.TABLE_PAGES`). A fee
stored under the wrong residency, or one fee changed to another fee the page also prints ($22.86 is printed 13 times),
passed. Here `synopsis_print.fee_table()` reads the table as a table (each cell by the column heading it is printed
under, no LLM) and every stored fee record of the fees entry (`zp:licence_fees`, hand-authored) must equal its printed
row: label, the price under each residency, which cells are "not available", "/day", and the row's place in the
entry's own transcription.

A difference is never fixed here: the book is read again by a person (data vs print, the row), as for `DISAGREE`.
"""
from __future__ import annotations

import copy
import json
import re

import pytest

from pipeline.common.synopsis_print import FEE_COLUMNS, FEE_PAGE, FeeRow, fee_table, fold
from pipeline.tests import under_test

FEES_ENTRY = "zp:licence_fees"
RESIDENCIES = tuple(r for _, r in FEE_COLUMNS)


def _fees_entry() -> dict:
    path = under_test.curated().regulations.synopsis / "region-provincial.json"
    return next(e for e in json.loads(path.read_text(encoding="utf-8"))["entries"] if e["entry_id"] == FEES_ENTRY)


def _words(text: str) -> str:
    """A row as compared: folded, footnote stars off the prices ("$5.71**" is the price $5.71 with a note mark)."""
    return re.sub(r"(\$\d+\.\d{2}(?:/day)?)\*+", r"\1", fold(text))


def fee_problems(entry: dict, rows: list[FeeRow]) -> list[str]:
    """Every way the entry's fee records differ from the printed table, in table order (empty = they agree)."""
    out: list[str] = []
    records = [r for r in entry.get("licensing") or [] if "fees_cad" in r]
    if entry.get("source_pages") != [FEE_PAGE]:
        out.append(f"{entry['entry_id']}: source_pages {entry.get('source_pages')}, the table is on p.{FEE_PAGE}")
    if len(records) != len(rows):
        out.append(f"{len(records)} fee records, {len(rows)} printed rows: "
                   f"{[r.get('name') for r in records]} vs {[r.label for r in rows]}")
    at = 0
    text = fold(entry.get("regs_verbatim") or "")
    for rec, row in zip(records, rows):
        rid = rec.get("id")
        if rec.get("name") != row.label:
            out.append(f"{rid}: named {rec.get('name')!r}, the row at its place is {row.label!r}")
            continue
        printed = dict(row.fees)
        for residency in RESIDENCIES:
            have, want = rec["fees_cad"].get(residency), printed.get(residency)
            if have != want:
                out.append(f"{rid}: {residency} {_money(have)}, printed {_money(want)} ({row.group}: {row.text})")
        extra = sorted(set(rec["fees_cad"]) - set(RESIDENCIES))
        if extra:
            out.append(f"{rid}: fees for {extra}, which are no column of the table")
        if (rec.get("sold") == "per_day") != bool(row.per_day) or (row.per_day and row.per_day != set(printed)):
            out.append(f"{rid}: sold {rec.get('sold')!r}, printed per day for {sorted(row.per_day) or 'no column'}")
        if _words(rec.get("verbatim") or "") != _words(row.text):
            out.append(f"{rid}: quote {rec.get('verbatim')!r}, printed {row.text!r}")
        found = text.find(fold(row.text), at)
        if found < 0:
            out.append(f"{rid}: the entry's regs_verbatim does not print the row {row.text!r} at its place in the table")
        else:
            at = found + len(fold(row.text))
    for group in dict.fromkeys(r.group for r in rows):
        if fold(group) not in text:
            out.append(f"the entry's regs_verbatim does not print the heading {group!r}")
    return out


def _money(v) -> str:
    return "not available" if v is None else f"${v:.2f}"


@pytest.fixture(scope="module")
def rows() -> list[FeeRow]:
    return fee_table()


def test_the_reader_reads_the_table_as_a_table(rows):
    """The reader's own contract on the repo book: every row has its heading, one cell under each column, and the
    cells that print no price are the "not available" ones — never dropped rows or a guessed column."""
    assert [h for h, _ in FEE_COLUMNS] == ["RESIDENT*", "NON-RESIDENT*", "NON-RESIDENT ALIEN*"]
    assert all(r.group and r.label and r.fees for r in rows)
    assert len({(r.group, r.label) for r in rows}) == len(rows), "a row is named by its heading and label"
    by = {(r.group.split(" (")[0], r.label): r for r in rows}
    annual = by["Basic Licences", "Annual Angling Licence"]
    assert [k for k, _ in annual.fees] == list(RESIDENCIES) and not annual.per_day
    assert len({v for _, v in annual.fees}) == 3, "three different prices: a swapped column cannot hide"
    disabled = by["Basic Licences", "Annual Licence for Disabled"]
    assert [k for k, _ in disabled.fees] == ["resident"]                       # ★ ★: not sold to non-residents
    class_i = by["Classified Waters Licences", "Class I Waters Licence"]
    assert [k for k, _ in class_i.fees] == ["non_resident", "non_resident_alien"]      # ★ under RESIDENT
    assert class_i.per_day == {"non_resident", "non_resident_alien"}
    # the same label under two headings is two rows
    assert by["White Sturgeon Conservation Licences", "Annual"].fees != annual.fees


def test_every_stored_fee_is_the_printed_fee_under_its_own_column(rows):
    problems = fee_problems(_fees_entry(), rows)
    assert not problems, ("the fee records differ from printed p.5 — read the book again (data vs print, the row); "
                          "never edit a value to pass:\n  " + "\n  ".join(problems))


def test_one_changed_fee_is_caught_wherever_it_is(rows):
    """Mutation, the acceptance line: EVERY fee of the transcription, changed one at a time (by a cent, and to another
    price the page prints, which the reading-order check passes), fails; so does the same change in the entry's own text."""
    entry = _fees_entry()
    assert fee_problems(entry, rows) == []
    n = 0
    for i, rec in enumerate(entry["licensing"]):
        for residency, fee in (rec.get("fees_cad") or {}).items():
            for other in (round(fee + 0.01, 2), next(v for v in (41.15, 22.86) if v != fee)):
                bad = copy.deepcopy(entry)
                bad["licensing"][i]["fees_cad"][residency] = other
                got = fee_problems(bad, rows)
                assert got and all(g.startswith(f"{rec['id']}: {residency} ") for g in got), (rec["id"], residency, got)
                n += 1
    assert n == 2 * sum(len(r.fees) for r in rows), "every printed price was changed, twice"
    for row in rows:
        price = f"${row.fees[0][1]:.2f}"
        bad = copy.deepcopy(entry)
        bad["regs_verbatim"] = _replace_in_row(entry["regs_verbatim"], row, price, "$99.99")
        assert any("regs_verbatim does not print the row" in g for g in fee_problems(bad, rows)), row.text


def _replace_in_row(text: str, row: FeeRow, old: str, new: str) -> str:
    """`text` with `old` -> `new` inside the one printed row (found by its heading first: "Annual" is two rows)."""
    start = text.index(row.group)
    at = text.index(row.text.replace("*", "") if row.text not in text else row.text, start)
    return text[:at] + text[at:].replace(old, new, 1)


def test_a_fee_under_the_wrong_column_a_lost_row_and_a_sold_cell_are_caught(rows):
    """What the reading-order check could never see: the right numbers in the wrong places."""
    entry = _fees_entry()
    recs = entry["licensing"]
    annual = next(i for i, r in enumerate(recs) if r.get("name") == "Annual Angling Licence")
    swapped = copy.deepcopy(entry)
    f = swapped["licensing"][annual]["fees_cad"]
    f["non_resident"], f["non_resident_alien"] = f["non_resident_alien"], f["non_resident"]
    assert len(fee_problems(swapped, rows)) == 2                               # both columns, nothing else

    sold = copy.deepcopy(entry)                                                # a ★ cell given a price
    disabled = next(i for i, r in enumerate(recs) if r.get("name") == "Annual Licence for Disabled")
    sold["licensing"][disabled]["fees_cad"]["non_resident"] = 1.14
    assert fee_problems(sold, rows) == [
        f"{recs[disabled]['id']}: non_resident $1.14, printed not available (Basic Licences: {rows[disabled].text})"]

    lost = copy.deepcopy(entry)
    del lost["licensing"][annual]
    assert any("fee records" in g and "printed rows" in g for g in fee_problems(lost, rows))

    per_day = copy.deepcopy(entry)
    daily = next(i for i, r in enumerate(recs) if r.get("sold") == "per_day")
    per_day["licensing"][daily]["sold"] = "per_licence_year"
    assert any("printed per day" in g for g in fee_problems(per_day, rows))


def test_the_reader_refuses_a_page_it_cannot_read_as_the_table(tmp_path):
    """No guessed table: a book with no file is named, never read as "no fees"."""
    with pytest.raises(FileNotFoundError, match="fishing_synopsis"):
        fee_table(tmp_path / "fishing_synopsis.pdf")
