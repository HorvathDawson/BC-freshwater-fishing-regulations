"""T801 item 2 (owner 2026-11-16, "Make sure licensing info is done properly there"): the Skeena's separate-licence
line. The book prints it once, inside the line naming Skeena River 2 (p.53): "(non-residents and non-resident aliens
require separate licences describing which section they are fishing)". Each classified section's box says it as one
plain sentence: `licensing[].non_resident_note` = {says}, derived from the records' verbatims (`read.add_non_resident_notes`),
carried into the store's licensing words (`store.common.LIC_KEEP`)."""
from __future__ import annotations

import pytest

from pipeline.deliver.export import read as RD
from pipeline.deliver.store import common as SC
from pipeline.tests import under_test

SAYS = "Non-residents and non-resident aliens require separate licences describing which section they are fishing."
ENTRY = "r6:skeena_river_mainstem_only@6-10"


def _des(rid, verbatim, entry=ENTRY, classified="II"):
    return {"kind": "designation", "entry_id": entry, "record_id": rid, "verbatim": verbatim,
            "fields": {"classified": classified} if classified else {}}


def _skeena():
    return {
        f"{ENTRY}#skeena_river_2": _des("skeena_river_2", "There are two separate Class II waters on the Skeena River "
                                        "(non-residents and non-resident aliens require separate licences describing\n"
                                        "which section they are fishing): (a) from Exchamsiks River to 1.5 km upstream "
                                        "of Kitsumkalum River (known as \"Skeena River 2\"), July 1-Sept 30"),
        f"{ENTRY}#skeena_river_section_4": _des("skeena_river_section_4", "1.5 km upstream of Zymoetz River (known as "
                                                "\"Skeena River Section 4\"), July 1-Dec 31"),
        f"{ENTRY}#licence": {"kind": "requirement", "entry_id": ENTRY, "record_id": "licence", "verbatim": "",
                             "fields": {}},
        "r6:babine_river@6-8#babine_river_i": _des("babine_river_i", "Class 1 water Sept 1-Oct 31",
                                                   entry="r6:babine_river@6-8", classified="I"),
    }


#: the entry's printed row (`entries[*].printed`), markup as the corpus keeps it
PRINTED = ("**There are two separate Class II waters on the Skeena River** (non-residents and non-resident aliens "
           "require separate licences describing\nwhich section they are fishing): **(a)** from Exchamsiks River …")


def test_both_skeena_sections_say_the_line_in_the_books_words():
    """Read from the entry's printed row, not a record's quote (each designation quotes only its own part, T801 item 3).
    MUTATION: reading the designations' verbatims finds no line once Skeena River 2 is trimmed."""
    lic = _skeena()
    lic[f"{ENTRY}#skeena_river_2"]["verbatim"] = ("from Exchamsiks River to 1.5 km upstream of Kitsumkalum River "
                                                  "(known as \"Skeena River 2\"), July 1-Sept 30")
    RD.add_non_resident_notes(lic, {ENTRY: {"printed": PRINTED}, "r6:babine_river@6-8": {"printed": "Class 1 water"}})
    assert lic[f"{ENTRY}#skeena_river_2"]["non_resident_note"] == {"says": SAYS}
    assert lic[f"{ENTRY}#skeena_river_section_4"]["non_resident_note"] == {"says": SAYS}    # printed once, holds for both
    assert "non_resident_note" not in lic[f"{ENTRY}#licence"]                              # a designation's line only
    assert "non_resident_note" not in lic["r6:babine_river@6-8#babine_river_i"]            # absent where not printed


def test_the_store_keeps_it_on_the_licensing_words():
    x = {**_skeena()[f"{ENTRY}#skeena_river_2"], "non_resident_note": {"says": SAYS}, "provenance": {}}
    assert SC.lic_words(x)["non_resident_note"] == {"says": SAYS}


@pytest.mark.needs_bundle
def test_the_export_carries_it_on_both_skeena_designations(request):
    from pipeline.deliver.export import files as export_files
    doc = export_files.load(under_test.export(request))
    got = {k: x.get("non_resident_note") for k, x in doc["licensing"].items() if x.get("non_resident_note")}
    assert {k.rsplit("#", 1)[1] for k in got} == {"skeena_river_2", "skeena_river_section_4"}, sorted(got)
    assert all(v == {"says": SAYS} for v in got.values())


def test_the_skeena_river_2_waiver_quotes_its_own_part_of_the_row():
    """T801 item 3 (owner 2026-11-17 15:11, "Trim to this water's part (Recommended)"): the Skeena River 2 designation
    quotes only its own (a) part of the two-waters row, so its stamp sentence does too; a data-level trim in the
    curated designation, no special case in code (every other waiver's sentence is unchanged: 9 waivers, 1 moved)."""
    import json
    rows = json.loads((under_test.curated().regulations.synopsis / "region-6.json").read_text(encoding="utf-8"))
    e = next(x for x in rows["entries"] if x["entry_id"] == ENTRY)
    des = {x["id"]: x for x in e["licensing"] if x["kind"] == "designation"}
    q = des["skeena_river_2"]["verbatim"]
    assert q == 'from Exchamsiks River to 1.5 km upstream of Kitsumkalum River (known as "Skeena River 2"), July 1-Sept 30'
    lic = {f"{ENTRY}#skeena_river_2": {
        "kind": "designation", "entry_id": ENTRY, "verbatim": q, "provenance": {"entry_name": "Skeena River"},
        "fields": {"steelhead_stamp_waived": des["skeena_river_2"]["steelhead_stamp_waived"],
                   "unit_name": "Skeena River 2"},
        "period": {"kind": "dates"}, "parts": {"when": "Jul 1-Sep 30"}}}
    RD.add_stamp_waivers(lic, {ENTRY: "Skeena River"})
    says = lic[f"{ENTRY}#skeena_river_2"]["stamp_waiver"]["says"]
    assert f"(“{q}”)" in says and "two separate" not in says, says
