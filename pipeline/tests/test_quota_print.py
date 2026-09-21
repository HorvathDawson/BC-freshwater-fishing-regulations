"""The standing quota tables against the printed synopsis, and every section as base + deltas.

Two invariants the owner asked for, so that a defect stops being "why is this number wrong?"
and becomes "which delta did this?":

  1. Every line of every region's printed "Daily Quotas" box is found in that region's base
     table, and every base line is found in the print — by content, from text extracted from
     the official PDF, never from a curated field. The disagreements that remain are pinned
     by name: a NEW one fails, and a VANISHED one fails too, because a known defect that
     silently disappears is a check that silently stopped looking.

  2. Every section's table is exactly its region's base plus named overrides: every counter
     a section shows is either a base counter or an override, and every base counter the
     section does NOT show is listed beneath a row with the override that replaced it.
     An unattributable difference is a failure.
"""
from __future__ import annotations
import os
import pytest

from pipeline.regs.table.authority import Scope
from pipeline.regs.table.build import (ledger, base, section_rules, section_regions,
                                       section_label, section_kind, name, D, WATERS)
from pipeline.regs.table.rows import rows
from pipeline.regs.table.subject import Origin

#: (region, kind, printed line) -> why. Every one is a finding on the curation list.
KNOWN_PRINT_DEFECTS = {
    # Haida Gwaii's box prints "3 Dolly Varden"; z1:hg_quota.r3 carries bull trout as well.
    ("1hg", "lake", "• 3 Dolly Varden"): "curation: the catalogue adds bull trout to a Dolly Varden line",
    ("1hg", "lake", "[table] 3 Bull trout, Dolly Varden"): "curation: the catalogue adds bull trout to a Dolly Varden line",
    ("1hg", "stream", "• 3 Dolly Varden"): "curation: the catalogue adds bull trout to a Dolly Varden line",
    ("1hg", "stream", "[table] 3 Bull trout, Dolly Varden"): "curation: the catalogue adds bull trout to a Dolly Varden line",
}


def test_the_source_is_the_edition_it_claims():
    """The repo's synopsis is edition 2025-2027, 88 pages, and the file this check was
    written against — a silently swapped PDF must fail here, not pass by luck."""
    from pipeline.regs.table.quota_print import PDF, PDF_MD5, EDITION, edition, _md5
    assert os.path.exists(PDF)
    assert edition(PDF) == EDITION
    assert _md5(os.path.dirname(PDF), os.path.basename(PDF)) == PDF_MD5


def test_every_box_in_the_repo_synopsis_is_the_box_gov_bc_ca_serves():
    """The repo copy is not byte-identical to today's download; what makes it a source is that
    every quota box in it reads, line for line, as the box in the chapter that is. The
    chapters are gov.bc.ca's own files (data/source/official/, or $SYNOPSIS_DIR); a chapter
    that is absent SKIPS this test by name — it never fails for a file nobody has fetched,
    and never passes for a comparison that did not happen."""
    from pipeline.regs.table.quota_print import cross_check, OFFICIAL_DIR
    same = cross_check(OFFICIAL_DIR)
    assert same
    wrong = {k: v for k, v in same.items() if v in ("differs",) or v.startswith("md5 changed")}
    assert not wrong, wrong
    missing = sorted({v for v in same.values() if v.startswith("missing")})
    if missing:
        pytest.skip("the gov.bc.ca chapter(s) are not on this machine — " + "; ".join(missing))
    assert all(v == "same" for v in same.values()), same


def test_every_standing_quota_table_matches_the_printed_box_line_by_line():
    from pipeline.regs.table.quota_print import all_panels, CHAPTERS
    panels = all_panels(name)
    assert len(panels) == 1 + 2 * len(CHAPTERS)
    checked = sum(len(P.checks) for P in panels)
    assert checked >= 350, f"only {checked} lines were read — an empty extraction cannot pass"
    unread = [(P.region, P.kind, c.line) for P in panels for c in P.checks if c.label == "unread"]
    assert not unread, f"printed lines nobody could read: {unread[:6]}"
    # every ✓ shows what it rests on: a matched rule carries the table's words AND the
    # catalogue sentence; a ✓ by argument (an override, a line of the other kind) says why
    bare = [(P.region, P.kind, c.line) for P in panels for c in P.checks if c.ok and not (c.found if c.rule else c.why)]
    assert not bare, f"a ✓ with nothing to show for it: {bare[:6]}"
    with_rule = [c for P in panels for c in P.checks if c.ok and c.rule]
    assert len(with_rule) >= 300 and all(" — “" in c.found for c in with_rule), "a ✓ without the sentence it was read from"
    new = [(P.region, P.kind, c.line) for P in panels for c in P.checks
           if not c.ok and (P.region, P.kind, c.line) not in KNOWN_PRINT_DEFECTS]
    assert not new, f"NEW disagreements with the printed synopsis: {new[:8]}"
    still = {(P.region, P.kind, c.line) for P in panels for c in P.checks if not c.ok}
    gone = [k for k in KNOWN_PRINT_DEFECTS if k not in still]
    assert not gone, f"known defects that VANISHED — fixed, or no longer checked? {gone}"
    # the sources are the official files: every panel names its file, edition, page and md5
    for P in panels:
        assert "2025-2027" in P.source and "page" in P.source and len(P.md5) == 8 and P.url.startswith("https://www2.gov.bc.ca/")


def test_the_extracted_text_is_from_the_pdf_not_from_the_catalogue():
    """A ✓ against our own transcription would be a tautology. The printed lines carry the
    print's spellings ("Trout/char: 5, but not more than", "0 quota, CLOSED TO FISHING"),
    which the catalogue's `verbatim` fields do not all share."""
    from pipeline.regs.table.quota_print import check_region, PDF_MD5
    P = check_region("4", "lake", name)
    assert any(l.startswith("Trout/char: 5, but not more than") for l in P.lines)
    assert any("Kokanee: 15 (none from streams), no more than 5 over 30 cm" in l for l in P.lines)
    assert P.md5 == PDF_MD5 and "page 36" in P.source


def test_every_section_table_is_its_base_plus_named_overrides():
    """For every section and every (fish, origin): each counter the section shows is a base
    counter or an override; each base counter it does not show is listed beneath its row
    with the override that replaced it, or is a stream/lake rule the base itself withholds.
    Nothing moves from base to section without a name on it."""
    attributed = 0                      # deltas this test actually checked
    overrides_seen = 0
    for w in WATERS:
        kind = section_kind(w)
        for run in range(len(D[w].get("runs") or [])):
            rs = section_rules(w, run)
            if not rs:
                continue
            here, label = section_regions(w, run), section_label(w, run)
            L = ledger(rs, kind, here, label)
            B = base(rs, kind)
            base_ids = {a.rule_id for a in B.allowances if a.derived_from is None}
            for r in rows(L, name):
                o = r.origin if r.origin is not Origin.both else Origin.wild
                for sp in r.fish:
                    shown = {a for a in r.counters if a.derived_from is None}
                    # every shown counter is base or override
                    for a in shown:
                        assert a.rule_id in base_ids or a.source.scope is not Scope.region, (
                            w, run + 1, sp, a.rule_id, "shown, but neither base nor override")
                        overrides_seen += a.rule_id not in base_ids
                    # every base counter that reaches this fish in the BASE and is not shown here
                    # is listed beneath the row with a named override
                    beneath = {a.rule_id: st for a, st in r.behind}
                    for b in B.allowances:
                        if b.derived_from is not None or not B.reaches(b, sp, o):
                            continue
                        if b.rule_id in {a.rule_id for a in shown}:
                            continue
                        st = beneath.get(b.rule_id)
                        assert st is not None, (w, run + 1, sp, b.rule_id, "a base counter vanished with no delta naming why")
                        attributed += 1
                        assert ("replaced" in st or "lifted" in st or st == "says the same thing"
                                or st == "not the strictest here"), (w, run + 1, sp, b.rule_id, st)
                        if "replaced" in st or "lifted" in st:
                            # the delta names a rule that is on this section and is not base
                            named = [a for a in L.allowances if a.rule_id != b.rule_id
                                     and a.source.words() in st] if "by " in st else []
                            if "by " in st:
                                assert named, (w, run + 1, sp, b.rule_id, st, "the delta names no rule on this section")
    # NOT VACUOUS: the corpus has hundreds of base counters replaced by water rules, and this
    # test must have walked them, or it has proved nothing.
    assert attributed >= 300, attributed
    assert overrides_seen >= 300, overrides_seen
