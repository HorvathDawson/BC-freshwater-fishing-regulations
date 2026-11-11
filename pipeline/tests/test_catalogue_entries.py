"""The authored catalogue entries — provincial and all eight regions.

These check the FILES, not the model: that every one parses, that no rule quietly loses the words
it came from, and that the review flags say why. `test_catalogue.py` covers the model itself.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.regs.parsing.catalogue import CatalogueFile, RuleType, label
from pipeline.tests import under_test
from pipeline.tests.conftest import need, ROWS_HINT

DIR = under_test.curated().regulations.synopsis


def _files() -> list[Path]:
    """The curated catalogue is tracked: an empty glob is a broken checkout, never a skip."""
    got = sorted(DIR.glob("region-*.json"))
    assert got, f"no catalogue files in {DIR} (tracked in git)"
    return got


def _entries():
    for p in _files():
        for e in CatalogueFile.model_validate(json.loads(p.read_text(encoding="utf-8"))).entries:
            yield p, e


def _authored_entries():
    """The HAND-AUTHORED zone/provincial entries only (`z…`), which is what the reference
    transcriptions cover.

    The same region files also hold the PARSED water rows (`r4:keen_creek@4-3`), and those quote
    the synopsis's per-water tables — text that is not in `reference/*.md` and never will be; the
    transcriptions are of the region and provincial CHAPTERS. Their chain of custody terminates
    somewhere stronger: ingest replaces `regs_verbatim` with the batch's own `raw_regs` and refuses
    anything that is not a contiguous quote of it, so a parsed rule cannot quote text the extractor
    did not hand it. Running the chapter guard over them asserted a claim nobody ever made and
    failed on 598 perfectly good rules."""
    for p, e in _entries():
        if (e.entry_id or "").startswith("z"):
            yield p, e


def test_every_file_validates():
    """Including chain of custody: each rule's verbatim inside its entry's regs_verbatim."""
    for p in _files():
        CatalogueFile.model_validate(json.loads(p.read_text(encoding="utf-8")))


def test_entry_ids_are_unique_across_every_file():
    seen: dict[str, Path] = {}
    for p, e in _entries():
        assert e.entry_id not in seen, f"{e.entry_id} in both {seen[e.entry_id].name} and {p.name}"
        seen[e.entry_id] = p


def test_every_rule_generates_a_label():
    """A label that comes back empty is a rule the reader never sees."""
    for p, e in _entries():
        for r in e.rules:
            got = label(r)
            assert got and got.strip(), f"{p.name}: {e.entry_id}::{r.rule_id} generates nothing"


def test_a_within_names_a_rule_that_exists_in_the_same_entry():
    """A sub-limit whose parent is missing cannot be checked against it, and `take <= parent take`
    is the only arithmetic guard the model has."""
    for p, e in _entries():
        ids = {r.rule_id for r in e.rules}
        for r in e.rules:
            if r.within:
                assert r.within in ids, (
                    f"{p.name}: {e.entry_id}::{r.rule_id} nests in {r.within!r}, which is not in "
                    f"this entry")


def test_a_sub_limit_never_exceeds_its_parent():
    """`z8:brook_trout` is the exception that proves it: 20 > 4 means it is NOT a sub-limit of the
    stream quota, it REPLACES it for brook trout. Anything still nested must fit inside."""
    for p, e in _entries():
        by_id = {r.rule_id: r for r in e.rules}
        for r in e.rules:
            if r.within and r.take is not None:
                parent = by_id[r.within]
                if parent.take is not None and not parent.unlimited:
                    assert r.take <= parent.take, (
                        f"{p.name}: {e.entry_id}::{r.rule_id} takes {r.take} inside a parent of "
                        f"{parent.take} — a sub-limit cannot exceed what it sits in")


def test_a_review_reason_is_long_enough_to_act_on():
    """`needs_review` was removed: it was exactly `bool(review_reason)`. The reason itself is now
    the flag, which makes an EMPTY-but-present reason the only way left to say "review this" and
    not say why — so the length floor moved here from the flag."""
    for p, e in _entries():
        for r in e.rules:
            if r.review_reason:
                assert len(r.review_reason) > 20, (
                    f"{p.name}: {e.entry_id}::{r.rule_id} asks for review with no usable reason")


def test_an_unbound_rule_is_flagged():
    """A rule that reaches no water must say so. A rule may carry its own extents — a row often
    binds its rules to different reaches — so the entry having none is only a problem for the rules
    that do not supply their own."""
    for p, e in _entries():
        for r in e.rules:
            if not e.extents and not r.extents:
                assert r.review_reason, (
                    f"{p.name}: {e.entry_id}::{r.rule_id} binds to nothing and does not say so")


def _norm(text: str) -> str:
    """The gate's `squash` (emphasis, blockquotes, bullets, dash variants, spacing, case) plus the two
    folds a hand transcription needs on top (T553 item 7): every markdown table `|` (squash takes only
    a line-start one) and the spacing round a dash, and curly quotes straight. One normaliser, two
    extra folds — no second copy of the first. Same output as the old copy on every zone quote and
    transcription (5,064 texts measured; the only differences were `[Includes Tributaries]` on water
    rows, which never reach this)."""
    import re
    from pipeline.regs.parsing.catalogue import squash
    t = squash(text.replace("|", " "))
    t = re.sub(r"\s*-\s*", "-", t)
    t = re.sub(r"[\u2018\u2019]", "'", t)
    return re.sub(r"[\u201c\u201d]", '"', t)


def _transcription_only(text: str) -> str:
    """Everything ABOVE the audit section.

    Each reference file is two documents: the synopsis transcribed verbatim, then a
    "Checked against the curated entries" audit written by us. A verbatim matched against the
    SECOND half is not provenance — it is our own commentary quoted back. That is exactly what
    happened: `z7a:stocked_lakes` stored a markdown table row, pipes and backticks included.
    """
    for marker in ("\n## Checked against", "\n## What this settles", "\n## What this decides",
                   "\n## What these definitions decide"):
        i = text.find(marker)
        if i >= 0:
            text = text[:i]
    return text


def _reference_corpus() -> str:
    """Every verbatim transcription, as one normalised blob — audit sections excluded."""
    ref = under_test.curated().regulations.reference
    if not ref.exists():
        return ""
    return _norm(" ".join(_transcription_only(p.read_text(encoding="utf-8"))
                          for p in sorted(ref.glob("*.md"))))


def test_every_verbatim_appears_in_the_reference_transcriptions():
    """THE GUARD THAT WAS MISSING.

    `CatalogueEntry` checks each rule's `verbatim` against its own entry's `regs_verbatim` — but
    the author writes both, so an invented sentence passes. Two did: a white sturgeon rule gained
    "all sturgeon must be released" and a National Parks rule gained "to fish in park waters",
    neither of which is in the synopsis. Chain of custody has to terminate at text nobody here
    wrote, which is `reference/*.md`.

    Only prose is checked. Where an entry stitches bullets or table rows into a sentence, the
    connective words are the author's; the substantive clause still has to be found.
    """
    corpus = _reference_corpus()
    assert corpus, "no reference transcriptions on disk (tracked in git)"
    missing: list[str] = []
    for p, e in _authored_entries():
        for r in e.rules:
            needle = _norm(r.verbatim)
            if len(needle) < 25:
                continue                      # a fragment; its parent passage carries it
            if needle not in corpus:
                missing.append(f"{p.name}: {e.entry_id}::{r.rule_id}\n      {r.verbatim[:110]}")
    assert not missing, (
        f"{len(missing)} rule(s) quote text that is in NO reference transcription:\n  "
        + "\n  ".join(missing[:12]))


def test_a_rule_quotes_its_OWN_region_not_another():
    """CROSS-REGION CONTAMINATION.

    Matching a verbatim against the whole reference corpus lets one region's sentence sit in
    another region's rule and still pass. It happened: `z7a:tagging_program` stored Region 3's
    notice, sending Omineca anglers to the Kamloops office on a Kamloops number, and
    `z7a:daily_quotas`'s passage carried Region 8's entire quota list.

    A rule must be traceable to the chapter it claims to come from. Provincial rules may quote any
    provincial source; regional rules may quote their own chapter or a provincial one, because a
    chapter legitimately restates provincial text.
    """
    ref = under_test.curated().regulations.reference
    assert ref.exists(), f"no reference transcriptions at {ref} (tracked in git)"

    strays: list[str] = []
    for p, e in _authored_entries():
        allowed = _own_chapter(p)
        assert allowed, f"{p.name}: no transcription of its chapter in {ref}"
        for r in e.rules:
            needle = _norm(r.verbatim)
            if len(needle) < 25:
                continue                      # the short ones: test_a_SHORT_quote_is_its_own_chapters_…
            if needle not in allowed:
                strays.append(f"{p.name}: {e.entry_id}::{r.rule_id}\n      {r.verbatim[:100]}")
    assert not strays, (
        f"{len(strays)} rule(s) quote another region's chapter:\n  " + "\n  ".join(strays[:12]))


_SHARED = ("provincial-regulations", "licensing", "definitions", "water-specific-tables")


def _own_chapter(p: Path) -> str:
    """The transcriptions a zone file may quote: its own region's chapter and the provincial ones."""
    ref = under_test.curated().regulations.reference
    names = (f"region-{p.stem.replace('region-', '')}",) + _SHARED
    return _norm(" ".join(_transcription_only((ref / f"{n}.md").read_text(encoding="utf-8"))
                          for n in names if (ref / f"{n}.md").exists()))


def _quotes(needle: str, hay: str) -> bool:
    """`needle` is in `hay` as whole words: "trout: 4" is not found inside "trout: 45", nor "1 over 50" inside
    "11 over 50" (T553: a short quote was skipped before, so a changed number in one was invisible)."""
    import re
    return bool(needle) and re.search(r"(?<![a-z0-9])" + re.escape(needle) + r"(?![a-z0-9])", hay) is not None


def test_a_SHORT_quote_is_its_own_chapters_on_word_boundaries():
    """T553 item 1.1. The two tests above skip a quote under 25 characters, and those are the quota lines:
    `z1:daily_quotas` "Trout: 4", `z1:hg_quota` "Kokanee: 10" — 99 rules, 91 carrying a `take`. Checked here against
    the rule's OWN chapter (plus the provincial ones), on word boundaries so the number cannot hide inside a longer one.
    Mutation: "Trout: 4" -> "Trout: 6" is not in Region 1's transcription and fails."""
    assert not _quotes(_norm("Trout: 6"), _own_chapter(DIR / "region-1.json"))
    assert not _quotes(_norm("Trout: 4"), _norm("Trout: 45"))
    bad: list[str] = []
    n = 0
    for p, e in _authored_entries():
        allowed = _own_chapter(p)
        for r in e.rules:
            needle = _norm(r.verbatim)
            if len(needle) >= 25:
                continue
            n += 1
            if not _quotes(needle, allowed):
                bad.append(f"{p.name}: {e.entry_id}::{r.rule_id} {r.verbatim!r}")
    assert n > 90, f"only {n} short zone quotes — the corpus or the length rule moved"
    assert not bad, f"{len(bad)} short zone quote(s) not in their own chapter's transcription:\n  " + "\n  ".join(bad)


def test_every_zone_LICENSING_quote_is_in_its_chapter_or_on_its_printed_page():
    """T553 item 1.2. Licensing quotes were never checked against anything outside the entry. Each must be in its own
    chapter's transcription (whole words) or printed word for word on its entry's `source_pages` of the repo PDF
    (`synopsis_print.quoted`): 8 of 47 are only the second — Classified Waters and basic licence paragraphs on p.6-7
    that `licensing.md` does not transcribe."""
    from pipeline.regs.parsing.catalogue_parts.licensing import licensing_verbatims
    from pipeline.common.synopsis_print import quoted
    assert quoted("each licence may not exceed 8 consecutive days", [7])
    assert not quoted("each licence may not exceed 9 consecutive days", [7])        # mutation
    bad: list[str] = []
    n = 0
    for p, e in _authored_entries():
        allowed = _own_chapter(p)
        for rec in e.licensing:
            for q in licensing_verbatims(rec):
                n += 1
                if _quotes(_norm(q), allowed) or quoted(q, e.source_pages):
                    continue
                bad.append(f"{p.name}: {e.entry_id}::{rec.id} p.{e.source_pages}\n      {q[:100]}")
    assert n > 40, f"only {n} zone licensing quotes — the corpus moved"
    assert not bad, (f"{len(bad)} zone licensing quote(s) in neither the transcription nor the printed page:\n  "
                     + "\n  ".join(bad))


@pytest.mark.needs_source
def test_every_entry_names_a_row_the_synopsis_actually_prints():
    """`entry_id` is the join between a synopsis row and its stored entry, so an id that is not a
    row names a water the book never printed.

    22 of those reached the corpus. The agent returns `entry_id` altered — the `@MU` suffix
    dropped, the slug rewritten, or a plausible MU pairing invented: the synopsis prints ONE
    Slocan Lake row, at 4-17, and the corpus grew a second at `4-16+4-17`. Neither the schema nor
    the chain of custody can see it, because each such entry is internally perfect and quotes its
    own passage correctly.

    NOT "one entry per water" — that was the first version of this test and it is wrong. CRAWFORD
    CREEK is printed twice on page 36, once for MU 4-6 (no fishing) and once for 4-33 (no fishing
    Jun 15-Oct 31). Two rows, two sets of rules, two entries, all correct.
    """
    from pipeline.regs.parsing.rows import load_synopsis_rows
    from pipeline.regs.parsing.batch_exporter import _row_entry_id

    class _M:
        def __init__(self, water): self.water = water

    need(None, "source", under_test.generated().regs.extraction / "synopsis_raw_data.json", ROWS_HINT)
    rows = list(load_synopsis_rows())
    assert rows, "the synopsis rows file holds no rows"
    real = {_row_entry_id(r, _M(r["water"])) for r in rows}

    phantom = [e.entry_id for _, e in _entries()
               if not (e.entry_id or "").startswith("z") and e.entry_id not in real]
    assert not phantom, ("these entry_ids are not rows the synopsis prints:\n  "
                         + "\n  ".join(sorted(phantom)))


#: Water rows whose stored text is a hand fix of an extraction defect (annual runbook step 2d), each pinned by THE FIX
#: ITSELF: (what the squashed extraction prints, exactly once; what the stored text says there; why). The stored text
#: must be the extraction with that one replacement and nothing else, so either side moving alone fails, naming the row.
HAND_FIXED_ROWS = {
    "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4": (
        "and hatch- ery cutthroat catch and release", "and hatchery cutthroat catch and release",
        "the extraction keeps a line-end hyphen: 'hatch- ery'; stored 'hatchery'"),
    "r2:coquihalla_river@2-17": (
        "downstream of the southern entrance to the lower most railway tunnel, nov",
        "downstream of the southern entrance to the lower most railway tunnel, nov 1-mar 31",
        "the extraction loses the last words: 'Nov'; stored 'Nov 1-Mar 31'"),
}


@pytest.mark.needs_source
def test_every_stored_row_text_is_the_extractions():
    """T553 item 3. `regs_verbatim` is copied from the batch once, at ingest; after that the region files and the
    extraction are independent, and every quote and number check compares against the STORED text — so a later hand or
    app edit that changes a printed number, together with its rule, passed every gate. Here every water row's stored
    text equals (squashed) its extraction row's `raw_regs`, and its `source_pages` that row's printed page; the two
    known hand fixes are pinned by the fix itself (`HAND_FIXED_ROWS`)."""
    from pipeline.regs.parsing.batch_exporter import _row_entry_id
    from pipeline.regs.parsing.catalogue import squash
    from pipeline.regs.parsing.rows import load_synopsis_rows

    class _M:
        def __init__(self, water): self.water = water

    need(None, "source", under_test.generated().regs.extraction / "synopsis_raw_data.json", ROWS_HINT)
    rows = {_row_entry_id(r, _M(r["water"])): r for r in load_synopsis_rows()}

    bad: list[str] = []
    n = 0
    for p, e in _entries():
        if (e.entry_id or "").startswith("z"):
            continue
        n += 1
        r = rows.get(e.entry_id)
        if r is None:
            continue                          # test_every_entry_names_a_row_the_synopsis_actually_prints
        if list(e.source_pages or []) != [r.get("page")]:
            bad.append(f"{e.entry_id}: source_pages {e.source_pages} but the row is printed on p.{r.get('page')}")
        fix = HAND_FIXED_ROWS.get(e.entry_id)
        if fix:
            printed, stored, why = fix
            got, raw = squash(e.regs_verbatim), squash(r["raw_regs"])
            if raw.count(printed) != 1 or raw.replace(printed, stored) != got or got == raw:
                bad.append(f"{e.entry_id}: a hand-fixed row moved — the stored text is no longer the extraction with "
                           f"{printed!r} -> {stored!r} ({why})\n      stored:    {got!r}\n      extracted: {raw!r}")
        elif squash(e.regs_verbatim) != squash(r["raw_regs"]):
            bad.append(f"{e.entry_id}: stored text is not the extraction's\n      stored:    "
                       f"{e.regs_verbatim[:90]!r}\n      extracted: {r['raw_regs'][:90]!r}")
    assert n > 1000, f"only {n} water rows"
    assert not bad, f"{len(bad)} water row(s) whose stored text left the extraction:\n  " + "\n  ".join(bad)


def test_a_quoted_prohibition_reads_as_one():
    """A QUOTE CAN BE TRUE AND INVERTED.

    `zp:conduct.r1` quotes "Waste the fish you catch." — faithfully, because the synopsis
    prints it as a bullet under the heading "It Is Unlawful To…". Every gate passed it: the
    words are in the passage, the passage is the batch's own text, no number is misplaced.
    And because a handling_rule has nothing to generate a label from, that quote WAS the
    label, so the app told anglers to waste their catch. Eleven rules read that way.

    Chain of custody checks that the words came from the source. It cannot check that the
    MEANING did — the heading is what made it a prohibition and the heading is not in the
    quote. So the prohibition has to be in the data (`required: false`) and be rendered.

    The check: where the run-up IMMEDIATELY before a quote is what forbids it, the rule must
    say so. Deliberately narrow — a negation elsewhere in the sentence usually belongs to a
    neighbouring clause ("no angling from boats, bait ban"), and widening it produced seven
    false positives against five real ones.
    """
    import re as _re
    from pipeline.regs.parsing.catalogue import squash, label
    governing = _re.compile(
        r"(?:it is )?(?:unlawful|illegal|an offence|prohibited)\s+(?:to|for)\s*$"
        r"|(?:must|may|do|does|shall|can)\s+not\s*$"
        r"|no person (?:may|shall)\s*$", _re.I)
    bad: list[str] = []
    for _, e in _entries():
        hay = squash(e.regs_verbatim)
        for r in e.rules:
            needle = squash(r.verbatim)
            if not needle or needle not in hay:
                continue
            run_up = hay[:hay.index(needle)].rsplit(".", 1)[-1]
            if not governing.search(run_up):
                continue
            # A GENERATED LABEL IS NOT A QUOTE DOING DUTY AS ONE. `gear` and `conduct` carry the
            # prohibition structurally — a `ban`, an `only`, a `must_be`, a `do_not_` act — and
            # the label is then BUILT from those fields rather than lifted from a bullet whose
            # forbidding heading is missing. The failure this test names is a quote standing in
            # for a label; where the label is generated, it cannot arise.
            if (r.gear or r.conduct) and squash(label(r)) != needle:
                continue
            bad.append(f"{e.entry_id}::{r.rule_id} [{r.type.value}]"
                       f"\n      forbidden by: …{run_up.strip()[-28:]!r}"
                       f"\n      but renders : {label(r)[:64]!r}")
    assert not bad, ("a quote is doing duty as a label without the clause that forbids it — "
                     "state the duty as a `conduct` act named in its lawful direction:\n  " + "\n  ".join(bad))


def test_a_quota_of_zero_is_not_a_prohibition():
    """"Kokanee: 5 (none from streams)" sets a quota. It does not close the streams.

    Eleven rules carried `may_target: False` off a quota-table parenthetical, which the label
    generator renders as "No fishing for kokanee in streams" — a claim the synopsis does not
    make, on every water in nine regions. The entries are titled `species_quotas` and hold
    nothing else: Bass 20, Crappie 20, Crayfish 25, Kokanee 5, Whitefish 15. The source section
    is headed "Region 2 Daily Quotas".

    The book has its own way of closing a water and uses it when it means it — Wood Lake's
    "No Fishing** for kokanee Sept 1-Mar 31" is a real closure and keeps `may_target: False`.
    A number in a quota column is a number.
    """
    for p, e in _authored_entries():
        for r in e.rules:
            if "KO" not in r.species or r.take != 0:
                continue
            v = (r.verbatim or "").strip().lower()
            if v.startswith("none from streams") or v.startswith("kokanee daily quota = 0"):
                assert r.may_target is not False, (
                    f"{p.name}: {e.entry_id}::{r.rule_id} reads a quota of zero as a closure — "
                    f"{r.verbatim!r}")


def test_a_condition_is_not_an_extent():
    """`extent_text` means "a place I was told about and could not draw", and a rule that has one
    is denied its entry's default extent — deliberately, so a 500 m closure is never widened to
    a whole lake. The cost of getting it wrong is total: the rule binds to no section and is
    lost on every water.

    Seven provincial rules held a CONDITION in that field — "alone in a boat on a lake", "warn
    others of your ice hole", "chumming", "the listed protected species only" — and so were lost
    everywhere. `reason` is the field for a qualifier with no structured slot.
    """
    bad = []
    for p, e in _authored_entries():
        for r in e.rules:
            t = (r.extent_text or "").strip().lower()
            if not t:
                continue
            # A place names one, or measures from one. These never do.
            if t in {"alone in a boat on a lake", "chumming",
                     "the listed protected species only",
                     "warn others of your ice hole; remove your hut before breakup",
                     "marked with name, address and telephone number",
                     "submerged and within 1 m of the hook",
                     "gear in the water during a No Fishing period".lower()}:
                bad.append(f"{p.name}: {e.entry_id}::{r.rule_id} extent_text={r.extent_text!r}")
    assert not bad, ("a condition is filed as a place, so the rule binds to nothing:\n  "
                     + "\n  ".join(bad))
