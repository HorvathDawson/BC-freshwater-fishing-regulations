"""The authored catalogue entries — provincial and all eight regions.

These check the FILES, not the model: that every one parses, that no rule quietly loses the words
it came from, and that the review flags say why. `test_catalogue.py` covers the model itself.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.parsing.catalogue import CatalogueFile, RuleType, label

DIR = CURATED.regulations.entries.catalogue


def _files() -> list[Path]:
    return sorted(DIR.glob("region-*.json")) if DIR.exists() else []


def _entries():
    for p in _files():
        for e in CatalogueFile.model_validate(json.loads(p.read_text(encoding="utf-8"))).entries:
            yield p, e


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
def test_every_file_validates():
    """Including chain of custody: each rule's verbatim inside its entry's regs_verbatim."""
    for p in _files():
        CatalogueFile.model_validate(json.loads(p.read_text(encoding="utf-8")))


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
def test_entry_ids_are_unique_across_every_file():
    seen: dict[str, Path] = {}
    for p, e in _entries():
        assert e.entry_id not in seen, f"{e.entry_id} in both {seen[e.entry_id].name} and {p.name}"
        seen[e.entry_id] = p


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
def test_every_rule_generates_a_label():
    """A label that comes back empty is a rule the reader never sees."""
    for p, e in _entries():
        for r in e.rules:
            got = label(r)
            assert got and got.strip(), f"{p.name}: {e.entry_id}::{r.rule_id} generates nothing"


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
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


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
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


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
def test_needs_review_always_says_why():
    for p, e in _entries():
        for r in e.rules:
            if r.needs_review:
                assert len(r.review_reason) > 20, (
                    f"{p.name}: {e.entry_id}::{r.rule_id} is flagged with no usable reason")


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
def test_an_unbound_rule_is_flagged():
    """A rule that reaches no water must say so. A rule may carry its own extents — a row often
    binds its rules to different reaches — so the entry having none is only a problem for the rules
    that do not supply their own."""
    for p, e in _entries():
        for r in e.rules:
            if not e.extents and not r.extents:
                assert r.needs_review, (
                    f"{p.name}: {e.entry_id}::{r.rule_id} binds to nothing and does not say so")


def _norm(text: str) -> str:
    """Normalise away everything that carries no meaning: markdown emphasis, the three dash
    characters the synopsis mixes, and spacing around them. What survives is the words."""
    import re
    t = text.replace("*", "").replace("**", "")
    t = re.sub(r"(?m)^\s*>\s?", " ", t)                 # blockquote markers
    t = re.sub(r"(?m)^\s*[-•]\s+", " ", t)              # list bullets
    t = t.replace("|", " ")                              # markdown table cells
    t = re.sub(r"[\u2010-\u2015\u2212-]", "-", t)      # hyphen, en/em dash, minus
    t = re.sub(r"\s*-\s*", "-", t)
    t = re.sub(r"[\u2018\u2019]", "'", t)
    t = re.sub(r"[\u201c\u201d]", '"', t)
    return " ".join(t.split()).lower()


def _transcription_only(text: str) -> str:
    """Everything ABOVE the audit section.

    Each reference file is two documents: the synopsis transcribed verbatim, then a
    "Checked against the curated entries" audit written by us. A verbatim matched against the
    SECOND half is not provenance — it is our own commentary quoted back. That is exactly what
    happened: `z7a:stocked_lakes` stored a markdown table row, pipes and backticks included.
    """
    for marker in ("\n## Checked against", "\n## What this settles", "\n## What this decides"):
        i = text.find(marker)
        if i >= 0:
            text = text[:i]
    return text


def _reference_corpus() -> str:
    """Every verbatim transcription, as one normalised blob — audit sections excluded."""
    ref = CURATED.regulations.entries.catalogue.parent.parent / "reference"
    if not ref.exists():
        return ""
    return _norm(" ".join(_transcription_only(p.read_text(encoding="utf-8"))
                          for p in sorted(ref.glob("*.md"))))


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
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
    if not corpus:
        pytest.skip("no reference transcriptions on disk")
    missing: list[str] = []
    for p, e in _entries():
        for r in e.rules:
            needle = _norm(r.verbatim)
            if len(needle) < 25:
                continue                      # a fragment; its parent passage carries it
            if needle not in corpus:
                missing.append(f"{p.name}: {e.entry_id}::{r.rule_id}\n      {r.verbatim[:110]}")
    assert not missing, (
        f"{len(missing)} rule(s) quote text that is in NO reference transcription:\n  "
        + "\n  ".join(missing[:12]))


@pytest.mark.skipif(not _files(), reason="no catalogue entries authored yet")
def test_a_rule_quotes_its_OWN_region_not_another():
    """CROSS-REGION CONTAMINATION.

    Matching a verbatim against the whole reference corpus lets one region's sentence sit in
    another region's rule and still pass. It happened: `z7a:tagging_program` stored Region 3's
    notice, sending Omineca anglers to the Kamloops office on a Kamloops number, and
    `z7a:trout_char_quota`'s passage carried Region 8's entire quota list.

    A rule must be traceable to the chapter it claims to come from. Provincial rules may quote any
    provincial source; regional rules may quote their own chapter or a provincial one, because a
    chapter legitimately restates provincial text.
    """
    ref = CURATED.regulations.entries.catalogue.parent.parent / "reference"
    if not ref.exists():
        pytest.skip("no reference transcriptions on disk")

    def blob(*names: str) -> str:
        out = []
        for n in names:
            p = ref / f"{n}.md"
            if p.exists():
                out.append(_transcription_only(p.read_text(encoding="utf-8")))
        return _norm(" ".join(out))

    shared = ("provincial-regulations", "licensing", "definitions", "water-specific-tables")
    strays: list[str] = []
    for p, e in _entries():
        region = p.stem.replace("region-", "")
        allowed = blob(f"region-{region}", *shared)
        if not allowed:
            continue
        for r in e.rules:
            needle = _norm(r.verbatim)
            if len(needle) < 25 or needle in allowed:
                continue
            strays.append(f"{p.name}: {e.entry_id}::{r.rule_id}\n      {r.verbatim[:100]}")
    assert not strays, (
        f"{len(strays)} rule(s) quote another region's chapter:\n  " + "\n  ".join(strays[:12]))
