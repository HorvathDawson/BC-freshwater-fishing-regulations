"""The zone/area regulation entries, checked the way only a machine can.

These are regulations written against a ZONE rather than a water — a region's quotas, a park
closure, a bait ban over two management units, an advisory on Indigenous territory. They are
ordinary `Entry` objects, which is the point: they bind through the same resolver, intern
into the same rulesets and reach the app by the same path as a river row.

What that reuse cannot check is the part unique to them: an area id that does not exist, an
`area_kind` naming no family, or a rule that says "in any stream" and forgets to say so in
`feature_types` and therefore closes every lake in a region. Each of those produces a
perfectly valid Entry that binds to the wrong water, so each is checked here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.parsing.entry_models import EntryFile

ZONES = CURATED.regulations.entries.synopsis.parent / "zones"

#: The area families a `within` extent may name. Anything else is a typo that would resolve
#: to nothing and close nothing, silently.
KNOWN_KINDS = {
    "region", "mu_group", "national_parks", "ecological_reserves", "park",
    "restricted_land_access", "permit_land_access", "indigenous_land", "wma",
    "watershed", "chilkoot_trail",
}


def _files() -> list[Path]:
    return sorted(ZONES.glob("region-*.json")) if ZONES.exists() else []


def _entries():
    for p in _files():
        for e in EntryFile.model_validate(json.loads(p.read_text(encoding="utf-8"))).entries:
            yield p, e


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_every_zone_file_validates():
    """Including the chain of custody: rule_text within regs_verbatim, dates within rule_text."""
    for p in _files():
        EntryFile.model_validate(json.loads(p.read_text(encoding="utf-8")))


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_entry_ids_are_unique_across_every_zone_file():
    """`read_all_entries` refuses a collision at build time; this says which file to fix."""
    seen: dict[str, Path] = {}
    for p, e in _entries():
        assert e.entry_id not in seen, (
            f"entry_id {e.entry_id!r} is in both {seen[e.entry_id].name} and {p.name}")
        seen[e.entry_id] = p


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_rule_ids_are_unique_within_their_entry():
    for p, e in _entries():
        ids = [r.rule_id for r in e.rules]
        dupes = {i for i in ids if ids.count(i) > 1}
        assert not dupes, f"{p.name}: {e.entry_id} repeats rule_id(s) {sorted(dupes)}"


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_every_area_kind_is_a_family_that_exists():
    """A misspelled kind resolves to no areas, so the rule closes nothing and says nothing."""
    for p, e in _entries():
        for r in e.rules:
            for ex in r.extents:
                if ex.area_kind:
                    assert ex.area_kind in KNOWN_KINDS, (
                        f"{p.name}: {e.entry_id}::{r.rule_id} names area_kind "
                        f"{ex.area_kind!r}, which is not one of {sorted(KNOWN_KINDS)}")


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_a_region_area_id_is_lower_case():
    """`area:region:7A` resolves to nothing; the registry writes `area:region:7a`."""
    for p, e in _entries():
        for r in e.rules:
            for ex in r.extents:
                aid = ex.area_id or ""
                if aid.startswith("area:region:"):
                    assert aid == aid.lower(), (
                        f"{p.name}: {e.entry_id}::{r.rule_id} names {aid!r} — region ids are "
                        f"lower-case, so this would resolve to nothing")


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_a_rule_that_names_one_kind_of_water_says_so_in_feature_types():
    """THE EXPENSIVE MISTAKE, and it is not hypothetical.

    "No fishing in any stream in Management Units 1-1 to 1-6" without `feature_types` bound
    all 23,391 sections in the area — 16,528 streams, and also 4,151 lakes and 2,712
    wetlands the regulation never mentions. A closure on water a rule did not name is the
    Fording River failure arrived at from the other side.

    Only the singular, unambiguous phrasings are checked. "in all lakes and streams" names
    both and needs nothing; a rule about neither applies to everything and must stay silent.
    """
    for p, e in _entries():
        for r in e.rules:
            text = r.rule_text.lower()
            for word, kind in (("any stream", "stream"), ("all streams", "stream"),
                               ("any lake", "lake"), ("all lakes", "lake")):
                if word not in text:
                    continue
                other = "lake" if kind == "stream" else "stream"
                if other in text:            # names both; nothing to narrow
                    continue
                declared = {t for ex in r.extents for t in ex.feature_types}
                assert declared == {kind}, (
                    f"{p.name}: {e.entry_id}::{r.rule_id} says {word!r} but declares "
                    f"feature_types={sorted(declared) or 'nothing'} — it would bind every "
                    f"kind of water inside the area")


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_a_within_extent_names_an_area_and_a_whole_extent_names_a_water():
    """The two ways a zone entry reaches water, and neither may be half-specified."""
    for p, e in _entries():
        for r in e.rules:
            for ex in r.extents:
                if ex.op.value == "within":
                    assert ex.area_id or ex.area_kind, (
                        f"{p.name}: {e.entry_id}::{r.rule_id} has a `within` naming no area")
                elif ex.op.value == "whole":
                    assert e.matched, (
                        f"{p.name}: {e.entry_id}::{r.rule_id} is `whole` but the entry "
                        f"matches no registry item, so it covers nothing")


def _registry_area_ids() -> set[str]:
    """Area ids from whichever atlas build is on disk, or an empty set if none is."""
    from pipeline.common.curated import GENERATED

    try:
        build = GENERATED.require_build()
    except Exception:
        return set()
    reg = build / "registry.json"
    if not reg.exists():
        return set()
    return {i["id"] for i in json.loads(reg.read_text(encoding="utf-8"))["items"]
            if i["id"].startswith("area:")}


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_every_named_area_id_exists_in_the_registry():
    """A typo here binds a regulation to nothing, and nothing raises.

    The reach builder counts it as `area_id_dangling` and carries on — which is right for a
    build, and useless as feedback: the number was 2 before these entries existed and would
    still look unremarkable at 3. Skipped when no atlas build is on disk, because that is a
    checkout without generated artifacts rather than a fault.
    """
    known = _registry_area_ids()
    if not known:
        pytest.skip("no atlas build on disk to check ids against")
    if not any(a.startswith("area:region:") for a in known):
        # NOT AN AUTHORING ERROR — a stale artifact. `config.yaml`'s `default_build` names
        # the build this checks against, and a build made before the region and MU-group
        # areas existed has none of them, so every zone entry would look wrong. Say which
        # rather than failing on the entries.
        pytest.skip("the default build predates the region areas — rebuild it, or point "
                    "config.yaml's atlas.default_build at a build that has them")
    for p, e in _entries():
        for r in e.rules:
            for ex in r.extents:
                if ex.area_id:
                    assert ex.area_id in known, (
                        f"{p.name}: {e.entry_id}::{r.rule_id} names {ex.area_id!r}, which is "
                        f"not an area in the registry — the rule would bind to no water")


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_every_area_kind_matches_at_least_one_area():
    known = _registry_area_ids()
    if not known:
        pytest.skip("no atlas build on disk to check kinds against")
    if not any(a.startswith("area:region:") for a in known):
        # Same stale-build case as the id check above: a build made before the region areas
        # existed matches no `area:region:*`, which is a fact about the artifact on disk and
        # not about these entries.
        pytest.skip("the default build predates the region areas — rebuild it, or point "
                    "config.yaml's atlas.default_build at a build that has them")
    for p, e in _entries():
        for r in e.rules:
            for ex in r.extents:
                if ex.area_kind:
                    pre = f"area:{ex.area_kind}:"
                    assert any(a.startswith(pre) for a in known), (
                        f"{p.name}: {e.entry_id}::{r.rule_id} names area_kind "
                        f"{ex.area_kind!r} and no `{pre}*` area exists")


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_a_limit_does_not_claim_more_than_its_own_sentence():
    """THE CHAIN OF CUSTODY, EXTENDED TO NUMBERS.

    `rule_text` is already required to be a substring of `regs_verbatim`, so no rule can be
    invented. A `limit` is the same claim in a different notation and needs the same
    discipline: authoring one is retyping a number, and retyping is where a number changes.

    Caught immediately — the first hand-authored sample gave "Trout/char: 5 (all species
    combined)" a "no more than 1 over 50 cm" sub-limit that belongs to a DIFFERENT rule in
    the same region. The quota was right and the sentence never said it.

    Only the checkable half is checked: a size bound has to have a size in the text, and a
    number that may be kept has to appear. Whether 5 is the right 5 is a reader's job.
    """
    import re

    for p, e in _entries():
        for r in e.rules:
            text = r.rule_text.lower()
            for lim in r.limits:
                if lim.over_cm is not None or lim.under_cm is not None:
                    cm = lim.over_cm if lim.over_cm is not None else lim.under_cm
                    assert re.search(rf"\b{cm}\s*cm", text), (
                        f"{p.name}: {e.entry_id}::{r.rule_id} has a limit at {cm} cm and its "
                        f"text never mentions it — {r.rule_text[:70]!r}")
                if lim.take not in (None, 0) and not lim.unlimited:
                    assert re.search(rf"\b{lim.take}\b", text), (
                        f"{p.name}: {e.entry_id}::{r.rule_id} takes {lim.take} and its text "
                        f"never says that number — {r.rule_text[:70]!r}")
                if lim.per_daily is not None:
                    assert re.search(rf"\b{lim.per_daily}\b", text), (
                        f"{p.name}: {e.entry_id}::{r.rule_id} claims a possession multiple "
                        f"its text does not state")


@pytest.mark.skipif(not _files(), reason="no zone entries authored yet")
def test_a_sub_limit_names_a_parent_that_exists():
    for p, e in _entries():
        for r in e.rules:
            ids = {x.id for x in r.limits if x.id}
            for lim in r.limits:
                if lim.within:
                    assert lim.within in ids, (
                        f"{p.name}: {e.entry_id}::{r.rule_id} has a sub-limit inside "
                        f"{lim.within!r}, which is not a limit on this rule")
