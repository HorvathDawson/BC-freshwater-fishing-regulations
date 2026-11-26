"""T1140 (owner 2026-11-25: "itshould have the real name. there should be no partt x of n nameas anywhewre in the app. that
is a fault. instead it will say eco reserve here etc all types will have a name"). A part with NO MAIN STEM whose side
channels lie wholly inside one area — every channel run from that area's cut to the same cut, as the export writes the
Squamish's two channels in Baynes Island Ecological Reserve — ships the end token `near:inside:<area token>` as both its
ends, said "<water> inside <Area>" by the one name function the app already has for `near:` (`spanName`). Anything less
(a channel that only RUNS TO an area's boundary, channels of two areas) is never said inside: no invented name.
Synthetic waters through `builders._names` and the one end resolver (`part.end_words`)."""
from pipeline.deliver.answers.display.builders import _names
from pipeline.deliver.answers.display.part import INSIDE, Water, end_table, end_words, inside_area

BAYNES = "area:BAYNES ISLAND ECOLOGICAL RESERVE"
SPLITS = {BAYNES: {"name": "Baynes Island Ecological Reserve boundary", "kind": "area"},
          "area:Pacific Rim National Park Reserve of Canada": {"name": "Pacific Rim National Park Reserve of Canada boundary",
                                                                "kind": "area"},
          "squamish__tidal": {"name": "Tidal boundary", "km": 3.0, "water_id": "gnis:1"}}


def _run(frm, to, km_from, km_to, branch=False):
    return {"from": frm, "to": to, "km_from": km_from, "km_to": km_to, **({"branch": True} if branch else {})}


def _water(parts):
    w = {"name": "Squamish River", "parts": [{"ruleset": i, "sections": 10 - i, "grp": i, "run": 0, "runs": runs}
                                             for i, runs in enumerate(parts)]}
    doc = {"waters": {"gnis:1": w}, "rulesets": [{} for _ in parts], "rules": {}, "splits": SPLITS}
    return Water("gnis:1", w, doc)


def _said(W):
    out = []
    for n in _names(W, {}):
        if n["lo"] is None:
            out.append(W.w["name"])
        elif n["lo"] == n["hi"] and n["lo"].startswith("near:"):
            w = end_words(W.doc, n["lo"], "from")                 # as the app composes it (`spanName`)
            out.append(f"{W.w['name']} {w[:1].lower() + w[1:]}")
        else:
            out.append(f"{end_words(W.doc, n['lo'], 'to')} → {end_words(W.doc, n['hi'], 'from')}")
    return out


MAIN = [_run("squamish__tidal", "mouth", 3.0, 0.0), _run("source", "squamish__tidal", 90.0, 3.0)]


def test_the_squamish_channels_inside_baynes_island_are_said_inside_it():
    inside = [_run(BAYNES, BAYNES, 11.6, 11.2, branch=True), _run(BAYNES, BAYNES, 11.6, 11.0, branch=True)]
    W = _water([MAIN, inside])
    names = _names(W, {})
    assert names[1] == {"lo": INSIDE + BAYNES, "hi": INSIDE + BAYNES}
    assert _said(W)[1] == "Squamish River inside Baynes Island Ecological Reserve"
    # the area's words are the export's split name, cased as every area end is — never the catalogue's capitals
    assert end_table(W.doc, [INSIDE + BAYNES])[INSIDE + BAYNES][0] == "Inside Baynes Island Ecological Reserve"


def test_a_national_park_reserve_is_said_as_its_boundary_end_is():
    pr = "area:Pacific Rim National Park Reserve of Canada"
    W = _water([MAIN, [_run(pr, pr, 0.2, 0.1, branch=True)]])
    assert _said(W)[1] == "Squamish River inside Pacific Rim National Park Reserve"


def test_a_channel_that_only_runs_to_an_areas_boundary_is_not_inside_it():
    # the Skeena's channel "from the Exchamsiks River confluence up to Skeena River Ecological Reserve boundary"; the
    # Okanagan River Oxbows' "source → the reserve's boundary" beside a "source → mouth" channel
    assert inside_area({"runs": [_run("source", BAYNES, 1.0, 0.5, branch=True)]}) is None
    assert inside_area({"runs": [_run(BAYNES, BAYNES, 1.0, 0.5, branch=True), _run("source", "mouth", 2.0, 0.0, branch=True)]}) is None
    W = _water([MAIN, [_run("source", BAYNES, 11.6, 11.2, branch=True)]])
    assert _said(W)[1] == "Squamish River"                       # no name invented: the water's, as before


def test_channels_of_two_areas_or_a_main_stem_are_never_said_inside_one():
    two = "area:Other Ecological Reserve"
    assert inside_area({"runs": [_run(BAYNES, BAYNES, 1.0, 0.5, branch=True), _run(two, two, 2.0, 1.5, branch=True)]}) is None
    assert inside_area({"runs": [_run(BAYNES, BAYNES, 1.0, 0.5)]}) is None          # a main-stem run: no branch at all
    assert inside_area({"runs": []}) is None
