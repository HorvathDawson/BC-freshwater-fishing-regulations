"""Parse-context builder + the worked examples embedded in PARSE_PROMPT.md."""

import json
import re

from pipeline.models import RegistryBoundary, RegistryItem
from pipeline.parsing.entry_models import Entry, validate_entry_splits
from pipeline.parsing.parse_context import (
    build_parse_context, load_system_prompt, render_user_message,
)


def _item():
    return RegistryItem(
        id="gnis:123", name="Atnarko River", kind="stream", variants=("Atnarko",),
        mus=("5-4",), section_ids=("n1", "n2", "n3"),
        boundaries=(
            RegistryBoundary(id="hunlen_falls", label="Hunlen Falls", kind="confluence", ref="split:hunlen_falls"),
            RegistryBoundary(id="goat_creek_into_atnarko_river", label="Goat Creek → Atnarko River",
                             kind="confluence", ref="split:goat_creek_into_atnarko_river"),
        ),
    )


def test_context_exposes_boundary_menu_and_bindable_ids():
    item = _item()
    ctx = build_parse_context(item, raw_regs="No fishing.", region="5")

    assert ctx.bindable_ids == {"hunlen_falls", "goat_creek_into_atnarko_river"}
    msg = render_user_message(ctx)
    assert "hunlen_falls" in msg and "Goat Creek → Atnarko River" in msg
    assert "RB\tRainbow Trout" in msg              # species menu embedded


def test_area_within_targets_not_in_parse_menu():
    # area `within` scopes are a curation step, not parser-bound — the menu never lists areas.
    item = _item()
    ctx = build_parse_context(item, raw_regs="No fishing in Tweedsmuir Park.")
    assert "area:" not in render_user_message(ctx)


def _examples_from_prompt():
    """Every ```json fence in the prompt is a COMPLETE example entry and must validate. A fragment
    (e.g. a bare `extents` snippet) uses a plain fence so it is not harvested here."""
    text = load_system_prompt()
    return [json.loads(m) for m in re.findall(r"```json\n(.*?)\n```", text, re.DOTALL)]


def test_prompt_examples_are_valid_entries():
    examples = _examples_from_prompt()
    assert len(examples) >= 2
    for data in examples:
        entry = Entry(**data)                       # must satisfy every model validator
        # every extent id used by the examples is one the example's own text lists as a boundary
        allowed = {sid for r in entry.rules for ex in r.extents for sid in ex.splits}
        assert validate_entry_splits(entry, allowed) == []


# --------------------------------------------------------------------------- #
# Combined-entry overrides: one synopsis row over several registry items
# --------------------------------------------------------------------------- #

def _ritem(iid, name, boundaries=(), mus=(), variants=()):
    from pipeline.models import RegistryBoundary, RegistryItem
    bs = tuple(RegistryBoundary(id=b, label=b.replace("_", " "), kind="split", ref=f"split:{b}")
               for b in boundaries)
    return RegistryItem(id=iid, name=name, kind="stream", variants=variants, mus=mus,
                        section_ids=(f"{iid}:0",), boundaries=bs)


def test_combined_override_unions_the_boundary_menu():
    """'CHILLIWACK / VEDDER RIVERS' is ONE row over three items; only the first pin used to survive,
    so the entry covered the Chilliwack with no Vedder cut-point to bind."""
    from pipeline.parsing.parse_context import build_parse_context

    chwk = _ritem("gnis:8634", "Chilliwack River", ("slesse_creek",), mus=("2-4",))
    vedd = _ritem("gnis:3062", "Vedder River", ("vedder_crossing_bridge",), mus=("2-3",))
    canal = _ritem("wbk:329707189", "Vedder Canal", ("canal_mouth",))
    ctx = build_parse_context(chwk, raw_regs="x", also_items=(vedd, canal))

    assert ctx.item_id == "gnis:8634"
    assert ctx.also_item_ids == ("gnis:3062", "wbk:329707189")
    assert ctx.bindable_ids == {"slesse_creek", "vedder_crossing_bridge", "canal_mouth"}
    assert set(ctx.mus) == {"2-3", "2-4"}
    assert "Vedder River" in ctx.variants and "Vedder Canal" in ctx.variants


def test_uncombined_entry_is_unchanged():
    from pipeline.parsing.parse_context import build_parse_context

    ctx = build_parse_context(_ritem("gnis:8634", "Chilliwack River", ("slesse_creek",), mus=("2-4",)),
                              raw_regs="x")
    assert ctx.also_item_ids == () and ctx.bindable_ids == {"slesse_creek"} and ctx.mus == ("2-4",)


def test_combined_entry_is_announced_in_the_prompt():
    from pipeline.parsing.parse_context import build_parse_context, render_user_message

    msg = render_user_message(build_parse_context(
        _ritem("gnis:8634", "Chilliwack River"), raw_regs="x",
        also_items=(_ritem("gnis:3062", "Vedder River"),)))
    assert "Combined entry" in msg and "gnis:3062" in msg


def test_item_id_scoped_extent_must_bind_a_cutpoint_on_that_item():
    """A combined entry's flat menu is a UNION, so the union check accepted a reach scoped to the
    Atnarko but bounded by a confluence that only exists on the Bella Coola."""
    from pipeline.parsing.entry_models import Entry, validate_entry_splits

    entry = Entry(**{
        "entry_id": "gnis:11611#x",
        "identity": {"name": "ATNARKO/BELLA COOLA RIVERS", "region": "5", "mus": []},
        "regs_verbatim": "**No Fishing**",
        "rules": [{"rule_id": "x.r1", "restriction_type": "closure", "details": "No fishing",
                   "rule_text": "**No Fishing**",
                   "extents": [{"op": "between", "item_id": "gnis:17209",
                                "splits": ["goat_creek", "talchako"]}]}],
    })
    allowed = {"goat_creek", "talchako"}
    by_item = {"gnis:17209": {"goat_creek"}, "gnis:11611": {"talchako"}}

    assert validate_entry_splits(entry, allowed) == [], "the flat union check cannot catch it"
    errs = validate_entry_splits(entry, allowed, by_item)
    assert len(errs) == 1 and "scoped to 'gnis:17209'" in errs[0] and "talchako" in errs[0]


def test_item_ids_lets_one_reach_span_two_waters():
    """A reach whose two ends sit on DIFFERENT waters must be scoped to both — scoping it to either
    alone puts the other end out of scope and the reach cannot resolve at all."""
    from pipeline.parsing.entry_models import Entry, validate_entry_splits

    def _entry(scope: dict) -> Entry:
        return Entry(**{
            "entry_id": "gnis:11611#x",
            "identity": {"name": "ATNARKO/BELLA COOLA RIVERS", "region": "5", "mus": []},
            "regs_verbatim": "**No Fishing**",
            "rules": [{"rule_id": "x.r1", "restriction_type": "closure", "details": "No fishing",
                       "rule_text": "**No Fishing**",
                       "extents": [{"op": "between", "splits": ["goat_creek", "talchako"], **scope}]}],
        })

    allowed = {"goat_creek", "talchako"}
    by_item = {"gnis:17209": {"goat_creek"}, "gnis:11611": {"talchako"}}

    both = _entry({"item_ids": ["gnis:17209", "gnis:11611"]})
    assert validate_entry_splits(both, allowed, by_item) == []

    one = _entry({"item_ids": ["gnis:17209"]})
    errs = validate_entry_splits(one, allowed, by_item)
    assert len(errs) == 1 and "talchako" in errs[0]


def test_unscoped_extent_may_span_both_waters():
    from pipeline.parsing.entry_models import Entry, validate_entry_splits

    entry = Entry(**{
        "entry_id": "gnis:11611#x",
        "identity": {"name": "ATNARKO/BELLA COOLA RIVERS", "region": "5", "mus": []},
        "regs_verbatim": "**No Fishing**",
        "rules": [{"rule_id": "x.r1", "restriction_type": "closure", "details": "No fishing",
                   "rule_text": "**No Fishing**",
                   "extents": [{"op": "between", "splits": ["goat_creek", "talchako"]}]}],
    })
    by_item = {"gnis:17209": {"goat_creek"}, "gnis:11611": {"talchako"}}
    assert validate_entry_splits(entry, {"goat_creek", "talchako"}, by_item) == []


def test_menu_names_the_owning_item_only_for_combined_entries():
    from pipeline.parsing.parse_context import build_parse_context, render_user_message

    a = _ritem("gnis:11611", "Bella Coola River", ("talchako",))
    b = _ritem("gnis:17209", "Atnarko River", ("goat_creek",))
    msg = render_user_message(build_parse_context(a, raw_regs="x", also_items=(b,)))
    assert "(on gnis:17209)" in msg and "(on gnis:11611)" in msg

    solo = render_user_message(build_parse_context(a, raw_regs="x"))
    assert "(on gnis:" not in solo
