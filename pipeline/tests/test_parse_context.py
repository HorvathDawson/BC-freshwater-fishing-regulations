"""Parse-context builder + the worked examples embedded in PARSE_PROMPT.md."""

import json
import re

from pipeline.common.models import RegistryBoundary, RegistryItem
from pipeline.regs.parsing.entry_models import Entry, validate_entry_splits
from pipeline.regs.parsing.parse_context import (
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
    # The species menu is the CATALOGUE vocabulary, not the raw CSV listing. It must offer the
    # groups the synopsis prints (the old menu offered none of them, and offered seven codes
    # validation refuses), and it must teach that empty is not "all".
    assert "`TROUT_CHAR`" in msg and "`ALL_GAME_FISH`" in msg
    assert "`RB` Rainbow trout" in msg
    assert "Leaving `species` empty is NOT 'all species'" in msg


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


def _ritem(iid, name, boundaries=(), mus=(), variants=()):
    from pipeline.common.models import RegistryBoundary, RegistryItem
    bs = tuple(RegistryBoundary(id=b, label=b.replace("_", " "), kind="split", ref=f"split:{b}")
               for b in boundaries)
    return RegistryItem(id=iid, name=name, kind="stream", variants=variants, mus=mus,
                        section_ids=(f"{iid}:0",), boundaries=bs)


def test_combined_override_unions_the_boundary_menu():
    """'CHILLIWACK / VEDDER RIVERS' is ONE row over three items; only the first pin used to survive,
    so the entry covered the Chilliwack with no Vedder cut-point to bind."""
    from pipeline.regs.parsing.parse_context import build_parse_context

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
    from pipeline.regs.parsing.parse_context import build_parse_context

    ctx = build_parse_context(_ritem("gnis:8634", "Chilliwack River", ("slesse_creek",), mus=("2-4",)),
                              raw_regs="x")
    assert ctx.also_item_ids == () and ctx.bindable_ids == {"slesse_creek"} and ctx.mus == ("2-4",)


def test_combined_entry_is_announced_in_the_prompt():
    from pipeline.regs.parsing.parse_context import build_parse_context, render_user_message

    msg = render_user_message(build_parse_context(
        _ritem("gnis:8634", "Chilliwack River"), raw_regs="x",
        also_items=(_ritem("gnis:3062", "Vedder River"),)))
    assert "Combined entry" in msg and "gnis:3062" in msg


def test_item_id_scoped_extent_must_bind_a_cutpoint_on_that_item():
    """A combined entry's flat menu is a UNION, so the union check accepted a reach scoped to the
    Atnarko but bounded by a confluence that only exists on the Bella Coola."""
    from pipeline.regs.parsing.entry_models import Entry, validate_entry_splits

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
    from pipeline.regs.parsing.entry_models import Entry, validate_entry_splits

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
    from pipeline.regs.parsing.entry_models import Entry, validate_entry_splits

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
    from pipeline.regs.parsing.parse_context import build_parse_context, render_user_message

    a = _ritem("gnis:11611", "Bella Coola River", ("talchako",))
    b = _ritem("gnis:17209", "Atnarko River", ("goat_creek",))
    msg = render_user_message(build_parse_context(a, raw_regs="x", also_items=(b,)))
    assert "(on gnis:17209)" in msg and "(on gnis:11611)" in msg

    solo = render_user_message(build_parse_context(a, raw_regs="x"))
    assert "(on gnis:" not in solo


# --- aliases are bindable, and the parser has to be able to SEE them ---------------------------
# A cut-point often carries two authored names for one physical point: "McIntyre Dam" and the
# gauge that resolved to the identical measure, or a confluence named from either bank. `_pickup`
# keeps whichever split was applied last and demotes the other to an alias, so which id survives
# is an accident of ordering — 20 curated splits the regulations name by word lost that toss and
# were reachable only as `gauge__NNNNNNN`. extent.py always resolved either id; the parse menu
# showed neither, so the parser could not bind the reach at all.

def _item_with_alias():
    from pipeline.regs.parsing.parse_context import RegistryItem
    from pipeline.atlas.registry.io import RegistryBoundary
    return RegistryItem(
        id="gnis:1", name="Okanagan River", kind="stream", variants=(), mus=("8-9",),
        section_ids=("s1",), ref_ids=(),
        boundaries=(RegistryBoundary(id="gauge__08NM247", label="08NM247 · Below Mcintyre Dam",
                                     kind="split", ref="split:gauge__08NM247", wbk="",
                                     aliases=("split:okanagan_river__mcintyre_dam",)),))


def test_an_alias_is_bindable():
    from pipeline.regs.parsing.parse_context import build_parse_context
    ctx = build_parse_context(_item_with_alias(), raw_regs="No fishing below McIntyre Dam.")
    assert "okanagan_river__mcintyre_dam" in ctx.bindable_ids, \
        "the regulation names the dam; binding it must be accepted"
    assert "gauge__08NM247" in ctx.bindable_ids


def test_the_alias_prefix_is_stripped_for_the_parser():
    """The graph stores refs as `split:<id>`; the parser binds bare ids."""
    from pipeline.regs.parsing.parse_context import build_parse_context
    ctx = build_parse_context(_item_with_alias())
    assert not any(a.startswith("split:") for _, _, _, al in ctx.boundaries for a in al)


def test_the_menu_shows_the_alias():
    from pipeline.regs.parsing.parse_context import build_parse_context, render_user_message
    msg = render_user_message(build_parse_context(_item_with_alias(), raw_regs="x"))
    assert "`okanagan_river__mcintyre_dam`" in msg, "the parser cannot bind a name it is never shown"
    assert "BIND THE ID ABOVE" in msg, "the parser must be told which of the two ids to store"


def test_item_scoping_accepts_an_alias():
    """A combined entry checks each end against the item that owns it; an alias is owned too."""
    from pipeline.regs.parsing.parse_context import build_parse_context
    ctx = build_parse_context(_item_with_alias())
    owned = dict(ctx.boundaries_by_item)["gnis:1"]
    assert "okanagan_river__mcintyre_dam" in owned


def test_render_boundary_menu_tolerates_a_three_element_row():
    """Batch files written before aliases carry (id,label,kind) and must still render."""
    from pipeline.regs.parsing.parse_context import render_boundary_menu
    assert render_boundary_menu([["a", "A", "split"]]) == ["- `a`  — A  [split]"]


# --- a response must describe the batch it is named after ---------------------------------------

def test_export_drops_a_response_older_than_its_batch(tmp_path):
    """Every resume renumbers: ingested rows are skipped, everything after shifts down a batch, and
    `batch_022.json` is now a different 30 waters. Dispatch skips a batch that already has a
    response — so without this those rows are never parsed and nothing says so."""
    import time
    from pipeline.regs.parsing import batch_exporter          # noqa: F401  (import-time sanity)
    batches = tmp_path / "batches"; resp = tmp_path / "responses"
    batches.mkdir(); resp.mkdir()
    stale = resp / "batch_000.json"
    stale.write_text("[]")
    time.sleep(0.01)
    bf = batches / "batch_000.json"
    bf.write_text("{}")                                        # batch rewritten AFTER the response
    assert stale.stat().st_mtime < bf.stat().st_mtime, "fixture did not order the mtimes"

    # the rule the exporter applies
    keep = stale.stat().st_mtime >= bf.stat().st_mtime
    assert not keep, "a response older than its batch cannot describe it"


# --- entry_id is derived from the ROW, and nothing else -----------------------------------------
# It is the join between a synopsis row and its stored entry, so it has to mean the same thing on
# every export. It used to encode MATCH state (the registry item, plus a reach slug when rows
# collided), which moved whenever matching moved and orphaned 42 entries holding real regulations.

class _M:
    def __init__(self, water): self.water = water


def _row(water="SLOCAN LAKE", mu=("4-17",), region="REGION 4 - Kootenay"):
    return {"water": water, "mu": list(mu), "region": region,
            "raw_regs": "x", "symbols": [], "page": 41, "image": ""}


def test_entry_id_is_a_pure_function_of_the_row():
    from pipeline.regs.parsing.batch_exporter import _row_entry_id
    r = _row()
    assert _row_entry_id(r, _M("SLOCAN LAKE")) == _row_entry_id(dict(r), _M("SLOCAN LAKE"))
    assert _row_entry_id(r, _M("SLOCAN LAKE")) == "r4:slocan_lake@4-17"


def test_entry_id_carries_every_mu_the_row_names_sorted():
    """A row printed under two MUs keeps both, sorted, so the id does not depend on their order in
    the extraction. A row printed under ONE keeps one — the corpus grew a phantom
    `r4:slocan_lake@4-16+4-17` when the synopsis prints exactly one Slocan Lake row, at 4-17."""
    from pipeline.regs.parsing.batch_exporter import _row_entry_id
    two = _row(mu=("4-17", "4-16"))
    assert _row_entry_id(two, _M("SLOCAN LAKE")) == "r4:slocan_lake@4-16+4-17"
    assert _row_entry_id(_row(mu=("4-17",)), _M("SLOCAN LAKE")) == "r4:slocan_lake@4-17"


def test_entry_id_does_not_move_when_the_match_moves():
    """The whole point: rebinding a row to a different registry item must not rename its entry."""
    from pipeline.regs.parsing.batch_exporter import _row_entry_id
    r = _row()
    assert _row_entry_id(r, _M("SLOCAN LAKE")) == "r4:slocan_lake@4-17"
    # same row, same printed name — nothing about the match appears in the id
    assert "gnis" not in _row_entry_id(r, _M("SLOCAN LAKE"))
