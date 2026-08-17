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
