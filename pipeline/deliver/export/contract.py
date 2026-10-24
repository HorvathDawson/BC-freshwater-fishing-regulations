"""What the export reads from the bundle, what it must never carry, and where the pair is written.

`REQUIRED_COLUMNS` are the bundle columns this export reads beyond the long-standing ones (`_need` refuses a
bundle without them); `RETIRED_*` are field names the model no longer has, refused anywhere in the output
(`checks.retired_keys`); `BUNDLE`, `OUT` and `GUIDE_NAME` are the default paths.
"""
from __future__ import annotations

import sqlite3
from pipeline.common.curated import GENERATED


BUNDLE = GENERATED.bundle / "bundle.sqlite"
OUT = GENERATED.base / "regs" / "ui-rules-export.json"


#: THE GUIDE FILE, written beside the data file by the same run (`codec`, C-E).
GUIDE_NAME = "ui-rules-guide.json"

# --------------------------------------------------------------------------------------------
# What the bundle must carry, and what it must never carry
# --------------------------------------------------------------------------------------------

#: Columns this export reads beyond the long-standing ones. A bundle without them is refused:
#: `entry.matched` is every water a synopsis row covers (`item_id` is only the first), and
#: `rule.unresolved` is why a rule could not be placed, and `rule.exempts` is what a rule lifts,
#: resolved to the entry each lift reaches — it left `conditions` for a column of its own, so a
#: bundle without the column is one whose lifts this export would silently drop.
REQUIRED_COLUMNS = {"entry": ("matched", "see"),
                    "rule": ("unresolved", "exempts", "undrawn_part", "parts"),
                    **{t: ("parts",) for t in ("designation", "not_classified", "requirement",
                                              "licence_terms", "exemption", "alternative")},
                    "item": ("part_of",), "outside_bc": ("sid",), "tidal": ("sid", "entry_id"),
                    "section_touch": ("a", "b"),
                    "province_except": ("area_kind", "sid"),
                    "steelhead_water": ("sid",),
                    "section_steelhead": ("sid", "code"),
                    "steelhead_known": ("sid", "anadromous", "rules"),
                    "steelhead_set": ("set_id", "code", "rules"),
                    "section_steelhead_rules": ("sid",),
                    "section_home": ("sid", "region"),
                    "steelhead_source": ("ord", "entry_id"),
                    "section_span": ("sid", "lo_m", "hi_m", "lo", "hi", "off_stem", "shape"),
                    "span_end": ("eid", "token"),
                    "split": ("split_id", "name", "kind", "at", "official_name",
                              "same_place_as", "lake_id"),
                    "item_alias": ("alias", "item_id")}

#: FIELD NAMES THE MODEL NO LONGER HAS. None may appear as a key anywhere in the output.
RETIRED_ANYWHERE = frozenset({
    "windows", "windows_are", "from_time", "to_time", "when_open",
    "over_cm", "under_cm", "band", "combined", "aggregation_domain",
    "required", "permitted", "allowed", "on_retention", "water_class", "licence_name",
    "issuing_jurisdiction", "grantor", "angler_class",
    "barbless", "hook_count", "max_lines", "max_flies", "max_weight_kg", "min_gap_cm",
    "max_gap_mm", "electric_only",
    "details", "rule_text", "restriction_type", "exempts_from", "subject",
    "needs_review", "locked", "reviewed_by", "reviewed_at", "registry_status", "registry_note",
    "parse_review", "revisit", "reference_only", "display_location", "location_text",
    "document_required", "access_permission",
})
#: Names that are current somewhere else in the model (`method` is a gear slot and a clause
#: condition, `document` a licence-terms field, `kind` the licensing discriminator, `dates` a
#: field of `when`) and retired only as a key on a RULE.
RETIRED_ON_RULE = RETIRED_ANYWHERE | frozenset({
    "method", "document", "allocation", "reason", "kind", "dates", "weekdays", "lure", "bait",
    "scope_text", "tributaries",
})
#: Rule types that were retired into `CatalogueEntry.licensing`.
RETIRED_TYPES = frozenset({"document_required", "access_permission",
                           "angling_from_vessel_prohibited"})


def _need(db: sqlite3.Connection) -> None:
    missing = [f"{t}.{c}" for t, cols in REQUIRED_COLUMNS.items()
               for c in cols if c not in {r[1] for r in db.execute(f"PRAGMA table_info({t})")}]
    if missing:
        raise SystemExit(
            f"pipeline.deliver.export: the bundle has no {', '.join(missing)}. Rebuild it with a "
            f"`pipeline/deliver/bundle/rules/` that writes `entry.matched` (every matched "
            f"item, JSON), `rule.unresolved` (the reach run's 'reason: detail', NULL when "
            f"bound), `rule.exempts` (each lift resolved to its entry, JSON), `item.part_of`, "
            f"the `outside_bc` table (the sections B.C. does not govern), `section_touch` "
            f"(which sections of a water border each other), and `section_span` / `span_end` / "
            f"`split` (where each section lies along its water, what ends it, the cuts by name).")
