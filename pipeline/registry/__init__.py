"""The REGISTRY — the parser's single source of truth (named items + areas over the stream graph).

Two concerns, one package:
- `build` — derive `RegistryItem`s from the built graph (`build_registry`, `add_mu_sets`, `item_id`).
- `io`    — persist/load `registry.json` so the parser tools never rebuild from the ~10GB FWA data.

Import from the package root: `from pipeline.registry import build_registry, load_registry`.
"""

from pipeline.registry.build import (
    add_curated_wbk_items, add_mu_sets, add_waterbody_items, build_registry, item_id,
)
from pipeline.registry.io import (
    default_registry_path,
    load_registry,
    write_registry,
)

__all__ = [
    "add_curated_wbk_items",
    "add_mu_sets",
    "add_waterbody_items",
    "build_registry",
    "item_id",
    "default_registry_path",
    "load_registry",
    "write_registry",
]
