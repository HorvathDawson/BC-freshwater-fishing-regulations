"""Persist / load the REGISTRY (parser truth) as JSON.

The registry is derived from the built stream graph (`pipeline.atlas.registry.build_registry`), which needs
the ~10GB FWA geopackage — far too heavy for the parser tools to rebuild. So the build writes it once
to `registry.json` and the agent-parsing tools (matcher, batch_exporter, ingest) load it from there.

    from pipeline.atlas.registry import write_registry, load_registry
    write_registry(registry, path)          # in the build, after build_registry + add_mu_sets
    registry = load_registry(path)          # {item_id: RegistryItem}
"""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.common.models import RegistryBoundary, RegistryItem


def _item_to_dict(it: RegistryItem) -> dict:
    return {
        "id": it.id,
        "name": it.name,
        "kind": it.kind,
        "variants": list(it.variants),
        "mus": list(it.mus),
        "section_ids": list(it.section_ids),
        "ref_ids": list(it.ref_ids),
        "boundaries": [
            # `aliases` is omitted when empty so the file stays diff-clean for the 99% of
            # boundaries that have none.
            {"id": b.id, "label": b.label, "kind": b.kind, "ref": b.ref, "wbk": b.wbk,
             **({"aliases": list(b.aliases)} if b.aliases else {})}
            for b in it.boundaries
        ],
    }


def _item_from_dict(d: dict) -> RegistryItem:
    return RegistryItem(
        id=d["id"],
        name=d.get("name", ""),
        kind=d.get("kind", "stream"),
        variants=tuple(d.get("variants", [])),
        mus=tuple(d.get("mus", [])),
        section_ids=tuple(d.get("section_ids", [])),
        ref_ids=tuple(d.get("ref_ids", [])),
        boundaries=tuple(
            RegistryBoundary(id=b["id"], label=b.get("label", ""), kind=b.get("kind", ""),
                             ref=b.get("ref", ""), wbk=b.get("wbk", ""),
                             aliases=tuple(b.get("aliases", ())))
            for b in d.get("boundaries", [])
        ),
    )


def write_registry(registry: dict[str, RegistryItem], path: str | Path) -> Path:
    """Serialize the registry to `path` (stable key order for clean diffs). Returns the path."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"items": [_item_to_dict(registry[k]) for k in sorted(registry)]}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_registry(path: str | Path) -> dict[str, RegistryItem]:
    """Load `registry.json` back into `{item_id: RegistryItem}`."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {d["id"]: _item_from_dict(d) for d in data.get("items", [])}


def default_registry_path() -> Path:
    """`registry.json` of the default build — and it must EXIST.

    This used to return `output/pipeline/graph/registry.json`, a directory that never
    existed on disk, while every build wrote `output/v2/full/registry.json`. It was the
    declared default of seven parser tools; they worked only because run_parse.sh always
    passed `--registry` explicitly. Two of them (dfo_salmon splitwork and dossier) FELL BACK
    to it, so a missing registry sent them looking somewhere impossible.

    `GENERATED.registry()` refuses a build that is not there and names the command that
    makes one, so the same situation is now one legible line instead of a FileNotFoundError
    naming a path nobody recognises.
    """
    from pipeline.common.curated import GENERATED
    return GENERATED.registry()
