"""
Centralized Configuration Management for BC Freshwater Fishing Regulations Project.

Provides a single source of truth for all configuration settings, paths, and environment variables.
All pipelines import from this module to access configuration.
"""

from pathlib import Path, PurePath
from typing import Dict, Any
from pydantic import BaseModel


class ProjectConfig:
    """
    Singleton configuration manager for the entire project.
    Loads configuration from config.yaml and manages environment variables.
    """

    _instance = None
    _config = None
    _model = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ProjectConfig, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self._load_config()

    @property
    def project_root(self) -> Path:
        """Get the project root directory."""
        return Path(__file__).parent

    def _load_config(self):
        """Load configuration from config.yaml (no `.env` is read)."""
        # config.yaml through the ONE schema (`pipeline.common.curated.Config`): an unknown, missing or `null` key
        # anywhere in the file stops here, naming it. `_config` is the parsed file with the schema's names (`read_config`:
        # what the build's keys digest), `_model` the validated view every accessor returns.
        from pipeline.common.curated import parse_config
        self._config, self._model = parse_config(self.project_root / "config.yaml")


    @property
    def config(self) -> Dict[str, Any]:
        """Get the full configuration dictionary. A build stage that takes the whole dictionary is keyed on the
        whole file (`*`)."""
        from pipeline import _trace
        _trace.config_read("*", self._config)
        return self._config

    # ========================================================================
    # Path Accessors
    # ========================================================================

    def get_path(self, *keys: str) -> Path:
        """
        Get a path from config by nested keys, as the validated model holds it (absolute, under the repo root).

        Args:
            *keys: Nested keys to traverse (e.g., "cache", "dfo_salmon")

        Raises:
            KeyError: the keys name no path — never `Path()` (the cwd); no `default=` either
        """
        value = self._config
        found = True
        for key in keys:
            if isinstance(value, dict):
                found = found and key in value
                value = value.get(key, {})
            else:
                value = None
                found = False
                break
        # a build stage is keyed on the config values it reads, an absent one as absent (`_trace.config_read`);
        # a path of a block with a root is that root joined with the value, so the root is read too (`curated.TREES`)
        from pipeline import _trace
        from pipeline.common.curated import TREES
        _trace.config_read(".".join(keys), value, missing=not found)
        if found and keys[0] in TREES and len(keys) > 1:
            _trace.config_read(f"roots.{keys[0]}", self._config["roots"][keys[0]])

        node = self._model
        for key in keys:
            if not isinstance(node, BaseModel) or key not in type(node).model_fields:
                node = None
                break
            node = getattr(node, key)
        if keys[:1] == ("source",):
            # one access path: `SOURCE` also records the source root each name is joined to, which this would not
            raise KeyError(f"config.yaml {'.'.join(keys)} is a named source file: read it as "
                           f"`SOURCE.{keys[-1]}` (pipeline.common.curated)")
        if keys[:1] == ("curated",) or keys[:2] == ("roots", "curated"):
            # the model holds the LIVE tree; `CURATED` honours the `CURATED_ROOT` sandbox, which this would not
            raise KeyError(f"config.yaml {'.'.join(keys)} is a curated path: read it as `CURATED"
                           f"{''.join('.' + k for k in keys[1:]) if keys[0] == 'curated' else '.base'}` "
                           f"(pipeline.common.curated): it honours CURATED_ROOT")
        if keys[:1] == ("generated",) or keys[:2] == ("roots", "generated"):
            # (T1192) the same door for the generated tree: `GENERATED` / `ROOTS.generated` honour `GENERATED_ROOT`
            raise KeyError(f"config.yaml {'.'.join(keys)} is a generated path: read it as `GENERATED"
                           f"{''.join('.' + k for k in keys[1:]) if keys[0] == 'generated' else ''}` or "
                           f"`ROOTS.generated` (pipeline.common.curated): they honour GENERATED_ROOT")
        if found and isinstance(node, PurePath):
            return Path(node)
        raise KeyError(f"config.yaml has no path at {'.'.join(keys)}")

    def get_str_path(self, *keys: str) -> str:
        """Get a path as string (useful for config values that expect strings)."""
        return str(self.get_path(*keys))

    # ========================================================================
    # Pipeline — Extraction & Parsing
    # ========================================================================
    #
    # `extraction_dir`, `parsing_dir`, `synopsis_raw_data_path`, `fwa_output_dir`,
    # `fwa_graph_path`, `builds_dir`, `review_build_dir` and `added_streams_dir` USED TO
    # LIVE HERE, over an `output:` tree in config.yaml. They are now
    # `pipeline.common.curated.GENERATED` — see the `generated:` block in config.yaml.
    #
    # They are gone rather than forwarded on purpose. `get_path` below returned `Path()` —
    # the CURRENT DIRECTORY — for a key that did not exist: no exception, no warning. That
    # is how `fwa_output_dir` came to name `output/pipeline/graph/`, a directory that never
    # existed, and hand it to seven parser tools as their default registry. Two front doors
    # to the same paths is the condition that let it hide; there is now one.

    # `synopsis_pdf_path`, `fwa_data_gpkg`, `fwa_coast_gpkg`, `fetch_output_gpkg_path` and `fetch_temp_dir` are the
    # named source files now, `pipeline.common.curated.SOURCE` (`SOURCE.fwa_gpkg`): the gpkg had three names here and
    # in config.yaml, and the fetcher wrote the coast file by a fourth (`_RETIRED` names each replacement).

    # ========================================================================
    # Caches
    # ========================================================================

    @property
    def dfo_salmon_cache(self) -> Path:
        """DFO's saved salmon region pages (`cache.dfo_salmon`, under `roots.cache`): raw/ (the newest per region),
        history/ (every page fetched), manifest.json. Gitignored machine state outside data/, written by
        `pipeline.regs.dfo_salmon.fetch` / `churn`; one name for every reader. Never typed as a literal: a relative
        `cache/dfo_salmon` is a second tree from any other working directory."""
        return self.get_path("cache", "dfo_salmon")


_RETIRED = {
    "extraction_dir": "GENERATED.regs.extraction",
    # No replacement: it held the retired prose parser's synopsis_parsed.json.
    "parsing_dir": None,
    "synopsis_raw_data_path": 'GENERATED.regs.extraction / "synopsis_raw_data.json"',
    "fwa_output_dir": "GENERATED.build()",
    "fwa_graph_path": 'GENERATED.build() / "graph.pkl"',
    "builds_dir": "GENERATED.atlas.builds",
    "review_build_dir": "GENERATED.build()",
    "added_streams_dir": "GENERATED.added_streams",
    # the named source files (config.yaml `source:`)
    "synopsis_pdf_path": "SOURCE.synopsis_pdf",
    "fwa_data_gpkg": "SOURCE.fwa_gpkg",
    "fwa_coast_gpkg": "SOURCE.coast_gpkg",
    "fetch_output_gpkg_path": "SOURCE.fwa_gpkg",
    "fetch_temp_dir": "SOURCE.fetch_temp",
}


def _retired(self, name):
    """Refuse an accessor that moved, and name its replacement.

    Without this, `get_config().builds_dir` would raise a bare AttributeError somewhere far
    from the fix. With it, the error IS the fix.
    """
    if name in _RETIRED and _RETIRED[name] is None:
        raise AttributeError(
            f"ProjectConfig.{name} was retired with nothing in its place: what it held "
            f"belonged to the retired prose parser. Parsed entries are "
            f"CURATED.regulations.synopsis (pipeline.common.curated)."
        )
    if name in _RETIRED:
        new = _RETIRED[name]
        was = "with the `output:` tree" if new.startswith("GENERATED") else "for config.yaml's `source:` block"
        raise AttributeError(
            f"ProjectConfig.{name} was retired {was}. "
            f"Use `from pipeline.common.curated import {new.split('.')[0]}` and {new}."
        )
    raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")


ProjectConfig.__getattr__ = _retired

# Global singleton instance
_config_instance = None


def get_config() -> ProjectConfig:
    """
    Get the global configuration instance.

    Returns:
        ProjectConfig singleton instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = ProjectConfig()
    return _config_instance


# Convenience functions for quick access
def get_project_root() -> Path:
    """Get the project root directory."""
    return get_config().project_root


def load_config() -> Dict[str, Any]:
    """Load and return the full configuration dictionary."""
    return get_config().config
