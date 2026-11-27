"""
Centralized Configuration Management for BC Freshwater Fishing Regulations Project.

Provides a single source of truth for all configuration settings, paths, and environment variables.
All pipelines import from this module to access configuration.
"""

from pathlib import Path, PurePath
from typing import Dict, Any
from dotenv import load_dotenv
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
        """Load configuration from config.yaml and .env file."""
        # Load environment variables
        load_dotenv(self.project_root / ".env")

        # (T1192) config.yaml through the ONE schema (`pipeline.common.curated.Config`): an unknown, missing or `null` key
        # anywhere in the file stops here, naming it. `_config` is the parsed YAML (what the build's keys digest),
        # `_model` the validated view every accessor returns.
        from pipeline.common.curated import parse_config
        self._config, self._model = parse_config(self.project_root / "config.yaml")


    @property
    def config(self) -> Dict[str, Any]:
        """Get the full configuration dictionary. (T1171) A build stage that takes the whole dictionary is keyed on the
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
            *keys: Nested keys to traverse (e.g., "data_accessor", "gpkg_path")

        Raises:
            KeyError: (T435) the keys name no path — never `Path()` (the cwd); (T1192) no `default=` either
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
        # (T1171) a build stage is keyed on the config values it reads, an absent one as absent (`_trace.config_read`)
        from pipeline import _trace
        _trace.config_read(".".join(keys), value, missing=not found)

        node = self._model
        for key in keys:
            if not isinstance(node, BaseModel) or key not in type(node).model_fields:
                node = None
                break
            node = getattr(node, key)
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

    @property
    def synopsis_pdf_path(self) -> Path:
        """Get path to fishing synopsis PDF."""
        return self.project_root / "data" / "source" / "fishing_synopsis.pdf"

    @property
    def fwa_data_gpkg(self) -> Path:
        """Get path to unified FWA GeoPackage for FWADataAccessor."""
        return self.get_path("data_accessor", "gpkg_path")

    @property
    def fwa_coast_gpkg(self) -> Path:
        """T678: the FWA river polygons (`rivers`) and land mass (`land`) for the coast-crossing tidal cut, fetched by
        `data/fetch_data.py` (`fwa_rivers`, `fwa_land`). Its own file so the main gpkg's stamp, an input of every
        shipped atlas, never moves for it."""
        return self.get_path("data_accessor", "coast_gpkg_path")

    # ========================================================================
    # Caches (T1021)
    # ========================================================================

    @property
    def dfo_salmon_cache(self) -> Path:
        """DFO's saved salmon region pages (`cache.dfo_salmon`): raw/ (the newest per region), history/ (every page
        fetched), manifest.json. Gitignored machine state outside data/, written by `pipeline.regs.dfo_salmon.fetch` /
        `churn`; one name for every reader. Never typed as a literal: a relative `cache/dfo_salmon` is a second tree
        from any other working directory."""
        return self.get_path("cache", "dfo_salmon")

    # ========================================================================
    # Data Fetch
    # ========================================================================

    @property
    def fetch_output_gpkg_path(self) -> Path:
        """Get path for data fetch output GeoPackage (legacy, for fetch_data.py).

        Everything `data/fetch_data.py` writes lands under data/source/, where this gpkg is: a fetcher that writes
        where no reader looks is how a freshness gate goes quiet about a roster nobody is reading."""
        return self.get_path("data", "fetch", "output_gpkg")

    @property
    def fetch_temp_dir(self) -> Path:
        """Get temporary directory for data fetch operations."""
        return self.get_path("data", "fetch", "temp_dir")


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
        raise AttributeError(
            f"ProjectConfig.{name} was retired with the `output:` tree. "
            f"Use `from pipeline.common.curated import GENERATED` and {_RETIRED[name]}."
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
