"""
Centralized Configuration Management for BC Freshwater Fishing Regulations Project.

Provides a single source of truth for all configuration settings, paths, and environment variables.
All pipelines import from this module to access configuration.
"""

import yaml
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv


class ProjectConfig:
    """
    Singleton configuration manager for the entire project.
    Loads configuration from config.yaml and manages environment variables.
    """

    _instance = None
    _config = None

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

        # Load YAML config
        config_path = self.project_root / "config.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, "r") as f:
            self._config = yaml.safe_load(f)


    @property
    def config(self) -> Dict[str, Any]:
        """Get the full configuration dictionary."""
        return self._config

    # ========================================================================
    # Path Accessors
    # ========================================================================

    def get_path(self, *keys: str, default: str = "") -> Path:
        """
        Get a path from config by nested keys, returning as Path object.

        Args:
            *keys: Nested keys to traverse (e.g., "output", "synopsis", "extract")
            default: Default value if path not found

        Returns:
            Path object (relative to project root if not absolute)
        """
        value = self._config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, {})
            else:
                return Path(default) if default else Path()

        if isinstance(value, str):
            path = Path(value)
            # Make relative paths relative to project root
            if not path.is_absolute():
                path = self.project_root / path
            return path

        return Path(default) if default else Path()

    def get_str_path(self, *keys: str, default: str = "") -> str:
        """Get a path as string (useful for config values that expect strings)."""
        return str(self.get_path(*keys, default=default))

    # ========================================================================
    # Pipeline — Extraction & Parsing
    # ========================================================================
    #
    # `extraction_dir`, `parsing_dir`, `synopsis_raw_data_path`, `fwa_output_dir`,
    # `fwa_graph_path`, `builds_dir`, `review_build_dir` and `added_streams_dir` USED TO
    # LIVE HERE, over an `output:` tree in config.yaml. They are now
    # `pipeline.common.curated.GENERATED` — see the `generated:` block in config.yaml.
    #
    # They are gone rather than forwarded on purpose. `get_path` below returns `Path()` —
    # the CURRENT DIRECTORY — for a key that does not exist: no exception, no warning. That
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

    # ========================================================================
    # Data Fetch
    # ========================================================================

    @property
    def fetch_output_gpkg_path(self) -> Path:
        """Get path for data fetch output GeoPackage (legacy, for fetch_data.py)."""
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
            f"CURATED.regulations.entries.catalogue (pipeline.common.curated)."
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
