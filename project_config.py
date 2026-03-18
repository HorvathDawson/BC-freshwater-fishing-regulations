"""
Centralized Configuration Management for BC Freshwater Fishing Regulations Project.

Provides a single source of truth for all configuration settings, paths, and environment variables.
All pipelines import from this module to access configuration.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
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

        # Process API keys with environment variables
        self._load_api_keys()

    def _load_api_keys(self):
        """Load API keys from environment variables."""
        llm_config = self._config.get("llm", {})
        api_keys_config = llm_config.get("api_keys", [])

        for key_config in api_keys_config:
            env_var = key_config.get("env_var")
            if env_var and env_var in os.environ:
                key_config["key"] = os.environ[env_var]

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

    @property
    def extraction_dir(self) -> Path:
        """Get extraction output directory."""
        return self.get_path("output", "pipeline", "extraction")

    @property
    def parsing_dir(self) -> Path:
        """Get parsing output directory."""
        return self.get_path("output", "pipeline", "parsing")

    @property
    def synopsis_pdf_path(self) -> Path:
        """Get path to fishing synopsis PDF."""
        return self.project_root / "data" / "fishing_synopsis.pdf"

    @property
    def synopsis_raw_data_path(self) -> Path:
        """Get path to extracted raw data JSON."""
        return self.extraction_dir / "synopsis_raw_data.json"

    def get_api_keys(self) -> List[Dict[str, str]]:
        """
        Get API keys for LLM parsing.

        Returns:
            List of dicts with 'id' and 'key' fields

        Raises:
            ValueError: If required API keys are missing
        """
        api_keys = []
        missing_keys = []

        llm_config = self._config.get("llm", {})
        for k in llm_config.get("api_keys", []):
            key_value = k.get("key")
            if not key_value:
                missing_keys.append(k.get("env_var", k.get("id")))
            else:
                api_keys.append({"id": k["id"], "key": key_value})

        if missing_keys:
            raise ValueError(
                f"Missing API keys in environment: {', '.join(missing_keys)}. "
                f"Please create a .env file in the project root with these variables."
            )

        return api_keys

    def get_llm_config(self) -> Dict[str, Any]:
        """Get LLM parsing configuration."""
        return self._config.get("llm", {})

    # ========================================================================
    # Graph Builder
    # ========================================================================

    @property
    def fwa_output_dir(self) -> Path:
        """Get graph builder output directory."""
        return self.get_path("output", "pipeline", "graph", "base")

    @property
    def fwa_graph_path(self) -> Path:
        """Get path to FWA graph pickle file."""
        return self.get_path("output", "pipeline", "graph", "graph")

    @property
    def fwa_metadata_path(self) -> Path:
        """Get path to FWA metadata pickle file."""
        return self.get_path("output", "pipeline", "graph", "metadata")

    @property
    def fwa_temp_dir(self) -> Path:
        """Get graph builder temporary files directory."""
        return self.get_path("output", "pipeline", "graph", "temp")

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


def get_api_keys() -> List[Dict[str, str]]:
    """Get API keys for LLM parsing."""
    return get_config().get_api_keys()
