"""
Configuration loader for the BC Fishing Regulations Pipeline.

Loads settings from config.yaml and allows environment variable overrides.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


def get_config_path(custom_path: Optional[str] = None) -> Path:
    """
    Get the path to the configuration file.

    Args:
        custom_path: Optional custom path to config file

    Returns:
        Path to configuration file
    """
    if custom_path:
        return Path(custom_path)

    # Default: look for config.yaml in the synopsis_pipeline directory
    return Path(__file__).parent / "config.yaml"


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file with environment variable overrides.

    Args:
        config_path: Optional custom path to config file

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
    """
    path = get_config_path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    # Override API keys with environment variables if present
    if "api_keys" in config:
        for key_config in config["api_keys"]:
            env_var = key_config.get("env_var")
            if env_var and env_var in os.environ:
                key_config["key"] = os.environ[env_var]
            elif "default" in key_config:
                key_config["key"] = key_config["default"]

    return config


def get_api_keys(config: Dict[str, Any]) -> list:
    """
    Extract API keys from config.

    Args:
        config: Configuration dictionary

    Returns:
        List of API key dictionaries with 'id' and 'key' fields
    """
    return [
        {"id": k["id"], "key": k.get("key", k.get("default", ""))}
        for k in config.get("api_keys", [])
    ]
