"""
Configuration loader for the BC Fishing Regulations Pipeline.

Loads settings from config.yaml and allows environment variable overrides.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment variables from .env file in project root
project_root = Path(__file__).parent.parent
load_dotenv(project_root / ".env")


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

    # Default: load from centralized config.yaml at project root
    return Path(__file__).parent.parent / "config.yaml"


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

    # Extract synopsis_pipeline section from central config
    synopsis_config = config.get("synopsis_pipeline", {})

    # Merge with top-level output and data paths
    config.update(
        {
            "directories": config.get("output", {}).get("synopsis", {}),
            "parsing": synopsis_config.get("llm", {}),
            "api_keys": synopsis_config.get("api_keys", []),
        }
    )

    # Override API keys with environment variables if present
    if "api_keys" in config:
        for key_config in config["api_keys"]:
            env_var = key_config.get("env_var")
            if env_var and env_var in os.environ:
                key_config["key"] = os.environ[env_var]
            elif "default" in key_config:
                key_config["key"] = key_config["default"]

    return config


def get_api_keys(config: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extract API keys from config.

    Args:
        config: Configuration dictionary

    Returns:
        List of API key dictionaries with 'id' and 'key' fields

    Raises:
        ValueError: If required API keys are not found in environment variables
    """
    api_keys = []
    missing_keys = []

    for k in config.get("api_keys", []):
        key_value = k.get("key")
        if not key_value:
            missing_keys.append(k.get("env_var", k.get("id")))
        else:
            api_keys.append({"id": k["id"], "key": key_value})

    if missing_keys:
        raise ValueError(
            f"Missing API keys in environment: {', '.join(missing_keys)}. "
            f"Please create a .env file in the project root with these variables. "
            f"See .env.example for template."
        )

    return api_keys
