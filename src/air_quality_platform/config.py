from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env", override=False)
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"


def _load_yaml() -> dict[str, Any]:
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


CONFIG = _load_yaml()


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value or ""


def snowflake_config(schema: str | None = None) -> dict[str, str]:
    return {
        "account": env("SNOWFLAKE_ACCOUNT", required=True),
        "user": env("SNOWFLAKE_USER", required=True),
        "role": env("SNOWFLAKE_ROLE", "SYSADMIN"),
        "warehouse": env("SNOWFLAKE_WAREHOUSE", "AQ_WH"),
        "database": env("SNOWFLAKE_DATABASE", "AQ_WAREHOUSE"),
        "schema": schema or env("SNOWFLAKE_SCHEMA", "CONTROL"),
    }


def private_key_path() -> Path:
    configured = Path(env("SNOWFLAKE_PRIVATE_KEY_PATH", ".secrets/rsa_key.p8"))
    if configured.is_absolute() and configured.exists():
        return configured
    candidate = (PROJECT_ROOT / configured).resolve()
    if candidate.exists():
        return candidate
    fallback = (PROJECT_ROOT / ".secrets" / "rsa_key.p8").resolve()
    return fallback


def waqi_cities() -> list[str]:
    return list(CONFIG.get("waqi", {}).get("cities", []))


def quality_config() -> dict[str, Any]:
    return dict(CONFIG.get("quality", {}))
