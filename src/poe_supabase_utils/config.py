from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class SupabaseConfig:
    """Configuration for Supabase connection."""

    url: str
    service_role_key: str
    anon_key: Optional[str] = None


_loaded_env: bool = False


def _load_env() -> None:
    """Load .env file, preferring local cwd over package root."""
    global _loaded_env
    if _loaded_env:
        return

    local = Path.cwd() / ".env"
    if local.exists():
        load_dotenv(local, override=False)
        _loaded_env = True
        return

    package_root = Path(__file__).resolve().parents[2] / ".env"
    if package_root.exists():
        load_dotenv(package_root, override=False)

    _loaded_env = True


def get_config() -> SupabaseConfig:
    """Load environment and return validated configuration.

    Raises:
        ValueError: If required environment variables are missing or empty.
    """
    _load_env()

    url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    anon_key = os.getenv("SUPABASE_ANON_KEY")

    if not url or not url.strip():
        raise ValueError("SUPABASE_URL environment variable must be set")
    if not service_role_key or not service_role_key.strip():
        raise ValueError("SUPABASE_SERVICE_ROLE_KEY environment variable must be set")

    return SupabaseConfig(
        url=url.strip(),
        service_role_key=service_role_key.strip(),
        anon_key=anon_key.strip() if anon_key else None,
    )
