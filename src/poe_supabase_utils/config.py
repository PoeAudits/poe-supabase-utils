import os
from pathlib import Path
from dotenv import load_dotenv

_LOADED_ENV = False


def load_env_prefer_local() -> None:
    global _LOADED_ENV
    if _LOADED_ENV:
        return
    local = Path.cwd() / ".env"
    if local.exists():
        load_dotenv(local, override=False)
        return
    package_root_env = Path(__file__).resolve().parents[2] / ".env"
    if package_root_env.exists():
        load_dotenv(package_root_env, override=False)


load_env_prefer_local()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
