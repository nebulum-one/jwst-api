from dotenv import load_dotenv
import os

load_dotenv()

def clean_env(value: str | None):
    """
    Remove masked Railway values like 'rwxp•••••••••••' and return None instead.
    """
    if not value:
        return None
    # Railway masks secrets using "••••" — treat them as invalid.
    if "••••" in value or "…" in value:
        return None
    return value

DATABASE_URL = clean_env(os.getenv("DATABASE_URL"))
