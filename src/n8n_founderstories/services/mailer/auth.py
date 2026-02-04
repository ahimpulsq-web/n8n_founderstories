from __future__ import annotations

import json
from pathlib import Path


AUTH_FILE = Path(__file__).parent / "secrets" / "mailer_auth.json"


def validate_mailer_auth(username: str, password: str) -> bool:
    username = (username or "").strip()
    password = (password or "").strip()

    if not username or not password:
        return False

    try:
        data = json.loads(AUTH_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return False
    except Exception:
        return False

    return username == (data.get("username") or "") and password == (data.get("password") or "")
