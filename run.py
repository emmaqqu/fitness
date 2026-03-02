from __future__ import annotations

import os

from app import app


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_port(value: str | None, default: int = 5000) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def main() -> None:
    host = os.getenv("HOST", "127.0.0.1")
    port = _as_port(os.getenv("PORT"), 5000)
    debug = _as_bool(os.getenv("DEBUG"), True)
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
