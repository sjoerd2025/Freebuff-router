"""CLI entrypoint: `python -m freebuffrouter` or the `freebuff-router` script."""
from __future__ import annotations

import logging
import os

import uvicorn

from . import config


def main() -> None:
    level = os.getenv("LOG_LEVEL", "info").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    uvicorn.run(
        "freebuffrouter.app:app",
        host=config.HOST,
        port=config.PORT,
        log_level=level.lower(),
    )


if __name__ == "__main__":
    main()
