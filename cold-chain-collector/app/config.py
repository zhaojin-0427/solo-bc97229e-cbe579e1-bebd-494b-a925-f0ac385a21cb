"""Runtime configuration loaded from environment variables.

All paths point inside the container; docker-compose bind-mounts two host
directories (the gateway's watch directory and a persistent data directory).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    watch_dir: str = os.environ.get("WATCH_DIR", "/data/gateway")
    # The active log file name the gateway keeps appending to.
    active_name: str = os.environ.get("ACTIVE_FILE", "temperature.log")
    db_path: str = os.environ.get("DB_PATH", "/data/collector/collector.db")
    poll_interval: float = float(os.environ.get("POLL_INTERVAL", "1.0"))
    # Maximum size in bytes of a single (newline-terminated) line.
    # Longer lines are recorded as bad lines and skipped, never buffered
    # into memory wholesale.
    max_line_bytes: int = int(os.environ.get("MAX_LINE_BYTES", str(1024 * 1024)))
    # How many good lines/errors are flushed (with progress) per transaction.
    batch_size: int = int(os.environ.get("BATCH_SIZE", "200"))
    autostart: bool = _bool("AUTOSTART", True)
    http_host: str = os.environ.get("HTTP_HOST", "0.0.0.0")
    http_port: int = int(os.environ.get("HTTP_PORT", "8000"))

    @property
    def active_path(self) -> str:
        return os.path.join(self.watch_dir, self.active_name)
