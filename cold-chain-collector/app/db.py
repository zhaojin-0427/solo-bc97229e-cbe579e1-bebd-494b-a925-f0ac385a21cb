"""SQLite connection management and schema.

The reader thread (collector) and the Flask request threads open separate
connections. WAL mode lets the writer commit while readers see the last
fully committed snapshot -- progress and rows become visible together.
"""
from __future__ import annotations

import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS ingest_progress (
    dev            INTEGER NOT NULL,
    ino            INTEGER NOT NULL,
    path           TEXT    NOT NULL,
    committed_off  INTEGER NOT NULL DEFAULT 0,
    line_no        INTEGER NOT NULL DEFAULT 0,
    pending_bytes  INTEGER NOT NULL DEFAULT 0,
    skipping       INTEGER NOT NULL DEFAULT 0,
    status         TEXT    NOT NULL DEFAULT 'active',
    first_seen     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (dev, ino)
);

CREATE TABLE IF NOT EXISTS readings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    waybill     TEXT    NOT NULL,
    ts_utc      TEXT    NOT NULL,
    temperature REAL    NOT NULL,
    source_dev  INTEGER NOT NULL,
    source_ino  INTEGER NOT NULL,
    source_path TEXT    NOT NULL,
    line_no     INTEGER NOT NULL,
    raw_line    TEXT
);

CREATE INDEX IF NOT EXISTS idx_readings_waybill
    ON readings (waybill, ts_utc, id);

CREATE TABLE IF NOT EXISTS ingest_errors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_dev  INTEGER NOT NULL,
    source_ino  INTEGER NOT NULL,
    source_path TEXT    NOT NULL,
    line_no     INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    reason      TEXT    NOT NULL,
    raw_line    TEXT,
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_errors_file
    ON ingest_errors (source_dev, source_ino, line_no);
"""


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
