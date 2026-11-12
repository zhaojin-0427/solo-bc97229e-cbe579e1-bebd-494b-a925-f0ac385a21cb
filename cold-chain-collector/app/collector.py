"""File-tailing collector with rotation support and exactly-once ingestion.

Design guarantees
-----------------
* Identity is the file's ``(st_dev, st_ino)`` pair, never its name. When the
  gateway rotates, the old file is renamed (inode unchanged) and a new active
  file appears under the old name; both are tracked independently.
* Progress is only ever advanced past newline-terminated records. A trailing
  half line is left in the file and re-read from the last committed offset on
  the next tick (or after a process restart), so partial writes are never lost
  and never stored.
* Every good line is inserted in the same SQLite transaction as the progress
  update (and bad-line rows). A crash therefore can only re-process lines that
  were never committed, which makes each complete valid line land exactly once.
* Lines are processed independently: identical content on two different raw
  lines produces two separate ``readings`` rows.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

from .db import connect

READ_CHUNK = 64 * 1024
MAX_RAW_STORED = 10_000  # truncate bad raw lines kept for diagnostics


def _validate_record(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    """Return ``(record, None)`` for a valid line or ``(None, reason)``."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid JSON: {exc.msg} at column {exc.colno}"
    if not isinstance(obj, dict):
        return None, "JSON value is not an object"

    waybill = obj.get("waybill")
    if not isinstance(waybill, str) or not waybill.strip():
        return None, "missing or empty 'waybill'"
    waybill = waybill.strip()

    ts_raw = obj.get("ts_utc", obj.get("timestamp"))
    if not isinstance(ts_raw, str) or not ts_raw.strip():
        return None, "missing or empty 'ts_utc'"
    try:
        dt = datetime.fromisoformat(ts_raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None, f"unparseable UTC timestamp: {ts_raw!r}"
    if dt.tzinfo is None or dt.utcoffset() != timezone.utc.utcoffset(None):
        return None, f"timestamp is not UTC: {ts_raw!r}"
    ts_utc = dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    temp = obj.get("temperature")
    if isinstance(temp, bool) or not isinstance(temp, (int, float)):
        return None, "missing or non-numeric 'temperature'"
    if temp != temp or temp in (float("inf"), float("-inf")):
        return None, "'temperature' is NaN or infinity"

    return (
        {"waybill": waybill, "ts_utc": ts_utc, "temperature": float(temp)},
        None,
    )


class Collector:
    """Background thread that scans ``watch_dir`` once per poll interval."""

    def __init__(self, config) -> None:
        self.config = config
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()  # serialises start/stop with status reads
        self.running = False
        self.last_tick_at: str | None = None
        self.last_error: str | None = None
        self.tick_count = 0

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        with self._lock:
            if self.running:
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="collector", daemon=True
            )
            self.running = True
            self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        with self._lock:
            if not self.running:
                return
            self._stop.set()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        with self._lock:
            self.running = False
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
                self.last_error = None
            except Exception as exc:  # keep the loop alive; surface in the UI
                self.last_error = f"{type(exc).__name__}: {exc}"
            self._stop.wait(self.config.poll_interval)

    # ------------------------------------------------------------------ main tick
    def tick(self) -> dict[str, Any]:
        """One scan of the watch directory. Exposed for tests."""
        cfg = self.config
        summary = {"files_seen": 0, "rows_inserted": 0, "errors_recorded": 0}
        if not os.path.isdir(cfg.watch_dir):
            raise FileNotFoundError(f"watch directory does not exist: {cfg.watch_dir}")

        seen: dict[tuple[int, int], tuple[str, os.stat_result]] = {}
        for name in sorted(os.listdir(cfg.watch_dir)):
            path = os.path.join(cfg.watch_dir, name)
            try:
                st = os.stat(path)
            except FileNotFoundError:
                continue  # rotated away between listdir and stat
            if os.path.islink(path) or not os.path.isfile(path):
                continue
            seen.setdefault((st.st_dev, st.st_ino), (path, st))
        summary["files_seen"] = len(seen)

        conn = connect(cfg.db_path)
        try:
            progress = {
                (row["dev"], row["ino"]): row
                for row in conn.execute("SELECT * FROM ingest_progress")
            }

            def order(item):
                (_key, (path, _st)) = item
                # finish old (rotated) files before touching the active one
                return (1 if path == cfg.active_path else 0, path)

            for key, (path, st) in sorted(seen.items(), key=order):
                row = progress.get(key)
                if row is None:
                    state = self._insert_progress(conn, key, path)
                else:
                    state = self._touch_progress(conn, row, path)
                inserted, errors = self._process_file(conn, st, state)
                summary["rows_inserted"] += inserted
                summary["errors_recorded"] += errors
            conn.commit()
        finally:
            conn.close()

        self.last_tick_at = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        self.tick_count += 1
        return summary

    # ------------------------------------------------------------- progress rows
    def _insert_progress(self, conn, key, path: str) -> dict[str, Any]:
        dev, ino = key
        status = "active" if path == self.config.active_path else "rotated"
        conn.execute(
            "INSERT INTO ingest_progress (dev, ino, path, status) "
            "VALUES (?, ?, ?, ?)",
            (dev, ino, path, status),
        )
        return {
            "dev": dev,
            "ino": ino,
            "path": path,
            "committed_off": 0,
            "line_no": 0,
            "pending_bytes": 0,
            "skipping": 0,
            "status": status,
        }

    def _touch_progress(self, conn, row, path: str) -> dict[str, Any]:
        if path == self.config.active_path:
            status = "active"
        elif row["status"] == "active":
            # inode lost the active name -> it has been renamed/rotated
            status = "rotated"
        else:
            status = row["status"]
        if row["path"] != path:
            conn.execute(
                "UPDATE ingest_progress SET path = ?, updated_at = "
                "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE dev = ? AND ino = ?",
                (path, row["dev"], row["ino"]),
            )
        return {
            "dev": row["dev"],
            "ino": row["ino"],
            "path": path,
            "committed_off": row["committed_off"],
            "line_no": row["line_no"],
            "pending_bytes": row["pending_bytes"],
            "skipping": row["skipping"],
            "status": status,
        }

    # ------------------------------------------------------------- per-file read
    def _process_file(
        self, conn: sqlite3.Connection, st: os.stat_result, state: dict[str, Any]
    ) -> tuple[int, int]:
        cfg = self.config
        dev, ino = state["dev"], state["ino"]
        path = state["path"]
        pos = line_start = int(state["committed_off"])
        line_no = int(state["line_no"])
        skipping = bool(state["skipping"])
        buf = bytearray()
        good: list[tuple] = []
        bad: list[tuple] = []
        inserted_total = 0
        errors_total = 0

        try:
            fd = os.open(path, os.O_RDONLY)
        except FileNotFoundError:
            return 0, 0  # vanished this tick; picked up again when it reappears

        try:
            if pos > st.st_size:
                # Inode was truncated or reused behind our back: restart file.
                bad.append(
                    (line_no + 1, 0,
                     f"file truncated; committed offset {pos} > size {st.st_size}",
                     None)
                )
                pos = line_start = 0
                line_no = 0
                skipping = False
                buf.clear()
            os.lseek(fd, pos, os.SEEK_SET)

            while True:
                chunk = os.read(fd, READ_CHUNK)
                if not chunk:
                    break

                if skipping:
                    nl = chunk.find(b"\n")
                    if nl == -1:
                        pos += len(chunk)
                        continue
                    pos += nl + 1
                    bad.append(
                        (line_no + 1, line_start,
                         f"line exceeds {cfg.max_line_bytes} bytes; skipped", None)
                    )
                    line_no += 1
                    skipping = False
                    line_start = pos
                    chunk = chunk[nl + 1:]
                    if not chunk:
                        continue

                buf += chunk
                pos += len(chunk)
                while b"\n" in buf:
                    idx = buf.index(b"\n")
                    line_bytes = bytes(buf[:idx])
                    del buf[: idx + 1]
                    line_end = line_start + idx + 1
                    self._handle_line(
                        line_bytes, line_no + 1, line_start, good, bad
                    )
                    line_no += 1
                    line_start = line_end

                    if len(good) + len(bad) >= cfg.batch_size:
                        self._flush(
                            conn, state, good, bad,
                            committed_off=line_start, line_no=line_no, st=st,
                            skipping=skipping,
                        )
                        inserted_total += len(good)
                        errors_total += len(bad)
                        good.clear()
                        bad.clear()

                if len(buf) > cfg.max_line_bytes:
                    # Unterminated and already too long: drop from memory and
                    # keep scanning byte-wise until its newline arrives.
                    skipping = True
                    buf.clear()

            # Fresh size: the file may have grown while we were reading, so the
            # st captured at tick start must not be used for the tail math.
            end_size = os.fstat(fd).st_size
            pending_bytes = max(end_size - line_start, 0)
            if path == cfg.active_path:
                status = "active"
            elif line_start >= end_size:
                status = "completed"  # every byte was part of a committed line
            else:
                status = "rotated_pending"  # half line still sits in an old file
            self._flush(
                conn, state, good, bad,
                committed_off=line_start, line_no=line_no, st=st,
                pending_bytes=pending_bytes, status=status,
                skipping=skipping,
            )
            inserted_total += len(good)
            errors_total += len(bad)
            return inserted_total, errors_total
        finally:
            os.close(fd)

    def _handle_line(
        self,
        line_bytes: bytes,
        line_no: int,
        offset: int,
        good: list[tuple],
        bad: list[tuple],
    ) -> None:
        if len(line_bytes) > self.config.max_line_bytes:
            bad.append(
                (line_no, offset,
                 f"line exceeds {self.config.max_line_bytes} bytes; skipped", None)
            )
            return
        try:
            raw = line_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            bad.append((line_no, offset, f"invalid UTF-8: {exc}", None))
            return
        if raw.endswith("\r"):
            raw = raw[:-1]
        record, reason = _validate_record(raw)
        if reason is not None:
            bad.append((line_no, offset, reason, raw[:MAX_RAW_STORED]))
            return
        good.append(
            (line_no, offset, record["waybill"], record["ts_utc"],
             record["temperature"], raw)
        )

    def _flush(
        self,
        conn: sqlite3.Connection,
        state: dict[str, Any],
        good: list[tuple],
        bad: list[tuple],
        committed_off: int,
        line_no: int,
        st: os.stat_result,
        pending_bytes: int | None = None,
        status: str | None = None,
        skipping: bool = False,
    ) -> None:
        """Persist rows and progress together in the surrounding transaction."""
        dev, ino, path = state["dev"], state["ino"], state["path"]
        if pending_bytes is None:
            pending_bytes = max(st.st_size - committed_off, 0)
        if status is None:
            status = "active" if path == self.config.active_path else state["status"]

        conn.executemany(
            "INSERT INTO readings (waybill, ts_utc, temperature, source_dev, "
            "source_ino, source_path, line_no, raw_line) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (waybill, ts, temp, dev, ino, path, row_line_no, raw)
                for (row_line_no, _off, waybill, ts, temp, raw) in good
            ],
        )
        conn.executemany(
            "INSERT INTO ingest_errors (source_dev, source_ino, source_path, "
            "line_no, byte_offset, reason, raw_line) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (dev, ino, path, row_line_no, off, reason, raw)
                for (row_line_no, off, reason, raw) in bad
            ],
        )
        conn.execute(
            "UPDATE ingest_progress SET committed_off = ?, line_no = ?, "
            "pending_bytes = ?, skipping = ?, status = ?, updated_at = "
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE dev = ? AND ino = ?",
            (committed_off, line_no, pending_bytes, int(skipping), status, dev, ino),
        )
        # keep in-memory state consistent for the rest of this file's pass
        state["committed_off"] = committed_off
        state["line_no"] = line_no
        state["pending_bytes"] = pending_bytes
        state["skipping"] = int(skipping)
        state["status"] = status
