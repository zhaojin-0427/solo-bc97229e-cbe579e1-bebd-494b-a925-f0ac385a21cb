#!/usr/bin/env python3
"""Local cold-chain gateway simulator.

Appends UTF-8 NDJSON temperature records to a single active log file and
performs *rename rotation*: once the active file reaches a line threshold it is
closed and renamed (never deleted), then a brand-new file reuses the original
name. It can also deliberately:

* append a record in two writes with the newline delayed (a half line), so the
  collector must wait for the continuation;
* append complete but malformed lines, so the collector's bad-line handling can
  be demonstrated.

Configuration is via CLI flags, environment variables, or both (flags win).
Designed to run forever until SIGTERM/SIGINT, which it handles gracefully.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

STOP = False


def _handle_signal(signum, frame):  # noqa: ARG001
    global STOP
    STOP = True


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def utc_now() -> str:
    # Millisecond precision keeps demo ordering readable.
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


class Gateway:
    def __init__(self, args: argparse.Namespace) -> None:
        self.dir = args.watch_dir
        self.file_name = args.file
        self.active_path = os.path.join(self.dir, self.file_name)
        self.interval = args.interval
        self.rotate_every = args.rotate_every
        self.half_line_every = args.half_line_every
        self.bad_every = args.bad_every
        self.waybills = [f"WB-{1001 + i}" for i in range(args.waybills)]
        self.rng = random.Random(args.seed)
        self.lines_in_file = 0
        self.total_lines = 0
        self.rotation_seq = 0
        self.fd = -1
        os.makedirs(self.dir, exist_ok=True)

    # ------------------------------------------------------------ file handling
    def _open_active(self) -> None:
        # O_APPEND: even if the collector is mid-scan, writes stay ordered and
        # the rename never disturbs the old inode.
        self.fd = os.open(
            self.active_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644
        )
        self.lines_in_file = 0

    def _write(self, data: str) -> None:
        os.write(self.fd, data.encode("utf-8"))
        os.fsync(self.fd)  # make partial writes durable, like a real gateway

    def _rotate(self) -> None:
        os.close(self.fd)
        self.rotation_seq += 1
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        rotated_name = f"{self.file_name}.{stamp}.{self.rotation_seq:03d}"
        os.rename(
            self.active_path, os.path.join(self.dir, rotated_name)
        )  # old file retained on disk, inode unchanged
        print(f"[gateway] rotated -> {rotated_name}", flush=True)
        self._open_active()

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    # ------------------------------------------------------------- line factories
    def _good_record(self) -> str:
        waybill = self.rng.choice(self.waybills)
        # Plausible reefer temperature with slow random walk.
        temp = round(self.rng.uniform(-4.0, 6.0), 2)
        return json.dumps(
            {"waybill": waybill, "ts_utc": utc_now(), "temperature": temp},
            ensure_ascii=False,
        )

    def _bad_line(self) -> str:
        kind = self.total_lines % 4
        if kind == 0:
            return '{"waybill":"WB-BAD","ts_utc":"' + utc_now() + '","temperature":}'
        if kind == 1:
            return json.dumps({"ts_utc": utc_now(), "temperature": 3.2})
        if kind == 2:
            return json.dumps(
                {"waybill": "WB-BAD", "ts_utc": "2026-11-12T10:00:00+08:00",
                 "temperature": 3.2}
            )
        return json.dumps(
            {"waybill": "WB-BAD", "ts_utc": utc_now(), "temperature": "cold"}
        )

    # -------------------------------------------------------------------- tick
    def tick(self) -> None:
        if self.rotate_every and self.lines_in_file >= self.rotate_every:
            self._rotate()

        self.total_lines += 1

        if self.bad_every and self.total_lines % self.bad_every == 0:
            self._write(self._bad_line() + "\n")
            self.lines_in_file += 1
            return

        line = self._good_record()
        if self.half_line_every and self.total_lines % self.half_line_every == 0:
            # Split a record across two writes; the first write has no newline,
            # so the collector must hold the half line and wait.
            cut = max(8, len(line) // 2)
            self._write(line[:cut])
            print("[gateway] half-line written, continuation next tick", flush=True)
            time.sleep(self.interval)
            self._write(line[cut:] + "\n")
        else:
            self._write(line + "\n")
        self.lines_in_file += 1

    def run(self) -> None:
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)
        self._open_active()
        print(
            f"[gateway] appending to {self.active_path} every {self.interval}s "
            f"(rotate={self.rotate_every}, half-line={self.half_line_every}, "
            f"bad={self.bad_every})",
            flush=True,
        )
        try:
            while not STOP:
                self.tick()
                # Interruptible sleep so SIGTERM exits promptly.
                deadline = time.monotonic() + self.interval
                while not STOP and time.monotonic() < deadline:
                    time.sleep(min(0.2, deadline - time.monotonic()))
        finally:
            self.close()
            print(f"[gateway] stopped after {self.total_lines} lines", flush=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cold-chain gateway simulator")
    p.add_argument("--watch-dir", default=env_str("WATCH_DIR", "/data/gateway"))
    p.add_argument("--file", default=env_str("ACTIVE_FILE", "temperature.log"))
    p.add_argument("--interval", type=float,
                   default=env_float("EMIT_INTERVAL", 1.0),
                   help="seconds between record writes")
    p.add_argument("--rotate-every", type=int,
                   default=env_int("ROTATE_EVERY", 20),
                   help="rename-rotate after N lines (0 disables)")
    p.add_argument("--half-line-every", type=int,
                   default=env_int("HALF_LINE_EVERY", 7),
                   help="emit one record as a half line every N lines (0 off)")
    p.add_argument("--bad-every", type=int,
                   default=env_int("BAD_LINE_EVERY", 11),
                   help="emit one complete malformed line every N lines (0 off)")
    p.add_argument("--waybills", type=int, default=env_int("WAYBILLS", 4))
    p.add_argument("--seed", type=int, default=env_int("SEED", 20261112))
    return p.parse_args(argv)


if __name__ == "__main__":
    Gateway(parse_args()).run()
    sys.exit(0)
