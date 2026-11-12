"""End-to-end tests for the collector's durability and rotation guarantees.

Run from the project root with:
    PYTHONPATH=/path/to/flask/site-packages python3 -m unittest -v
(plain `python3 -m unittest` works when Flask is already installed.)
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from app.collector import Collector, _validate_record
from app.config import Config
from app.db import connect, init_db


def make_config(root: str, **overrides) -> Config:
    cfg = Config(
        watch_dir=os.path.join(root, "gateway"),
        active_name="temperature.log",
        db_path=os.path.join(root, "db", "collector.db"),
        poll_interval=0.05,
        max_line_bytes=4096,
        batch_size=50,
        autostart=False,
        http_host="127.0.0.1",
        http_port=8000,
    )
    os.makedirs(cfg.watch_dir, exist_ok=True)
    for key, value in overrides.items():
        object.__setattr__(cfg, key, value)
    return cfg


def good_line(waybill: str = "WB-1001", temp: float = 2.5) -> str:
    return json.dumps(
        {
            "waybill": waybill,
            "ts_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "temperature": temp,
        },
        ensure_ascii=False,
    )


def append(path: str, text: str) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(text)


def rows(cfg: Config) -> list[sqlite3.Row]:
    conn = connect(cfg.db_path)
    try:
        return list(conn.execute("SELECT * FROM readings ORDER BY id"))
    finally:
        conn.close()


def progress(cfg: Config) -> list[sqlite3.Row]:
    conn = connect(cfg.db_path)
    try:
        return list(conn.execute("SELECT * FROM ingest_progress"))
    finally:
        conn.close()


class ValidatorTests(unittest.TestCase):
    def test_valid_record(self):
        rec, err = _validate_record(good_line())
        self.assertIsNone(err)
        self.assertEqual(rec["waybill"], "WB-1001")
        self.assertTrue(rec["ts_utc"].endswith("Z"))

    def test_bad_json(self):
        _, err = _validate_record("{not json")
        self.assertIn("invalid JSON", err)

    def test_non_utc_rejected(self):
        line = json.dumps(
            {"waybill": "W", "ts_utc": "2026-01-01T10:00:00+08:00",
             "temperature": 1}
        )
        _, err = _validate_record(line)
        self.assertIn("not UTC", err)

    def test_naive_timestamp_rejected(self):
        line = json.dumps(
            {"waybill": "W", "ts_utc": "2026-01-01T10:00:00", "temperature": 1}
        )
        _, err = _validate_record(line)
        self.assertIn("not UTC", err)

    def test_boolean_temperature_rejected(self):
        line = json.dumps(
            {"waybill": "W", "ts_utc": "2026-01-01T10:00:00Z",
             "temperature": True}
        )
        _, err = _validate_record(line)
        self.assertIn("numeric", err)

    def test_missing_waybill(self):
        line = json.dumps(
            {"ts_utc": "2026-01-01T10:00:00Z", "temperature": 1})
        _, err = _validate_record(line)
        self.assertIn("waybill", err)


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.cfg = make_config(self.root)
        init_db(self.cfg.db_path)
        self.collector = Collector(self.cfg)
        self.active = self.cfg.active_path

    def tearDown(self):
        self.tmp.cleanup()

    def test_basic_ingest_and_half_line_wait(self):
        append(self.active, good_line() + "\n")
        append(self.active, good_line("WB-1002", 4.0) + "\n")
        self.collector.tick()
        self.assertEqual(len(rows(self.cfg)), 2)

        # half line with no newline: nothing new may be stored
        line = good_line("WB-1003", -1.0)
        append(self.active, line[:12])
        self.collector.tick()
        self.assertEqual(len(rows(self.cfg)), 2)
        p = progress(self.cfg)[0]
        self.assertGreater(p["pending_bytes"], 0)
        self.assertEqual(p["status"], "active")

        # continuation arrives; the reassembled line is ingested once
        append(self.active, line[12:] + "\n")
        self.collector.tick()
        data = rows(self.cfg)
        self.assertEqual(len(data), 3)
        self.assertEqual(data[-1]["waybill"], "WB-1003")

    def test_bad_complete_lines_are_logged_and_skipped(self):
        append(self.active, good_line() + "\n")
        append(self.active, "{broken json\n")
        append(self.active, json.dumps({"waybill": "W"}) + "\n")  # missing fields
        append(self.active, good_line("WB-1009", 0.5) + "\n")
        self.collector.tick()

        data = rows(self.cfg)
        self.assertEqual([r["waybill"] for r in data], ["WB-1001", "WB-1009"])
        conn = connect(self.cfg.db_path)
        try:
            errs = list(conn.execute("SELECT * FROM ingest_errors ORDER BY id"))
        finally:
            conn.close()
        self.assertEqual(len(errs), 2)
        # positions are byte offsets of the start of each bad line
        self.assertEqual(errs[0]["byte_offset"], len(good_line()) + 1)
        self.assertTrue(errs[0]["reason"])
        self.assertIn("broken json", errs[0]["raw_line"])
        # bad lines still count in the source line numbering
        self.assertEqual(data[1]["line_no"], 4)

    def test_identical_lines_both_persisted(self):
        line = good_line()
        append(self.active, (line + "\n") * 3)
        self.collector.tick()
        self.assertEqual(len(rows(self.cfg)), 3)

    def test_rename_rotation_keeps_file_identity(self):
        append(self.active, (good_line() + "\n") * 5)
        self.collector.tick()
        self.assertEqual(len(rows(self.cfg)), 5)
        old_stat = os.stat(self.active)

        # gateway-style rotation: rename old file, new active file gets name
        rotated = os.path.join(self.cfg.watch_dir, "temperature.log.rot1")
        os.rename(self.active, rotated)
        with open(self.active, "w", encoding="utf-8") as fh:
            fh.write(good_line("WB-2001", 9.9) + "\n")
        new_stat = os.stat(self.active)
        self.assertNotEqual(
            (old_stat.st_dev, old_stat.st_ino),
            (new_stat.st_dev, new_stat.st_ino),
        )

        self.collector.tick()
        data = rows(self.cfg)
        self.assertEqual(len(data), 6)
        p = progress(self.cfg)
        by_ino = {(x["dev"], x["ino"]): x for x in p}
        old = by_ino[(old_stat.st_dev, old_stat.st_ino)]
        new = by_ino[(new_stat.st_dev, new_stat.st_ino)]
        self.assertEqual(old["path"], rotated)
        self.assertEqual(old["status"], "completed")
        self.assertEqual(old["committed_off"], old_stat.st_size)
        self.assertEqual(new["status"], "active")
        self.assertEqual(new["committed_off"], new_stat.st_size)

    def test_rotated_file_with_half_line_is_completed_on_continuation(self):
        line = good_line("WB-3001", 3.3)
        append(self.active, (good_line() + "\n") * 2)
        append(self.active, line[:15])  # unterminated half line
        self.collector.tick()
        self.assertEqual(len(rows(self.cfg)), 2)

        rotated = os.path.join(self.cfg.watch_dir, "temperature.log.rot1")
        os.rename(self.active, rotated)
        with open(self.active, "w", encoding="utf-8") as fh:
            fh.write(good_line("WB-NEW", 1.1) + "\n")
        self.collector.tick()

        old = next(p for p in progress(self.cfg) if p["path"] == rotated)
        self.assertEqual(old["status"], "rotated_pending")
        self.assertGreater(old["pending_bytes"], 0)

        # old gateway process finishes the record inside the renamed file
        append(rotated, line[15:] + "\n")
        self.collector.tick()
        waybills = [r["waybill"] for r in rows(self.cfg)]
        self.assertEqual(waybills.count("WB-3001"), 1)
        old = next(p for p in progress(self.cfg) if p["path"] == rotated)
        self.assertEqual(old["status"], "completed")
        self.assertEqual(old["pending_bytes"], 0)

    def test_restart_resumes_from_committed_offset_without_duplicates(self):
        append(self.active, (good_line() + "\n") * 10)
        self.collector.tick()
        self.assertEqual(len(rows(self.cfg)), 10)
        off_before = progress(self.cfg)[0]["committed_off"]

        # simulate process restart: brand-new collector over the same DB
        collector2 = Collector(self.cfg)
        append(self.active, (good_line("WB-R", 7.7) + "\n") * 4)
        collector2.tick()

        data = rows(self.cfg)
        self.assertEqual(len(data), 14)
        self.assertEqual(off_before, progress(self.cfg)[0]["committed_off"] -
                         sum(len(good_line("WB-R", 7.7)) + 1 for _ in range(4)))
        self.assertEqual(sum(1 for r in data if r["waybill"] == "WB-R"), 4)

    def test_exactly_once_under_repeated_ticks_and_rotations(self):
        total = 0
        for rot in range(3):
            for _ in range(7):
                append(self.active, good_line(f"WB-{rot}", 1.0) + "\n")
                total += 1
                self.collector.tick()  # ticks while file grows
            if rot < 2:
                os.rename(
                    self.active,
                    os.path.join(self.cfg.watch_dir, f"temperature.log.r{rot}"),
                )
        self.collector.tick()
        data = rows(self.cfg)
        self.assertEqual(len(data), total)
        # every (source inode, line number) pair appears at most once
        pairs = [(r["source_ino"], r["line_no"]) for r in data]
        self.assertEqual(len(pairs), len(set(pairs)))

    def test_overlong_unterminated_line_is_skipped_when_newline_arrives(self):
        # threshold large enough for a normal line, small enough for the junk
        cfg = make_config(self.root, max_line_bytes=128)
        collector = Collector(cfg)
        append(self.active, good_line() + "\n")
        append(self.active, "x" * 200)  # no newline yet -> must not buffer all
        collector.tick()
        self.assertEqual(len(rows(cfg)), 1)
        p = progress(cfg)[0]
        self.assertEqual(p["skipping"], 1)  # state survives between ticks
        append(cfg.active_path, "\n")  # the bad line terminates
        append(self.active, good_line("WB-AFTER", 2.2) + "\n")
        collector.tick()
        data = rows(cfg)
        self.assertEqual([r["waybill"] for r in data], ["WB-1001", "WB-AFTER"])
        conn = connect(cfg.db_path)
        try:
            errs = list(conn.execute("SELECT * FROM ingest_errors"))
        finally:
            conn.close()
        self.assertEqual(len(errs), 1)
        self.assertIn("exceeds", errs[0]["reason"])
        self.assertEqual(progress(cfg)[0]["skipping"], 0)

    def test_complete_overlong_line_is_bad_line(self):
        cfg = make_config(self.root, max_line_bytes=128)
        collector = Collector(cfg)
        append(self.active, good_line() + "\n")
        append(self.active, ("y" * 200) + "\n")  # terminated, but too long
        append(self.active, good_line("WB-AFTER", 2.2) + "\n")
        collector.tick()
        data = rows(cfg)
        self.assertEqual([r["waybill"] for r in data], ["WB-1001", "WB-AFTER"])
        conn = connect(cfg.db_path)
        try:
            errs = list(conn.execute("SELECT * FROM ingest_errors"))
        finally:
            conn.close()
        self.assertEqual(len(errs), 1)
        self.assertIn("exceeds", errs[0]["reason"])

    def test_invalid_utf8_recorded_as_bad_line(self):
        append(self.active, good_line() + "\n")
        with open(self.active, "ab") as fh:
            fh.write(b"\xff\xfe\x00not-utf8\n")
        append(self.active, good_line("WB-OK", 5.5) + "\n")
        self.collector.tick()
        self.assertEqual(
            [r["waybill"] for r in rows(self.cfg)], ["WB-1001", "WB-OK"]
        )

    def test_new_collector_picks_up_preexisting_rotated_files(self):
        # process was down during rotation: files it has never seen exist now
        old_path = os.path.join(self.cfg.watch_dir, "temperature.log.old")
        with open(old_path, "w", encoding="utf-8") as fh:
            for _ in range(3):
                fh.write(good_line("WB-OLD", 1.0) + "\n")
        with open(self.active, "w", encoding="utf-8") as fh:
            fh.write(good_line("WB-NEW", 2.0) + "\n")
        Collector(self.cfg).tick()
        data = rows(self.cfg)
        self.assertEqual(len(data), 4)
        statuses = {p["path"]: p["status"] for p in progress(self.cfg)}
        self.assertEqual(statuses[old_path], "completed")
        self.assertEqual(statuses[self.active], "active")


class ThreadLifecycleTests(unittest.TestCase):
    def test_start_stop_thread(self):
        with tempfile.TemporaryDirectory() as root:
            cfg = make_config(root, poll_interval=0.02)
            init_db(cfg.db_path)
            c = Collector(cfg)
            c.start()
            c.start()  # idempotent
            self.assertTrue(c.running)
            time.sleep(0.1)
            c.stop()
            self.assertFalse(c.running)
            self.assertGreaterEqual(c.tick_count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
