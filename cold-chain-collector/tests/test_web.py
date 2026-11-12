"""HTTP/API tests using Flask's test client (no network sockets needed)."""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone

from tests.test_collector import good_line, make_config
from app.web import create_app


class WebApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = make_config(self.tmp.name, autostart=False)
        self.app, self.collector = create_app(self.cfg)
        self.client = self.app.test_client()

    def tearDown(self):
        self.collector.stop()
        self.tmp.cleanup()

    def _seed(self, n=3, waybill="WB-1001"):
        with open(self.cfg.active_path, "w", encoding="utf-8") as fh:
            for i in range(n):
                fh.write(good_line(waybill, float(i)) + "\n")
        self.collector.tick()

    def test_health_and_index(self):
        self.assertEqual(self.client.get("/healthz").status_code, 200)
        page = self.client.get("/")
        self.assertEqual(page.status_code, 200)
        self.assertIn("冷链", page.get_data(as_text=True))

    def test_status_progress_and_errors(self):
        self._seed()
        with open(self.cfg.active_path, "a", encoding="utf-8") as fh:
            fh.write("{bad\n")
        self.collector.tick()

        body = self.client.get("/api/status").get_json()
        self.assertFalse(body["collector"]["running"])  # autostart disabled
        self.assertEqual(body["totals"]["readings"], 3)
        self.assertEqual(body["totals"]["errors"], 1)
        self.assertEqual(len(body["progress"]), 1)
        self.assertEqual(body["progress"][0]["status"], "active")

        errs = self.client.get("/api/errors").get_json()
        self.assertEqual(errs["count"], 1)
        self.assertEqual(errs["errors"][0]["byte_offset"],
                         sum(len(good_line("WB-1001", float(i))) + 1 for i in range(3)))

    def test_start_stop_endpoints(self):
        r = self.client.post("/api/start")
        self.assertTrue(r.get_json()["running"])
        r = self.client.post("/api/stop")
        self.assertFalse(r.get_json()["running"])

    def test_waybill_endpoint_and_duplicate_content(self):
        line = good_line("WB-DUP", 2.0)
        with open(self.cfg.active_path, "w", encoding="utf-8") as fh:
            fh.write((line + "\n") * 2)
            fh.write(good_line("WB-OTHER", 9.0) + "\n")
        self.collector.tick()

        body = self.client.get("/api/waybills/WB-DUP").get_json()
        self.assertEqual(body["count"], 2)  # identical raw lines kept separately
        self.assertEqual(body["stats"]["max_temperature"], 2.0)
        self.assertEqual(len(body["readings"]), 2)

        other = self.client.get("/api/waybills/WB-OTHER").get_json()
        self.assertEqual(other["count"], 1)
        missing = self.client.get("/api/waybills/NOPE").get_json()
        self.assertEqual(missing["count"], 0)

        listing = self.client.get("/api/waybills").get_json()
        self.assertEqual(listing["count"], 2)

    def test_waybill_url_encoded(self):
        self._seed(1, waybill="WB 1001")
        body = self.client.get("/api/waybills/WB%201001").get_json()
        self.assertEqual(body["count"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
