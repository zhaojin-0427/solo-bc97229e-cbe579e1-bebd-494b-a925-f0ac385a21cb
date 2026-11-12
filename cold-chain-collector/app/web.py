"""Flask application: control panel, progress/error views, waybill API."""
from __future__ import annotations

from flask import Flask, abort, jsonify, render_template, request

from .collector import Collector
from .config import Config
from .db import connect, init_db


def create_app(config: Config | None = None) -> tuple[Flask, Collector]:
    cfg = config or Config()
    init_db(cfg.db_path)
    collector = Collector(cfg)

    app = Flask(__name__)
    app.config["COLLECTOR"] = collector

    if cfg.autostart:
        collector.start()

    # ----------------------------------------------------------------- pages
    @app.get("/")
    def index():
        return render_template("index.html", poll_interval=cfg.poll_interval)

    # ----------------------------------------------------------------- status
    @app.get("/api/status")
    def api_status():
        conn = connect(cfg.db_path)
        try:
            progress = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM ingest_progress ORDER BY status, path"
                )
            ]
            totals = conn.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM readings) AS readings, "
                "(SELECT COUNT(*) FROM ingest_errors) AS errors, "
                "(SELECT COUNT(DISTINCT waybill) FROM readings) AS waybills"
            ).fetchone()
        finally:
            conn.close()
        return jsonify(
            {
                "collector": {
                    "running": collector.running,
                    "tick_count": collector.tick_count,
                    "last_tick_at": collector.last_tick_at,
                    "last_error": collector.last_error,
                    "poll_interval_seconds": cfg.poll_interval,
                    "watch_dir": cfg.watch_dir,
                    "active_file": cfg.active_name,
                },
                "totals": dict(totals),
                "progress": progress,
            }
        )

    # ----------------------------------------------------------------- control
    @app.post("/api/start")
    def api_start():
        collector.start()
        return jsonify({"running": collector.running})

    @app.post("/api/stop")
    def api_stop():
        collector.stop()
        return jsonify({"running": collector.running})

    # ------------------------------------------------------------------ errors
    @app.get("/api/errors")
    def api_errors():
        limit = min(int(request.args.get("limit", 100)), 1000)
        conn = connect(cfg.db_path)
        try:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM ingest_errors ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
            ]
        finally:
            conn.close()
        return jsonify({"errors": rows, "count": len(rows)})

    # ------------------------------------------------------------- waybill API
    @app.get("/api/waybills/<waybill>")
    def api_waybill(waybill):
        if not waybill:
            abort(400, "waybill is required")
        limit = min(int(request.args.get("limit", 500)), 10_000)
        conn = connect(cfg.db_path)
        try:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT id, waybill, ts_utc, temperature, source_path, "
                    "line_no, raw_line FROM readings WHERE waybill = ? "
                    "ORDER BY ts_utc ASC, id ASC LIMIT ?",
                    (waybill, limit),
                )
            ]
            stats = conn.execute(
                "SELECT COUNT(*) AS n, MIN(ts_utc) AS first_ts, "
                "MAX(ts_utc) AS last_ts, MIN(temperature) AS min_temperature, "
                "MAX(temperature) AS max_temperature, "
                "AVG(temperature) AS avg_temperature "
                "FROM readings WHERE waybill = ?",
                (waybill,),
            ).fetchone()
        finally:
            conn.close()
        return jsonify(
            {
                "waybill": waybill,
                "count": len(rows),
                "stats": dict(stats),
                "readings": rows,
            }
        )

    @app.get("/api/waybills")
    def api_waybills():
        conn = connect(cfg.db_path)
        try:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT waybill, COUNT(*) AS readings, MIN(ts_utc) AS first_ts, "
                    "MAX(ts_utc) AS last_ts, MIN(temperature) AS min_temperature, "
                    "MAX(temperature) AS max_temperature "
                    "FROM readings GROUP BY waybill ORDER BY waybill"
                )
            ]
        finally:
            conn.close()
        return jsonify({"waybills": rows, "count": len(rows)})

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app, collector
