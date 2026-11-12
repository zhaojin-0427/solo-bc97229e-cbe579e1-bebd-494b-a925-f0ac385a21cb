"""Entry point: build the app/collector and serve HTTP."""
from __future__ import annotations

import atexit
import logging

from .config import Config
from .web import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("cold-chain")

config = Config()
app, collector = create_app(config)
atexit.register(collector.stop)


if __name__ == "__main__":
    log.info(
        "starting collector web on %s:%s (watch=%s, db=%s, autostart=%s)",
        config.http_host,
        config.http_port,
        config.watch_dir,
        config.db_path,
        config.autostart,
    )
    app.run(
        host=config.http_host,
        port=config.http_port,
        threaded=True,
        use_reloader=False,
    )
