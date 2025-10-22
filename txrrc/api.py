from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from .database import Database


class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in {"/", "/status"}:
            self.send_response(404)
            self.end_headers()
            return
        db = Database()
        with db.connect() as conn:
            counts = {}
            for table in ("well", "drilling_permit", "organization", "production_monthly", "field"):
                counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            last_lineage = conn.execute(
                "SELECT dataset_key, finished_at FROM lineage_ingest ORDER BY finished_at DESC LIMIT 1"
            ).fetchone()
        payload = {
            "counts": counts,
            "last_ingest": dict(last_lineage) if last_lineage else None,
        }
        body = json.dumps(payload).encode("utf8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve(port: int = 8080) -> None:
    server = HTTPServer(("0.0.0.0", port), StatusHandler)
    server.serve_forever()
