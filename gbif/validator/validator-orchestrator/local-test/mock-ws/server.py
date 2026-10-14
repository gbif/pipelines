"""Small in-memory stand-in for validator-ws; logs requests and stores reports."""
import json
import logging
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
validations = {}
reports = {}
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logging.info("HTTP %s", fmt % args)

    def respond(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length))

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            return self.respond(200, {"ok": True})

        match = re.fullmatch(r"/validation/([0-9a-fA-F-]{36})", path)
        if match:
            with lock:
                validation = validations.get(match.group(1))
            return self.respond(200 if validation else 404, validation or {"error": "not found"})

        return self.respond(404, {"error": "unsupported GET"})

    def do_POST(self):
        if self.path != "/__test/validations":
            return self.respond(404, {"error": "unsupported POST"})

        validation = self.read_json()
        key = validation["key"]
        with lock:
            validations[key] = validation
        logging.info("SEED validation %s steps=%s", key,
                     [s["stepType"] for s in validation["metrics"]["stepTypes"]])
        self.respond(201, validation)

    def do_PUT(self):
        path = urlparse(self.path).path
        report_match = re.fullmatch(
            r"/validation/([0-9a-fA-F-]{36})/validationreport/(\d+)/([^/]+)", path)
        if report_match:
            key, attempt, report_type = report_match.groups()
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length).decode()
            with lock:
                reports[(key, attempt, report_type)] = raw
            logging.info("REPORT key=%s attempt=%s type=%s bytes=%s body=%s",
                         key, attempt, report_type, len(raw), raw[:400])
            return self.respond(200, {"stored": True})

        match = re.fullmatch(r"/validation/([0-9a-fA-F-]{36})", path)
        if match:
            validation = self.read_json()
            key = match.group(1)
            if validation.get("key") != key:
                return self.respond(400, {"error": "path/body key mismatch"})
            with lock:
                validations[key] = validation
            statuses = [(s.get("stepType"), s.get("status"))
                        for s in validation.get("metrics", {}).get("stepTypes", [])]
            logging.info("UPDATE validation=%s status=%s steps=%s",
                         key, validation.get("status"), statuses)
            return self.respond(200, validation)

        self.respond(404, {"error": "unsupported PUT"})


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
