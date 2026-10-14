#!/usr/bin/env python3
"""Seed mock validator-ws, provision analyser queue, and publish a start request."""
import argparse
import base64
import json
import sys
import time
import urllib.error
import urllib.request
import uuid

API = "http://127.0.0.1:15672/api"
WS = "http://127.0.0.1:18080"
AUTH = "Basic " + base64.b64encode(b"localtest:localtest").decode()


def request(url, method, payload=None, auth=False):
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = AUTH
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=8) as response:
        return response.read().decode()


def publish(routing_key, payload):
    result = json.loads(request(f"{API}/exchanges/%2F/occurrence/publish", "POST", {
        "properties": {},
        "routing_key": routing_key,
        "payload": json.dumps(payload),
        "payload_encoding": "string",
    }, auth=True))
    if not result.get("routed"):
        sys.exit(f"Message not routed to a queue: {routing_key}. Is orchestrator running?")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--complete", metavar="UUID", help="simulate analyser completion")
    parser.add_argument("--uuid", help="use a preselected validation UUID")
    args = parser.parse_args()

    if args.complete:
        publish("validator.dwcdp.analysis.finished", {
            "datasetUuid": args.complete,
            "attempt": 1,
            "valid": True,
            "validationReport": {"test": "simulated result"},
        })
        print(f"Published simulated analyser completion for {args.complete}")
        return

    key = args.uuid or str(uuid.uuid4())
    validation = {
        "key": key,
        "status": "QUEUED",
        "fileFormat": "DWCDP",
        "metrics": {"stepTypes": [{
            "stepType": "VALIDATOR_VALIDATE_DWCDP",
            "executionOrder": 1,
            "status": None,
        }]},
    }
    request(f"{WS}/__test/validations", "POST", validation)
    # Ensure the analyser input queue exists before the orchestrator publishes to it.
    request(f"{API}/exchanges/%2F/occurrence", "PUT",
            {"type": "topic", "durable": True, "auto_delete": False, "arguments": {}}, auth=True)
    request(f"{API}/queues/%2F/validator_dwcdp_analysis", "PUT",
            {"durable": True, "auto_delete": False, "arguments": {}}, auth=True)
    request(f"{API}/bindings/%2F/e/occurrence/q/validator_dwcdp_analysis", "POST",
            {"routing_key": "validator.dwcdp.analysis", "arguments": {}}, auth=True)
    publish("validator.orchestrator.start", {"datasetUuid": key, "attempt": 1})
    print(f"Started {key}")
    print(f"Inspect mock: curl {WS}/validation/{key}")
    print(f"If analyser is not running: python3 local-test/trigger.py --complete {key}")


if __name__ == "__main__":
    main()
