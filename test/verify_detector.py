"""Verifies the detector publishes a HEARTBEAT_CRITICAL alert to `to_mailing`.

Run after producer.py has sent heartbeats and then stopped — the detector's 3s
threshold will trip within a few seconds, and the alert should land on the queue.
Exits 0 on success, 1 on timeout or wrong payload.
"""

import os
import sys
import time

import pika

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
QUEUE = "to_mailing"
TIMEOUT_SECONDS = 20
POLL_INTERVAL = 0.5


def main() -> None:
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)

    deadline = time.time() + TIMEOUT_SECONDS
    while time.time() < deadline:
        method, _props, body = channel.basic_get(queue=QUEUE, auto_ack=True)
        if method is not None:
            payload = body.decode("utf-8", errors="replace")
            print(f"Received alert:\n{payload}")
            connection.close()
            if "<type>HEARTBEAT_CRITICAL</type>" not in payload:
                print("FAIL: payload missing HEARTBEAT_CRITICAL type tag", file=sys.stderr)
                sys.exit(1)
            print("OK: detector emitted HEARTBEAT_CRITICAL alert")
            return
        time.sleep(POLL_INTERVAL)

    connection.close()
    print(f"FAIL: no message on '{QUEUE}' within {TIMEOUT_SECONDS}s", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
