"""
Test producer for the heartbeat pipeline.

Sends a set of heartbeat XML messages to the 'heartbeat' RabbitMQ queue,
covering all code paths in the Logstash pipeline:
  - One valid message per known system (should land in heartbeats-*)
  - One message with an unknown system name (should land in quarantine)
  - One invalid XML message (should land in quarantine)

Exits 0 if all messages are published successfully, non-zero on any error.
"""

import os
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pika

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ["RABBITMQMONITORING_USER"]
RABBITMQ_PASS = os.environ["RABBITMQMONITORING_PASS"]
QUEUE = "heartbeat"

KNOWN_SYSTEMS = ["planning", "crm", "kassa", "facturatie", "monitoring", "frontend", "identity-service"]


def build_heartbeat(system: str, uptime: int, status: str = "online") -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    message = ET.Element("message")

    header = ET.SubElement(message, "header")
    ET.SubElement(header, "message_id").text = str(uuid.uuid4())
    ET.SubElement(header, "timestamp").text = timestamp
    ET.SubElement(header, "source").text = system
    ET.SubElement(header, "type").text = "heartbeat"
    ET.SubElement(header, "version").text = "2.0"

    body = ET.SubElement(message, "body")
    ET.SubElement(body, "status").text = status
    ET.SubElement(body, "uptime").text = str(uptime)

    return ET.tostring(message, encoding="unicode")


def connect(retries: int = 10) -> tuple:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    for attempt in range(1, retries + 1):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            print(f"Connected to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print(f"Connection attempt {attempt}/{retries} failed, retrying in 2s...")
            time.sleep(2)
    print("ERROR: Could not connect to RabbitMQ after all retries.", file=sys.stderr)
    sys.exit(1)


def publish(channel, body: str, label: str) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"  [sent] {label}")


def main() -> None:
    connection, channel = connect()

    print("\nSending valid heartbeats for all known systems:")
    for i, system in enumerate(KNOWN_SYSTEMS, start=1):
        publish(channel, build_heartbeat(system, i * 10), f"system={system}")

    print("\nSending offline heartbeat (status=offline, no uptime):")
    publish(channel, build_heartbeat(KNOWN_SYSTEMS[0], 0, "offline"), f"system={KNOWN_SYSTEMS[0]} offline")

    print("\nSending edge-case messages (should be quarantined by Logstash):")
    publish(channel, build_heartbeat("unknown-team", 1), "unknown system name")
    publish(channel, "this is not valid xml <<<", "invalid XML")

    connection.close()
    print(f"\nDone. {len(KNOWN_SYSTEMS) + 3} messages published to queue '{QUEUE}'.")


if __name__ == "__main__":
    main()
