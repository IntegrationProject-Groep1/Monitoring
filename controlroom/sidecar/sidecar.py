import socket
import time
import os
import sys
import pika
from datetime import datetime, timezone

SYSTEM_NAME = os.environ.get("SYSTEM_NAME")
TARGET_HOST = os.environ.get("TARGET_HOST")
TARGET_PORT = os.environ.get("TARGET_PORT")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS")

if not all([SYSTEM_NAME, TARGET_HOST, TARGET_PORT, RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS]):
    print("FOUT: stel alle environment variables in:")
    print("  - SYSTEM_NAME, TARGET_HOST, TARGET_PORT")
    print("  - RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS")
    sys.exit(1)

TARGET_PORT = int(TARGET_PORT)
uptime_seconds = 0

def is_alive(host, port, timeout=2):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def build_heartbeat_xml(system_name, uptime):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return (
        f"<heartbeat>"
        f"<system>{system_name}</system>"
        f"<timestamp>{timestamp}</timestamp>"
        f"<uptime>{uptime}</uptime>"
        f"</heartbeat>"
    )

def connect_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            )
            channel = connection.channel()
            channel.queue_declare(queue="heartbeat", durable=True)
            print("Verbonden met RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ nog niet bereikbaar, opnieuw proberen in 5 sec...")
            time.sleep(5)

print(f"Sidecar gestart voor systeem: {SYSTEM_NAME}")
print(f"Controleert: {TARGET_HOST}:{TARGET_PORT}")

connection, channel = connect_rabbitmq()

while True:
    if is_alive(TARGET_HOST, TARGET_PORT):
        uptime_seconds += 1
        xml = build_heartbeat_xml(SYSTEM_NAME, uptime_seconds)
        try:
            channel.basic_publish(
                exchange="",
                routing_key="heartbeat",
                body=xml,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"[ALIVE] {xml}")
        except pika.exceptions.AMQPError:
            print("RabbitMQ verbinding verloren, opnieuw verbinden...")
            connection, channel = connect_rabbitmq()
    else:
        uptime_seconds = 0
        print(f"[DOWN] {SYSTEM_NAME} niet bereikbaar op {TARGET_HOST}:{TARGET_PORT}")

    time.sleep(1)
