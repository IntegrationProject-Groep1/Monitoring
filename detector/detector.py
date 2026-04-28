import time
import os
import logging
import pika
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch, NotFoundError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("detector")

# Configuraties (via Env)
ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_USER = os.getenv("ES_ADMIN_USER", "elastic")
ES_PASS = os.getenv("ES_ADMIN_PASS")
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = int(os.environ["RABBITMQ_PORT"])
RABBITMQ_USER = os.environ["RABBITMQMONITORING_USER"]
RABBITMQ_PASS = os.environ["RABBITMQMONITORING_PASS"]
RABBITMQ_VHOST = os.environ["RABBITMQ_VHOST"]
THRESHOLD_SECONDS = 3
COOLDOWN_MINUTES = 5

es = Elasticsearch([ES_HOST], basic_auth=(ES_USER, ES_PASS) if ES_PASS else None)
cooldown_list: dict[str, datetime] = {} # Om spam te voorkomen

_rabbit_conn = None
_rabbit_channel = None

def _get_rabbit_channel():
    """Geeft een open channel terug; (her)opent de verbinding indien nodig."""
    global _rabbit_conn, _rabbit_channel
    if (
        _rabbit_conn is None
        or _rabbit_conn.is_closed
        or _rabbit_channel is None
        or _rabbit_channel.is_closed
    ):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        _rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=30,
        ))
        _rabbit_channel = _rabbit_conn.channel()
        _rabbit_channel.queue_declare(queue="to_mailing", durable=True)
        logger.info("RabbitMQ-verbinding opgezet")
    return _rabbit_channel

def send_alert_xml(system_name):
    """Bouwt en verstuurt de Alert XML naar de mailing queue."""
    global _rabbit_conn, _rabbit_channel

    alert = ET.Element("alert")
    ET.SubElement(alert, "type").text = "HEARTBEAT_CRITICAL"
    ET.SubElement(alert, "system").text = system_name
    ET.SubElement(alert, "message").text = (
        f"Systeem {system_name} heeft al meer dan {THRESHOLD_SECONDS}s geen heartbeat gestuurd."
    )
    ET.SubElement(alert, "timestamp").text = datetime.now(timezone.utc).isoformat()
    xml_payload = ET.tostring(alert, encoding="unicode")

    for attempt in (1, 2):
        try:
            channel = _get_rabbit_channel()
            channel.basic_publish(exchange='', routing_key='to_mailing', body=xml_payload)
            logger.info("Alert verzonden voor %s", system_name)
            return
        except pika.exceptions.AMQPError as exc:
            logger.warning("Publish poging %d voor %s mislukt: %s", attempt, system_name, exc)
            try:
                if _rabbit_conn is not None and _rabbit_conn.is_open:
                    _rabbit_conn.close()
            except Exception:
                pass
            _rabbit_conn = None
            _rabbit_channel = None
    logger.error("Alert versturen voor %s mislukt na 2 pogingen", system_name)

while True:
    try:
        # Query: Zoek de laatste heartbeat per systeem
        try:
            res = es.search(
                index="heartbeats-*",
                size=0,
                aggs={
                    "systems": {
                        "terms": {"field": "system.keyword"},
                        "aggs": {"last_heartbeat": {"max": {"field": "@timestamp"}}},
                    }
                },
            )
        except NotFoundError:
            logger.debug("Index heartbeats-* bestaat nog niet; wachten op eerste heartbeat.")
            res = {"aggregations": {"systems": {"buckets": []}}}

        now = datetime.now(timezone.utc)

        for bucket in res['aggregations']['systems']['buckets']:
            system = bucket['key']
            last_ts = datetime.fromtimestamp(bucket['last_heartbeat']['value'] / 1000.0, tz=timezone.utc)
            
            diff = (now - last_ts).total_seconds()

            if diff > THRESHOLD_SECONDS:
                # Check cooldown
                last_alert = cooldown_list.get(system)
                if not last_alert or (now - last_alert) > timedelta(minutes=COOLDOWN_MINUTES):
                    send_alert_xml(system)
                    cooldown_list[system] = now
            else:
                # Systeem is weer up? Haal uit cooldown
                if system in cooldown_list:
                    logger.info("%s is weer online.", system)
                    del cooldown_list[system]

    except Exception:
        logger.exception("Fout in detector")

    # Houd de RabbitMQ-verbinding levend tijdens stille periodes.
    if _rabbit_conn is not None and _rabbit_conn.is_open:
        try:
            _rabbit_conn.process_data_events(time_limit=0)
        except pika.exceptions.AMQPError as exc:
            logger.warning("Heartbeat-onderhoud mislukt: %s", exc)
            try:
                _rabbit_conn.close()
            except Exception:
                pass
            _rabbit_conn = None
            _rabbit_channel = None

    time.sleep(1) # Check elke seconde
