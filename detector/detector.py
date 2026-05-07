import time
import os
import pika
import uuid
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

# Configuraties (via Env)
ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
THRESHOLD_SECONDS = 3
COOLDOWN_MINUTES = 5

es = Elasticsearch([ES_HOST])
cooldown_list = {} # Om spam te voorkomen: { "kassa": timestamp_last_alert }

def send_alert_xml(system_name):
    """Bouwt en verstuurt de Alert XML naar de mailing queue conform v2.3 contract."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue="monitoring.alerts", durable=True)

    msg_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat() + "Z"

    xml_payload = f"""<message>
  <header>
    <message_id>{msg_id}</message_id>
    <timestamp>{timestamp}</timestamp>
    <source>monitoring</source>
    <type>system_alert</type>
    <version>2.0</version>
  </header>
  <body>
    <system>{system_name}</system>
    <status>down</status>
    <message>Systeem {system_name} heeft al meer dan {THRESHOLD_SECONDS}s geen heartbeat gestuurd.</message>
  </body>
</message>"""

    channel.basic_publish(exchange='', routing_key='monitoring.alerts', body=xml_payload)
    connection.close()
    print(f"Standard Alert verzonden voor {system_name} (msg_id: {msg_id})")

while True:
    try:
        # Query: Zoek de laatste heartbeat per systeem
        res = es.search(index="heartbeats-*", body={
            "size": 0,
            "aggs": {
                "systems": {
                    "terms": {"field": "system.keyword"},
                    "aggs": { "last_heartbeat": {"max": {"field": "@timestamp"}} }
                }
            }
        })

        now = datetime.utcnow()

        for bucket in res['aggregations']['systems']['buckets']:
            system = bucket['key']
            last_ts = datetime.fromtimestamp(bucket['last_heartbeat']['value'] / 1000.0)

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
                    print(f"{system} is weer online.")
                    del cooldown_list[system]

    except Exception as e:
        print(f"Fout in detector: {e}")

    time.sleep(1) # Check elke seconde