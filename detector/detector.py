import time
import os
import pika
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
    """Bouwt en verstuurt de Alert XML naar de mailing queue."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue="to_mailing", durable=True)
    
    xml_payload = f"""<alert>
    <type>HEARTBEAT_CRITICAL</type>
    <system>{system_name}</system>
    <message>Systeem {system_name} heeft al meer dan {THRESHOLD_SECONDS}s geen heartbeat gestuurd.</message>
    <timestamp>{datetime.now().isoformat()}</timestamp>
</alert>"""

    channel.basic_publish(exchange='', routing_key='to_mailing', body=xml_payload)
    connection.close()
    print(f"Alert verzonden voor {system_name}")

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
