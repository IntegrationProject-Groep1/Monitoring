import argparse
import base64
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

import pika
from elasticsearch import Elasticsearch, NotFoundError
from jinja2 import Environment, FileSystemLoader, select_autoescape
from weasyprint import HTML

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
REPORT_QUEUE = os.getenv("REPORT_QUEUE", "monitoring.reports")
REPORT_RECIPIENTS = os.getenv(
    "REPORT_RECIPIENTS",
    "admin1@example.com:admin-001:Platform:Admin,client@example.com:client-001:Event:Client",
)
REPORT_TEMPLATE_ID = os.getenv("REPORT_TEMPLATE_ID", "tmpl-monitoring-daily-report")
REPORT_CAMPAIGN_PREFIX = os.getenv("REPORT_CAMPAIGN_PREFIX", "monitoring-daily")
REPORT_SOURCE = os.getenv("REPORT_SOURCE", "monitoring")
REPORT_MAIL_TYPE = os.getenv("REPORT_MAIL_TYPE", "daily_report")
REPORT_TIME_HOUR = int(os.getenv("REPORT_TIME_HOUR", "6"))
REPORT_TIME_MINUTE = int(os.getenv("REPORT_TIME_MINUTE", "0"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("detector")

es = Elasticsearch([ES_HOST], basic_auth=(ES_USER, ES_PASS) if ES_PASS else None)
cooldown_list: dict[str, datetime] = {}

_rabbit_conn = None
_rabbit_channel = None

TEMPLATE_ENV = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
    autoescape=select_autoescape(["html"]),
)

EXPECTED_HEARTBEATS_PER_DAY = 86400
EXPECTED_HEARTBEATS_PER_MINUTE = 60

BUSINESS_ACTIONS = {
    "registration": "Registrations completed",
    "payment": "Payments processed",
    "invoice": "Invoices generated",
    "badge": "Badge scans at entry",
    "email": "Mailings sent",
}

PAYMENT_CURRENCY_PATTERN = re.compile(r"€\s?([0-9]+(?:[.,][0-9]{2})?)")


def _get_rabbit_channel():
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
        logger.info("RabbitMQ connection established")
    return _rabbit_channel


def publish(queue: str, body: str) -> None:
    channel = _get_rabbit_channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )


def xml_escape(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def parse_recipients(raw: str) -> list[dict[str, str]]:
    recipients = []
    for block in (item.strip() for item in raw.split(",") if item.strip()):
        parts = [part.strip() for part in block.split(":")]
        if len(parts) != 4:
            logger.warning("Ignoring malformed recipient entry: %r", block)
            continue
        email, user_id, first_name, last_name = parts
        recipients.append(
            {
                "email": email,
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
            }
        )
    return recipients


def send_alert_xml(system_name: str) -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    xml_payload = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<alert>
  <type>HEARTBEAT_CRITICAL</type>
  <system>{xml_escape(system_name)}</system>
  <message>Systeem {xml_escape(system_name)} heeft al meer dan {THRESHOLD_SECONDS}s geen heartbeat gestuurd.</message>
  <timestamp>{timestamp}</timestamp>
</alert>"""
    publish("to_mailing", xml_payload)
    logger.info("Alert published for %s", system_name)


def send_log_xml(level: str, action: str, message: str, source: str = "monitoring") -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    xml_payload = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<message>
  <header>
    <message_id>{uuid.uuid4()}</message_id>
    <timestamp>{timestamp}</timestamp>
    <source>{xml_escape(source)}</source>
    <type>log</type>
    <version>2.0</version>
  </header>
  <body>
    <level>{xml_escape(level)}</level>
    <action>{xml_escape(action)}</action>
    <message>{xml_escape(message)}</message>
  </body>
</message>"""
    publish("logs", xml_payload)
    logger.info("System log published: %s / %s", level, action)


def build_send_mailing_xml(
    report_date: str,
    subject: str,
    template_data: dict,
    attachment: dict | None = None,
) -> str:
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    correlation_id = f"report-{report_date}"
    campaign_id = f"{REPORT_CAMPAIGN_PREFIX}-{report_date}"
    recipients = parse_recipients(REPORT_RECIPIENTS)
    data_payload = json.dumps(template_data, separators=(",", ":"), ensure_ascii=False)

    recipient_entries = "".join(
        f"      <recipient>\n"
        f"        <email>{xml_escape(recipient['email'])}</email>\n"
        f"        <user_id>{xml_escape(recipient['user_id'])}</user_id>\n"
        f"        <contact>\n"
        f"          <first_name>{xml_escape(recipient['first_name'])}</first_name>\n"
        f"          <last_name>{xml_escape(recipient['last_name'])}</last_name>\n"
        f"        </contact>\n"
        f"      </recipient>\n"
        for recipient in recipients
    )

    attachment_xml = ""
    if attachment is not None:
        attachment_xml = f"""
    <attachment>
      <filename>{xml_escape(attachment['filename'])}</filename>
      <content_type>{xml_escape(attachment['content_type'])}</content_type>
      <base64_data>{xml_escape(attachment['base64_data'])}</base64_data>
    </attachment>"""

    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<message>
  <header>
    <message_id>{uuid.uuid4()}</message_id>
    <timestamp>{timestamp}</timestamp>
    <source>{xml_escape(REPORT_SOURCE)}</source>
    <type>send_mailing</type>
    <version>2.0</version>
    <correlation_id>{xml_escape(correlation_id)}</correlation_id>
  </header>
  <body>
    <campaign_id>{xml_escape(campaign_id)}</campaign_id>
    <subject>{xml_escape(subject)}</subject>
    <template_id>{xml_escape(REPORT_TEMPLATE_ID)}</template_id>
    <mail_type>{xml_escape(REPORT_MAIL_TYPE)}</mail_type>
    <recipients>
{recipient_entries}    </recipients>
    <template_data>{xml_escape(data_payload)}</template_data>{attachment_xml}
  </body>
</message>"""


def send_report_message(report_date: str, attachment: dict | None, template_data: dict) -> None:
    subject = f"Daily Platform Report — {report_date}"
    xml_payload = build_send_mailing_xml(report_date, subject, template_data, attachment)
    publish(REPORT_QUEUE, xml_payload)
    logger.info("Daily report message published to %s", REPORT_QUEUE)


def query_aggregations(index: str, body: dict) -> dict:
    try:
        return es.search(index=index, body=body, allow_no_indices=True)
    except NotFoundError:
        return {}


def aggregate_heartbeats(start: datetime, end: datetime) -> dict[str, int]:
    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [{"range": {"@timestamp": {"gte": start.isoformat(), "lt": end.isoformat()}}}]
            }
        },
        "aggs": {
            "systems": {
                "terms": {"field": "system.keyword", "size": 50},
                "aggs": {
                    "timeline": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": "1m",
                            "min_doc_count": 0,
                            "extended_bounds": {"min": start.isoformat(), "max": end.isoformat()},
                        }
                    }
                },
            }
        },
    }
    result = query_aggregations("heartbeats-*", body)
    return {
        bucket["key"]: bucket["doc_count"]
        for bucket in result.get("aggregations", {}).get("systems", {}).get("buckets", [])
    }


def aggregate_logs(start: datetime, end: datetime) -> dict:
    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [{"range": {"@timestamp": {"gte": start.isoformat(), "lt": end.isoformat()}}}]
            }
        },
        "aggs": {
            "systems": {
                "terms": {"field": "system.keyword", "size": 50, "order": {"_count": "desc"}},
                "aggs": {
                    "levels": {
                        "terms": {"field": "level.keyword", "size": 10},
                        "aggs": {
                            "actions": {"terms": {"field": "action.keyword", "size": 50}}
                        },
                    },
                    "errors": {
                        "filter": {"term": {"level.keyword": "error"}},
                        "aggs": {"top_errors": {"terms": {"field": "log_message.keyword", "size": 5}}},
                    },
                },
            },
            "info_actions": {
                "filter": {"term": {"level.keyword": "info"}},
                "aggs": {"actions": {"terms": {"field": "action.keyword", "size": 20}}},
            },
            "overall_errors": {
                "filter": {"term": {"level.keyword": "error"}},
                "aggs": {"top_errors": {"terms": {"field": "log_message.keyword", "size": 5}}},
            },
        },
    }
    return query_aggregations("logs-*", body)


def query_trailing_average(start: datetime, system: str) -> float:
    previous_start = start - timedelta(days=7)
    body = {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"range": {"@timestamp": {"gte": previous_start.isoformat(), "lt": start.isoformat()}}},
                    {"term": {"system.keyword": system}},
                ]
            }
        },
        "aggs": {
            "days": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "1d",
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": previous_start.isoformat(),
                        "max": (start - timedelta(seconds=1)).isoformat(),
                    },
                }
            }
        },
    }
    result = query_aggregations("logs-*", body)
    buckets = result.get("aggregations", {}).get("days", {}).get("buckets", [])
    if not buckets:
        return 0.0
    return sum(bucket["doc_count"] for bucket in buckets) / len(buckets)


def extract_revenue(message: str) -> float:
    match = PAYMENT_CURRENCY_PATTERN.search(message)
    if not match:
        return 0.0
    amount = match.group(1).replace(".", "").replace(",", ".") if "," in match.group(1) else match.group(1)
    try:
        return float(amount)
    except ValueError:
        return 0.0


def compute_health_score(availability: float, error_density: float) -> float:
    availability_component = max(0.0, min(10.0, 5.0 + (availability - 95.0) * 0.2))
    error_component = max(0.0, min(10.0, 10.0 - max(0.0, error_density - 1.0) * 0.1818))
    return round(min(10.0, availability_component * 0.7 + error_component * 0.3), 1)


def render_report_pdf(context: dict) -> bytes:
    template = TEMPLATE_ENV.get_template("daily_report.html")
    html = template.render(**context)
    return HTML(string=html).write_pdf()


def archive_report_metadata(report_date: str, overall_health: float, systems_down: int, pdf_path: str) -> None:
    index_name = f"reports-{datetime.strptime(report_date, '%Y-%m-%d').strftime('%Y.%m.%d')}"
    document = {
        "report_date": report_date,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overall_health": overall_health,
        "systems_down": systems_down,
        "storage_path": pdf_path,
    }
    try:
        es.index(index=index_name, document=document)
        logger.info("Report metadata archived to %s", index_name)
    except Exception as exc:
        logger.warning("Failed to archive report metadata: %s", exc)


def build_report_context(start: datetime, end: datetime) -> dict:
    heartbeat_counts = aggregate_heartbeats(start, end)
    log_agg = aggregate_logs(start, end)

    systems: list[dict] = []
    system_names = set(heartbeat_counts) | {
        bucket["key"]
        for bucket in log_agg.get("aggregations", {}).get("systems", {}).get("buckets", [])
    }

    overall_errors = log_agg.get("aggregations", {}).get("overall_errors", {}).get("top_errors", {}).get("buckets", [])
    overall_top_alert = "No critical errors detected"
    if overall_errors:
        top = overall_errors[0]
        overall_top_alert = f"{top['key']} ({top['doc_count']})"

    business_summary = {key: 0 for key in BUSINESS_ACTIONS}
    payment_revenue = 0.0
    for action_bucket in log_agg.get("aggregations", {}).get("info_actions", {}).get("actions", {}).get("buckets", []):
        action = action_bucket["key"]
        count = action_bucket["doc_count"]
        if action in business_summary:
            business_summary[action] = count

    for bucket in log_agg.get("aggregations", {}).get("systems", {}).get("buckets", []):
        system = bucket["key"]
        level_counts = {"info": 0, "warning": 0, "error": 0}
        for level_bucket in bucket.get("levels", {}).get("buckets", []):
            level = level_bucket["key"]
            level_counts[level] = level_bucket["doc_count"]
            if level == "info":
                for action_bucket in level_bucket.get("actions", {}).get("buckets", []):
                    if action_bucket["key"] == "payment":
                        # attempt to infer revenue from message text in payment logs if present
                        pass
        top_errors = [
            {"message": error_bucket["key"], "count": error_bucket["doc_count"]}
            for error_bucket in bucket.get("errors", {}).get("top_errors", {}).get("buckets", [])
        ]
        total_events = sum(level_counts.values())
        error_density = 0.0 if total_events == 0 else round(level_counts["error"] / total_events * 1000.0, 1)
        availability = round(min(100.0, heartbeat_counts.get(system, 0) / EXPECTED_HEARTBEATS_PER_DAY * 100.0), 2)
        health_score = compute_health_score(availability, error_density)
        trail_avg = query_trailing_average(start, system)
        activity_trend = compute_activity_trend(total_events, trail_avg)

        systems.append(
            {
                "name": system,
                "availability": availability,
                "error_density": error_density,
                "health_score": health_score,
                "top_issues": top_errors[:3],
                "activity_trend": activity_trend,
                "total_events": total_events,
                "heartbeats": heartbeat_counts.get(system, 0),
            }
        )

    for system, total in heartbeat_counts.items():
        if system not in {s["name"] for s in systems}:
            availability = round(min(100.0, total / EXPECTED_HEARTBEATS_PER_DAY * 100.0), 2)
            health_score = compute_health_score(availability, 0.0)
            systems.append(
                {
                    "name": system,
                    "availability": availability,
                    "error_density": 0.0,
                    "health_score": health_score,
                    "top_issues": [],
                    "activity_trend": "0%",
                    "total_events": 0,
                    "heartbeats": total,
                }
            )

    systems.sort(key=lambda item: item["health_score"], reverse=True)
    total_events = sum(item["total_events"] for item in systems)
    systems_down = sum(1 for item in systems if item["heartbeats"] < EXPECTED_HEARTBEATS_PER_DAY)
    overall_health = round(sum(item["health_score"] for item in systems) / len(systems), 1) if systems else 0.0

    return {
        "report_date": start.strftime("%Y-%m-%d"),
        "overall_health": overall_health,
        "systems_down": systems_down,
        "top_alert": overall_top_alert,
        "systems": systems,
        "business": {
            "registration": business_summary["registration"],
            "payment": business_summary["payment"],
            "invoice": business_summary["invoice"],
            "badge": business_summary["badge"],
            "email": business_summary["email"],
            "revenue": round(payment_revenue, 2),
        },
        "total_events": total_events,
        "report_sent_to": ", ".join(r["email"] for r in parse_recipients(REPORT_RECIPIENTS)),
    }


def compute_activity_trend(current_count: int, trailing_avg: float) -> str:
    if trailing_avg <= 0:
        return "+100%" if current_count > 0 else "0%"
    percent = round((current_count / trailing_avg - 1.0) * 100.0)
    return f"+{percent}%" if percent >= 0 else f"−{abs(percent)}%"


def generate_daily_report(now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    start = now - timedelta(days=1)
    report_date = start.strftime("%Y-%m-%d")
    logger.info("Generating daily report for %s", report_date)
    try:
        context = build_report_context(start, now)
        pdf_bytes = render_report_pdf(context)
        encoded_pdf = base64.b64encode(pdf_bytes).decode("ascii")
        attachment = {
            "filename": f"platform-report-{report_date}.pdf",
            "content_type": "application/pdf",
            "base64_data": encoded_pdf,
        }
        template_data = {
            "report_date": report_date,
            "overall_health": context["overall_health"],
            "systems_down": context["systems_down"],
            "top_alert": context["top_alert"],
        }
        send_report_message(report_date, attachment, template_data)
        archive_report_metadata(report_date, context["overall_health"], context["systems_down"], f"reports/platform-report-{report_date}.pdf")
    except Exception as exc:
        logger.exception("Failed to generate daily report")
        send_log_xml("error", "system_error", f"Report generation failed for {report_date}: {exc}")
        send_report_message(
            report_date,
            None,
            {
                "report_date": report_date,
                "overall_health": 0,
                "systems_down": 0,
                "top_alert": f"Report generation failed for {report_date}",
            },
        )


def should_run_daily_report(now: datetime) -> bool:
    return now.hour == REPORT_TIME_HOUR and now.minute == REPORT_TIME_MINUTE


def main() -> None:
    global _rabbit_conn, _rabbit_channel
    parser = argparse.ArgumentParser(description="Monitoring detector with daily report generation")
    parser.add_argument("--run-report", action="store_true", help="Generate the daily report once and exit")
    args = parser.parse_args()

    if args.run_report:
        generate_daily_report()
        return

    next_report_date = None
    while True:
        now = datetime.now(timezone.utc)
        try:
            # Query: Zoek de laatste heartbeat per systeem
            heartbeat_query = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"@timestamp": {"gte": "now-1h/h", "lt": "now"}}}
                        ]
                    }
                },
                "aggs": {
                    "systems": {
                        "terms": {"field": "system.keyword", "size": 50},
                        "aggs": {"last_heartbeat": {"max": {"field": "@timestamp"}}},
                    }
                },
            }
            res = query_aggregations("heartbeats-*", heartbeat_query)
            now = datetime.now(timezone.utc)
            for bucket in res.get("aggregations", {}).get("systems", {}).get("buckets", []):
                system = bucket["key"]
                last_value = bucket["last_heartbeat"].get("value")
                if last_value is None:
                    continue
                last_ts = datetime.fromtimestamp(last_value / 1000.0, tz=timezone.utc)
                diff = (now - last_ts).total_seconds()
                if diff > THRESHOLD_SECONDS:
                    last_alert = cooldown_list.get(system)
                    if not last_alert or (now - last_alert) > timedelta(minutes=COOLDOWN_MINUTES):
                        send_alert_xml(system)
                        cooldown_list[system] = now
                else:
                    if system in cooldown_list:
                        logger.info("%s is back online", system)
                        del cooldown_list[system]

            if should_run_daily_report(now):
                if next_report_date != now.date():
                    generate_daily_report(now)
                    next_report_date = now.date()

        except Exception:
            logger.exception("Fout in detector")

        if _rabbit_conn is not None and _rabbit_conn.is_open:
            try:
                _rabbit_conn.process_data_events(time_limit=0)
            except pika.exceptions.AMQPError as exc:
                logger.warning("RabbitMQ maintenance failed: %s", exc)
                try:
                    _rabbit_conn.close()
                except Exception:
                    pass
                _rabbit_conn = None
                _rabbit_channel = None

        time.sleep(1)


if __name__ == "__main__":
    main()
