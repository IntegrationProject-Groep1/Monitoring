# Monitoring — ELK Heartbeat Stack

This is the central monitoring repository of the integration project. The stack receives an XML heartbeat from every participating service via RabbitMQ every second, processes it in Logstash, and indexes the result in Elasticsearch. Kibana provides a dashboard to view the status of all teams in real-time.

## What it does

- Each service runs a sidecar that publishes a heartbeat XML to the RabbitMQ queue `heartbeat` every second 
- Logstash consumes that queue, parses the XML, maps the `system` name to a team, and indexes the document
- Messages with invalid XML, an unknown `system` name, or an incorrect timestamp are sent to a quarantine index
- Kibana visualizes the uptime and status per team

## Monitored teams

| Team | `system` name in heartbeat |
|---|---|
| Planning | `planning` |
| CRM | `crm` |
| Kassa | `kassa` |
| Facturatie | `facturatie` |
| Monitoring | `monitoring` |

The `system` name must match exactly (case-insensitive). Adding new teams is handled via the mapping in `monitoring_elk/logstash/pipeline/logstash.conf`.

## Ports

| Service | Host port | Usage |
|---|---|---|
| Elasticsearch REST API | `30060` | Internal queries, setting ILM policies |
| Kibana UI | `30061` | Dashboard, login for team members |

## Elasticsearch Indices

| Index | Content |
|---|---|
| `heartbeats-YYYY.MM.dd` | Valid, processed heartbeats |
| `heartbeats-quarantine-YYYY.MM.dd` | Invalid XML, unknown systems, incorrect timestamps |

## Authentication

| Account | Usage |
|---|---|
| `elastic` | Administrative login for Kibana and direct ES queries |
| `kibana_system` | Internal service account for Kibana (not for logging in) |
| `monitoring_rabbitmq` | RabbitMQ user for Logstash |
