# Monitoring — ELK Heartbeat & Log Stack

This is the central monitoring repository of the integration project. The stack receives XML heartbeats and platform logs from every participating service via RabbitMQ, processes them in Logstash, and indexes the result in Elasticsearch. Kibana provides a dashboard to view the status of all teams in real-time.

## What it does

- Each service runs a sidecar that publishes a heartbeat XML to the RabbitMQ queue `heartbeat` every second
- Each team also publishes platform logs (info / warning / error) to the RabbitMQ queue `logs` whenever a flow completes, something looks suspicious, or an error happens
- Logstash consumes both queues, parses the XML envelope, maps the `source` to a team, and indexes the document
- Messages with invalid XML, an unknown source, an incorrect timestamp, an unknown level, or the wrong `header.type` are sent to a per-pipeline quarantine index
- Kibana visualizes the uptime and status per team and surfaces error/warning logs

## Monitored teams

The accepted `source` value in the XML header differs per pipeline (per contract v2.3):

| Team | Heartbeat | Log |
|---|:---:|:---:|
| Planning (`planning`) | ✓ | ✓ |
| CRM (`crm`) | ✓ | ✓ |
| Kassa (`kassa`) | ✓ | ✓ |
| Facturatie (`facturatie`) | ✓ | ✓ |
| Frontend (`frontend`) | ✓ | ✓ |
| Mailing (`mailing`) | ✓ | ✓ |
| Monitoring (`monitoring`) | ✓ | — |
| Identity service (`identity-service`) | — | ✓ |

Monitoring sends heartbeats but does not log to itself. Identity service is exempt from the heartbeat sidecar (RPC-only) but does emit logs. Anything outside the per-pipeline whitelist is sent to the quarantine index. Matching is case-insensitive — Logstash lowercases the value. Whitelists live in `monitoring_elk/logstash/pipeline/logstash.conf`.

## Ports

| Service | Host port | Usage |
|---|---|---|
| Elasticsearch REST API | `30060` | Internal queries, setting ILM policies |
| Kibana UI | `30061` | Dashboard, login for team members |

## Elasticsearch Indices

| Index | Content |
|---|---|
| `heartbeats-YYYY.MM.dd` | Valid, processed heartbeats |
| `heartbeats-quarantine-YYYY.MM.dd` | Invalid XML, unknown source, bad timestamp, unsupported contract version |
| `logs-YYYY.MM.dd` | Valid, processed platform logs |
| `logs-quarantine-YYYY.MM.dd` | Invalid XML, unknown source, bad timestamp, wrong message type, unknown level, unsupported contract version |
| `reports-YYYY.MM.dd` | Daily report metadata (no PDF body) |

## Message contracts

Both heartbeats and platform logs use the same `<message><header><body>` envelope. Header carries `message_id`, `timestamp` (UTC ISO 8601), `source`, `type` (`heartbeat`, `log`, or `send_mailing`), and `version`.

**Heartbeat body**: `status` (`online`/`offline`) and `uptime` (seconds, integer, required).

**Platform log body**: `level` (`info`/`warning`/`error`), `action` (a category from a closed set — see `logstash.conf`), and `message` (free text describing what happened). Log only at flow boundaries — successful completions, suspicious-but-non-critical events, or failures. Don't log every intermediate step.

**Daily report body**: `send_mailing` messages are published to `monitoring.reports` with `<source>monitoring</source>` and `<mail_type>daily_report</mail_type>`. A short JSON preview is carried in `<template_data>`, and the generated PDF is attached via `<attachment>`.

## Authentication

| Account | Usage |
|---|---|
| `elastic` | Administrative login for Kibana and direct ES queries |
| `kibana_system` | Internal service account for Kibana (not for logging in) |
| `monitoring_rabbitmq` | RabbitMQ user for Logstash |
