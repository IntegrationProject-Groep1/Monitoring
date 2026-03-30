# Monitoring — ELK Heartbeat Stack

Dit is de centrale monitoring-repository van het integratieproject. De stack ontvangt elke seconde een XML-heartbeat van elke deelnemende service via RabbitMQ, verwerkt die in Logstash, en indexeert het resultaat in Elasticsearch. Kibana biedt een dashboard om de status van alle teams in realtime te bekijken.

## Wat het doet

- Elke service draait een sidecar die elke seconde een heartbeat-XML publiceert naar de RabbitMQ-queue `heartbeat`
- Logstash consumeert die queue, parsed de XML, mapt de `system`-naam naar een team, en indexeert het document
- Berichten met een ongeldige XML, onbekende `system`-naam of slechte timestamp gaan naar een quarantine-index
- Kibana visualiseert de uptime en status per team

## Gemonitorde teams

| Team | `system`-naam in heartbeat |
|---|---|
| Planning | `planning` |
| CRM | `crm` |
| Kassa | `kassa` |
| Facturatie | `facturatie` |
| Monitoring | `monitoring` |

De `system`-naam moet exact overeenkomen (case-insensitive). Nieuwe teams toevoegen gaat via de mapping in `monitoring_elk/logstash/pipeline/logstash.conf`.

## Poorten

| Service | Host-poort | Gebruik |
|---|---|---|
| Elasticsearch REST API | `30060` | Interne queries, ILM-beleid instellen |
| Kibana UI | `30061` | Dashboard, login voor teamleden |

## Elasticsearch-indices

| Index | Inhoud |
|---|---|
| `heartbeats-YYYY.MM.dd` | Geldige, verwerkte heartbeats |
| `heartbeats-quarantine-YYYY.MM.dd` | Ongeldige XML, onbekende systemen, slechte timestamps |

## Authenticatie

| Account | Gebruik |
|---|---|
| `elastic` | Beheer-login voor Kibana en directe ES-queries |
| `kibana_system` | Interne service-account voor Kibana (niet voor inloggen) |
| `monitoring_rabbitmq` | RabbitMQ-gebruiker voor Logstash |
