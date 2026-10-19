#!/bin/sh
# Registers Debezium Postgres connectors that capture the outbox_events table of
# booking_command_db and payment_db and route rows to Kafka topics:
#   aggregate_type "booking" -> booking-events,  aggregate_type "payment" -> payment-events
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:8089}"
DB_HOST="${DB_HOST:-postgres}"
DB_USER="${DB_USER:-railway}"
DB_PASSWORD="${DB_PASSWORD:-railway}"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -fsS "${CONNECT_URL}/connectors" > /dev/null 2>&1; do sleep 3; done

register() {
  name="$1"; db="$2"; slot="$3"; prefix="$4"
  echo "Registering connector ${name} (database ${db})"
  curl -fsS -X PUT -H "Content-Type: application/json" \
    "${CONNECT_URL}/connectors/${name}/config" -d @- <<JSON
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "${DB_HOST}",
  "database.port": "5432",
  "database.user": "${DB_USER}",
  "database.password": "${DB_PASSWORD}",
  "database.dbname": "${db}",
  "topic.prefix": "${prefix}",
  "plugin.name": "pgoutput",
  "slot.name": "${slot}",
  "publication.name": "${slot}_pub",
  "publication.autocreate.mode": "filtered",
  "table.include.list": "public.outbox_events",
  "tombstones.on.delete": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "header.converter": "org.apache.kafka.connect.storage.SimpleHeaderConverter",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "id",
  "transforms.outbox.table.field.event.key": "aggregate_id",
  "transforms.outbox.table.field.event.type": "event_type",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.table.expand.json.payload": "true",
  "transforms.outbox.table.fields.additional.placement": "event_type:header:eventType,id:header:eventId",
  "transforms.outbox.route.by.field": "aggregate_type",
  "transforms.outbox.route.topic.replacement": "\${routedByValue}-events"
}
JSON
  echo
}

register booking-outbox-connector booking_command_db booking_outbox_slot booking-cmd
register payment-outbox-connector payment_db payment_outbox_slot payment-cmd
echo "Debezium connectors registered."
