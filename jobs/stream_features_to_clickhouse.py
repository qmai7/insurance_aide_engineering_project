"""
Sink the Flink streaming features from Kafka into ClickHouse.

Step 4 of the streaming path:
  Kafka topic insurance_events_features  (written by the Flink HOP-window job)
      -> this consumer
      -> ClickHouse table gold_insurance.feat_stream_30m

The Flink connector landscape in the vanilla image has no ClickHouse sink, so we
keep Flink doing the windowing and use a small, robust Python consumer to load
the feature topic into ClickHouse. This matches the documented architecture
(... -> features topic -> ClickHouse) and is easy to run/grade locally.

For coursework this drains the current backlog and stops once the topic is idle
(consumer_timeout_ms). In production this would run continuously as a service.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import clickhouse_connect
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "insurance_events_features"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "gold_insurance")

TARGET_TABLE = f"{CLICKHOUSE_DATABASE}.feat_stream_30m"
COLUMNS = [
    "customer_id",
    "window_start",
    "window_end",
    "f_stream_quote_views_30m",
    "f_stream_claim_submitted_30m",
    "f_stream_payment_failed_30m",
    "f_stream_burst_activity_flag",
]
BATCH_SIZE = 20_000
IDLE_TIMEOUT_MS = 10_000


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def create_target_table(client) -> None:
    """Recreate the streaming feature table so each run produces a clean load."""
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
    client.command(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    client.command(
        f"""
        CREATE TABLE {TARGET_TABLE}
        (
            customer_id String,
            window_start DateTime,
            window_end DateTime,
            f_stream_quote_views_30m UInt64,
            f_stream_claim_submitted_30m UInt64,
            f_stream_payment_failed_30m UInt64,
            f_stream_burst_activity_flag Bool,
            ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree
        ORDER BY (window_start, customer_id)
        """
    )


def parse_ts(value: str) -> datetime:
    """Parse Flink ISO-8601 timestamps; ClickHouse DateTime is second precision."""
    return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")


def to_row(event: dict) -> list:
    return [
        str(event["customer_id"]),
        parse_ts(event["window_start"]),
        parse_ts(event["window_end"]),
        int(event["f_stream_quote_views_30m"]),
        int(event["f_stream_claim_submitted_30m"]),
        int(event["f_stream_payment_failed_30m"]),
        bool(event["f_stream_burst_activity_flag"]),
    ]


def main() -> None:
    client = clickhouse_client()
    create_target_table(client)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="clickhouse_stream_sink",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=IDLE_TIMEOUT_MS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    batch: list[list] = []
    total = 0

    def flush() -> None:
        nonlocal batch, total
        if batch:
            client.insert(TARGET_TABLE, batch, column_names=COLUMNS)
            total += len(batch)
            print(f"inserted {len(batch)} rows (running total {total})")
            batch = []

    for message in consumer:
        batch.append(to_row(message.value))
        if len(batch) >= BATCH_SIZE:
            flush()
    flush()
    consumer.close()

    table_count = client.query(f"SELECT count() FROM {TARGET_TABLE}").result_rows[0][0]
    print(f"done. consumed {total} feature messages; {TARGET_TABLE} now has {table_count} rows")


if __name__ == "__main__":
    main()
