from __future__ import annotations

import json
import signal
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from air_quality_platform.config import env
from air_quality_platform.snowflake import connect

RUNNING = True


def _stop(*_: Any) -> None:
    global RUNNING
    RUNNING = False


def _flush_batch(
    conn,
    consumer: Consumer,
    rows: list[tuple],
    rejected: list[tuple],
) -> int:
    """
    Persist every polled Kafka message before committing Kafka offsets.

    Valid events:
        Kafka -> BRONZE.API_RAW

    Invalid events:
        Kafka -> CONTROL.REJECTED_EVENTS

    Kafka offsets are committed only after Snowflake commits successfully,
    preserving at-least-once delivery semantics.
    """
    if not rows and not rejected:
        return 0

    cur = conn.cursor()

    try:
        # ---------------------------------------------------------
        # 1. VALID EVENTS
        # ---------------------------------------------------------
        #
        # Do NOT use cursor.executemany() with:
        #
        # INSERT ... SELECT PARSE_JSON(...)
        #
        # Snowflake Connector attempts to rewrite it as a multi-row
        # INSERT and can raise:
        #
        #   252001: Failed to rewrite multi-row insert
        #
        # Instead, generate one VALUES block for the whole batch and
        # perform JSON/timestamp conversion in the outer SELECT.
        #
        if rows:
            placeholders = ",".join(
                ["(%s,%s,%s,%s,%s,%s,%s)"] * len(rows)
            )

            sql = f"""
                INSERT INTO BRONZE.API_RAW (
                    event_id,
                    queried_city,
                    raw_payload,
                    source_event_at,
                    produced_at,
                    kafka_partition,
                    kafka_offset
                )
                SELECT
                    column1,
                    column2,
                    PARSE_JSON(column3),
                    TRY_TO_TIMESTAMP_TZ(column4),
                    TRY_TO_TIMESTAMP_TZ(column5),
                    column6,
                    column7
                FROM VALUES
                    {placeholders}
            """

            flat_params = tuple(
                value
                for row in rows
                for value in row
            )

            cur.execute(sql, flat_params)

        # ---------------------------------------------------------
        # 2. REJECTED / POISON EVENTS
        # ---------------------------------------------------------
        if rejected:
            placeholders = ",".join(
                ["(%s,%s,%s,%s,%s)"] * len(rejected)
            )

            sql = f"""
                INSERT INTO CONTROL.REJECTED_EVENTS (
                    source,
                    raw_payload,
                    reason,
                    kafka_partition,
                    kafka_offset
                )
                SELECT
                    column1,
                    column2,
                    column3,
                    column4,
                    column5
                FROM VALUES
                    {placeholders}
            """

            flat_rejected = tuple(
                value
                for row in rejected
                for value in row
            )

            cur.execute(sql, flat_rejected)

        # ---------------------------------------------------------
        # 3. COMMIT SNOWFLAKE FIRST
        # ---------------------------------------------------------
        conn.commit()

        # ---------------------------------------------------------
        # 4. ONLY THEN COMMIT KAFKA OFFSETS
        # ---------------------------------------------------------
        consumer.commit(asynchronous=False)

        count = len(rows) + len(rejected)

        print(
            f"[consumer] committed messages={count} "
            f"valid={len(rows)} rejected={len(rejected)}",
            flush=True,
        )

        return count

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()


def main() -> int:
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = env("KAFKA_TOPIC_AIR_QUALITY", "air_quality.raw")
    group_id = env("KAFKA_CONSUMER_GROUP", "air-quality-snowflake-loader")
    batch_size = int(env("KAFKA_BATCH_SIZE", "50"))
    flush_seconds = int(env("KAFKA_FLUSH_SECONDS", "15"))

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([topic])

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    conn = connect("BRONZE")
    rows: list[tuple] = []
    rejected: list[tuple] = []
    last_flush = time.monotonic()
    print(f"[consumer] subscribed topic={topic} group={group_id}", flush=True)

    try:
        while RUNNING:
            msg = consumer.poll(1.0)
            if msg is None:
                if (rows or rejected) and time.monotonic() - last_flush >= flush_seconds:
                    _flush_batch(conn, consumer, rows, rejected)
                    rows.clear()
                    rejected.clear()
                    last_flush = time.monotonic()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            raw_text = msg.value().decode("utf-8", errors="replace")
            try:
                event = json.loads(raw_text)
                rows.append(
                    (
                        event["event_id"],
                        event["queried_city"],
                        json.dumps(event["payload"], ensure_ascii=False),
                        event.get("source_event_at"),
                        event.get("produced_at"),
                        msg.partition(),
                        msg.offset(),
                    )
                )
            except Exception as exc:
                rejected.append(
                    (
                        "kafka",
                        raw_text[:15000],
                        str(exc)[:2000],
                        msg.partition(),
                        msg.offset(),
                    )
                )
                print(f"[consumer] rejected offset={msg.offset()}: {exc}", flush=True)

            if len(rows) + len(rejected) >= batch_size:
                _flush_batch(conn, consumer, rows, rejected)
                rows.clear()
                rejected.clear()
                last_flush = time.monotonic()
    finally:
        if rows or rejected:
            _flush_batch(conn, consumer, rows, rejected)
        consumer.close()
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
