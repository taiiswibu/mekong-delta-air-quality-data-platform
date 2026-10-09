from __future__ import annotations

import json
import uuid

from air_quality_platform.snowflake import connection


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM AQ_WAREHOUSE.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
        """,
        (schema.upper(), table.upper()),
    )
    return cur.fetchone()[0] > 0


def start_pipeline_run(pipeline_name: str, orchestrator_run_id: str | None = None) -> str:
    run_id = str(uuid.uuid4())
    with connection("CONTROL") as conn:
        cur = conn.cursor()
        try:
            silver_rows_before = 0
            if _table_exists(cur, "SILVER", "FACT_AQI"):
                cur.execute("SELECT COUNT(*) FROM SILVER.FACT_AQI")
                silver_rows_before = int(cur.fetchone()[0] or 0)
            cur.execute(
                """
                INSERT INTO CONTROL.PIPELINE_RUNS
                  (run_id, pipeline_name, orchestrator_run_id, status, started_at, silver_rows_before)
                VALUES (%s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP(), %s)
                """,
                (run_id, pipeline_name, orchestrator_run_id, silver_rows_before),
            )
            conn.commit()
        finally:
            cur.close()
    return run_id


def finish_pipeline_run(run_id: str, status: str, message: str = "") -> None:
    with connection("CONTROL") as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT started_at, COALESCE(silver_rows_before, 0) FROM CONTROL.PIPELINE_RUNS WHERE run_id=%s",
                (run_id,),
            )
            run_row = cur.fetchone()
            if not run_row:
                raise RuntimeError(f"Unknown pipeline run_id: {run_id}")
            started_at, silver_rows_before = run_row

            current_rows = 0
            if _table_exists(cur, "SILVER", "FACT_AQI"):
                cur.execute("SELECT COUNT(*) FROM SILVER.FACT_AQI")
                current_rows = int(cur.fetchone()[0] or 0)
            processed_rows = max(0, current_rows - int(silver_rows_before or 0))

            failed_rows = 0
            if _table_exists(cur, "BRONZE", "API_RAW"):
                cur.execute(
                    "SELECT COUNT(*) FROM BRONZE.API_RAW WHERE raw_payload:data:aqi IS NULL AND ingested_at >= %s",
                    (started_at,),
                )
                failed_rows += int(cur.fetchone()[0] or 0)
            if _table_exists(cur, "BRONZE", "HISTORICAL_CSV"):
                cur.execute(
                    """
                    SELECT COUNT(*) FROM BRONZE.HISTORICAL_CSV
                    WHERE ingested_at >= %s
                      AND (TRY_TO_NUMBER(station_id) IS NULL OR TRY_TO_TIMESTAMP_TZ(data_time_s) IS NULL)
                    """,
                    (started_at,),
                )
                failed_rows += int(cur.fetchone()[0] or 0)
            if _table_exists(cur, "CONTROL", "REJECTED_EVENTS"):
                cur.execute(
                    "SELECT COUNT(*) FROM CONTROL.REJECTED_EVENTS WHERE rejected_at >= %s",
                    (started_at,),
                )
                failed_rows += int(cur.fetchone()[0] or 0)

            cur.execute(
                """
                UPDATE CONTROL.PIPELINE_RUNS
                SET status=%s, finished_at=CURRENT_TIMESTAMP(), processed_rows=%s, failed_rows=%s, message=%s
                WHERE run_id=%s
                """,
                (status, processed_rows, failed_rows, message[:4000], run_id),
            )
            conn.commit()
        finally:
            cur.close()


def log_task(run_id: str, task_name: str, status: str, detail: dict | None = None) -> None:
    with connection("CONTROL") as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO CONTROL.PIPELINE_TASK_EVENTS
                  (run_id, task_name, status, detail, event_at)
                SELECT %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
                """,
                (run_id, task_name, status, json.dumps(detail or {})),
            )
            conn.commit()
        finally:
            cur.close()


def capture_metrics(run_id: str | None = None) -> None:
    metric_specs = [
        ("bronze_api_rows", "BRONZE", "API_RAW", "SELECT COUNT(*) FROM BRONZE.API_RAW"),
        ("bronze_historical_rows", "BRONZE", "HISTORICAL_CSV", "SELECT COUNT(*) FROM BRONZE.HISTORICAL_CSV"),
        ("silver_fact_rows", "SILVER", "FACT_AQI", "SELECT COUNT(*) FROM SILVER.FACT_AQI"),
        ("gold_latest_rows", "GOLD", "AQI_LATEST_STATUS", "SELECT COUNT(*) FROM GOLD.AQI_LATEST_STATUS"),
        ("latest_api_ingested_at", "BRONZE", "API_RAW", "SELECT MAX(ingested_at) FROM BRONZE.API_RAW"),
        (
            "api_ingestion_lag_seconds",
            "BRONZE",
            "API_RAW",
            "SELECT DATEDIFF('second', MAX(ingested_at), CURRENT_TIMESTAMP()) FROM BRONZE.API_RAW",
        ),
        ("rejected_kafka_events", "CONTROL", "REJECTED_EVENTS", "SELECT COUNT(*) FROM CONTROL.REJECTED_EVENTS"),
    ]
    with connection("CONTROL") as conn:
        cur = conn.cursor()
        try:
            for name, schema, table, sql in metric_specs:
                if not _table_exists(cur, schema, table):
                    value = None
                    text = "not_ready"
                else:
                    cur.execute(sql)
                    value = cur.fetchone()[0]
                    text = str(value) if value is not None else None
                cur.execute(
                    """
                    INSERT INTO CONTROL.PIPELINE_METRICS
                      (run_id, metric_name, metric_value, metric_text, captured_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())
                    """,
                    (
                        run_id,
                        name,
                        float(value) if isinstance(value, (int, float)) else None,
                        text,
                    ),
                )

            rejected_rows = 0
            if _table_exists(cur, "BRONZE", "API_RAW"):
                cur.execute("SELECT COUNT(*) FROM BRONZE.API_RAW WHERE raw_payload:data:aqi IS NULL")
                rejected_rows += cur.fetchone()[0]
            if _table_exists(cur, "BRONZE", "HISTORICAL_CSV"):
                cur.execute(
                    """
                    SELECT COUNT(*) FROM BRONZE.HISTORICAL_CSV
                    WHERE TRY_TO_NUMBER(station_id) IS NULL OR TRY_TO_TIMESTAMP_TZ(data_time_s) IS NULL
                    """
                )
                rejected_rows += cur.fetchone()[0]
            if _table_exists(cur, "CONTROL", "REJECTED_EVENTS"):
                cur.execute("SELECT COUNT(*) FROM CONTROL.REJECTED_EVENTS")
                rejected_rows += cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO CONTROL.PIPELINE_METRICS
                  (run_id, metric_name, metric_value, metric_text, captured_at)
                VALUES (%s, 'rejected_rows', %s, %s, CURRENT_TIMESTAMP())
                """,
                (run_id, rejected_rows, str(rejected_rows)),
            )
            conn.commit()
        finally:
            cur.close()
