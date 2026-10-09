from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from air_quality_platform.observability import capture_metrics
from air_quality_platform.snowflake import execute


@dag(
    dag_id="air_quality_observability",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["observability", "snowflake"],
)
def observability():
    @task
    def snapshot() -> None:
        capture_metrics(None)

    @task
    def prune() -> None:
        execute("CALL CONTROL.PRUNE_OBSERVABILITY(90)", schema="CONTROL")

    snapshot() >> prune()


observability()
