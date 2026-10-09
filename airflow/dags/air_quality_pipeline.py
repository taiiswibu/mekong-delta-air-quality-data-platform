from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

from air_quality_platform.batch_loader import load_historical_csv
from air_quality_platform.observability import (
    capture_metrics,
    finish_pipeline_run,
    log_task,
    start_pipeline_run,
)
from quality.run_quality import run_quality_gate


PROJECT = Path("/opt/project")
DBT_DIR = PROJECT / "dbt_air_quality"


# ============================================================
# Helper: run dbt command
# ============================================================
def _run_dbt(
    pipeline_run_id: str,
    label: str,
    args: list[str],
) -> str:
    """
    Run a dbt command inside the Airflow container.

    This function is NOT an Airflow @task, so its argument names are
    not subject to Airflow TaskFlow reserved context keys.
    """

    log_task(
        pipeline_run_id,
        label,
        "STARTED",
        {"args": args},
    )

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_DIR)

    command = [
        "dbt",
        *args,
        "--profiles-dir",
        str(DBT_DIR),
    ]

    try:
        result = subprocess.run(
            command,
            cwd=DBT_DIR,
            env=env,
            text=True,
            capture_output=True,
            check=True,
        )

        log_task(
            pipeline_run_id,
            label,
            "SUCCESS",
            {
                "command": command,
                "stdout": result.stdout[-4000:],
            },
        )

        return result.stdout[-2000:]

    except subprocess.CalledProcessError as exc:
        error_detail = {
            "command": command,
            "returncode": exc.returncode,
            "stdout": (exc.stdout or "")[-3000:],
            "stderr": (exc.stderr or "")[-3000:],
        }

        log_task(
            pipeline_run_id,
            label,
            "FAILED",
            error_detail,
        )

        finish_pipeline_run(
            pipeline_run_id,
            "FAILED",
            f"{label} failed",
        )

        raise RuntimeError(
            f"{label} failed with exit code {exc.returncode}\n"
            f"STDERR:\n{(exc.stderr or '')[-2000:]}"
        ) from exc


# ============================================================
# DAG
# ============================================================
@dag(
    dag_id="air_quality_end_to_end",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Asia/Ho_Chi_Minh",
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "portfolio",
        "snowflake",
        "dbt",
        "data-quality",
    ],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=1),
    },
)
def air_quality_end_to_end():

    # ========================================================
    # 1. Start pipeline run
    # ========================================================
    @task
    def start() -> str:
        """
        Create one pipeline run record in CONTROL.PIPELINE_RUNS.

        Airflow's context key `run_id` is accessed from context here,
        but it is NOT used as a decorated-task function argument.
        """
        context = get_current_context()

        airflow_run_id = str(
            context.get(
                "run_id",
                "airflow",
            )
        )

        return start_pipeline_run(
            pipeline_name="air_quality_end_to_end",
            orchestrator_run_id=airflow_run_id,
        )

    # ========================================================
    # 2. Validate Airflow -> Snowflake connection
    # ========================================================
    @task
    def validate_snowflake(
        pipeline_run_id: str,
    ) -> str:
        """
        Validate Airflow's snowflake_default connection before the
        pipeline performs batch ingestion or dbt transformations.
        """
        from airflow.providers.snowflake.hooks.snowflake import (
            SnowflakeHook,
        )

        log_task(
            pipeline_run_id,
            "snowflake_connection",
            "STARTED",
        )

        try:
            hook = SnowflakeHook(
                snowflake_conn_id="snowflake_default"
            )

            with hook.get_conn() as conn:
                cur = conn.cursor()

                try:
                    cur.execute(
                        """
                        SELECT
                            CURRENT_ACCOUNT(),
                            CURRENT_USER(),
                            CURRENT_ROLE(),
                            CURRENT_WAREHOUSE(),
                            CURRENT_DATABASE()
                        """
                    )

                    (
                        account,
                        user,
                        role,
                        warehouse,
                        database,
                    ) = cur.fetchone()

                finally:
                    cur.close()

            detail = {
                "account": account,
                "user": user,
                "role": role,
                "warehouse": warehouse,
                "database": database,
            }

            log_task(
                pipeline_run_id,
                "snowflake_connection",
                "SUCCESS",
                detail,
            )

            return str(detail)

        except Exception as exc:
            log_task(
                pipeline_run_id,
                "snowflake_connection",
                "FAILED",
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
            )

            finish_pipeline_run(
                pipeline_run_id,
                "FAILED",
                "Airflow Snowflake connection failed",
            )

            raise

    # ========================================================
    # 3. Historical batch ingestion
    # ========================================================
    @task
    def historical(
        pipeline_run_id: str,
    ) -> str:
        """
        Load the historical CSV into Snowflake Bronze.

        CONTROL.LOADED_FILES makes this path idempotent.
        """
        log_task(
            pipeline_run_id,
            "historical_ingestion",
            "STARTED",
        )

        try:
            result = load_historical_csv()

            log_task(
                pipeline_run_id,
                "historical_ingestion",
                "SUCCESS",
                {
                    "result": result,
                },
            )

            return result

        except Exception as exc:
            log_task(
                pipeline_run_id,
                "historical_ingestion",
                "FAILED",
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
            )

            finish_pipeline_run(
                pipeline_run_id,
                "FAILED",
                "historical ingestion failed",
            )

            raise

    # ========================================================
    # 4. dbt Silver
    # ========================================================
    @task
    def dbt_silver(
        pipeline_run_id: str,
    ) -> str:
        return _run_dbt(
            pipeline_run_id=pipeline_run_id,
            label="dbt_silver",
            args=[
                "run",
                "--select",
                "tag:silver",
            ],
        )

    # ========================================================
    # 5. Data Quality Gate
    # ========================================================
    @task
    def quality_gate(
        pipeline_run_id: str,
    ) -> dict:
        """
        Run Great Expectations + SQL-based validation.

        A critical failure raises an exception and prevents Gold
        models from running.
        """
        log_task(
            pipeline_run_id,
            "quality_gate",
            "STARTED",
        )

        try:
            result = run_quality_gate(
                pipeline_run_id
            )

            log_task(
                pipeline_run_id,
                "quality_gate",
                "SUCCESS",
                result,
            )

            return result

        except Exception as exc:
            log_task(
                pipeline_run_id,
                "quality_gate",
                "FAILED",
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
            )

            finish_pipeline_run(
                pipeline_run_id,
                "FAILED",
                "quality gate failed",
            )

            raise

    # ========================================================
    # 6. dbt Gold
    # ========================================================
    @task
    def dbt_gold(
        pipeline_run_id: str,
    ) -> str:
        return _run_dbt(
            pipeline_run_id=pipeline_run_id,
            label="dbt_gold",
            args=[
                "run",
                "--select",
                "tag:gold",
            ],
        )

    # ========================================================
    # 7. dbt tests
    # ========================================================
    @task
    def dbt_tests(
        pipeline_run_id: str,
    ) -> str:
        return _run_dbt(
            pipeline_run_id=pipeline_run_id,
            label="dbt_tests",
            args=[
                "test",
            ],
        )

    # ========================================================
    # 8. Finish + observability
    # ========================================================
    @task
    def finish(
        pipeline_run_id: str,
    ) -> None:
        """
        Snapshot operational metrics and mark the pipeline run
        successful.
        """

        capture_metrics(
            pipeline_run_id
        )

        log_task(
            pipeline_run_id,
            "pipeline",
            "SUCCESS",
        )

        finish_pipeline_run(
            pipeline_run_id,
            "SUCCESS",
            "Bronze -> Silver -> DQ -> Gold completed",
        )

    # ========================================================
    # DAG dependency graph
    # ========================================================

    pipeline_run_id = start()

    snowflake_validation = validate_snowflake(
        pipeline_run_id
    )

    historical_ingestion = historical(
        pipeline_run_id
    )

    silver = dbt_silver(
        pipeline_run_id
    )

    dq = quality_gate(
        pipeline_run_id
    )

    gold = dbt_gold(
        pipeline_run_id
    )

    tests = dbt_tests(
        pipeline_run_id
    )

    completed = finish(
        pipeline_run_id
    )

    # Explicit business workflow order.
    snowflake_validation >> historical_ingestion
    historical_ingestion >> silver
    silver >> dq
    dq >> gold
    gold >> tests
    tests >> completed


air_quality_end_to_end()