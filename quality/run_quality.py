from __future__ import annotations

import json
import uuid
from typing import Any

import great_expectations as gx

from air_quality_platform.config import quality_config
from air_quality_platform.snowflake import connection

CRITICAL_COLUMNS = [
    "MEASUREMENT_KEY",
    "DATE_KEY",
    "TIME_KEY",
    "CITY_KEY",
    "STATION_KEY",
    "MEASURED_AT",
    "AQI",
    "SOURCE",
    "INGESTED_AT",
]


def _fetch_fact_dataframe():
    with connection("SILVER") as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM SILVER.FACT_AQI")
            return cur.fetch_pandas_all()
        finally:
            cur.close()


def _persist(
    run_id: str,
    framework: str,
    name: str,
    severity: str,
    passed: bool,
    detail: Any,
) -> None:
    payload = json.dumps(detail, default=str, ensure_ascii=False)[:15000]

    with connection("CONTROL") as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO CONTROL.DQ_RESULTS
                  (run_id, framework, check_name, severity, passed, detail, checked_at)
                SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
                """,
                (run_id, framework, name, severity, passed, payload),
            )
            conn.commit()
        finally:
            cur.close()


def _sql_check(
    run_id: str,
    name: str,
    sql: str,
    *,
    success_when_zero: bool = True,
    severity: str = "critical",
) -> bool:
    with connection("SILVER") as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            value = cur.fetchone()[0]
        finally:
            cur.close()

    passed = (value == 0) if success_when_zero else bool(value)

    _persist(
        run_id,
        "sql",
        name,
        severity,
        passed,
        {"observed_value": value},
    )

    return passed


def run_quality_gate(run_id: str | None = None) -> dict[str, Any]:
    run_id = run_id or str(uuid.uuid4())
    cfg = quality_config()
    df = _fetch_fact_dataframe()

    failures: list[str] = []

    if df.empty:
        _persist(
            run_id,
            "great_expectations",
            "table_not_empty",
            "critical",
            False,
            {"rows": 0},
        )
        raise RuntimeError("DQ gate failed: SILVER.FACT_AQI is empty")

    missing = sorted(set(CRITICAL_COLUMNS) - set(df.columns))
    schema_ok = not missing

    _persist(
        run_id,
        "python",
        "schema_validation",
        "critical",
        schema_ok,
        {"missing_columns": missing},
    )

    if not schema_ok:
        failures.append("schema_validation")

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("silver_fact_source")
    asset = datasource.add_dataframe_asset(name="silver_fact_aqi")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_fact")
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    expectations: list[tuple[str, Any, str]] = [
        (
            "row_count",
            gx.expectations.ExpectTableRowCountToBeBetween(
                min_value=int(cfg.get("min_row_count", 1000))
            ),
            "critical",
        ),
        (
            "aqi_range",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="AQI",
                min_value=float(cfg.get("min_aqi", 0)),
                max_value=float(cfg.get("max_aqi", 500)),
            ),
            "critical",
        ),
        (
            "source_validity",
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="SOURCE",
                value_set=list(
                    cfg.get(
                        "expected_sources",
                        ["api", "kaggle"],
                    )
                ),
            ),
            "critical",
        ),
    ]

    for column in [
        "MEASUREMENT_KEY",
        "DATE_KEY",
        "TIME_KEY",
        "CITY_KEY",
        "STATION_KEY",
        "MEASURED_AT",
        "AQI",
    ]:
        expectations.append(
            (
                f"not_null_{column.lower()}",
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=column
                ),
                "critical",
            )
        )

    for name, expectation, severity in expectations:
        result = batch.validate(expectation)
        passed = bool(result.success)

        detail = {
            "expectation": expectation.__class__.__name__,
            "result": (
                result.to_json_dict().get("result", {})
                if hasattr(result, "to_json_dict")
                else str(result)
            ),
        }

        _persist(
            run_id,
            "great_expectations",
            name,
            severity,
            passed,
            detail,
        )

        if not passed and severity == "critical":
            failures.append(name)

    max_null = float(cfg.get("max_null_percent", 5.0))
    expected_city_count = int(cfg.get("expected_city_count", 13))

    custom_checks = [
        (
            "critical_null_rate",
            f"""
            SELECT IFF(
              GREATEST(
                COUNT_IF(measurement_key IS NULL),
                COUNT_IF(date_key IS NULL),
                COUNT_IF(time_key IS NULL),
                COUNT_IF(city_key IS NULL),
                COUNT_IF(station_key IS NULL),
                COUNT_IF(measured_at IS NULL),
                COUNT_IF(aqi IS NULL)
              )
              / NULLIF(GREATEST(COUNT(*), 1), 0) * 100 <= {max_null},
              1,
              0
            )
            FROM SILVER.FACT_AQI
            """,
            False,
        ),
        (
            "duplicate_measurement_key",
            """
            SELECT COUNT(*)
            FROM (
                SELECT measurement_key
                FROM SILVER.FACT_AQI
                GROUP BY measurement_key
                HAVING COUNT(*) > 1
            )
            """,
            True,
        ),
        (
            "referential_integrity",
            """
            SELECT COUNT(*)
            FROM SILVER.FACT_AQI f
            LEFT JOIN SILVER.DIM_DATE d
              ON d.date_key = f.date_key
            LEFT JOIN SILVER.DIM_TIME t
              ON t.time_key = f.time_key
            LEFT JOIN SILVER.DIM_CITY c
              ON c.city_key = f.city_key
            LEFT JOIN SILVER.DIM_STATION s
              ON s.station_key = f.station_key
            WHERE d.date_key IS NULL
               OR t.time_key IS NULL
               OR c.city_key IS NULL
               OR s.station_key IS NULL
            """,
            True,
        ),
        (
            "city_reference_count",
            f"""
            SELECT IFF(
                COUNT(DISTINCT city_slug) = {expected_city_count},
                1,
                0
            )
            FROM CONTROL.CITY_REFERENCE
            """,
            False,
        ),
        (
            "mekong_city_coverage",
            """
            SELECT COUNT(*)
            FROM CONTROL.CITY_REFERENCE r
            WHERE NOT EXISTS (
                SELECT 1
                FROM SILVER.FACT_AQI f
                JOIN SILVER.DIM_CITY c
                  ON c.city_key = f.city_key
                WHERE c.city_slug = r.city_slug
            )
            """,
            True,
        ),
        (
            "no_non_mekong_cities",
            """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT c.city_slug
                FROM SILVER.FACT_AQI f
                JOIN SILVER.DIM_CITY c
                  ON c.city_key = f.city_key
                LEFT JOIN CONTROL.CITY_REFERENCE r
                  ON r.city_slug = c.city_slug
                WHERE r.city_slug IS NULL
            )
            """,
            True,
        ),
        (
            "freshness",
            f"""
            SELECT IFF(
                MAX(ingested_at)
                  >= DATEADD(
                       HOUR,
                       -{int(cfg.get("freshness_hours", 48))},
                       CURRENT_TIMESTAMP()
                     ),
                1,
                0
            )
            FROM SILVER.FACT_AQI
            """,
            False,
        ),
    ]

    for name, sql, success_when_zero in custom_checks:
        if not _sql_check(
            run_id,
            name,
            sql,
            success_when_zero=success_when_zero,
        ):
            failures.append(name)

    summary = {
        "run_id": run_id,
        "passed": not failures,
        "failures": sorted(set(failures)),
    }

    if failures:
        raise RuntimeError(
            "DQ gate failed: "
            + ", ".join(sorted(set(failures)))
        )

    return summary


if __name__ == "__main__":
    print(run_quality_gate())
