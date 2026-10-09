from __future__ import annotations

from pathlib import Path

from air_quality_platform.config import PROJECT_ROOT
from air_quality_platform.snowflake import connection


DEFAULT_HISTORICAL_FILE = (
    PROJECT_ROOT
    / "data"
    / "sample"
    / "historical_air_quality_mekong_delta.csv"
)


def _snowflake_pattern(path: Path) -> str:
    """Return a safe regex pattern for the staged seed filename."""
    return ".*" + path.name.replace(".", "[.]")


def load_historical_csv(csv_path: str | Path | None = None) -> str:
    path = Path(csv_path or DEFAULT_HISTORICAL_FILE).resolve()

    if not path.exists():
        return f"SKIPPED: historical file not found: {path}"

    file_uri = path.as_uri()
    pattern = _snowflake_pattern(path)

    with connection("BRONZE") as conn:
        cur = conn.cursor()

        try:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM CONTROL.LOADED_FILES
                WHERE file_name = %s
                  AND layer = 'bronze_historical'
                """,
                (path.name,),
            )

            if cur.fetchone()[0] > 0:
                return f"SKIPPED: {path.name} already loaded"

            cur.execute(
                f"""
                PUT '{file_uri}'
                @BRONZE.AQ_INTERNAL_STAGE
                AUTO_COMPRESS=FALSE
                OVERWRITE=TRUE
                """
            )

            cur.execute(
                f"""
                COPY INTO BRONZE.HISTORICAL_CSV (
                    station_id,
                    aqi_index,
                    location,
                    station_name,
                    url,
                    dominant_pollutant,
                    co,
                    dew,
                    humidity,
                    no2,
                    o3,
                    pressure,
                    pm10,
                    pm25,
                    so2,
                    temperature,
                    wind,
                    data_time_s,
                    data_time_tz,
                    status,
                    alert_level,
                    source_file
                )
                FROM (
                    SELECT
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
                        $12,$13,$14,$15,$16,$17,$18,$19,$20,$21,
                        METADATA$FILENAME
                    FROM @BRONZE.AQ_INTERNAL_STAGE
                )
                PATTERN='{pattern}'
                FILE_FORMAT=BRONZE.AQ_CSV_FORMAT
                ON_ERROR='CONTINUE'
                """
            )

            cur.execute(
                """
                INSERT INTO CONTROL.LOADED_FILES(file_name, layer)
                VALUES (%s, 'bronze_historical')
                """,
                (path.name,),
            )

            conn.commit()
            return f"LOADED: {path.name}"

        except Exception:
            conn.rollback()
            raise

        finally:
            cur.close()
