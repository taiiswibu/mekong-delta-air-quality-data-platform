from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_core_artifacts_exist():
    expected = [
        "docker-compose.yml",
        "airflow/dags/air_quality_pipeline.py",
        "dbt_air_quality/dbt_project.yml",
        "quality/run_quality.py",
        "kafka/producer/waqi_producer.py",
        "kafka/consumer/snowflake_consumer.py",
        "dashboard/app.py",
        "snowflake/sql/003_bronze.sql",
    ]
    for rel in expected:
        assert (ROOT / rel).exists(), rel
