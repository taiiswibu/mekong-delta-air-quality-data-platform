from __future__ import annotations

import argparse
import sys

from confluent_kafka.admin import AdminClient

from air_quality_platform.config import env, private_key_path
from air_quality_platform.snowflake import connection


def check_local_config() -> None:
    required = {
        "SNOWFLAKE_ACCOUNT": env("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER": env("SNOWFLAKE_USER"),
        "WAQI_TOKEN": env("WAQI_TOKEN"),
    }
    bad = [name for name, value in required.items() if not value or value in {"replace_me", "your_account_identifier", "your_username"}]
    if bad:
        raise RuntimeError(f"Update these values in .env first: {', '.join(bad)}")
    key = private_key_path()
    if not key.exists():
        raise FileNotFoundError(f"Snowflake RSA private key not found: {key}")
    print(f"[OK] local config + RSA key: {key}")


def check_snowflake() -> None:
    with connection("CONTROL") as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
            account, user, warehouse, database = cur.fetchone()
            cur.execute(
                """
                SELECT COUNT(*)
                FROM AQ_WAREHOUSE.INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME IN ('BRONZE', 'SILVER', 'GOLD', 'CONTROL')
                """
            )
            schema_count = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT COUNT(*)
                FROM AQ_WAREHOUSE.INFORMATION_SCHEMA.TABLES
                WHERE (TABLE_SCHEMA='BRONZE' AND TABLE_NAME IN ('API_RAW','HISTORICAL_CSV'))
                   OR (TABLE_SCHEMA='CONTROL' AND TABLE_NAME IN ('PIPELINE_RUNS','DQ_RESULTS','REJECTED_EVENTS'))
                """
            )
            core_table_count = int(cur.fetchone()[0])
        finally:
            cur.close()
    if schema_count != 4 or core_table_count < 5:
        raise RuntimeError(
            f"Snowflake bootstrap incomplete: schemas={schema_count}/4 core_tables={core_table_count}/5"
        )
    print(f"[OK] Snowflake account={account} user={user} warehouse={warehouse} database={database}")


def check_kafka() -> None:
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = env("KAFKA_TOPIC_AIR_QUALITY", "air_quality.raw")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=8)
    if topic not in metadata.topics:
        raise RuntimeError(f"Kafka is reachable but topic '{topic}' does not exist")
    partitions = len(metadata.topics[topic].partitions)
    print(f"[OK] Kafka bootstrap={bootstrap} topic={topic} partitions={partitions}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify local + Snowflake + Kafka project setup")
    parser.add_argument("--skip-kafka", action="store_true", help="Verify only local config and Snowflake")
    args = parser.parse_args()

    checks = [("local configuration", check_local_config), ("Snowflake", check_snowflake)]
    if not args.skip_kafka:
        checks.append(("Kafka", check_kafka))

    failures: list[str] = []
    for name, fn in checks:
        try:
            fn()
        except Exception as exc:
            failures.append(f"{name}: {exc}")
            print(f"[FAIL] {name}: {exc}", file=sys.stderr)

    if failures:
        print("\nSetup verification failed:", file=sys.stderr)
        for item in failures:
            print(f" - {item}", file=sys.stderr)
        return 1
    print("\nAll requested checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
