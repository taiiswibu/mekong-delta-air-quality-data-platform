from __future__ import annotations

import snowflake.connector

from air_quality_platform.config import env, private_key_path, snowflake_config, PROJECT_ROOT


def bootstrap_connection():
    cfg = snowflake_config(schema="CONTROL")
    key = private_key_path()
    if not key.exists():
        raise FileNotFoundError(
            f"Snowflake private key not found at {key}. Run: python scripts/generate_keypair.py"
        )
    # Deliberately do NOT select AQ_WH / AQ_WAREHOUSE here because the bootstrap
    # script creates them from scratch in 001_infrastructure.sql.
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        role=cfg["role"],
        private_key_file=str(key),
        private_key_file_pwd=env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None,
        autocommit=False,
    )


def main() -> int:
    sql_dir = PROJECT_ROOT / "snowflake" / "sql"
    conn = bootstrap_connection()
    try:
        for path in sorted(sql_dir.glob("*.sql")):
            print(f"[bootstrap] {path.name}")
            with path.open("r", encoding="utf-8") as fh:
                for cursor in conn.execute_stream(fh, remove_comments=True):
                    for _ in cursor:
                        pass
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print("[bootstrap] Snowflake objects ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
