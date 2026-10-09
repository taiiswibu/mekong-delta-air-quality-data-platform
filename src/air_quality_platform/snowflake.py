from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import snowflake.connector

from air_quality_platform.config import env, private_key_path, snowflake_config


def connect(schema: str | None = None):
    cfg = snowflake_config(schema=schema)
    key = private_key_path()
    if not key.exists():
        raise FileNotFoundError(
            f"Snowflake private key not found at {key}. Run: python scripts/generate_keypair.py"
        )
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        role=cfg["role"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        private_key_file=str(key),
        private_key_file_pwd=env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None,
        autocommit=False,
    )


@contextmanager
def connection(schema: str | None = None) -> Iterator:
    conn = connect(schema=schema)
    try:
        yield conn
    finally:
        conn.close()


def fetch_one(sql: str, params: tuple | None = None, schema: str | None = None):
    with connection(schema=schema) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            return cur.fetchone()
        finally:
            cur.close()


def execute(sql: str, params: tuple | None = None, schema: str | None = None) -> None:
    with connection(schema=schema) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
