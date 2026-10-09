from __future__ import annotations

import csv
import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

from air_quality_platform.config import PROJECT_ROOT, env
from air_quality_platform.events import event_to_json

CITY_MAP = {
    "Ha Noi": "ha-noi",
    "Hanoi": "ha-noi",
    "Ho Chi Minh City": "ho-chi-minh-city",
    "Ho Chi Minh": "ho-chi-minh-city",
    "Da Nang": "da-nang",
    "Gia Lai": "gia-lai",
    "Cao Bang": "cao-bang",
}


def fnum(row: dict[str, str], key: str):
    value = row.get(key)
    try:
        return float(value) if value not in (None, "", "NA", "N/A") else None
    except ValueError:
        return None


def _row_to_event(row: dict[str, str], index: int) -> dict:
    city = CITY_MAP.get(row.get("location", ""), row.get("location", "unknown").strip().lower().replace(" ", "-"))
    source_time = row.get("data_time_s") or datetime.now(timezone.utc).isoformat()
    raw_key = f"replay|{city}|{row.get('station_id')}|{source_time}|{index}"
    event_id = hashlib.sha256(raw_key.encode()).hexdigest()
    iaqi = {}
    for target, source in [
        ("pm25", "pm25"), ("pm10", "pm10"), ("co", "co"), ("no2", "no2"), ("o3", "o3"),
        ("so2", "so2"), ("h", "humidity"), ("t", "temperature"), ("p", "pressure"), ("w", "wind")
    ]:
        value = fnum(row, source)
        if value is not None:
            iaqi[target] = {"v": value}

    payload = {
        "status": "ok",
        "data": {
            "aqi": fnum(row, "aqi_index"),
            "idx": int(float(row.get("station_id") or 0)),
            "dominentpol": row.get("dominant_pollutant") or "pm25",
            "time": {"iso": source_time},
            "city": {
                "name": row.get("station_name") or city,
                "url": row.get("url") or "",
                "geo": [None, None],
            },
            "iaqi": iaqi,
            "_queried_city": city,
        },
    }
    return {
        "event_id": event_id,
        "queried_city": city,
        "source_event_at": source_time,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def main() -> int:
    path = Path(os.getenv("REPLAY_CSV", PROJECT_ROOT / "data" / "sample" / "historical_air_quality_2021_en.csv"))
    if not path.exists():
        raise FileNotFoundError(f"Replay CSV not found: {path}. Run scripts/generate_seed.py first.")

    producer = Producer(
        {
            "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "client.id": "historical-replay-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    topic = env("KAFKA_TOPIC_AIR_QUALITY", "air_quality.raw")
    delay = float(os.getenv("REPLAY_DELAY_SECONDS", "0.15"))

    with path.open("r", encoding="utf-8", newline="") as f:
        for idx, row in enumerate(csv.DictReader(f), start=1):
            event = _row_to_event(row, idx)
            producer.produce(topic, key=event["queried_city"], value=event_to_json(event))
            producer.poll(0)
            if idx % 100 == 0:
                print(f"[replay] published {idx} events", flush=True)
            time.sleep(delay)

    producer.flush(15)
    print("[replay] completed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
