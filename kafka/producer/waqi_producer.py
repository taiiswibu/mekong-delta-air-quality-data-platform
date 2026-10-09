from __future__ import annotations

import json
import signal
import sys
import time
from typing import Any

import requests
from confluent_kafka import Producer

from air_quality_platform.config import env, waqi_cities
from air_quality_platform.events import build_event, event_to_json

RUNNING = True


def _stop(*_: Any) -> None:
    global RUNNING
    RUNNING = False


def _delivery(err, msg) -> None:
    if err is not None:
        print(f"[producer] delivery failed: {err}", flush=True)
    else:
        print(
            f"[producer] delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}",
            flush=True,
        )


def fetch_waqi(city: str, token: str) -> dict[str, Any]:
    url = f"https://api.waqi.info/feed/{city}/"
    response = requests.get(url, params={"token": token}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "ok":
        raise RuntimeError(f"WAQI returned status={payload.get('status')} data={payload.get('data')}")
    return payload


def main() -> int:
    token = env("WAQI_TOKEN", required=True)
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = env("KAFKA_TOPIC_AIR_QUALITY", "air_quality.raw")
    poll_seconds = int(env("WAQI_POLL_SECONDS", "60"))
    cities = waqi_cities()

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "waqi-live-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "snappy",
        }
    )

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"[producer] topic={topic} bootstrap={bootstrap} cities={cities}", flush=True)
    while RUNNING:
        cycle_start = time.time()
        for city in cities:
            if not RUNNING:
                break
            try:
                payload = fetch_waqi(city, token)
                event = build_event(city, payload)
                producer.produce(
                    topic,
                    key=city.encode("utf-8"),
                    value=event_to_json(event),
                    on_delivery=_delivery,
                    headers={"event_type": "waqi.measurement.v1"},
                )
                producer.poll(0)
                print(
                    "[producer] published "
                    + json.dumps(
                        {
                            "city": city,
                            "event_id": event["event_id"][:12],
                            "source_event_at": event["source_event_at"],
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            except Exception as exc:
                print(f"[producer] city={city} failed: {exc}", file=sys.stderr, flush=True)
            time.sleep(0.3)

        producer.flush(10)
        elapsed = time.time() - cycle_start
        sleep_for = max(1.0, poll_seconds - elapsed)
        if RUNNING:
            time.sleep(sleep_for)

    producer.flush(10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
