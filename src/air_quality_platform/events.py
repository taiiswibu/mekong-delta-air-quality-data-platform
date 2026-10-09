from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def build_event(city: str, payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("status") != "ok" or not isinstance(payload.get("data"), dict):
        raise ValueError(f"WAQI response is not usable for {city}: status={payload.get('status')}")

    data = dict(payload["data"])
    data["_queried_city"] = city
    normalized = dict(payload)
    normalized["data"] = data

    station = data.get("idx")
    source_time = ((data.get("time") or {}).get("iso") or "unknown")
    raw_key = f"{city}|{station}|{source_time}"
    event_id = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    return {
        "event_id": event_id,
        "queried_city": city,
        "source_event_at": source_time,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "payload": normalized,
    }


def event_to_json(event: dict[str, Any]) -> bytes:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
