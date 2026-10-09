from air_quality_platform.events import build_event


def sample_payload():
    return {
        "status": "ok",
        "data": {
            "aqi": 77,
            "idx": 123,
            "time": {"iso": "2026-10-07T10:00:00+07:00"},
            "city": {"name": "Station A", "geo": [10.0, 105.0], "url": "https://example.com"},
            "iaqi": {"pm25": {"v": 42.0}},
        },
    }


def test_build_event_preserves_full_waqi_shape():
    event = build_event("ha-noi", sample_payload())
    assert event["payload"]["status"] == "ok"
    assert event["payload"]["data"]["_queried_city"] == "ha-noi"
    assert event["payload"]["data"]["aqi"] == 77


def test_event_id_is_deterministic_for_same_measurement():
    a = build_event("ha-noi", sample_payload())
    b = build_event("ha-noi", sample_payload())
    assert a["event_id"] == b["event_id"]
