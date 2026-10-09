from __future__ import annotations

import csv
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "sample"


# ============================================================
# Mekong Delta - 13 traditional provinces/cities
# ============================================================
#
# NOTE:
# This project intentionally uses the traditional 13-province
# Mekong Delta geography for historical/analytical continuity.
#
# This makes comparisons easier across historical datasets.
# ============================================================

CITIES = {
    "can-tho": {
        "display": "Can Tho",
        "region": "Central Mekong",
        "station": (
            1001,
            "Can Tho Urban Station",
            10.0452,
            105.7469,
        ),
        "base": 78,
    },

    "an-giang": {
        "display": "An Giang",
        "region": "Upper Mekong",
        "station": (
            1101,
            "Long Xuyen Station",
            10.3864,
            105.4352,
        ),
        "base": 67,
    },

    "dong-thap": {
        "display": "Dong Thap",
        "region": "Upper Mekong",
        "station": (
            1201,
            "Cao Lanh Station",
            10.4602,
            105.6320,
        ),
        "base": 61,
    },

    "long-an": {
        "display": "Long An",
        "region": "Gateway",
        "station": (
            1301,
            "Tan An Station",
            10.5359,
            106.4137,
        ),
        "base": 74,
    },

    "tien-giang": {
        "display": "Tien Giang",
        "region": "Tien River",
        "station": (
            1401,
            "My Tho Station",
            10.3600,
            106.3600,
        ),
        "base": 70,
    },

    "ben-tre": {
        "display": "Ben Tre",
        "region": "Coastal",
        "station": (
            1501,
            "Ben Tre Station",
            10.2434,
            106.3756,
        ),
        "base": 53,
    },

    "vinh-long": {
        "display": "Vinh Long",
        "region": "Central Mekong",
        "station": (
            1601,
            "Vinh Long Station",
            10.2537,
            105.9722,
        ),
        "base": 58,
    },

    "tra-vinh": {
        "display": "Tra Vinh",
        "region": "Coastal",
        "station": (
            1701,
            "Tra Vinh Station",
            9.9347,
            106.3453,
        ),
        "base": 50,
    },

    "hau-giang": {
        "display": "Hau Giang",
        "region": "Central Mekong",
        "station": (
            1801,
            "Vi Thanh Station",
            9.7845,
            105.4701,
        ),
        "base": 56,
    },

    "soc-trang": {
        "display": "Soc Trang",
        "region": "Coastal",
        "station": (
            1901,
            "Soc Trang Station",
            9.6025,
            105.9800,
        ),
        "base": 54,
    },

    "bac-lieu": {
        "display": "Bac Lieu",
        "region": "Coastal",
        "station": (
            2001,
            "Bac Lieu Station",
            9.2940,
            105.7278,
        ),
        "base": 49,
    },

    "ca-mau": {
        "display": "Ca Mau",
        "region": "Coastal",
        "station": (
            2101,
            "Ca Mau Station",
            9.1769,
            105.1524,
        ),
        "base": 52,
    },

    "kien-giang": {
        "display": "Kien Giang",
        "region": "Western Coastal",
        "station": (
            2201,
            "Rach Gia Station",
            10.0125,
            105.0809,
        ),
        "base": 57,
    },
}


# Four measurements per day.
TIMES = [6, 12, 18, 23]


# ============================================================
# Pollution model
# ============================================================
def hourly_factor(hour: int) -> float:
    """
    Simulate traffic / atmospheric patterns.

    Morning + evening tend to be slightly higher.
    Midday tends to disperse pollution better.
    """

    factors = {
        6: 1.10,
        12: 0.87,
        18: 1.15,
        23: 0.93,
    }

    return factors.get(hour, 1.0)


def seasonal_factor(day_of_year: int) -> float:
    """
    Approximate seasonal behaviour.

    Dry months:
        slightly higher particulate pollution.

    Rainy months:
        lower particulate concentration.
    """

    # Simple yearly wave.
    return 1 + 0.12 * math.cos(
        2 * math.pi * (day_of_year - 30) / 365
    )


def pollutants(
    aqi: float,
    rng: random.Random,
) -> dict[str, float]:

    pm25 = max(
        2,
        round(
            aqi * rng.uniform(0.68, 0.95),
            1,
        ),
    )

    pm10 = round(
        pm25 * rng.uniform(1.25, 1.65),
        1,
    )

    return {
        "pm25": pm25,

        "pm10": pm10,

        "co": round(
            rng.uniform(250, 850)
            + aqi * 1.4,
            1,
        ),

        "no2": round(
            rng.uniform(6, 25)
            + aqi * 0.10,
            1,
        ),

        "o3": round(
            rng.uniform(12, 55),
            1,
        ),

        "so2": round(
            rng.uniform(2, 18),
            1,
        ),

        "h": round(
            rng.uniform(60, 92),
            1,
        ),

        "t": round(
            rng.uniform(24, 34),
            1,
        ),

        "p": round(
            rng.uniform(997, 1015),
            1,
        ),

        "w": round(
            rng.uniform(0.5, 4.8),
            1,
        ),
    }


def build_aqi(
    base: float,
    dt: datetime,
    hour: int,
    rng: random.Random,
) -> float:

    seasonal = seasonal_factor(
        dt.timetuple().tm_yday
    )

    hourly = hourly_factor(hour)

    weekly_cycle = (
        6
        * math.sin(
            2 * math.pi * dt.weekday() / 7
        )
    )

    random_noise = rng.gauss(
        0,
        7,
    )

    value = (
        base
        * seasonal
        * hourly
        + weekly_cycle
        + random_noise
    )

    return round(
        max(
            15,
            min(
                220,
                value,
            ),
        ),
        1,
    )


# ============================================================
# Live / streaming seed
# ============================================================
def generate_api_data(
    rng: random.Random,
) -> None:

    api_dir = OUT / "api"

    api_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 90 days gives the dashboard enough history
    # for trend + temporal analysis.
    start_live = (
        datetime.now(timezone.utc)
        - timedelta(days=89)
    )

    rows_generated = 0

    for slug, meta in CITIES.items():

        (
            station_id,
            station_name,
            lat,
            lon,
        ) = meta["station"]

        city_dir = api_dir / slug

        city_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        # Remove previous seed JSONs.
        for old in city_dir.glob("*.json"):
            old.unlink()

        for day in range(90):

            current_day = (
                start_live
                + timedelta(days=day)
            )

            for hour in TIMES:

                ts = current_day.replace(
                    hour=hour,
                    minute=0,
                    second=0,
                    microsecond=0,
                )

                aqi = build_aqi(
                    meta["base"],
                    ts,
                    hour,
                    rng,
                )

                p = pollutants(
                    aqi,
                    rng,
                )

                payload = {
                    "status": "ok",

                    "data": {
                        "aqi": aqi,

                        "idx": station_id,

                        "dominentpol": "pm25",

                        "time": {
                            "iso": ts.isoformat(),
                        },

                        "city": {
                            "name": station_name,

                            "geo": [
                                lat,
                                lon,
                            ],

                            "url": (
                                "https://aqicn.org/"
                                f"station/{station_id}"
                            ),
                        },

                        "iaqi": {
                            key: {
                                "v": value
                            }
                            for key, value
                            in p.items()
                        },

                        "_queried_city": slug,

                        "_region": meta["region"],
                    },
                }

                filename = (
                    f"{slug}_"
                    f"{ts.strftime('%Y%m%dT%H%M%S')}"
                    ".json"
                )

                (
                    city_dir / filename
                ).write_text(
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )

                rows_generated += 1

    print(
        "[seed] API JSON generated:",
        rows_generated,
    )


# ============================================================
# Historical batch seed
# ============================================================
def generate_historical_data(
    rng: random.Random,
) -> None:

    csv_path = (
        OUT
        / "historical_air_quality_mekong_delta.csv"
    )

    fields = [
        "station_id",
        "aqi_index",
        "location",
        "station_name",
        "url",
        "dominant_pollutant",
        "co",
        "dew",
        "humidity",
        "no2",
        "o3",
        "pressure",
        "pm10",
        "pm25",
        "so2",
        "temperature",
        "wind",
        "data_time_s",
        "data_time_tz",
        "status",
        "alert_level",
    ]

    # One full year provides meaningful seasonal analysis.
    historical_start = datetime(
        2024,
        1,
        1,
    )

    rows_generated = 0

    with csv_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fields,
        )

        writer.writeheader()

        for slug, meta in CITIES.items():

            (
                station_id,
                station_name,
                _lat,
                _lon,
            ) = meta["station"]

            for day in range(365):

                current_day = (
                    historical_start
                    + timedelta(days=day)
                )

                for hour in TIMES:

                    ts = current_day.replace(
                        hour=hour,
                    )

                    aqi = build_aqi(
                        meta["base"] - 4,
                        ts,
                        hour,
                        rng,
                    )

                    p = pollutants(
                        aqi,
                        rng,
                    )

                    if aqi <= 50:
                        alert = "good"

                    elif aqi <= 100:
                        alert = "moderate"

                    elif aqi <= 150:
                        alert = "sensitive"

                    elif aqi <= 200:
                        alert = "unhealthy"

                    else:
                        alert = "very_unhealthy"

                    writer.writerow(
                        {
                            "station_id": station_id,

                            "aqi_index": aqi,

                            "location": meta[
                                "display"
                            ],

                            "station_name": (
                                station_name
                            ),

                            "url": (
                                "https://aqicn.org/"
                                f"station/{station_id}"
                            ),

                            "dominant_pollutant":
                                "pm25",

                            "co": p["co"],

                            "dew": round(
                                p["t"] - 5,
                                1,
                            ),

                            "humidity":
                                p["h"],

                            "no2":
                                p["no2"],

                            "o3":
                                p["o3"],

                            "pressure":
                                p["p"],

                            "pm10":
                                p["pm10"],

                            "pm25":
                                p["pm25"],

                            "so2":
                                p["so2"],

                            "temperature":
                                p["t"],

                            "wind":
                                p["w"],

                            "data_time_s":
                                ts.isoformat(),

                            "data_time_tz":
                                "+07:00",

                            "status":
                                "ok",

                            "alert_level":
                                alert,
                        }
                    )

                    rows_generated += 1

    print(
        "[seed] Historical rows generated:",
        rows_generated,
    )

    print(
        "[seed] Historical file:",
        csv_path,
    )


# ============================================================
# Main
# ============================================================
def generate() -> None:

    rng = random.Random(42)

    OUT.mkdir(
        parents=True,
        exist_ok=True,
    )

    generate_api_data(
        rng,
    )

    generate_historical_data(
        rng,
    )

    print()
    print(
        f"Seed data written to: {OUT}"
    )

    print(
        f"Provinces/cities: {len(CITIES)}"
    )


if __name__ == "__main__":
    generate()