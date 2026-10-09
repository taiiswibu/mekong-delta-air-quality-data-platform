from air_quality_platform.config import waqi_cities


def test_expected_demo_cities_present():
    cities = waqi_cities()
    assert "ha-noi" in cities
    assert "ho-chi-minh-city" in cities
    assert len(cities) >= 5
