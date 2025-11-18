import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from openaq import OpenAQ

from config import OPENAQ_API_KEY

logger = logging.getLogger(__name__)

UTC = timezone.utc
MAX_SENSOR_PAGES = 20
MAX_RADIUS_METERS = 25_000  # OpenAQ API radius limit


def _serialize_coordinates(coords) -> Dict[str, float]:
    if not coords:
        return {}

    latitude = getattr(coords, "latitude", None)
    longitude = getattr(coords, "longitude", None)

    if isinstance(coords, dict):
        latitude = coords.get("latitude")
        longitude = coords.get("longitude")

    return {"latitude": latitude, "longitude": longitude}


def _parse_utc(value: str) -> datetime:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(UTC)


def _build_measurement_payload(entry, location_name: str, location_coords: Dict[str, float]):
    coords = _serialize_coordinates(entry.coordinates) or location_coords.copy()
    parameter = getattr(entry, "parameter", None)
    if isinstance(parameter, dict):
        parameter_name = parameter.get("name")
    else:
        parameter_name = getattr(parameter, "name", None)

    return {
        "period": {
            "label": entry.period.label,
            "interval": entry.period.interval,
            "datetimeFrom": {
                "utc": entry.period.datetime_from.utc,
                "local": entry.period.datetime_from.local,
            },
            "datetimeTo": {
                "utc": entry.period.datetime_to.utc,
                "local": entry.period.datetime_to.local,
            },
        },
        "value": entry.value,
        "parameter": {"name": parameter_name},
        "coordinates": coords,
        "location": location_name,
    }


def _fetch_locations(
    client: OpenAQ,
    coordinates: Tuple[float, float],
    radius_m: int,
    limit: int,
):
    response = client.locations.list(
        limit=limit,
        radius=radius_m,
        coordinates=coordinates,
        parameters_id=[2],
        sort_order="asc",
    )
    return response.results


def _fetch_pm25_sensors_for_location(client: OpenAQ, location_id: int):
    response = client.locations.sensors(location_id)
    sensors = []
    for sensor in response.results:
        parameter = getattr(sensor, "parameter", None)
        if isinstance(parameter, dict):
            param_name = (parameter.get("name") or "").lower()
        else:
            param_name = (getattr(parameter, "name", "") or "").lower()
        if param_name in ("pm25", "pm2.5"):
            sensors.append(sensor)
    return sensors


def _fetch_sensor_measurements(
    client: OpenAQ,
    sensor_id: int,
    location_name: str,
    location_coords: Dict[str, float],
    cutoff_dt: datetime,
    datetime_to: datetime,
    limit: int,
    max_pages: int,
) -> List[Dict]:
    measurements = []
    page = 1

    while page <= max_pages:
        response = client.measurements.list(
            sensors_id=sensor_id,
            datetime_from=cutoff_dt,
            datetime_to=datetime_to,
            page=page,
            limit=limit,
        )
        results = response.results or []
        if not results:
            break

        stop_fetching = False
        for entry in results:
            entry_timestamp = _parse_utc(entry.period.datetime_to.utc)
            if entry_timestamp and entry_timestamp < cutoff_dt:
                stop_fetching = True
                break
            measurements.append(
                _build_measurement_payload(entry, location_name, location_coords)
            )

        if stop_fetching or len(results) < limit:
            break

        page += 1

    return measurements


def fetch_pm25_measurements(
    *,
    hours: int,
    center_lat: float,
    center_lon: float,
    radius_km: float,
    max_locations: int = 10,
    limit_per_page: int = 100,
    max_pages: int = MAX_SENSOR_PAGES,
    raise_on_error: bool = False,
) -> List[Dict]:
    """
    Fetch PM2.5 measurements for a given region using the OpenAQ SDK.
    """
    if not OPENAQ_API_KEY:
        raise ValueError(
            "OpenAQ API key missing. Set OPENAQ_API_KEY env variable for data collection."
        )

    utc_now = datetime.now(UTC)
    cutoff_dt = utc_now - timedelta(hours=hours)
    radius_m = int(min(radius_km * 1000, MAX_RADIUS_METERS))
    coordinates = (center_lat, center_lon)

    measurements: List[Dict] = []

    try:
        with OpenAQ(api_key=OPENAQ_API_KEY) as client:
            locations = _fetch_locations(
                client,
                coordinates=coordinates,
                radius_m=radius_m,
                limit=max_locations,
            )

            if not locations:
                logger.warning(
                    "No OpenAQ locations returned for coordinates (%s, %s).",
                    center_lat,
                    center_lon,
                )
                return []

            total_locations = len(locations[:max_locations])
            logger.info(
                "Found %s OpenAQ locations within %.1f km radius (center: %.4f, %.4f)",
                total_locations,
                radius_km,
                center_lat,
                center_lon,
            )

            for idx, location in enumerate(locations[:max_locations], start=1):
                logger.info(
                    "Processing OpenAQ location %s/%s: %s (id=%s)",
                    idx,
                    total_locations,
                    getattr(location, "name", "Unknown"),
                    location.id,
                )
                sensors = _fetch_pm25_sensors_for_location(client, location.id)
                if not sensors:
                    logger.warning("No PM2.5 sensors found for location %s", location.id)
                    continue

                location_coords = _serialize_coordinates(location.coordinates)
                for sensor in sensors:
                    logger.info(
                        "  Fetching sensor %s for location %s (up to %s pages)",
                        sensor.id,
                        location.id,
                        max_pages,
                    )
                    sensor_measurements = _fetch_sensor_measurements(
                        client=client,
                        sensor_id=sensor.id,
                        location_name=location.name,
                        location_coords=location_coords,
                        cutoff_dt=cutoff_dt,
                        datetime_to=utc_now,
                        limit=limit_per_page,
                        max_pages=max_pages,
                    )
                    if sensor_measurements:
                        measurements.extend(sensor_measurements)
                        logger.info(
                            "  Collected %s measurements from sensor %s (running total: %s)",
                            len(sensor_measurements),
                            sensor.id,
                            len(measurements),
                        )
                logger.info(
                    "Completed OpenAQ location %s/%s (running total: %s measurements)",
                    idx,
                    total_locations,
                    len(measurements),
                )
    except Exception as exc:
        logger.error("Failed to fetch OpenAQ measurements: %s", exc)
        if raise_on_error:
            raise
        return []

    return measurements

