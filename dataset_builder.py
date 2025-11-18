"""Helpers for building offline datasets for traffic and air quality."""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytz
import requests

from config import (
    AIR_RAW_FILE_PATH,
    DISTRICT10_CENTER_LAT,
    DISTRICT10_CENTER_LON,
    DISTRICT10_RADIUS_KM,
    HCMC_TIMEZONE,
    TOMTOM_API_KEY,
    TOMTOM_FLOW_SEGMENT_ENDPOINT,
    DISTRICT10_LAT_MIN,
    DISTRICT10_LAT_MAX,
    DISTRICT10_LON_MIN,
    DISTRICT10_LON_MAX,
    DISTRICT10_GRID_SIZE,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    TRAFFIC_RAW_FILE_PATH,
)
from openaq_client import fetch_pm25_measurements
from utils.data_loader import load_json, save_json

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

HCMC_TZ = pytz.timezone(HCMC_TIMEZONE)


def _fetch_traffic_raw_responses():
    """
    Fetch raw API responses from TomTom API and return as list.
    Returns list of raw API response dictionaries.
    """
    if not TOMTOM_API_KEY:
        error_msg = "TomTom API key is required but not found."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    import numpy as np
    
    lat_points = np.linspace(DISTRICT10_LAT_MIN, DISTRICT10_LAT_MAX, DISTRICT10_GRID_SIZE)
    lon_points = np.linspace(DISTRICT10_LON_MIN, DISTRICT10_LON_MAX, DISTRICT10_GRID_SIZE)
    
    current_time = datetime.now(HCMC_TZ)
    raw_responses = []
    
    total_points = len(lat_points) * len(lon_points)
    processed_points = 0
    logger.info("Querying TomTom API grid (%s points) ...", total_points)

    for lat in lat_points:
        for lon in lon_points:
            params = {
                "key": TOMTOM_API_KEY,
                "point": f"{lat},{lon}"
            }
            
            for attempt in range(MAX_RETRIES):
                try:
                    response = requests.get(TOMTOM_FLOW_SEGMENT_ENDPOINT, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    # Add timestamp to the response for tracking
                    data["_fetched_at"] = current_time.isoformat()
                    raw_responses.append(data)
                    break
                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_SECONDS)
                    else:
                        logger.warning(f"Failed to fetch traffic data for point ({lat}, {lon}): {e}")
            
            time.sleep(0.1)  # Rate limiting

            processed_points += 1
            if processed_points % 5 == 0 or processed_points == total_points:
                logger.info(
                    "TomTom grid progress: %s/%s points processed (%s responses collected)",
                    processed_points,
                    total_points,
                    len(raw_responses),
                )
    
    return raw_responses


def _load_existing_json(path: Path):
    """Load existing JSON file, return empty list if not found."""
    if not path.exists():
        return []
    try:
        data = load_json(path)
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning(f"Failed to load existing dataset at {path}: {exc}. Starting fresh.")
        return []


def _append_traffic_with_rolling_window(
    new_responses: list,
    output_file: Path,
    rolling_hours: int = 24,
) -> list:
    """Append traffic responses to JSON, keep only last N hours."""
    existing = _load_existing_json(output_file)
    combined = existing + new_responses
    
    # Filter by timestamp if responses have _fetched_at field
    cutoff = datetime.now(HCMC_TZ) - timedelta(hours=rolling_hours)
    filtered = []
    for resp in combined:
        fetched_at = resp.get("_fetched_at")
        if fetched_at:
            try:
                resp_time = datetime.fromisoformat(fetched_at.replace('Z', '+00:00'))
                if resp_time.tzinfo is None:
                    resp_time = HCMC_TZ.localize(resp_time)
                else:
                    resp_time = resp_time.astimezone(HCMC_TZ)
                if resp_time >= cutoff:
                    filtered.append(resp)
            except Exception:
                # If timestamp parsing fails, keep the response
                filtered.append(resp)
        else:
            # If no timestamp, keep it
            filtered.append(resp)
    
    save_json(filtered, output_file)
    logger.info(
        "Saved %s traffic responses to %s (rolling window %sh)",
        len(filtered),
        output_file,
        rolling_hours,
    )
    return filtered


# ---------------------------------------------------------------------------
# Traffic dataset helpers


def build_traffic_snapshot(output_file: Path = TRAFFIC_RAW_FILE_PATH):
    """Fetch a single snapshot of traffic data and overwrite dataset file with raw JSON."""
    logger.info("Starting traffic snapshot collection (TomTom API)...")
    raw_responses = _fetch_traffic_raw_responses()
    if not raw_responses:
        logger.warning("No traffic data fetched for snapshot.")
        return
    save_json(raw_responses, output_file)
    logger.info("Traffic snapshot saved to %s (%s responses)", output_file, len(raw_responses))


def run_traffic_collector(
    output_file: Path = TRAFFIC_RAW_FILE_PATH,
    interval_minutes: int = 30,
    duration_hours: int = 24,
):
    """Run a collector loop that appends raw API responses every interval for duration_hours."""
    end_time = datetime.now(HCMC_TZ) + timedelta(hours=duration_hours)
    logger.info(
        "Starting traffic collector for %sh at %s-minute intervals -> %s",
        duration_hours,
        interval_minutes,
        output_file,
    )

    iteration = 0
    try:
        while datetime.now(HCMC_TZ) < end_time:
            iteration += 1
            logger.info("Collector iteration %s", iteration)
            raw_responses = _fetch_traffic_raw_responses()
            if not raw_responses:
                logger.warning("No traffic data fetched in iteration %s", iteration)
            else:
                _append_traffic_with_rolling_window(
                    raw_responses,
                    output_file,
                    rolling_hours=24,
                )

            if datetime.now(HCMC_TZ) >= end_time:
                break
            logger.info("Sleeping %s minutes...", interval_minutes)
            time.sleep(max(interval_minutes, 1) * 60)
    except KeyboardInterrupt:
        logger.info("Traffic collector interrupted by user.")


# ---------------------------------------------------------------------------
# Air quality dataset helpers
# ---------------------------------------------------------------------------


def _fetch_air_quality_last_24h(hours: int = 24):
    try:
        logger.info("Fetching last %s hours of air quality data from OpenAQ...", hours)
        return fetch_pm25_measurements(
            hours=hours,
            center_lat=DISTRICT10_CENTER_LAT,
            center_lon=DISTRICT10_CENTER_LON,
            radius_km=DISTRICT10_RADIUS_KM,
            max_locations=10,
        )
    except Exception as exc:
        logger.error(f"Failed to fetch OpenAQ dataset: {exc}")
        return []


def build_air_dataset(output_file: Path = AIR_RAW_FILE_PATH, hours: int = 24):
    """Fetch last `hours` of air quality measurements and save as raw JSON."""
    logger.info("Starting air quality dataset collection (OpenAQ)...")
    measurements = _fetch_air_quality_last_24h(hours=hours)
    if not measurements:
        logger.warning("No air quality measurements fetched for dataset.")
        return

    # Save measurements directly as JSON array
    save_json(measurements, output_file)
    logger.info("Air quality dataset saved to %s (%s measurements)", output_file, len(measurements))

