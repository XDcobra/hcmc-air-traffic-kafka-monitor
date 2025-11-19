"""Speed layer processor for real-time air quality data from OpenAQ API."""

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from config import (
    AIR_QUALITY_SPEED_FILE,
    AIR_RAW_FILE_PATH,
    HCMC_LATITUDE,
    HCMC_LONGITUDE,
    HCMC_RADIUS_KM,
    HCMC_TIMEZONE,
    OPENAQ_API_KEY,
    POLLING_INTERVAL_MINUTES,
    SPEED_VIEWS_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_AIR_QUALITY_TOPIC,
    KAFKA_SPEED_CONSUMER_GROUP,
)
from openaq_client import fetch_pm25_measurements
from utils.data_loader import load_csv, load_json, parse_timestamp, save_csv
from kafka_client import (
    check_kafka_connection,
    read_air_quality_from_kafka,
)

logger = logging.getLogger(__name__)


def fetch_air_quality_data(
    lat=HCMC_LATITUDE,
    lon=HCMC_LONGITUDE,
    radius_km=HCMC_RADIUS_KM,
    parameter="pm25",
    hours=24,
    max_locations=10,
):
    """
    Fetch air quality data using the official OpenAQ Python SDK.

    Args:
        lat: Latitude (default: HCMC)
        lon: Longitude (default: HCMC)
        radius_km: Search radius in kilometers (max 25km for v3)
        parameter: Parameter to fetch (currently only pm25 supported)
        hours: Number of recent hours to request from the API
        max_locations: Maximum number of nearby locations to query

    Returns:
        list: List of measurement dictionaries

    Raises:
        ValueError: If the API key is missing or parameter unsupported
    """
    if parameter.lower() not in ("pm25", "pm2.5"):
        raise ValueError("Only PM2.5 measurements are currently supported.")

    if not OPENAQ_API_KEY:
        error_msg = (
            "OpenAQ API key is required but not found.\n"
            "Please set the OPENAQ_API_KEY environment variable or add it to config.py.\n"
            "Register for an API key at: https://explore.openaq.org/register"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        measurements = fetch_pm25_measurements(
            hours=hours,
            center_lat=lat,
            center_lon=lon,
            radius_km=radius_km,
            max_locations=max_locations,
            raise_on_error=True,
        )
    except Exception as exc:
        logger.error("Failed to fetch air quality data via OpenAQ SDK: %s", exc)
        raise

    if not measurements:
        logger.warning("OpenAQ SDK returned no PM2.5 measurements for the requested window.")

    return measurements


def parse_measurements(measurements):
    """
    Parse OpenAQ API measurements into a DataFrame.
    
    Args:
        measurements: List of measurement dictionaries from OpenAQ API
        
    Returns:
        pandas.DataFrame: Parsed measurements with columns: timestamp, location, pm25, latitude, longitude
    """
    if not measurements:
        logger.warning("No measurements to parse")
        return pd.DataFrame(columns=["timestamp", "location", "pm25", "latitude", "longitude"])
    
    records = []
    
    for measurement in measurements:
        try:
            # Extract timestamp (handle both v2 and v3 API formats)
            dt_str = None
            if "date" in measurement:
                if isinstance(measurement["date"], dict):
                    # Prefer local time (Vietnam timezone) over UTC
                    dt_str = measurement["date"].get("local") or measurement["date"].get("LOCAL") or measurement["date"].get("utc") or measurement["date"].get("UTC")
                else:
                    dt_str = measurement["date"]
            elif "datetime" in measurement:
                # Handle datetime field - prefer local time over UTC
                datetime_info = measurement["datetime"]
                if isinstance(datetime_info, dict):
                    # Prefer local time (Vietnam timezone) over UTC
                    dt_str = datetime_info.get("local") or datetime_info.get("LOCAL") or datetime_info.get("utc") or datetime_info.get("UTC")
                else:
                    dt_str = datetime_info
            
            if not dt_str:
                # OpenAQ v3 sensor endpoints sometimes embed timestamps inside the period field
                period_info = measurement.get("period")
                if isinstance(period_info, dict):
                    datetime_to = period_info.get("datetimeTo") or period_info.get("datetime_to")
                    datetime_from = period_info.get("datetimeFrom") or period_info.get("datetime_from")

                    # Prefer datetimeFrom (start of interval) to match generate_traffic_data.py logic.
                    # Fall back to datetimeTo if datetimeFrom is not available.
                    for candidate in (datetime_from, datetime_to):
                        if isinstance(candidate, dict):
                            # Prefer local time (Vietnam timezone) over UTC
                            dt_str = (
                                candidate.get("local")
                                or candidate.get("LOCAL")
                                or candidate.get("utc")
                                or candidate.get("UTC")
                            )
                        elif isinstance(candidate, str):
                            dt_str = candidate

                        if dt_str:
                            break

            if not dt_str:
                logger.warning(f"Measurement missing timestamp: {list(measurement.keys())}")
                logger.debug("Offending measurement: %s", measurement)
                continue
            
            # Parse timestamp
            dt = pd.to_datetime(dt_str)
            # Ensure timezone is set correctly
            hcmc_tz = pytz.timezone(HCMC_TIMEZONE)
            if dt.tzinfo is None:
                # No timezone info - if we got here, it means we're using a fallback (UTC)
                # Convert from UTC to HCMC timezone
                dt = pytz.UTC.localize(dt)
                dt = dt.astimezone(hcmc_tz)
            else:
                # Has timezone info (should be +07:00 for local Vietnam time)
                # Ensure it's in HCMC timezone (should already be, but normalize just in case)
                dt = dt.astimezone(hcmc_tz)
            
            # Extract location info (handle both v2 and v3 formats)
            location_name = measurement.get("location") or measurement.get("locationName", "Unknown")
            coordinates = measurement.get("coordinates", {})
            if not coordinates or not isinstance(coordinates, dict):
                # Try to get from nested structure
                location_obj = measurement.get("location", {})
                if isinstance(location_obj, dict):
                    coordinates = location_obj.get("coordinates", {})
            
            lat = coordinates.get("latitude") if isinstance(coordinates, dict) else None
            lon = coordinates.get("longitude") if isinstance(coordinates, dict) else None
            
            # Extract parameter value
            value = measurement.get("value")
            
            # For v3 API, we already filtered by sensorsId, so all measurements should be PM2.5
            # But check parameter info if available
            parameter_name = None
            if "parameter" in measurement:
                param = measurement.get("parameter", {})
                if isinstance(param, dict):
                    parameter_name = param.get("name", "").lower()
                else:
                    parameter_name = str(param).lower()
            elif "parameter_id" in measurement:
                # parameter_id = 2 means PM2.5
                if measurement.get("parameter_id") == 2 or str(measurement.get("parameter_id")) == "2":
                    parameter_name = "pm25"
            
            # Include if it's PM2.5 or if we don't have parameter info (assume it's PM2.5 since we filtered by sensorsId)
            if value is not None:
                # Only include if explicitly PM2.5 or if no parameter info (meaning we filtered by sensorsId)
                if parameter_name is None or "pm25" in parameter_name or "pm2.5" in parameter_name:
                    records.append({
                        "timestamp": dt,
                        "location": location_name,
                        "pm25": float(value),
                        "latitude": lat,
                        "longitude": lon
                    })
        except Exception as e:
            logger.warning(f"Error parsing measurement: {e}")
            logger.debug(f"Measurement that failed: {measurement}")
            continue
    
    if not records:
        logger.warning("No valid PM2.5 measurements found")
        return pd.DataFrame(columns=["timestamp", "location", "pm25", "latitude", "longitude"])
    
    df = pd.DataFrame(records)
    
    # Remove duplicates (same location and timestamp)
    df = df.drop_duplicates(subset=["timestamp", "location"], keep="first")
    
    # Sort by timestamp
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    logger.info(f"Parsed {len(df)} PM2.5 measurements")
    
    return df


def load_existing_air_quality_data(file_path=None):
    """
    Load existing air quality data from CSV.
    
    Args:
        file_path: Optional path to CSV file
        
    Returns:
        pandas.DataFrame: Existing air quality data
    """
    if file_path is None:
        file_path = SPEED_VIEWS_DIR / AIR_QUALITY_SPEED_FILE
    else:
        file_path = Path(file_path)
    
    if not file_path.exists():
        logger.info(f"No existing air quality data file found at {file_path}")
        return pd.DataFrame(columns=["timestamp", "location", "pm25", "latitude", "longitude"])
    
    try:
        df = load_csv(file_path)
        df = parse_timestamp(df, "timestamp")
        logger.info(f"Loaded {len(df)} existing air quality records")
        return df
    except Exception as e:
        logger.warning(f"Error loading existing data: {e}. Starting fresh.")
        return pd.DataFrame(columns=["timestamp", "location", "pm25", "latitude", "longitude"])


def append_air_quality_data(new_df, file_path=None, overwrite=False):
    """
    Append new air quality data to existing CSV file or overwrite it.
    
    Args:
        new_df: New DataFrame with air quality measurements
        file_path: Optional path to CSV file
        overwrite: If True, replace entire file instead of appending
    """
    if new_df.empty:
        logger.warning("No new data to append")
        return
    
    if file_path is None:
        file_path = SPEED_VIEWS_DIR / AIR_QUALITY_SPEED_FILE
    else:
        file_path = Path(file_path)
    
    if overwrite:
        # Overwrite existing file
        if file_path.exists():
            logger.info(f"Overwriting existing air quality data at {file_path}")
        save_csv(new_df, file_path)
        logger.info(f"Saved {len(new_df)} air quality records (overwritten existing data)")
    else:
        # Load existing data and append
        existing_df = load_existing_air_quality_data(file_path)
        
        if existing_df.empty:
            # No existing data, just save the new data
            save_csv(new_df, file_path)
            logger.info(f"Saved {len(new_df)} new air quality records")
        else:
            # Combine and remove duplicates
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=["timestamp", "location"], keep="last")
            combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)
            
            save_csv(combined_df, file_path)
            logger.info(f"Appended {len(new_df)} new records. Total: {len(combined_df)} records")


def collect_air_quality_once(use_api=True, dataset_file=None, use_kafka=False):
    """
    Collect air quality data once from the API, Kafka, or load from raw JSON dataset.
    
    Args:
        use_api: If True, fetch from OpenAQ API and write to speed view CSV.
                 If False and use_kafka=False, load from raw JSON dataset file.
        dataset_file: Optional path to raw JSON dataset file (default: AIR_RAW_FILE_PATH)
                     Ignored if use_kafka=True.
        use_kafka: If True, read from Kafka topic; if False, use API or JSON file.
    
    Returns:
        pandas.DataFrame: Air quality measurements
    
    Raises:
        FileNotFoundError: If dataset file doesn't exist when use_api=False and use_kafka=False
        ConnectionError: If Kafka is unavailable when use_kafka=True
        ValueError: If dataset file is empty when use_api=False and use_kafka=False
    """
    if use_kafka:
        # Check Kafka connection before proceeding
        if not check_kafka_connection(KAFKA_BOOTSTRAP_SERVERS):
            error_msg = (
                f"Kafka unavailable at {KAFKA_BOOTSTRAP_SERVERS}. "
                "Start Kafka with: docker-compose up -d"
            )
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        
        logger.info(f"Loading air quality data from Kafka topic: {KAFKA_AIR_QUALITY_TOPIC}")
        
        try:
            measurements = read_air_quality_from_kafka(
                KAFKA_BOOTSTRAP_SERVERS,
                KAFKA_AIR_QUALITY_TOPIC,
                KAFKA_SPEED_CONSUMER_GROUP,
                timeout_ms=10000,
                raise_on_error=True,
            )
            
            if not measurements:
                error_msg = (
                    f"Error: No messages found in Kafka topic '{KAFKA_AIR_QUALITY_TOPIC}'. "
                    f"Please populate it first with: uv run python main.py dataset --use-api air --use-kafka"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Parse measurements to DataFrame
            df = parse_measurements(measurements)
            
            if df.empty:
                logger.warning("No valid measurements found in Kafka topic")
                return df
            
            # Write to speed view CSV
            target_path = SPEED_VIEWS_DIR / AIR_QUALITY_SPEED_FILE
            append_air_quality_data(df, file_path=target_path, overwrite=True)
            logger.info(
                f"Processed {len(df)} air quality records from Kafka to {target_path}"
            )
            
            return df
            
        except Exception as e:
            error_msg = f"Error reading from Kafka topic '{KAFKA_AIR_QUALITY_TOPIC}': {e}"
            logger.error(error_msg)
            raise
    elif use_api:
        logger.info("Collecting air quality data from OpenAQ API...")
        
        try:
            measurements = fetch_air_quality_data()
            df = parse_measurements(measurements)
            
            if not df.empty:
                # Write to speed view CSV (DO NOT write to raw JSON)
                append_air_quality_data(df, overwrite=True)
                logger.info(f"Successfully collected {len(df)} new air quality measurements")
            else:
                logger.warning("No air quality data collected from API")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to collect air quality data from API: {e}")
            raise
    else:
        # Load from raw JSON dataset file
        if dataset_file is None:
            dataset_file = AIR_RAW_FILE_PATH
        else:
            dataset_file = Path(dataset_file)
        
        if not dataset_file.exists():
            error_msg = (
                f"Error: Dataset file '{dataset_file}' not found or empty. "
                f"Please create it first with: uv run python main.py dataset --use-api air"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            logger.info(f"Loading air quality data from raw JSON dataset: {dataset_file}")
            measurements = load_json(dataset_file)
            
            if not measurements:
                error_msg = (
                    f"Error: Dataset file '{dataset_file}' is empty. "
                    f"Please create it first with: uv run python main.py dataset --use-api air"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Parse measurements to DataFrame
            df = parse_measurements(measurements)
            
            if df.empty:
                logger.warning("No valid measurements found in dataset file")
                return df
            
            # Write to speed view CSV
            target_path = SPEED_VIEWS_DIR / AIR_QUALITY_SPEED_FILE
            append_air_quality_data(df, file_path=target_path, overwrite=True)
            logger.info(
                f"Processed {len(df)} air quality records from dataset to {target_path}"
            )
            
            return df
            
        except (FileNotFoundError, ValueError):
            # Re-raise these specific errors
            raise
        except Exception as e:
            error_msg = (
                f"Error loading dataset file '{dataset_file}': {e}. "
                f"Please create it first with: uv run python main.py dataset --use-api air"
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e


def collect_air_quality_continuous(interval_minutes=POLLING_INTERVAL_MINUTES, max_iterations=None, use_api=True, use_kafka=False):
    """
    Continuously poll air quality API or Kafka at specified intervals.
    
    Args:
        interval_minutes: Polling interval in minutes
        max_iterations: Maximum number of iterations (None = infinite)
        use_api: If True, fetch from OpenAQ API; if False and use_kafka=False, load from JSON file
        use_kafka: If True, read from Kafka topic; if False, use API or JSON file
    """
    logger.info(f"Starting continuous air quality collection (interval: {interval_minutes} minutes, use_api={use_api}, use_kafka={use_kafka})")
    
    iteration = 0
    
    try:
        while True:
            if max_iterations and iteration >= max_iterations:
                logger.info(f"Reached maximum iterations ({max_iterations})")
                break
            
            iteration += 1
            logger.info(f"Iteration {iteration}: Collecting air quality data...")
            
            try:
                collect_air_quality_once(use_api=use_api, use_kafka=use_kafka)
            except Exception as e:
                logger.error(f"Error in iteration {iteration}: {e}")
                # Continue to next iteration even if this one failed
            
            # Wait for next interval
            if max_iterations is None or iteration < max_iterations:
                wait_seconds = interval_minutes * 60
                logger.info(f"Waiting {interval_minutes} minutes until next collection...")
                time.sleep(wait_seconds)
                
    except KeyboardInterrupt:
        logger.info("Continuous collection interrupted by user")
    except Exception as e:
        logger.error(f"Continuous collection failed: {e}")
        raise


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test one-time collection
    try:
        df = collect_air_quality_once()
        if not df.empty:
            print(f"\nCollected {len(df)} air quality measurements")
            print(f"\nFirst few rows:")
            print(df.head())
        else:
            print("\nNo air quality data collected. This might be normal if no stations are reporting.")
    except Exception as e:
        logger.error(f"Air quality collection failed: {e}", exc_info=True)
        raise

