"""Batch layer collector for real-time traffic data from APIs or simulation."""

import pandas as pd
import numpy as np
import requests
import time
from pathlib import Path
from datetime import datetime, timedelta
import pytz
import logging
import random

from config import (
    HCMC_TIMEZONE,
    BATCH_VIEWS_DIR,
    TRAFFIC_BATCH_VIEW_FILE,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    TOMTOM_API_KEY,
    TOMTOM_FLOW_SEGMENT_ENDPOINT,
    DISTRICT10_LAT_MIN,
    DISTRICT10_LAT_MAX,
    DISTRICT10_LON_MIN,
    DISTRICT10_LON_MAX,
    DISTRICT10_GRID_SIZE,
)
from utils.data_loader import parse_timestamp, floor_to_hour, save_csv

logger = logging.getLogger(__name__)


def fetch_traffic_data_from_api(
    lat_min=DISTRICT10_LAT_MIN,
    lat_max=DISTRICT10_LAT_MAX,
    lon_min=DISTRICT10_LON_MIN,
    lon_max=DISTRICT10_LON_MAX,
    grid_size=DISTRICT10_GRID_SIZE,
):
    """
    Fetch real-time traffic data from TomTom Traffic API.
    
    Uses Flow Segment Data API to get current traffic speeds at multiple points
    around HCMC area. Queries a grid of coordinates to get comprehensive coverage.
    
    Returns:
        list: List of traffic measurement dictionaries, or None if API fails
        
    Raises:
        ValueError: If API key is missing
        requests.RequestException: If API request fails
    """
    # Validate API key
    if not TOMTOM_API_KEY:
        error_msg = (
            "TomTom API key is required but not found.\n"
            "Please set the TOMTOM_API_KEY environment variable or add it to config.py.\n"
            "Register for an API key at: https://developer.tomtom.com/"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("Fetching real-time traffic data from TomTom Traffic API...")
    
    # Create a grid of coordinates within District 10 bounding box
    lat_points = np.linspace(lat_min, lat_max, grid_size)
    lon_points = np.linspace(lon_min, lon_max, grid_size)
    
    hcmc_tz = pytz.timezone(HCMC_TIMEZONE)
    current_time = datetime.now(hcmc_tz)
    
    traffic_data = []
    successful_requests = 0
    failed_requests = 0
    
    for lat in lat_points:
        for lon in lon_points:
            
            # Query TomTom Flow Segment Data API
            params = {
                "key": TOMTOM_API_KEY,
                "point": f"{lat},{lon}"
            }
            
            for attempt in range(MAX_RETRIES):
                try:
                    logger.debug(f"Querying TomTom API for point ({lat:.4f}, {lon:.4f}) - attempt {attempt + 1}/{MAX_RETRIES}")
                    response = requests.get(TOMTOM_FLOW_SEGMENT_ENDPOINT, params=params, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    # Parse TomTom API response
                    # Response structure: {"flowSegmentData": {...}}
                    flow_data = data.get("flowSegmentData", {})
                    
                    if not flow_data:
                        logger.warning(f"No flow segment data in response for point ({lat}, {lon})")
                        break
                    
                    # Extract current speed (in m/s, convert to km/h)
                    current_speed_ms = flow_data.get("currentSpeed", 0)
                    free_flow_speed_ms = flow_data.get("freeFlowSpeed", 0)
                    
                    # Use current speed if available, otherwise use free flow speed
                    speed_ms = current_speed_ms if current_speed_ms > 0 else free_flow_speed_ms
                    speed_kmh = speed_ms * 3.6  # Convert m/s to km/h
                    
                    if speed_kmh <= 0:
                        logger.debug(f"Invalid speed value for point ({lat}, {lon}): {speed_kmh}")
                        break
                    
                    # Extract coordinates from response
                    coordinates = flow_data.get("coordinates", {})
                    if isinstance(coordinates, dict):
                        coord = coordinates.get("coordinate", [])
                        if isinstance(coord, list) and len(coord) > 0:
                            # Use first coordinate point
                            point = coord[0]
                            if isinstance(point, dict):
                                segment_lat = point.get("latitude", lat)
                                segment_lon = point.get("longitude", lon)
                            else:
                                segment_lat = lat
                                segment_lon = lon
                        else:
                            segment_lat = lat
                            segment_lon = lon
                    else:
                        segment_lat = lat
                        segment_lon = lon
                    
                    # Create segment ID from coordinates (rounded for grouping)
                    segment_id = f"segment_{round(segment_lat * 100):.0f}_{round(segment_lon * 100):.0f}"
                    
                    # Use current time as timestamp (API doesn't provide exact timestamp for flow segment)
                    traffic_data.append({
                        "timestamp": current_time,
                        "segment_id": segment_id,
                        "velocity": round(speed_kmh, 2)
                    })
                    
                    successful_requests += 1
                    break  # Success, exit retry loop
                    
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        # Rate limit exceeded
                        logger.warning(f"Rate limit exceeded. Waiting before retry...")
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(RETRY_DELAY_SECONDS * 2)  # Longer wait for rate limits
                        else:
                            failed_requests += 1
                    elif e.response.status_code == 401:
                        # Invalid API key
                        error_msg = "Invalid TomTom API key. Please check your TOMTOM_API_KEY."
                        logger.error(error_msg)
                        raise ValueError(error_msg) from e
                    else:
                        logger.warning(f"HTTP error for point ({lat}, {lon}): {e}")
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(RETRY_DELAY_SECONDS)
                        else:
                            failed_requests += 1
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Request error for point ({lat}, {lon}): {e}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_SECONDS)
                    else:
                        failed_requests += 1
                except Exception as e:
                    logger.warning(f"Error processing response for point ({lat}, {lon}): {e}")
                    failed_requests += 1
                    break
            
            # Small delay between requests to respect rate limits
            time.sleep(0.1)
    
    if traffic_data:
        logger.info(f"Successfully fetched {len(traffic_data)} traffic measurements from TomTom API")
        logger.info(f"Successful requests: {successful_requests}, Failed requests: {failed_requests}")
        return traffic_data
    else:
        error_msg = "No traffic data could be fetched from TomTom API. All requests failed."
        logger.error(error_msg)
        return None


def generate_simulated_traffic_data(num_segments=50, hours_back=24):
    """
    Generate simulated traffic data with current timestamps.
    Uses realistic patterns based on time of day.
    
    Args:
        num_segments: Number of road segments to simulate
        hours_back: How many hours of historical data to generate
        
    Returns:
        list: List of traffic measurement dictionaries
    """
    logger.info(f"Generating simulated traffic data for {num_segments} segments over last {hours_back} hours...")
    
    hcmc_tz = pytz.timezone(HCMC_TIMEZONE)
    now = datetime.now(hcmc_tz)
    
    traffic_data = []
    
    # Generate data for each hour going back
    for hour_offset in range(hours_back):
        timestamp = now - timedelta(hours=hour_offset)
        
        # Determine base speed based on time of day (realistic HCMC patterns)
        hour_of_day = timestamp.hour
        
        # Rush hours: 7-9 AM and 5-7 PM have lower speeds
        if 7 <= hour_of_day < 9 or 17 <= hour_of_day < 19:
            base_speed = random.uniform(15, 30)  # Rush hour: 15-30 km/h
        elif 22 <= hour_of_day or hour_of_day < 6:
            base_speed = random.uniform(40, 60)  # Night: 40-60 km/h
        else:
            base_speed = random.uniform(25, 45)  # Normal hours: 25-45 km/h
        
        # Generate data for multiple road segments
        for segment_id in range(1, num_segments + 1):
            # Add some variation per segment
            speed_variation = random.uniform(-5, 5)
            speed = max(5, base_speed + speed_variation)  # Minimum 5 km/h
            
            traffic_data.append({
                "timestamp": timestamp,
                "segment_id": f"segment_{segment_id:03d}",
                "velocity": round(speed, 2)
            })
    
    logger.info(f"Generated {len(traffic_data)} simulated traffic measurements")
    return traffic_data


def collect_traffic_data_once(use_api=False, num_segments=50, hours_back=24):
    """
    Collect traffic data once (from API or simulation).
    
    Args:
        use_api: Whether to use API. If True and API fails, raises exception (no fallback to simulation)
        num_segments: Number of segments for simulation (only used if use_api=False)
        hours_back: Hours of data to generate for simulation (only used if use_api=False)
        
    Returns:
        pandas.DataFrame: Traffic data DataFrame
        
    Raises:
        ValueError: If use_api=True and API fails
    """
    if use_api:
        logger.info("Collecting traffic data from TomTom API...")
        api_data = fetch_traffic_data_from_api()
        if api_data:
            df = pd.DataFrame(api_data)
            logger.info(f"Collected {len(df)} traffic measurements from API")
            return df
        else:
            # API failed, raise error instead of falling back to simulation
            error_msg = "Failed to fetch traffic data from TomTom API. No fallback to simulation when use_api=True."
            logger.error(error_msg)
            raise ValueError(error_msg)
    else:
        # Use simulation
        logger.info("Collecting traffic data from simulation...")
        simulated_data = generate_simulated_traffic_data(num_segments, hours_back)
        df = pd.DataFrame(simulated_data)
        logger.info(f"Collected {len(df)} simulated traffic measurements")
        return df


def process_traffic_to_batch_view(traffic_df):
    """
    Process raw traffic data into batch view (hourly aggregations).
    
    Args:
        traffic_df: DataFrame with columns: timestamp, segment_id, velocity
        
    Returns:
        pandas.DataFrame: Aggregated batch view
    """
    logger.info("Processing traffic data into batch view...")
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(traffic_df["timestamp"]):
        traffic_df = parse_timestamp(traffic_df, "timestamp")
    
    # Floor timestamps to hourly windows
    traffic_df["hour_window"] = floor_to_hour(traffic_df["timestamp"])
    
    # Group by hour and segment
    agg_dict = {
        "velocity": ["mean", "min", "max", "count"]
    }
    
    grouped = traffic_df.groupby(["hour_window", "segment_id"], as_index=False).agg(agg_dict)
    
    # Flatten column names
    grouped.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in grouped.columns.values]
    
    # Rename columns
    grouped = grouped.rename(columns={
        "hour_window": "timestamp",
        "velocity_mean": "avg_speed",
        "velocity_min": "min_speed",
        "velocity_max": "max_speed",
        "velocity_count": "record_count"
    })
    
    # Sort by timestamp
    grouped = grouped.sort_values("timestamp").reset_index(drop=True)
    
    logger.info(f"Created batch view with {len(grouped)} aggregated records")
    logger.info(f"Time range: {grouped['timestamp'].min()} to {grouped['timestamp'].max()}")
    
    return grouped


def run_batch_processing_from_api(use_api=True, num_segments=50, hours_back=24):
    """
    Complete batch processing pipeline using API or simulation.
    
    Args:
        use_api: Whether to use API (default: True). If False, uses simulation.
        num_segments: Number of segments for simulation (only used if use_api=False)
        hours_back: Hours of data to generate (only used if use_api=False)
        
    Returns:
        pandas.DataFrame: The created batch view
    """
    if use_api:
        logger.info("Starting batch layer processing from TomTom API...")
    else:
        logger.info("Starting batch layer processing from simulation...")
    
    # Collect traffic data
    traffic_df = collect_traffic_data_once(use_api, num_segments, hours_back)
    
    # Process into batch view
    batch_df = process_traffic_to_batch_view(traffic_df)
    
    # Save batch view (overwrites existing file)
    output_file = BATCH_VIEWS_DIR / TRAFFIC_BATCH_VIEW_FILE
    if output_file.exists():
        logger.info(f"Overwriting existing batch view at {output_file}")
    save_csv(batch_df, output_file)
    logger.info(f"Batch view saved to {output_file}")
    
    logger.info("Batch layer processing completed successfully")
    
    return batch_df


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run batch processing with simulation
    try:
        batch_view = run_batch_processing_from_api(use_api=False, num_segments=50, hours_back=24)
        print(f"\nBatch view created successfully!")
        print(f"Records: {len(batch_view)}")
        print(f"\nFirst few rows:")
        print(batch_view.head())
    except Exception as e:
        logger.error(f"Batch processing failed: {e}", exc_info=True)
        raise

