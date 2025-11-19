"""Batch layer processor for historical traffic data."""

import pandas as pd
from pathlib import Path
import logging

from config import (
    BATCH_VIEWS_DIR,
    TRAFFIC_RAW_FILE_PATH,
    TRAFFIC_BATCH_VIEW_FILE,
    HCMC_TIMEZONE,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TRAFFIC_TOPIC,
    KAFKA_BATCH_CONSUMER_GROUP,
)
from utils.data_loader import load_json, parse_timestamp, floor_to_hour, save_csv
from kafka_client import (
    check_kafka_connection,
    read_traffic_from_kafka,
)

logger = logging.getLogger(__name__)

# Import traffic collector for API-based collection
try:
    from batch_layer.traffic_collector import run_batch_processing_from_api
    TRAFFIC_COLLECTOR_AVAILABLE = True
except ImportError:
    TRAFFIC_COLLECTOR_AVAILABLE = False


def find_traffic_file():
    """
    Find the traffic data file in the raw data directory.
    
    Returns:
        Path: Path to the traffic data file
        
    Raises:
        FileNotFoundError: If no traffic file is found
    """
    if TRAFFIC_RAW_FILE_PATH.exists():
        return TRAFFIC_RAW_FILE_PATH
    
    raise FileNotFoundError(
        f"No traffic data file found. "
        f"Expected file: {TRAFFIC_RAW_FILE_PATH}. "
        f"Please create it first with: uv run python main.py dataset --use-api traffic"
    )


def parse_traffic_json(raw_responses):
    """
    Parse raw TomTom API responses from JSON into a DataFrame.
    
    Args:
        raw_responses: List of raw API response dictionaries
        
    Returns:
        pandas.DataFrame: DataFrame with columns: timestamp, segment_id, velocity
    """
    import pytz
    from datetime import datetime
    
    records = []
    hcmc_tz = pytz.timezone(HCMC_TIMEZONE)
    
    for response in raw_responses:
        try:
            # Extract flowSegmentData
            flow_data = response.get("flowSegmentData", {})
            if not flow_data:
                continue
            
            # Extract timestamp (use _fetched_at if available, otherwise current time)
            fetched_at = response.get("_fetched_at")
            if fetched_at:
                try:
                    timestamp = datetime.fromisoformat(fetched_at.replace('Z', '+00:00'))
                    if timestamp.tzinfo is None:
                        timestamp = hcmc_tz.localize(timestamp)
                    else:
                        timestamp = timestamp.astimezone(hcmc_tz)
                except Exception:
                    timestamp = datetime.now(hcmc_tz)
            else:
                timestamp = datetime.now(hcmc_tz)
            
            # Extract speed (in m/s, convert to km/h)
            current_speed_ms = flow_data.get("currentSpeed", 0)
            free_flow_speed_ms = flow_data.get("freeFlowSpeed", 0)
            speed_ms = current_speed_ms if current_speed_ms > 0 else free_flow_speed_ms
            speed_kmh = speed_ms * 3.6  # Convert m/s to km/h
            
            if speed_kmh <= 0:
                continue
            
            # Extract coordinates
            coordinates = flow_data.get("coordinates", {})
            segment_lat = None
            segment_lon = None
            
            if isinstance(coordinates, dict):
                coord = coordinates.get("coordinate", [])
                if isinstance(coord, list) and len(coord) > 0:
                    point = coord[0]
                    if isinstance(point, dict):
                        segment_lat = point.get("latitude")
                        segment_lon = point.get("longitude")
            
            # Create segment ID from coordinates (rounded for grouping)
            if segment_lat is not None and segment_lon is not None:
                segment_id = f"segment_{round(segment_lat * 100):.0f}_{round(segment_lon * 100):.0f}"
            else:
                # Fallback: use a default segment ID
                segment_id = "segment_unknown"
            
            records.append({
                "timestamp": timestamp,
                "segment_id": segment_id,
                "velocity": round(speed_kmh, 2)
            })
        except Exception as e:
            logger.warning(f"Error parsing traffic response: {e}")
            continue
    
    if not records:
        return pd.DataFrame(columns=["timestamp", "segment_id", "velocity"])
    
    df = pd.DataFrame(records)
    return df




def process_batch_layer(traffic_file=None, use_kafka=False):
    """
    Process historical traffic data to create batch views.
    
    Args:
        traffic_file: Optional path to traffic data JSON file. If None, will search for it.
                     Ignored if use_kafka=True.
        use_kafka: If True, read from Kafka topic; if False, read from JSON file.
        
    Returns:
        pandas.DataFrame: Aggregated batch view
        
    Raises:
        FileNotFoundError: If traffic file is not found (when use_kafka=False)
        ConnectionError: If Kafka is unavailable (when use_kafka=True)
        ValueError: If required columns are missing or file is empty
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
        
        logger.info(f"Processing batch layer from Kafka topic: {KAFKA_TRAFFIC_TOPIC}")
        
        # Read from Kafka
        try:
            raw_responses = read_traffic_from_kafka(
                KAFKA_BOOTSTRAP_SERVERS,
                KAFKA_TRAFFIC_TOPIC,
                KAFKA_BATCH_CONSUMER_GROUP,
                timeout_ms=10000,
                raise_on_error=True,
            )
            if not raw_responses:
                error_msg = (
                    f"Error: No messages found in Kafka topic '{KAFKA_TRAFFIC_TOPIC}'. "
                    f"Please populate it first with: uv run python main.py dataset --use-api traffic --use-kafka"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            df = parse_traffic_json(raw_responses)
        except Exception as e:
            error_msg = f"Error reading from Kafka topic '{KAFKA_TRAFFIC_TOPIC}': {e}"
            logger.error(error_msg)
            raise
    else:
        # Find traffic file if not provided
        if traffic_file is None:
            traffic_file = find_traffic_file()
        else:
            traffic_file = Path(traffic_file)
        
        if not traffic_file.exists():
            error_msg = (
                f"Error: Dataset file '{traffic_file}' not found or empty. "
                f"Please create it first with: uv run python main.py dataset --use-api traffic"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        logger.info(f"Processing batch layer with file: {traffic_file}")
        
        # Load and parse JSON
        try:
            raw_responses = load_json(traffic_file)
            if not raw_responses:
                error_msg = (
                    f"Error: Dataset file '{traffic_file}' is empty. "
                    f"Please create it first with: uv run python main.py dataset --use-api traffic"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            df = parse_traffic_json(raw_responses)
        except (FileNotFoundError, ValueError) as e:
            # Re-raise these specific errors
            raise
        except Exception as e:
            error_msg = (
                f"Error loading dataset file '{traffic_file}': {e}. "
                f"Please create it first with: uv run python main.py dataset --use-api traffic"
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    logger.info(f"Loaded {len(df)} rows of traffic data")
    
    # Validate required columns (from JSON parsing, we expect: timestamp, segment_id, velocity)
    required_cols = ['timestamp', 'velocity']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in traffic data: {missing_cols}")
    
    # Parse timestamps
    df = parse_timestamp(df, 'timestamp')
    
    # Floor timestamps to hourly windows
    df['hour_window'] = floor_to_hour(df['timestamp'])
    
    # Prepare aggregation columns
    agg_dict = {
        'velocity': ['mean', 'min', 'max', 'count']
    }
    
    # Group by hour window and segment (if available)
    if 'segment_id' in df.columns:
        group_cols = ['hour_window', 'segment_id']
        logger.info("Aggregating by hour and road segment")
    else:
        group_cols = ['hour_window']
        logger.info("Aggregating by hour only (no segment_id column found)")
    
    # Perform aggregation
    grouped = df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    # Flatten column names
    grouped.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in grouped.columns.values]
    
    # Rename columns for clarity
    rename_map = {
        'hour_window': 'timestamp',
        'velocity_mean': 'avg_speed',
        'velocity_min': 'min_speed',
        'velocity_max': 'max_speed',
        'velocity_count': 'record_count'
    }
    
    if 'segment_id' in grouped.columns:
        rename_map['segment_id'] = 'road_segment_id'
    
    grouped = grouped.rename(columns=rename_map)
    
    # Ensure timestamp is the first column
    cols = ['timestamp'] + [c for c in grouped.columns if c != 'timestamp']
    grouped = grouped[cols]
    
    # Sort by timestamp
    grouped = grouped.sort_values('timestamp').reset_index(drop=True)
    
    logger.info(f"Created batch view with {len(grouped)} aggregated records")
    logger.info(f"Time range: {grouped['timestamp'].min()} to {grouped['timestamp'].max()}")
    
    return grouped


def save_batch_view(batch_df, output_file=None):
    """
    Save batch view to CSV.
    
    Args:
        batch_df: Aggregated batch view DataFrame
        output_file: Optional output file path. If None, uses default from config.
    """
    if output_file is None:
        output_file = BATCH_VIEWS_DIR / TRAFFIC_BATCH_VIEW_FILE
    else:
        output_file = Path(output_file)
    
    save_csv(batch_df, output_file)
    logger.info(f"Batch view saved to {output_file}")


def run_batch_processing(traffic_file=None, output_file=None, use_api=False, use_kafka=False):
    """
    Complete batch processing pipeline.
    
    Args:
        traffic_file: Optional path to traffic data file (ignored if use_api=True or use_kafka=True)
        output_file: Optional output file path (ignored if use_api=True, uses default)
        use_api: If True, use TomTom API for traffic data. If False, use file or Kafka.
        use_kafka: If True, read from Kafka topic. If False and use_api=False, use JSON file.
        
    Returns:
        pandas.DataFrame: The created batch view
    """
    logger.info("Starting batch layer processing...")
    
    # Check if API-based collection is requested and available
    if use_api:
        if TRAFFIC_COLLECTOR_AVAILABLE:
            logger.info("Using TomTom API for traffic data collection...")
            batch_df = run_batch_processing_from_api(use_api=True, num_segments=50, hours_back=24)
            return batch_df
        else:
            error_msg = "API collection requested but traffic_collector module not available."
            logger.error(error_msg)
            raise ImportError(error_msg)
    
    # Process data (from Kafka or file)
    if use_kafka:
        logger.info("Using Kafka topic for traffic data...")
        batch_df = process_batch_layer(use_kafka=True)
    else:
        logger.info("Using local dataset file for traffic data...")
        batch_df = process_batch_layer(traffic_file, use_kafka=False)
    
    # Save batch view
    save_batch_view(batch_df, output_file)
    
    logger.info("Batch layer processing completed successfully")
    
    return batch_df


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run batch processing
    try:
        batch_view = run_batch_processing()
        print(f"\nBatch view created successfully!")
        print(f"Records: {len(batch_view)}")
        print(f"\nFirst few rows:")
        print(batch_view.head())
    except Exception as e:
        logger.error(f"Batch processing failed: {e}", exc_info=True)
        raise

