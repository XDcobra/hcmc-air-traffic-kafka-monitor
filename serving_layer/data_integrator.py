"""Serving layer data integrator for combining batch and speed layer data."""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

from config import (
    BATCH_VIEWS_DIR,
    SPEED_VIEWS_DIR,
    SERVING_VIEWS_DIR,
    TRAFFIC_BATCH_VIEW_FILE,
    AIR_QUALITY_SPEED_FILE,
    COMBINED_VIEW_FILE,
    CRITICAL_PERIODS_FILE,
    PM25_THRESHOLD,
    SPEED_THRESHOLD,
    LAKEHOUSE_TRAFFIC_BATCH_VIEW_PATH,
    LAKEHOUSE_AIR_QUALITY_SPEED_VIEW_PATH,
    LAKEHOUSE_SERVING_COMBINED_VIEW_PATH,
)
from utils.data_loader import (
    load_csv,
    parse_timestamp,
    floor_to_hour,
    save_csv,
    check_lakehouse_available,
    save_to_delta_table,
    load_from_delta_table,
)
from utils.visualizer import create_all_plots

logger = logging.getLogger(__name__)


def load_batch_view(file_path=None, use_lakehouse=False):
    """
    Load batch view (traffic data) from CSV or Delta Lake table.
    
    Args:
        file_path: Optional path to batch view file (ignored if use_lakehouse=True)
        use_lakehouse: If True, load from Delta Lake table; if False, load from CSV
        
    Returns:
        pandas.DataFrame: Batch view data
    """
    if use_lakehouse:
        if not check_lakehouse_available():
            logger.warning("Lakehouse unavailable, falling back to CSV")
            use_lakehouse = False
        
        if use_lakehouse:
            try:
                df = load_from_delta_table(LAKEHOUSE_TRAFFIC_BATCH_VIEW_PATH, raise_on_error=True)
                if df is not None and not df.empty:
                    df = parse_timestamp(df, "timestamp")
                    logger.info(f"Loaded {len(df)} batch view records from Lakehouse")
                    return df
                else:
                    raise FileNotFoundError(f"Batch view not found in Lakehouse: {LAKEHOUSE_TRAFFIC_BATCH_VIEW_PATH}")
            except Exception as e:
                logger.warning(f"Error loading from Lakehouse: {e}, falling back to CSV")
                use_lakehouse = False
    
    # Fallback to CSV
    if file_path is None:
        file_path = BATCH_VIEWS_DIR / TRAFFIC_BATCH_VIEW_FILE
    else:
        file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Batch view file not found: {file_path}")
    
    logger.info(f"Loading batch view from {file_path}")
    df = load_csv(file_path)
    df = parse_timestamp(df, "timestamp")
    
    logger.info(f"Loaded {len(df)} batch view records")
    return df


def load_speed_view(file_path=None, use_lakehouse=False):
    """
    Load speed view (air quality data) from CSV or Delta Lake table.
    
    Args:
        file_path: Optional path to speed view file (ignored if use_lakehouse=True)
        use_lakehouse: If True, load from Delta Lake table; if False, load from CSV
        
    Returns:
        pandas.DataFrame: Speed view data
    """
    if use_lakehouse:
        if not check_lakehouse_available():
            logger.warning("Lakehouse unavailable, falling back to CSV")
            use_lakehouse = False
        
        if use_lakehouse:
            try:
                df = load_from_delta_table(LAKEHOUSE_AIR_QUALITY_SPEED_VIEW_PATH, raise_on_error=False)
                if df is not None and not df.empty:
                    df = parse_timestamp(df, "timestamp")
                    logger.info(f"Loaded {len(df)} speed view records from Lakehouse")
                    return df
                else:
                    logger.warning(f"Speed view not found in Lakehouse: {LAKEHOUSE_AIR_QUALITY_SPEED_VIEW_PATH}. Returning empty DataFrame.")
                    return pd.DataFrame(columns=["timestamp", "location", "pm25", "latitude", "longitude"])
            except Exception as e:
                logger.warning(f"Error loading from Lakehouse: {e}, falling back to CSV")
                use_lakehouse = False
    
    # Fallback to CSV
    if file_path is None:
        file_path = SPEED_VIEWS_DIR / AIR_QUALITY_SPEED_FILE
    else:
        file_path = Path(file_path)
    
    if not file_path.exists():
        logger.warning(f"Speed view file not found: {file_path}. Returning empty DataFrame.")
        return pd.DataFrame(columns=["timestamp", "location", "pm25", "latitude", "longitude"])
    
    logger.info(f"Loading speed view from {file_path}")
    df = load_csv(file_path)
    df = parse_timestamp(df, "timestamp")
    
    logger.info(f"Loaded {len(df)} speed view records")
    return df


def aggregate_air_quality_by_hour(air_quality_df):
    """
    Aggregate air quality data by hour (average PM2.5 per hour).
    
    Args:
        air_quality_df: DataFrame with air quality measurements
        
    Returns:
        pandas.DataFrame: Hourly aggregated air quality data
    """
    if air_quality_df.empty:
        logger.warning("Empty air quality DataFrame, returning empty aggregated data")
        return pd.DataFrame(columns=["timestamp", "avg_pm25", "pm25_count", "location_count"])
    
    # Floor timestamps to hour
    air_quality_df = air_quality_df.copy()
    
    # Filter out invalid PM2.5 values (negative values or missing value flags like -999)
    if "pm25" in air_quality_df.columns:
        initial_count = len(air_quality_df)
        air_quality_df = air_quality_df[air_quality_df["pm25"] > 0].copy()
        filtered_count = initial_count - len(air_quality_df)
        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} invalid PM2.5 values (negative or zero)")
    
    if air_quality_df.empty:
        logger.warning("No valid PM2.5 values after filtering")
        return pd.DataFrame(columns=["timestamp", "avg_pm25", "pm25_count", "location_count"])
    
    air_quality_df["hour_window"] = floor_to_hour(air_quality_df["timestamp"])
    
    # Group by hour and aggregate
    agg_dict = {
        "pm25": ["mean", "count"],
        "location": "nunique"  # Count unique locations
    }
    
    aggregated = air_quality_df.groupby("hour_window", as_index=False).agg(agg_dict)
    
    # Flatten column names
    aggregated.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                         for col in aggregated.columns.values]
    
    # Rename columns
    aggregated = aggregated.rename(columns={
        "hour_window": "timestamp",
        "pm25_mean": "avg_pm25",
        "pm25_count": "pm25_count",
        "location_nunique": "location_count"
    })
    
    logger.info(f"Aggregated {len(air_quality_df)} measurements into {len(aggregated)} hourly records")
    
    return aggregated


def merge_batch_and_speed(batch_df, speed_df):
    """
    Merge batch view (traffic) and speed view (air quality) on timestamp.
    
    Args:
        batch_df: Batch view DataFrame with traffic data
        speed_df: Speed view DataFrame with air quality data
        
    Returns:
        pandas.DataFrame: Combined view with both traffic and air quality data
    """
    logger.info("Merging batch and speed layer data...")
    
    # Aggregate air quality by hour
    speed_aggregated = aggregate_air_quality_by_hour(speed_df)
    
    if speed_aggregated.empty:
        logger.warning("No air quality data to merge. Creating combined view with traffic data only.")
        combined_df = batch_df.copy()
        combined_df["avg_pm25"] = np.nan
        combined_df["pm25_count"] = 0
        combined_df["location_count"] = 0
        return combined_df
    
    # Try to align timestamps if they're close but not exact
    # If timestamps are within 2 hours of each other, align them to the same hour
    time_tolerance_hours = 2
    
    batch_min = batch_df['timestamp'].min()
    batch_max = batch_df['timestamp'].max()
    speed_min = speed_aggregated['timestamp'].min()
    speed_max = speed_aggregated['timestamp'].max()
    
    # Use only the most recent data from each source (within last 24 hours)
    # This helps when APIs return data with different timestamps
    from datetime import datetime, timedelta
    import pytz
    from config import HCMC_TIMEZONE
    
    hcmc_tz = pytz.timezone(HCMC_TIMEZONE)
    now = datetime.now(hcmc_tz)
    cutoff_time = now - timedelta(hours=24)
    
    # Filter to only recent data
    batch_df_recent = batch_df[batch_df['timestamp'] >= cutoff_time].copy()
    speed_aggregated_recent = speed_aggregated[speed_aggregated['timestamp'] >= cutoff_time].copy()
    
    if len(batch_df_recent) > 0:
        logger.info(f"Filtered batch data to last 24h: {len(batch_df_recent)} records (from {len(batch_df)} total)")
        batch_df = batch_df_recent
    if len(speed_aggregated_recent) > 0:
        logger.info(f"Filtered air quality data to last 24h: {len(speed_aggregated_recent)} records (from {len(speed_aggregated)} total)")
        speed_aggregated = speed_aggregated_recent
    
    # Recalculate min/max after filtering
    if not batch_df.empty and not speed_aggregated.empty:
        batch_min = batch_df['timestamp'].min()
        batch_max = batch_df['timestamp'].max()
        speed_min = speed_aggregated['timestamp'].min()
        speed_max = speed_aggregated['timestamp'].max()
        
        # Calculate time difference between the closest timestamps
        time_diff = abs((batch_max - speed_max).total_seconds() / 3600)  # hours - use max timestamps
    
    if not batch_df.empty and not speed_aggregated.empty and time_diff <= time_tolerance_hours:
        # Timestamps are close - align them to the same hour (use the most recent)
        target_hour = max(batch_max, speed_max)
        target_hour = target_hour.floor('h')  # Round down to hour
        
        logger.info(f"Timestamps are within {time_tolerance_hours}h tolerance ({time_diff:.1f}h difference). Aligning to hour: {target_hour}")
        
        # Update timestamps to target hour for records within tolerance
        batch_df_aligned = batch_df.copy()
        speed_aggregated_aligned = speed_aggregated.copy()
        
        # Align batch timestamps within tolerance
        batch_mask = (batch_df['timestamp'] >= target_hour - pd.Timedelta(hours=time_tolerance_hours)) & \
                     (batch_df['timestamp'] <= target_hour + pd.Timedelta(hours=time_tolerance_hours))
        if batch_mask.any():
            batch_df_aligned.loc[batch_mask, 'timestamp'] = target_hour
        
        # Align speed timestamps within tolerance
        speed_mask = (speed_aggregated['timestamp'] >= target_hour - pd.Timedelta(hours=time_tolerance_hours)) & \
                    (speed_aggregated['timestamp'] <= target_hour + pd.Timedelta(hours=time_tolerance_hours))
        if speed_mask.any():
            speed_aggregated_aligned.loc[speed_mask, 'timestamp'] = target_hour
        
        # Re-aggregate if we changed timestamps (in case multiple records now have same timestamp)
        # Check if road_segment_id column exists
        group_cols = ['timestamp']
        if 'road_segment_id' in batch_df_aligned.columns:
            group_cols.append('road_segment_id')
            dup_subset = ['timestamp', 'road_segment_id']
        else:
            dup_subset = ['timestamp']
        
        if batch_mask.any() and batch_df_aligned[batch_mask].duplicated(subset=dup_subset).any():
            logger.info("Re-aggregating batch data after timestamp alignment")
            batch_df_aligned = batch_df_aligned.groupby(group_cols).agg({
                'avg_speed': 'mean',
                'min_speed': 'min',
                'max_speed': 'max',
                'record_count': 'sum'
            }).reset_index()
        
        if speed_mask.any() and speed_aggregated_aligned[speed_mask].duplicated(subset=['timestamp']).any():
            logger.info("Re-aggregating air quality data after timestamp alignment")
            speed_aggregated_aligned = speed_aggregated_aligned.groupby('timestamp').agg({
                'avg_pm25': 'mean',
                'pm25_count': 'sum',
                'location_count': 'sum'
            }).reset_index()
        
        batch_df = batch_df_aligned
        speed_aggregated = speed_aggregated_aligned
    else:
        logger.info(f"Timestamps are {time_diff:.1f}h apart (exceeds {time_tolerance_hours}h tolerance). No alignment performed.")
    
    # Merge on timestamp
    combined_df = pd.merge(
        batch_df,
        speed_aggregated[["timestamp", "avg_pm25", "pm25_count", "location_count"]],
        on="timestamp",
        how="outer"  # Keep all timestamps from both datasets
    )
    
    # Sort by timestamp
    combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)
    
    logger.info(f"Merged data: {len(combined_df)} records")
    logger.info(f"  - Records with traffic data: {combined_df['avg_speed'].notna().sum()}")
    logger.info(f"  - Records with air quality data: {combined_df['avg_pm25'].notna().sum()}")
    records_with_both = (combined_df['avg_speed'].notna() & combined_df['avg_pm25'].notna()).sum()
    logger.info(f"  - Records with both: {records_with_both}")
    
    # Warn if time ranges don't overlap
    if records_with_both == 0 and not speed_aggregated.empty:
        batch_time_range = f"{batch_df['timestamp'].min()} to {batch_df['timestamp'].max()}"
        speed_time_range = f"{speed_aggregated['timestamp'].min()} to {speed_aggregated['timestamp'].max()}"
        logger.warning(f"Time ranges don't overlap! Traffic: {batch_time_range}, Air Quality: {speed_time_range}")
        logger.warning("This is expected if using historical traffic data with current air quality data.")
    
    return combined_df


def identify_critical_periods(combined_df, pm25_threshold=PM25_THRESHOLD, speed_threshold=SPEED_THRESHOLD):
    """
    Identify critical periods where both PM2.5 is high and speed is low.
    
    Args:
        combined_df: Combined DataFrame with traffic and air quality data
        pm25_threshold: PM2.5 threshold (default from config)
        speed_threshold: Speed threshold (default from config)
        
    Returns:
        pandas.DataFrame: Combined DataFrame with is_critical and severity_index columns added
    """
    logger.info(f"Identifying critical periods (PM2.5 > {pm25_threshold} µg/m³ AND speed < {speed_threshold} km/h)...")
    
    # Create a copy to avoid modifying original
    df = combined_df.copy()
    
    # Mark critical periods
    df["is_critical"] = (
        (df["avg_pm25"] > pm25_threshold) & 
        (df["avg_speed"] < speed_threshold) &
        (df["avg_pm25"].notna()) &
        (df["avg_speed"].notna())
    )
    
    # Calculate severity index (optional)
    # Normalize and combine: higher PM2.5 and lower speed = higher severity
    if "avg_pm25" in df.columns and "avg_speed" in df.columns:
        # Normalize PM2.5 (0-1 scale, assuming max 200 µg/m³)
        pm25_normalized = (df["avg_pm25"] / 200.0).clip(0, 1)
        # Normalize speed (0-1 scale, inverted: lower speed = higher value)
        speed_normalized = 1 - (df["avg_speed"] / 100.0).clip(0, 1)
        # Combined severity (average of both)
        df["severity_index"] = (pm25_normalized + speed_normalized) / 2.0
    
    # Count critical periods
    critical_count = df["is_critical"].sum()
    logger.info(f"Found {critical_count} critical periods")
    
    return df


def generate_summary_statistics(combined_df, critical_df):
    """
    Generate summary statistics for the combined data and critical periods.
    
    Args:
        combined_df: Combined DataFrame
        critical_df: Critical periods DataFrame
        
    Returns:
        dict: Summary statistics
    """
    stats = {}
    
    # Overall statistics
    if "avg_speed" in combined_df.columns:
        stats["traffic"] = {
            "total_records": len(combined_df),
            "records_with_speed": combined_df["avg_speed"].notna().sum(),
            "avg_speed_mean": combined_df["avg_speed"].mean() if combined_df["avg_speed"].notna().any() else None,
            "avg_speed_min": combined_df["avg_speed"].min() if combined_df["avg_speed"].notna().any() else None,
            "avg_speed_max": combined_df["avg_speed"].max() if combined_df["avg_speed"].notna().any() else None,
        }
    
    if "avg_pm25" in combined_df.columns:
        stats["air_quality"] = {
            "records_with_pm25": combined_df["avg_pm25"].notna().sum(),
            "avg_pm25_mean": combined_df["avg_pm25"].mean() if combined_df["avg_pm25"].notna().any() else None,
            "avg_pm25_min": combined_df["avg_pm25"].min() if combined_df["avg_pm25"].notna().any() else None,
            "avg_pm25_max": combined_df["avg_pm25"].max() if combined_df["avg_pm25"].notna().any() else None,
        }
    
    # Critical periods statistics
    stats["critical_periods"] = {
        "total_count": len(critical_df),
        "percentage_of_total": (len(critical_df) / len(combined_df) * 100) if len(combined_df) > 0 else 0,
    }
    
    if not critical_df.empty:
        if "avg_speed" in critical_df.columns:
            stats["critical_periods"]["avg_speed_mean"] = critical_df["avg_speed"].mean()
        if "avg_pm25" in critical_df.columns:
            stats["critical_periods"]["avg_pm25_mean"] = critical_df["avg_pm25"].mean()
        if "severity_index" in critical_df.columns:
            stats["critical_periods"]["severity_index_mean"] = critical_df["severity_index"].mean()
    
    return stats


def run_serving_layer(batch_file=None, speed_file=None, create_plots=True, use_lakehouse=False):
    """
    Complete serving layer pipeline: merge data, identify critical periods, generate outputs.
    
    Args:
        batch_file: Optional path to batch view file (ignored if use_lakehouse=True)
        speed_file: Optional path to speed view file (ignored if use_lakehouse=True)
        create_plots: Whether to create visualization plots
        use_lakehouse: If True, read from/write to Lakehouse; if False, use CSV files
        
    Returns:
        tuple: (combined_df, critical_df, stats)
    """
    logger.info("Starting serving layer processing...")
    
    # Load data
    batch_df = load_batch_view(batch_file, use_lakehouse=use_lakehouse)
    speed_df = load_speed_view(speed_file, use_lakehouse=use_lakehouse)
    
    # Merge data
    combined_df = merge_batch_and_speed(batch_df, speed_df)
    
    # Identify critical periods (adds is_critical and severity_index columns to combined_df)
    combined_df = identify_critical_periods(combined_df)
    
    # Extract critical periods for separate file
    critical_df = combined_df[combined_df["is_critical"] == True].copy()
    
    # Generate statistics
    stats = generate_summary_statistics(combined_df, critical_df)
    
    # Save combined view (includes is_critical column)
    if use_lakehouse and check_lakehouse_available():
        try:
            # Add partition columns for better query performance
            from lakehouse_client import add_partition_columns
            combined_df_for_save = add_partition_columns(combined_df, timestamp_col="timestamp")
            
            success = save_to_delta_table(
                df=combined_df_for_save,
                table_path=LAKEHOUSE_SERVING_COMBINED_VIEW_PATH,
                mode="overwrite",
                partition_by=["year", "month"],
                raise_on_error=False,
            )
            if success:
                logger.info(f"Combined view saved to Lakehouse: {LAKEHOUSE_SERVING_COMBINED_VIEW_PATH}")
            else:
                logger.warning("Failed to save to Lakehouse, falling back to CSV")
                use_lakehouse = False
        except Exception as e:
            logger.warning(f"Error saving to Lakehouse: {e}, falling back to CSV")
            use_lakehouse = False
    
    if not use_lakehouse:
        combined_output = SERVING_VIEWS_DIR / COMBINED_VIEW_FILE
        save_csv(combined_df, combined_output)
        logger.info(f"Combined view saved to {combined_output}")
    
    # Save critical periods (always to CSV for now, could be extended to Lakehouse)
    if not critical_df.empty:
        critical_output = SERVING_VIEWS_DIR / CRITICAL_PERIODS_FILE
        save_csv(critical_df, critical_output)
        logger.info(f"Critical periods saved to {critical_output}")
    else:
        logger.warning("No critical periods found")
    
    # Create visualizations
    if create_plots:
        try:
            create_all_plots(combined_df, critical_df)
        except Exception as e:
            logger.warning(f"Error creating plots: {e}")
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("SERVING LAYER SUMMARY")
    logger.info("="*60)
    logger.info(f"Total combined records: {len(combined_df)}")
    logger.info(f"Critical periods found: {len(critical_df)}")
    if stats.get("traffic"):
        avg_speed = stats['traffic'].get('avg_speed_mean')
        if avg_speed is not None:
            logger.info(f"Average traffic speed: {avg_speed:.2f} km/h")
        else:
            logger.info("Average traffic speed: N/A")
    if stats.get("air_quality"):
        avg_pm25 = stats['air_quality'].get('avg_pm25_mean')
        if avg_pm25 is not None:
            logger.info(f"Average PM2.5: {avg_pm25:.2f} µg/m³")
        else:
            logger.info("Average PM2.5: N/A")
    logger.info("="*60)
    
    logger.info("Serving layer processing completed successfully")
    
    return combined_df, critical_df, stats


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run serving layer
    try:
        combined_df, critical_df, stats = run_serving_layer()
        
        print(f"\nServing layer completed successfully!")
        print(f"\nCombined view: {len(combined_df)} records")
        print(f"Critical periods: {len(critical_df)} records")
        
        if not critical_df.empty:
            print(f"\nFirst few critical periods:")
            print(critical_df[["timestamp", "avg_speed", "avg_pm25", "severity_index"]].head())
    except Exception as e:
        logger.error(f"Serving layer processing failed: {e}", exc_info=True)
        raise

