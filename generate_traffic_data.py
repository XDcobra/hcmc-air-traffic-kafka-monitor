"""Generate synthetic traffic data for testing critical periods.
Dynamically extends existing data or creates new data with 1-3 critical periods
that match air quality critical periods (PM2.5 > 50 µg/m³).
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
import pytz
import pandas as pd

# Configuration
HCMC_TZ = pytz.timezone("Asia/Ho_Chi_Minh")
OUTPUT_FILE = Path("data/raw/traffic_raw.json")
AIR_RAW_FILE = Path("data/raw/air_raw.json")
AIR_SPEED_FILE = Path("data/speed_views/air_speed_layer_append.csv")
PM25_THRESHOLD = 50.0  # µg/m³ - Air quality threshold

# Base coordinates for District 10
BASE_COORDS = {
    "coordinate": [
        {"latitude": 10.7715, "longitude": 106.6664}
    ]
}


def load_existing_data(file_path: Path) -> list:
    """Load existing traffic data from JSON file."""
    if file_path.exists():
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load existing data: {e}")
    return []


def get_air_quality_time_range(
    air_raw_file: Path = AIR_RAW_FILE,
    air_speed_file: Path = AIR_SPEED_FILE
) -> Optional[Tuple[datetime, datetime]]:
    """
    Extract the time range from air quality data.
    Returns (start_time, end_time) or None if no data found.
    """
    # Try to load from CSV first (aggregated data)
    if air_speed_file.exists():
        try:
            df = pd.read_csv(air_speed_file)
            if "timestamp" in df.columns and len(df) > 0:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                start_time = df["timestamp"].min().to_pydatetime()
                end_time = df["timestamp"].max().to_pydatetime()
                # Round to hours
                start_time = start_time.replace(minute=0, second=0, microsecond=0)
                end_time = end_time.replace(minute=0, second=0, microsecond=0)
                return start_time, end_time
        except Exception as e:
            print(f"Warning: Could not load time range from CSV: {e}")
    
    # Fallback: try to load from raw JSON
    if air_raw_file.exists():
        try:
            with open(air_raw_file, "r", encoding="utf-8") as f:
                air_data = json.load(f)
            
            if isinstance(air_data, list) and len(air_data) > 0:
                timestamps = []
                
                for measurement in air_data:
                    period = measurement.get("period", {})
                    datetime_from = period.get("datetimeFrom", {})
                    local_time_str = datetime_from.get("local", "")
                    
                    if local_time_str:
                        try:
                            dt = datetime.fromisoformat(local_time_str.replace("Z", "+00:00"))
                            dt_hcmc = dt.astimezone(HCMC_TZ)
                            timestamps.append(dt_hcmc.replace(minute=0, second=0, microsecond=0))
                        except (ValueError, KeyError):
                            continue
                
                if timestamps:
                    start_time = min(timestamps)
                    end_time = max(timestamps)
                    return start_time, end_time
        except Exception as e:
            print(f"Warning: Could not load time range from JSON: {e}")
    
    return None


def get_last_timestamp(entries: list) -> datetime:
    """Extract the last timestamp from existing entries."""
    if not entries:
        # Default: start from 24 hours ago
        return HCMC_TZ.localize(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)) - timedelta(days=1)
    
    last_entry = entries[-1]
    fetched_at_str = last_entry.get("_fetched_at", "")
    if fetched_at_str:
        try:
            # Parse UTC timestamp
            dt_utc = datetime.fromisoformat(fetched_at_str.replace("Z", "+00:00"))
            # Convert to HCMC timezone
            dt_hcmc = dt_utc.astimezone(HCMC_TZ)
            # Round to hour and add 1 hour for next entry
            return dt_hcmc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        except ValueError:
            pass
    
    # Fallback: use current time
    return HCMC_TZ.localize(datetime.now().replace(minute=0, second=0, microsecond=0))


def load_air_quality_critical_periods(
    start_time: datetime,
    hours: int,
    air_raw_file: Path = AIR_RAW_FILE,
    air_speed_file: Path = AIR_SPEED_FILE
) -> set:
    """
    Load air quality data and identify critical periods (PM2.5 > threshold).
    Returns a set of hour offsets (0-based) within the time range that have high PM2.5.
    """
    critical_hour_offsets = set()
    
    # Try to load from CSV first (aggregated data)
    if air_speed_file.exists():
        try:
            df = pd.read_csv(air_speed_file)
            if "timestamp" in df.columns and "pm25" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                
                # Filter for the time range (with some flexibility - look 24h before and after)
                end_time = start_time + timedelta(hours=hours - 1)
                # Extend search window to find matching patterns
                search_start = start_time - timedelta(hours=24)
                search_end = end_time + timedelta(hours=24)
                mask = (df["timestamp"] >= search_start) & (df["timestamp"] <= search_end)
                df_filtered = df[mask].copy()
                
                if len(df_filtered) == 0:
                    return set()
                
                # Group by hour and calculate average PM2.5
                df_filtered["hour"] = df_filtered["timestamp"].dt.floor("h")
                hourly_pm25 = df_filtered.groupby("hour")["pm25"].mean()
                
                # Find hours with PM2.5 > threshold within the target time range
                for hour_dt, avg_pm25 in hourly_pm25.items():
                    if avg_pm25 > PM25_THRESHOLD and start_time <= hour_dt <= end_time:
                        # Calculate hour offset from start_time
                        hour_offset = int((hour_dt - start_time).total_seconds() / 3600)
                        if 0 <= hour_offset < hours:
                            critical_hour_offsets.add(hour_offset)
                
                return critical_hour_offsets
        except Exception as e:
            print(f"Warning: Could not load air quality from CSV: {e}")
    
    # Fallback: try to load from raw JSON
    if air_raw_file.exists():
        try:
            with open(air_raw_file, "r", encoding="utf-8") as f:
                air_data = json.load(f)
            
            if isinstance(air_data, list):
                # Group measurements by hour
                hourly_pm25 = {}
                
                for measurement in air_data:
                    # Extract timestamp from OpenAQ format
                    period = measurement.get("period", {})
                    datetime_from = period.get("datetimeFrom", {})
                    local_time_str = datetime_from.get("local", "")
                    
                    if local_time_str:
                        try:
                            # Parse local timestamp
                            dt = datetime.fromisoformat(local_time_str.replace("Z", "+00:00"))
                            dt_hcmc = dt.astimezone(HCMC_TZ)
                            hour_dt = dt_hcmc.replace(minute=0, second=0, microsecond=0)
                            
                            # Check if within time range
                            if start_time <= hour_dt <= (start_time + timedelta(hours=hours - 1)):
                                value = measurement.get("value", 0)
                                if hour_dt not in hourly_pm25:
                                    hourly_pm25[hour_dt] = []
                                hourly_pm25[hour_dt].append(value)
                        except (ValueError, KeyError):
                            continue
                
                # Calculate averages and find critical periods
                for hour_dt, values in hourly_pm25.items():
                    avg_pm25 = sum(values) / len(values) if values else 0
                    if avg_pm25 > PM25_THRESHOLD:
                        hour_offset = int((hour_dt - start_time).total_seconds() / 3600)
                        if 0 <= hour_offset < hours:
                            critical_hour_offsets.add(hour_offset)
                
                return critical_hour_offsets
        except Exception as e:
            print(f"Warning: Could not load air quality from JSON: {e}")
    
    return set()


def generate_critical_periods(
    start_time: datetime,
    hours: int,
    num_periods: int = None
) -> set:
    """
    Generate 1-3 critical periods that match air quality critical periods.
    If air quality data is available, uses those periods. Otherwise, generates random periods.
    """
    # Try to load critical periods from air quality data
    air_critical_hours = load_air_quality_critical_periods(start_time, hours)
    
    if air_critical_hours:
        # Use air quality critical periods, but ensure we have 1-3 periods total
        if num_periods is None:
            num_periods = random.randint(1, 3)
        
        # Group consecutive hours into periods
        sorted_hours = sorted(air_critical_hours)
        periods = []
        current_period = [sorted_hours[0]] if sorted_hours else []
        
        for i in range(1, len(sorted_hours)):
            if sorted_hours[i] == sorted_hours[i-1] + 1:
                current_period.append(sorted_hours[i])
            else:
                if current_period:
                    periods.append(current_period)
                current_period = [sorted_hours[i]]
        
        if current_period:
            periods.append(current_period)
        
        # Start with air quality critical periods
        selected_hours = set()
        for period in periods:
            selected_hours.update(period)
        
        # If we need more periods to reach 1-3, generate additional random ones
        if len(periods) < num_periods:
            needed_periods = num_periods - len(periods)
            available_hours = set(range(hours)) - selected_hours
            
            for _ in range(needed_periods):
                if not available_hours:
                    break
                
                # Each additional period is 1-3 hours long
                period_length = random.randint(1, 3)
                # Try to find a slot that doesn't overlap
                max_attempts = 20
                for _ in range(max_attempts):
                    if available_hours:
                        start_hour = random.choice(list(available_hours))
                        period_hours = set(range(start_hour, min(start_hour + period_length, hours)))
                        period_hours = period_hours & available_hours  # Only use available hours
                        
                        if len(period_hours) >= 1:  # At least 1 hour
                            selected_hours.update(period_hours)
                            available_hours -= period_hours
                            break
        
        return selected_hours
    
    # Fallback: generate random critical periods if no air quality data
    if num_periods is None:
        num_periods = random.randint(1, 3)
    
    critical_hours = set()
    
    for _ in range(num_periods):
        # Each critical period is 1-3 hours long
        period_length = random.randint(1, 3)
        # Random start hour (avoid overlapping with existing periods)
        max_start = max(0, hours - period_length) if hours >= period_length else 0
        if max_start > 0:
            start_hour = random.randint(0, max_start)
        else:
            start_hour = 0
        
        # Check for overlap
        period_hours = set(range(start_hour, min(start_hour + period_length, hours)))
        if not critical_hours & period_hours:
            critical_hours.update(period_hours)
    
    return critical_hours


def get_speed_for_hour(hour: int, is_critical: bool, previous_speed: float = None) -> float:
    """Generate speed for a given hour with realistic variation and smooth transitions."""
    if is_critical:
        # Critical periods: very low speed (8-20 km/h)
        base_speed = random.uniform(8.0, 20.0)
    else:
        # Normal periods: variable speed based on time of day
        if hour in [0, 1, 5, 6]:  # Early morning - moderate speeds
            base_speed = random.uniform(50.0, 90.0)
        elif hour in [7, 8, 17, 18]:  # Rush hours - lower speeds with high variation
            base_speed = random.uniform(25.0, 80.0)
        elif hour in [9, 10, 11, 12, 13, 14, 15, 16]:  # Daytime - moderate to high speeds
            base_speed = random.uniform(40.0, 95.0)
        elif hour in [19, 20, 21]:  # Evening - variable speeds
            base_speed = random.uniform(35.0, 100.0)
        else:  # Late night (22, 23) - higher speeds
            base_speed = random.uniform(60.0, 110.0)
        
        # Add smooth transitions from previous speed (if available)
        if previous_speed is not None:
            # Blend 30% of previous speed for smoother transitions
            base_speed = 0.7 * base_speed + 0.3 * previous_speed
    
    # Add random variation (±10%)
    variation = random.uniform(-0.1, 0.1)
    speed_kmh = base_speed * (1 + variation)
    
    # Ensure speed is within reasonable bounds
    speed_kmh = max(5.0, min(120.0, speed_kmh))
    
    return round(speed_kmh, 2)


def generate_traffic_data(
    start_time: datetime,
    hours: int = 24,
    existing_entries: list = None
) -> tuple[list, set]:
    """Generate traffic data entries for the specified time range."""
    if existing_entries is None:
        existing_entries = []
    
    entries = existing_entries.copy()
    critical_hours = generate_critical_periods(start_time, hours)
    
    current_time = start_time
    previous_speed_kmh = None
    
    # If we have existing entries, use the last speed as starting point
    if entries:
        last_entry = entries[-1]
        last_speed_ms = last_entry.get("flowSegmentData", {}).get("currentSpeed", 0)
        previous_speed_kmh = last_speed_ms * 3.6
    
    for hour_offset in range(hours):
        hour = current_time.hour
        is_critical = hour_offset in critical_hours
        
        # Get speed with smooth transitions
        speed_kmh = get_speed_for_hour(hour, is_critical, previous_speed_kmh)
        previous_speed_kmh = speed_kmh
        
        # Convert to m/s for API format
        current_speed_ms = round(speed_kmh / 3.6, 2)
        free_flow_speed_ms = 33.0  # ~120 km/h free flow
        
        entry = {
            "flowSegmentData": {
                "frc": "FRC2",
                "currentSpeed": current_speed_ms,
                "freeFlowSpeed": free_flow_speed_ms,
                "currentTravelTime": 500,
                "freeFlowTravelTime": 360,
                "confidence": 1,
                "roadClosure": False,
                "coordinates": BASE_COORDS,
            },
            "_fetched_at": current_time.astimezone(pytz.UTC).isoformat(),
        }
        
        entries.append(entry)
        current_time += timedelta(hours=1)
    
    return entries, critical_hours


def main():
    """Main function to generate or extend traffic data."""
    # Load existing data
    existing_entries = load_existing_data(OUTPUT_FILE)
    
    # Get time range from air quality data (priority)
    air_time_range = get_air_quality_time_range()
    
    if air_time_range:
        air_start, air_end = air_time_range
        # Calculate hours in the range
        hours_to_generate = int((air_end - air_start).total_seconds() / 3600) + 1
        start_time = air_start
        
        print(f"Using time range from air quality data:")
        print(f"  Start: {air_start.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"  End: {air_end.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"  Duration: {hours_to_generate} hours")
        
        # Filter existing entries to only keep those within the air quality time range
        if existing_entries:
            filtered_entries = []
            for entry in existing_entries:
                fetched_at_str = entry.get("_fetched_at", "")
                if fetched_at_str:
                    try:
                        dt_utc = datetime.fromisoformat(fetched_at_str.replace("Z", "+00:00"))
                        dt_hcmc = dt_utc.astimezone(HCMC_TZ)
                        if air_start <= dt_hcmc <= air_end:
                            filtered_entries.append(entry)
                    except ValueError:
                        continue
            existing_entries = filtered_entries
            print(f"Filtered existing entries to match air quality time range: {len(existing_entries)} entries")
    else:
        # Fallback: use existing traffic data or default
        if existing_entries:
            start_time = get_last_timestamp(existing_entries)
            print(f"Found {len(existing_entries)} existing entries")
            print(f"Extending from: {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            hours_to_generate = 24
        else:
            # Start from 24 hours ago if no existing data
            start_time = HCMC_TZ.localize(
                datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            ) - timedelta(days=1)
            print("No existing data found. Creating new dataset.")
            print(f"Starting from: {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            hours_to_generate = 24
    
    # Generate traffic data for the determined time range
    entries, critical_hours = generate_traffic_data(start_time, hours_to_generate, existing_entries)
    
    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2)
    
    # Print summary
    end_time = start_time + timedelta(hours=hours_to_generate - 1)
    new_entries_count = len(entries) - len(existing_entries)
    
    print(f"\n{'='*60}")
    print(f"Generated {new_entries_count} new traffic entries")
    print(f"Total entries in file: {len(entries)}")
    print(f"Time range: {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Saved to: {OUTPUT_FILE}")
    
    # Show critical periods
    if critical_hours:
        critical_times = sorted(critical_hours)
        print(f"\nCritical periods (low speed < 20 km/h, matching air quality PM2.5 > {PM25_THRESHOLD} µg/m³): {len(critical_hours)} hours")
        for hour_offset in critical_times:
            critical_time = start_time + timedelta(hours=hour_offset)
            entry = entries[len(existing_entries) + hour_offset]
            speed_kmh = entry["flowSegmentData"]["currentSpeed"] * 3.6
            print(f"  - {critical_time.strftime('%Y-%m-%d %H:%M:%S %Z')}: {speed_kmh:.2f} km/h")
    
    # Check if air quality data was used
    air_critical = load_air_quality_critical_periods(start_time, hours_to_generate)
    if air_critical:
        print(f"\nNote: Critical periods matched with air quality data ({len(air_critical)} hours with PM2.5 > {PM25_THRESHOLD} µg/m³ found)")
    else:
        print(f"\nNote: No air quality data found, generated random critical periods")
    
    print(f"\nSpeed variation:")
    print(f"  - Critical periods: 8-20 km/h (congestion)")
    print(f"  - Rush hours (07:00-08:00, 17:00-18:00): 25-80 km/h")
    print(f"  - Daytime (09:00-16:00): 40-95 km/h")
    print(f"  - Evening (19:00-21:00): 35-100 km/h")
    print(f"  - Late night/Early morning: 50-110 km/h")
    print(f"{'='*60}")


if __name__ == "__main__":
    # Use current time as seed for randomness (different each run)
    random.seed()
    main()

