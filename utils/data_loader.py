"""Common data loading and processing utilities."""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import pytz
import logging
import json

from config import HCMC_TIMEZONE, UTC_OFFSET_HOURS

logger = logging.getLogger(__name__)


def load_csv(file_path, **kwargs):
    """
    Load a CSV file with error handling.
    
    Args:
        file_path: Path to the CSV file
        **kwargs: Additional arguments to pass to pd.read_csv
        
    Returns:
        pandas.DataFrame: Loaded data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty or cannot be parsed
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        df = pd.read_csv(file_path, **kwargs)
        
        if df.empty:
            raise ValueError(f"File is empty: {file_path}")
        
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        return df
        
    except pd.errors.EmptyDataError:
        raise ValueError(f"File is empty or invalid: {file_path}")
    except Exception as e:
        logger.error(f"Error loading CSV file {file_path}: {e}")
        raise


def parse_timestamp(df, timestamp_col, timezone=HCMC_TIMEZONE):
    """
    Parse timestamp column and convert to HCMC timezone.
    
    Args:
        df: DataFrame with timestamp column
        timestamp_col: Name of the timestamp column
        timezone: Target timezone (default: HCMC)
        
    Returns:
        pandas.DataFrame: DataFrame with parsed datetime column
    """
    if timestamp_col not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_col}' not found in DataFrame")
    
    # Try to parse the timestamp
    try:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
    except Exception as e:
        logger.warning(f"Error parsing timestamps: {e}")
        raise ValueError(f"Could not parse timestamp column '{timestamp_col}'")
    
    # Check for null timestamps
    null_count = df[timestamp_col].isna().sum()
    if null_count > 0:
        logger.warning(f"Found {null_count} null timestamps, these rows will be dropped")
        df = df.dropna(subset=[timestamp_col])
    
    # Convert to HCMC timezone if not already
    try:
        hcmc_tz = pytz.timezone(timezone)
        if df[timestamp_col].dt.tz is None:
            # Assume UTC if no timezone info
            df[timestamp_col] = df[timestamp_col].dt.tz_localize('UTC')
        df[timestamp_col] = df[timestamp_col].dt.tz_convert(hcmc_tz)
    except Exception as e:
        logger.warning(f"Timezone conversion issue: {e}. Continuing with original timezone.")
    
    return df


def floor_to_hour(timestamp_series):
    """
    Floor timestamps to the nearest hour.
    
    Args:
        timestamp_series: pandas Series of datetime objects
        
    Returns:
        pandas.Series: Floored timestamps
    """
    return timestamp_series.dt.floor('h')


def validate_dataframe(df, required_columns, name="DataFrame"):
    """
    Validate that DataFrame has required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        name: Name for error messages
        
    Raises:
        ValueError: If required columns are missing
    """
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"{name} is missing required columns: {missing_cols}")


def save_csv(df, file_path, index=False):
    """
    Save DataFrame to CSV with error handling.
    
    Args:
        df: DataFrame to save
        file_path: Path to save the CSV
        index: Whether to include index in CSV
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df.to_csv(file_path, index=index)
        logger.info(f"Saved {len(df)} rows to {file_path}")
    except Exception as e:
        logger.error(f"Error saving CSV to {file_path}: {e}")
        raise


def load_json(file_path):
    """
    Load a JSON file with error handling.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        dict or list: Parsed JSON data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty or cannot be parsed
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            raise ValueError(f"File is empty: {file_path}")
        
        logger.info(f"Loaded JSON data from {file_path}")
        return data
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file {file_path}: {e}")
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {e}")
        raise


def save_json(data, file_path, indent=2):
    """
    Save data to JSON file with error handling.
    
    Args:
        data: Data to save (dict, list, etc.)
        file_path: Path to save the JSON file
        indent: JSON indentation level (default: 2)
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
        
        # Count items if it's a list or dict
        if isinstance(data, list):
            count = len(data)
        elif isinstance(data, dict):
            count = len(data)
        else:
            count = 1
        
        logger.info(f"Saved JSON data ({count} items) to {file_path}")
    except Exception as e:
        logger.error(f"Error saving JSON to {file_path}: {e}")
        raise

