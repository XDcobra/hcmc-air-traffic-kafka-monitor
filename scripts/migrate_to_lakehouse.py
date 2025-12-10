"""Migration script to convert existing CSV/JSON data to Delta Lake tables."""

import logging
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    TRAFFIC_RAW_FILE_PATH,
    AIR_RAW_FILE_PATH,
    BATCH_VIEWS_DIR,
    SPEED_VIEWS_DIR,
    SERVING_VIEWS_DIR,
    TRAFFIC_BATCH_VIEW_FILE,
    AIR_QUALITY_SPEED_FILE,
    COMBINED_VIEW_FILE,
    LAKEHOUSE_TRAFFIC_RAW_PATH,
    LAKEHOUSE_AIR_QUALITY_RAW_PATH,
    LAKEHOUSE_TRAFFIC_BATCH_VIEW_PATH,
    LAKEHOUSE_AIR_QUALITY_SPEED_VIEW_PATH,
    LAKEHOUSE_SERVING_COMBINED_VIEW_PATH,
)
from utils.data_loader import (
    load_json,
    load_csv,
    check_lakehouse_available,
    save_to_delta_table,
)
from lakehouse_client import add_partition_columns

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def migrate_traffic_raw():
    """Migrate raw traffic JSON data to Delta Lake."""
    logger.info("Migrating raw traffic data...")
    
    if not TRAFFIC_RAW_FILE_PATH.exists():
        logger.warning(f"Traffic raw file not found: {TRAFFIC_RAW_FILE_PATH}")
        return False
    
    try:
        # Load JSON
        raw_responses = load_json(TRAFFIC_RAW_FILE_PATH)
        if not raw_responses:
            logger.warning("No data in traffic raw file")
            return False
        
        # Parse to DataFrame
        from batch_layer.batch_processor import parse_traffic_json
        df = parse_traffic_json(raw_responses)
        
        if df.empty:
            logger.warning("No valid traffic data to migrate")
            return False
        
        # Add partition columns
        df = add_partition_columns(df, timestamp_col="timestamp")
        
        # Write to Delta Lake
        success = save_to_delta_table(
            df=df,
            table_path=LAKEHOUSE_TRAFFIC_RAW_PATH,
            mode="overwrite",
            partition_by=["year", "month", "day"],
            raise_on_error=True,
        )
        
        if success:
            logger.info(f"Successfully migrated {len(df)} traffic records to Lakehouse")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Failed to migrate traffic raw data: {e}")
        return False


def migrate_air_quality_raw():
    """Migrate raw air quality JSON data to Delta Lake."""
    logger.info("Migrating raw air quality data...")
    
    if not AIR_RAW_FILE_PATH.exists():
        logger.warning(f"Air quality raw file not found: {AIR_RAW_FILE_PATH}")
        return False
    
    try:
        # Load JSON
        measurements = load_json(AIR_RAW_FILE_PATH)
        if not measurements:
            logger.warning("No data in air quality raw file")
            return False
        
        # Parse to DataFrame
        from speed_layer.speed_processor import parse_measurements
        df = parse_measurements(measurements)
        
        if df.empty:
            logger.warning("No valid air quality data to migrate")
            return False
        
        # Add partition columns
        df = add_partition_columns(df, timestamp_col="timestamp")
        
        # Write to Delta Lake
        success = save_to_delta_table(
            df=df,
            table_path=LAKEHOUSE_AIR_QUALITY_RAW_PATH,
            mode="overwrite",
            partition_by=["year", "month", "day"],
            raise_on_error=True,
        )
        
        if success:
            logger.info(f"Successfully migrated {len(df)} air quality records to Lakehouse")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Failed to migrate air quality raw data: {e}")
        return False


def migrate_batch_view():
    """Migrate batch view CSV to Delta Lake."""
    logger.info("Migrating batch view...")
    
    batch_file = BATCH_VIEWS_DIR / TRAFFIC_BATCH_VIEW_FILE
    if not batch_file.exists():
        logger.warning(f"Batch view file not found: {batch_file}")
        return False
    
    try:
        # Load CSV
        df = load_csv(batch_file)
        if df.empty:
            logger.warning("No data in batch view file")
            return False
        
        # Parse timestamp
        from utils.data_loader import parse_timestamp
        df = parse_timestamp(df, "timestamp")
        
        # Add partition columns
        df = add_partition_columns(df, timestamp_col="timestamp")
        
        # Write to Delta Lake
        success = save_to_delta_table(
            df=df,
            table_path=LAKEHOUSE_TRAFFIC_BATCH_VIEW_PATH,
            mode="overwrite",
            partition_by=["year", "month"],
            raise_on_error=True,
        )
        
        if success:
            logger.info(f"Successfully migrated {len(df)} batch view records to Lakehouse")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Failed to migrate batch view: {e}")
        return False


def migrate_speed_view():
    """Migrate speed view CSV to Delta Lake."""
    logger.info("Migrating speed view...")
    
    speed_file = SPEED_VIEWS_DIR / AIR_QUALITY_SPEED_FILE
    if not speed_file.exists():
        logger.warning(f"Speed view file not found: {speed_file}")
        return False
    
    try:
        # Load CSV
        df = load_csv(speed_file)
        if df.empty:
            logger.warning("No data in speed view file")
            return False
        
        # Parse timestamp
        from utils.data_loader import parse_timestamp
        df = parse_timestamp(df, "timestamp")
        
        # Add partition columns
        df = add_partition_columns(df, timestamp_col="timestamp")
        
        # Write to Delta Lake
        success = save_to_delta_table(
            df=df,
            table_path=LAKEHOUSE_AIR_QUALITY_SPEED_VIEW_PATH,
            mode="overwrite",
            partition_by=["year", "month"],
            raise_on_error=True,
        )
        
        if success:
            logger.info(f"Successfully migrated {len(df)} speed view records to Lakehouse")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Failed to migrate speed view: {e}")
        return False


def migrate_serving_view():
    """Migrate serving view CSV to Delta Lake."""
    logger.info("Migrating serving view...")
    
    serving_file = SERVING_VIEWS_DIR / COMBINED_VIEW_FILE
    if not serving_file.exists():
        logger.warning(f"Serving view file not found: {serving_file}")
        return False
    
    try:
        # Load CSV
        df = load_csv(serving_file)
        if df.empty:
            logger.warning("No data in serving view file")
            return False
        
        # Parse timestamp
        from utils.data_loader import parse_timestamp
        df = parse_timestamp(df, "timestamp")
        
        # Add partition columns
        df = add_partition_columns(df, timestamp_col="timestamp")
        
        # Write to Delta Lake
        success = save_to_delta_table(
            df=df,
            table_path=LAKEHOUSE_SERVING_COMBINED_VIEW_PATH,
            mode="overwrite",
            partition_by=["year", "month"],
            raise_on_error=True,
        )
        
        if success:
            logger.info(f"Successfully migrated {len(df)} serving view records to Lakehouse")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Failed to migrate serving view: {e}")
        return False


def main():
    """Main migration function."""
    logger.info("="*60)
    logger.info("MIGRATION TO LAKEHOUSE")
    logger.info("="*60)
    
    # Check Lakehouse availability
    if not check_lakehouse_available():
        logger.error("Lakehouse unavailable. Start MinIO with: docker-compose up -d minio")
        return 1
    
    logger.info("Lakehouse connection verified")
    logger.info("Starting migration...")
    logger.info("Note: Original files will be preserved")
    
    results = {
        "traffic_raw": migrate_traffic_raw(),
        "air_quality_raw": migrate_air_quality_raw(),
        "batch_view": migrate_batch_view(),
        "speed_view": migrate_speed_view(),
        "serving_view": migrate_serving_view(),
    }
    
    logger.info("\n" + "="*60)
    logger.info("MIGRATION SUMMARY")
    logger.info("="*60)
    
    for name, success in results.items():
        status = "SUCCESS" if success else "SKIPPED/FAILED"
        logger.info(f"{name}: {status}")
    
    successful = sum(1 for v in results.values() if v)
    total = len(results)
    
    logger.info(f"\nMigrated {successful}/{total} datasets")
    logger.info("Original files preserved in data/ directory")
    
    return 0 if successful > 0 else 1


if __name__ == "__main__":
    sys.exit(main())



