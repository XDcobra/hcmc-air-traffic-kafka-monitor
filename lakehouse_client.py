"""Lakehouse client for Delta Lake operations with MinIO S3 storage using PySpark."""

import logging
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
from datetime import datetime
import os

logger = logging.getLogger(__name__)

# Try to import required libraries
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.utils import AnalysisException
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    logger.warning(
        "pyspark not installed. Lakehouse functionality will be unavailable. "
        "Install with: pip install pyspark delta-spark"
    )

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_NAME,
    MINIO_USE_SSL,
)


# Global SparkSession (created on first use)
_spark_session: Optional[SparkSession] = None


def _get_spark_session() -> SparkSession:
    """
    Get or create SparkSession configured for Delta Lake with MinIO.
    
    Returns:
        SparkSession: Configured SparkSession
    """
    global _spark_session
    
    if _spark_session is not None:
        return _spark_session
    
    if not PYSPARK_AVAILABLE:
        raise ImportError("pyspark not installed. Cannot create SparkSession.")
    
    try:
        # Configure Spark for Delta Lake and MinIO
        spark_builder = SparkSession.builder.appName("LambdaLakehouseApp")
        
        # Delta Lake configuration
        spark_builder = spark_builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        spark_builder = spark_builder.config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        
        # MinIO/S3 configuration
        # Ensure endpoint has protocol (http:// or https://)
        minio_endpoint = MINIO_ENDPOINT
        if not minio_endpoint.startswith(("http://", "https://")):
            minio_endpoint = f"http{'s' if MINIO_USE_SSL else ''}://{minio_endpoint}"
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_builder = spark_builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        
        # Delta Lake and Hadoop AWS JARs (PySpark will download them automatically)
        spark_builder = spark_builder.config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.1"
        )
        
        # Reduce logging
        spark_builder = spark_builder.config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        
        # Enable Arrow for better pandas/Spark DataFrame conversion
        spark_builder = spark_builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark_builder = spark_builder.config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        
        # Note: Native IO issues on Windows are resolved by adding hadoop/bin to PATH
        
        # Windows-specific: Use real Hadoop installation
        import platform
        if platform.system() == "Windows":
            # Use the Hadoop installation in the project directory
            import pathlib
            project_root = pathlib.Path(__file__).parent.absolute()
            hadoop_home = project_root / "hadoop" / "hadoop-3.3.6"
            if hadoop_home.exists():
                hadoop_home_str = str(hadoop_home)
                os.environ["HADOOP_HOME"] = hadoop_home_str
                os.environ["hadoop.home.dir"] = hadoop_home_str
                logger.info(f"Using Hadoop installation at: {hadoop_home_str}")
            else:
                logger.warning(f"Hadoop installation not found at {hadoop_home}, Spark may fail on Windows")
        
        _spark_session = spark_builder.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
        
        logger.info("SparkSession created successfully for Delta Lake with MinIO")
        return _spark_session
        
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        raise


def check_minio_connection() -> bool:
    """
    Check if MinIO is reachable at the configured endpoint.
    
    Returns:
        bool: True if MinIO is reachable, False otherwise
    """
    if not PYSPARK_AVAILABLE:
        return False
    
    try:
        spark = _get_spark_session()
        # Try to list files in the bucket
        test_path = f"s3a://{MINIO_BUCKET_NAME}/"
        try:
            spark.read.format("delta").load(test_path).limit(0).collect()
        except AnalysisException:
            # Table doesn't exist, but that's OK - we just want to test connectivity
            pass
        return True
    except Exception as e:
        logger.debug(f"MinIO connection check failed: {e}")
        return False


def setup_minio_bucket() -> bool:
    """
    Create MinIO bucket if it doesn't exist.
    Note: MinIO buckets are created automatically on first write, so this is mainly a check.
    
    Returns:
        bool: True if bucket setup is successful, False otherwise
    """
    if not PYSPARK_AVAILABLE:
        error_msg = "pyspark not installed. Cannot setup MinIO bucket."
        logger.error(error_msg)
        return False
    
    try:
        # MinIO buckets are created automatically when first written to
        # We just verify the SparkSession can be created
        _get_spark_session()
        logger.info(f"MinIO bucket '{MINIO_BUCKET_NAME}' will be created automatically on first write")
        return True
    except Exception as e:
        logger.error(f"Failed to setup MinIO bucket: {e}")
        return False


def _normalize_s3_path(path: str) -> str:
    """Convert s3:// to s3a:// for PySpark compatibility."""
    if path.startswith("s3://"):
        return path.replace("s3://", "s3a://", 1)
    return path


def write_to_delta_table(
    df: pd.DataFrame,
    table_path: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    raise_on_error: bool = False,
) -> bool:
    """
    Write DataFrame to Delta Lake table using PySpark.
    
    Args:
        df: DataFrame to write
        table_path: S3 path to Delta table (e.g., "s3://bucket/delta/table/" or "s3a://bucket/delta/table/")
        mode: Write mode - "overwrite", "append", or "error"
        partition_by: List of column names to partition by
        raise_on_error: If True, raise exception on error; if False, log and return False
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not PYSPARK_AVAILABLE:
        error_msg = "pyspark not installed. Cannot write to Delta Lake."
        logger.error(error_msg)
        if raise_on_error:
            raise ImportError(error_msg)
        return False
    
    if df.empty:
        logger.warning("No data to write to Delta Lake table")
        return True
    
    try:
        spark = _get_spark_session()
        
        # Ensure bucket exists (will be created automatically)
        if not setup_minio_bucket():
            raise ConnectionError("Failed to setup MinIO bucket")
        
        # Normalize path (s3:// -> s3a://)
        table_path = _normalize_s3_path(table_path)
        
        # Convert pandas DataFrame to Spark DataFrame
        # Reset index to ensure clean conversion
        df_clean = df.reset_index(drop=True)
        
        # Try direct Arrow conversion first (fastest, no temp files)
        # Arrow is enabled in Spark config, so this should work
        try:
            spark_df = spark.createDataFrame(df_clean)
        except Exception as arrow_error:
            # Fallback: Use temporary Parquet file (avoids serialization issues)
            logger.warning(f"Direct Arrow conversion failed, using Parquet fallback: {arrow_error}")
            import tempfile
            import shutil
            
            temp_dir = tempfile.mkdtemp()
            temp_parquet = os.path.join(temp_dir, "temp_data.parquet")
            
            try:
                # Write pandas DataFrame to temporary Parquet file
                df_clean.to_parquet(temp_parquet, index=False, engine='pyarrow')
                
                # Read Parquet file with Spark (no serialization needed)
                # Use file:/// prefix for Windows compatibility
                spark_df = spark.read.parquet(f"file:///{temp_parquet.replace(os.sep, '/')}")
            finally:
                # Clean up temporary directory
                try:
                    shutil.rmtree(temp_dir)
                except Exception:
                    pass
        
        # Prepare write operation
        writer = spark_df.write.format("delta").mode(mode)
        
        # Add partitioning if specified
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        # Write to Delta Lake
        writer.save(table_path)
        
        logger.info(f"Successfully wrote {len(df)} rows to Delta table '{table_path}' (mode: {mode})")
        return True
        
    except Exception as e:
        error_msg = f"Error writing to Delta table '{table_path}': {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return False


def read_from_delta_table(
    table_path: str,
    raise_on_error: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Read data from Delta Lake table using PySpark.
    
    Args:
        table_path: S3 path to Delta table (e.g., "s3://bucket/delta/table/" or "s3a://bucket/delta/table/")
        raise_on_error: If True, raise exception on error; if False, log and return None
        
    Returns:
        pd.DataFrame: DataFrame with table data, or None on error
    """
    if not PYSPARK_AVAILABLE:
        error_msg = "pyspark not installed. Cannot read from Delta Lake."
        logger.error(error_msg)
        if raise_on_error:
            raise ImportError(error_msg)
        return None
    
    try:
        spark = _get_spark_session()
        
        # Normalize path (s3:// -> s3a://)
        table_path = _normalize_s3_path(table_path)
        
        # Read from Delta Lake
        spark_df = spark.read.format("delta").load(table_path)
        
        # Convert Spark DataFrame to pandas DataFrame
        df = spark_df.toPandas()
        
        logger.info(f"Successfully read {len(df)} rows from Delta table '{table_path}'")
        return df
        
    except AnalysisException as e:
        error_msg = f"Delta table not found at '{table_path}': {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise FileNotFoundError(error_msg)
        return None
    except Exception as e:
        error_msg = f"Error reading from Delta table '{table_path}': {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return None


def append_to_delta_table(
    df: pd.DataFrame,
    table_path: str,
    partition_by: Optional[List[str]] = None,
    raise_on_error: bool = False,
) -> bool:
    """
    Append data to existing Delta Lake table.
    
    Args:
        df: DataFrame to append
        table_path: S3 path to Delta table
        partition_by: List of column names to partition by (only used if table doesn't exist)
        raise_on_error: If True, raise exception on error; if False, log and return False
        
    Returns:
        bool: True if successful, False otherwise
    """
    return write_to_delta_table(
        df=df,
        table_path=table_path,
        mode="append",
        partition_by=partition_by,
        raise_on_error=raise_on_error,
    )


def create_delta_table(
    df: pd.DataFrame,
    table_path: str,
    partition_by: Optional[List[str]] = None,
    raise_on_error: bool = False,
) -> bool:
    """
    Create a new Delta Lake table.
    
    Args:
        df: DataFrame with initial data
        table_path: S3 path to Delta table
        partition_by: List of column names to partition by
        raise_on_error: If True, raise exception on error; if False, log and return False
        
    Returns:
        bool: True if successful, False otherwise
    """
    return write_to_delta_table(
        df=df,
        table_path=table_path,
        mode="error",  # Fail if table already exists
        partition_by=partition_by,
        raise_on_error=raise_on_error,
    )


def add_partition_columns(df: pd.DataFrame, timestamp_col: str = "timestamp") -> pd.DataFrame:
    """
    Add partition columns (year, month, day) based on timestamp column.
    
    Args:
        df: DataFrame with timestamp column
        timestamp_col: Name of timestamp column
        
    Returns:
        pd.DataFrame: DataFrame with added partition columns
    """
    if timestamp_col not in df.columns:
        logger.warning(f"Timestamp column '{timestamp_col}' not found, skipping partition columns")
        return df
    
    df = df.copy()
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Add partition columns
    df["year"] = df[timestamp_col].dt.year
    df["month"] = df[timestamp_col].dt.month
    df["day"] = df[timestamp_col].dt.day
    
    return df


def stop_spark_session():
    """Stop the global SparkSession if it exists."""
    global _spark_session
    if _spark_session is not None:
        try:
            _spark_session.stop()
            _spark_session = None
            logger.info("SparkSession stopped")
        except Exception as e:
            logger.warning(f"Error stopping SparkSession: {e}")
