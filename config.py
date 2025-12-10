"""Configuration settings for the Lambda Architecture prototype."""

import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
BATCH_VIEWS_DIR = DATA_DIR / "batch_views"
SPEED_VIEWS_DIR = DATA_DIR / "speed_views"
SERVING_VIEWS_DIR = DATA_DIR / "serving_views"
PLOTS_DIR = SERVING_VIEWS_DIR / "plots"

# Ensure directories exist
for directory in [RAW_DATA_DIR, BATCH_VIEWS_DIR, SPEED_VIEWS_DIR, SERVING_VIEWS_DIR, PLOTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# OpenAQ API Key (get from https://explore.openaq.org/register)
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", None)

# Traffic API Configuration (TomTom Traffic API)
TOMTOM_API_BASE_URL = "https://api.tomtom.com/traffic/services"
TOMTOM_API_VERSION = "4"  # Flow Segment Data API version
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", None)
TOMTOM_FLOW_SEGMENT_ENDPOINT = (
    f"{TOMTOM_API_BASE_URL}/{TOMTOM_API_VERSION}/flowSegmentData/absolute/10/json"
)

# District 10 (Ho Chi Minh City) focus
DISTRICT10_CENTER_LAT = 10.7715
DISTRICT10_CENTER_LON = 106.6664
DISTRICT10_RADIUS_KM = 3  # smaller radius to stay inside District 10

# Bounding box for generating traffic grid inside District 10
DISTRICT10_LAT_MIN = 10.7600
DISTRICT10_LAT_MAX = 10.7830
DISTRICT10_LON_MIN = 106.6500
DISTRICT10_LON_MAX = 106.6830
DISTRICT10_GRID_SIZE = 5  # 5x5 grid focused on district 10

# Ho Chi Minh City coordinates (approximate center)
HCMC_LATITUDE = 10.8231
HCMC_LONGITUDE = 106.6297
HCMC_RADIUS_KM = 25  # Max 25km for v3 API (reduced from 50)

# Timezone settings
HCMC_TIMEZONE = "Asia/Ho_Chi_Minh"  # UTC+7
UTC_OFFSET_HOURS = 7

# Data file names
TRAFFIC_RAW_FILE = "traffic_raw.json"  # Raw JSON data in data/raw/
AIR_RAW_FILE = "air_raw.json"  # Raw JSON data in data/raw/
TRAFFIC_BATCH_VIEW_FILE = "traffic_batch_view.csv"
AIR_QUALITY_SPEED_FILE = "air_speed_layer_append.csv"
COMBINED_VIEW_FILE = "combined_view.csv"
CRITICAL_PERIODS_FILE = "critical_periods.csv"

# Dataset builder output files (raw JSON)
TRAFFIC_RAW_FILE_PATH = RAW_DATA_DIR / TRAFFIC_RAW_FILE
AIR_RAW_FILE_PATH = RAW_DATA_DIR / AIR_RAW_FILE

# Thresholds for critical periods
PM25_THRESHOLD = 50.0  # µg/m³ - Air quality threshold
SPEED_THRESHOLD = 20.0  # km/h - Traffic speed threshold (below this is considered congested)

# Speed layer polling settings
POLLING_INTERVAL_MINUTES = 5  # Poll air quality API every 5 minutes
MAX_RETRIES = 3  # Maximum retry attempts for API calls
RETRY_DELAY_SECONDS = 5  # Delay between retries

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TRAFFIC_TOPIC = os.getenv("KAFKA_TRAFFIC_TOPIC", "traffic-raw")
KAFKA_AIR_QUALITY_TOPIC = os.getenv("KAFKA_AIR_QUALITY_TOPIC", "air-quality-raw")
KAFKA_BATCH_CONSUMER_GROUP = os.getenv("KAFKA_BATCH_CONSUMER_GROUP", "lambda-batch-consumer")
KAFKA_SPEED_CONSUMER_GROUP = os.getenv("KAFKA_SPEED_CONSUMER_GROUP", "lambda-speed-consumer")

# MinIO / Lakehouse Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "lakehouse")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
LAKEHOUSE_BASE_PATH = f"s3a://{MINIO_BUCKET_NAME}/delta/"

# Lakehouse Table Paths
LAKEHOUSE_TRAFFIC_RAW_PATH = f"{LAKEHOUSE_BASE_PATH}traffic/raw/"
LAKEHOUSE_AIR_QUALITY_RAW_PATH = f"{LAKEHOUSE_BASE_PATH}air_quality/raw/"
LAKEHOUSE_TRAFFIC_BATCH_VIEW_PATH = f"{LAKEHOUSE_BASE_PATH}traffic/batch_view/"
LAKEHOUSE_AIR_QUALITY_SPEED_VIEW_PATH = f"{LAKEHOUSE_BASE_PATH}air_quality/speed_view/"
LAKEHOUSE_SERVING_COMBINED_VIEW_PATH = f"{LAKEHOUSE_BASE_PATH}serving/combined_view/"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

