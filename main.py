"""Main entry point for the Lambda Architecture prototype."""

import argparse
import logging
import sys

from config import (
    LOG_FORMAT,
    LOG_LEVEL,
    TRAFFIC_RAW_FILE_PATH,
    AIR_RAW_FILE_PATH,
)
from batch_layer.batch_processor import run_batch_processing
from speed_layer.speed_processor import (
    collect_air_quality_once,
    collect_air_quality_continuous,
)
from serving_layer.data_integrator import run_serving_layer
from dataset_builder import (
    build_air_dataset,
    build_traffic_snapshot,
    run_traffic_collector,
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT
)
logger = logging.getLogger(__name__)


def _resolve_traffic_file(file_arg):
    return file_arg or str(TRAFFIC_RAW_FILE_PATH)


def _resolve_air_dataset_file(file_arg):
    return file_arg or str(AIR_RAW_FILE_PATH)


def run_batch_layer(args):
    """Run batch layer processing."""
    logger.info("="*60)
    logger.info("BATCH LAYER PROCESSING")
    logger.info("="*60)
    
    try:
        # Check if API or Kafka should be used for traffic data
        use_traffic_api = getattr(args, 'use_api', False)
        use_kafka = getattr(args, 'use_kafka', False)
        
        # If using API or Kafka, don't pass traffic_file (it will be ignored anyway)
        traffic_file = None if (use_traffic_api or use_kafka) else _resolve_traffic_file(getattr(args, 'traffic_file', None))

        batch_df = run_batch_processing(
            traffic_file=traffic_file,
            output_file=getattr(args, 'output_file', None),
            use_api=use_traffic_api,
            use_kafka=use_kafka
        )
        logger.info("Batch layer completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Batch layer failed: {e}", exc_info=True)
        return 1


def run_speed_layer(args):
    """Run speed layer processing."""
    logger.info("="*60)
    logger.info("SPEED LAYER PROCESSING")
    logger.info("="*60)
    
    try:
        # Check if API or Kafka should be used for air quality data
        use_air_api = getattr(args, 'use_api', False)
        use_kafka = getattr(args, 'use_kafka', False)
        
        # If using API or Kafka, don't pass dataset_file (it will be ignored)
        dataset_air_file = None if (use_air_api or use_kafka) else _resolve_air_dataset_file(getattr(args, "air_file", None))

        if getattr(args, 'continuous', False):
            if not use_air_api and not use_kafka:
                logger.error("Continuous mode requires API or Kafka usage for air quality data.")
                return 1
            logger.info("Running in continuous mode...")
            collect_air_quality_continuous(
                interval_minutes=getattr(args, 'interval', 5),
                max_iterations=getattr(args, 'max_iterations', None),
                use_api=use_air_api,
                use_kafka=use_kafka
            )
        else:
            logger.info("Running one-time collection...")
            df = collect_air_quality_once(
                use_api=use_air_api,
                dataset_file=dataset_air_file,
                use_kafka=use_kafka
            )
            if not df.empty:
                logger.info(f"Collected {len(df)} air quality measurements")
            else:
                logger.warning("No air quality data collected")
        
        logger.info("Speed layer completed successfully")
        return 0
    except KeyboardInterrupt:
        logger.info("Speed layer interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"Speed layer failed: {e}", exc_info=True)
        return 1


def run_serving_layer_cmd(args):
    """Run serving layer processing."""
    logger.info("="*60)
    logger.info("SERVING LAYER PROCESSING")
    logger.info("="*60)
    
    try:
        combined_df, critical_df, stats = run_serving_layer(
            batch_file=args.batch_file,
            speed_file=args.speed_file,
            create_plots=not args.no_plots
        )
        
        logger.info("Serving layer completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Serving layer failed: {e}", exc_info=True)
        return 1


def run_full_pipeline(args):
    """Run the complete Lambda Architecture pipeline."""
    logger.info("="*60)
    logger.info("FULL LAMBDA ARCHITECTURE PIPELINE")
    logger.info("="*60)
    
    exit_code = 0
    
    # Check for --no-api flag
    no_api = getattr(args, 'no_api', False)
    
    # Extract API mode and Kafka flag for dataset creation and layer processing
    api_mode = getattr(args, 'use_api', 'none')
    use_kafka = getattr(args, 'use_kafka', False)
    use_traffic_api_for_layers = api_mode in ["both", "traffic"]
    use_air_api_for_layers = api_mode in ["both", "air"]
    
    logger.info(f"API usage mode: {api_mode} (traffic={use_traffic_api_for_layers}, air={use_air_api_for_layers})")
    logger.info(f"Kafka usage: {use_kafka}")
    logger.info(f"--no-api flag: {no_api}")
    
    # Step 0: Dataset creation (skip if --no-api)
    if not no_api:
        logger.info("\n[0/4] Creating datasets...")
        try:
            # Create both datasets by default (dataset command always uses API)
            if api_mode in ["both", "traffic"]:
                build_traffic_snapshot(use_kafka=use_kafka)
            if api_mode in ["both", "air"]:
                build_air_dataset(hours=24, use_kafka=use_kafka)
        except Exception as e:
            logger.error(f"Dataset creation failed: {e}", exc_info=True)
            exit_code = 1
            return exit_code
    else:
        logger.info("\n[0/4] Skipping dataset creation (--no-api flag set)")
    
    # Determine default input files when not using APIs or Kafka for layers
    traffic_input = None if (use_traffic_api_for_layers or use_kafka) else _resolve_traffic_file(getattr(args, 'traffic_file', None))
    air_input = None if (use_air_api_for_layers or use_kafka) else _resolve_air_dataset_file(getattr(args, "air_file", None))

    # Step 1: Batch layer
    logger.info("\n[1/4] Processing batch layer...")
    try:
        run_batch_processing(traffic_file=traffic_input, use_api=use_traffic_api_for_layers, use_kafka=use_kafka)
    except Exception as e:
        logger.error(f"Batch layer failed: {e}", exc_info=True)
        exit_code = 1
        return exit_code
    
    # Step 2: Speed layer (one-time collection)
    logger.info("\n[2/4] Collecting air quality data (one-time)...")
    try:
        collect_air_quality_once(
            use_api=use_air_api_for_layers,
            dataset_file=air_input,
            use_kafka=use_kafka
        )
    except Exception as e:
        logger.warning(f"Speed layer collection failed: {e}. Continuing with existing data if available.")
        # Don't fail the pipeline if speed layer fails - might have existing data
    
    # Step 3: Serving layer
    logger.info("\n[3/4] Processing serving layer...")
    try:
        run_serving_layer(create_plots=not getattr(args, 'no_plots', False))
    except Exception as e:
        logger.error(f"Serving layer failed: {e}", exc_info=True)
        exit_code = 1
        return exit_code
    
    logger.info("\n" + "="*60)
    logger.info("FULL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("="*60)
    
    return exit_code


def run_dataset_builder(args):
    """Create or update local datasets."""
    logger.info("=" * 60)
    logger.info("DATASET BUILDER")
    logger.info("=" * 60)

    if args.collector and args.current:
        logger.error("Please choose either --current or --collector for traffic dataset, not both.")
        return 1

    if (args.collector or args.current) and args.use_api not in ("traffic", "both"):
        logger.error("--current/--collector options require --use-api traffic or both.")
        return 1

    if args.collector and args.interval <= 0:
        logger.error("--interval must be greater than 0.")
        return 1

    if args.collector and args.duration <= 0:
        logger.error("--duration must be greater than 0.")
        return 1

    if args.hours <= 0:
        logger.error("--hours must be greater than 0.")
        return 1

    worked = False
    use_kafka = getattr(args, 'use_kafka', False)
    from_file = getattr(args, 'from_file', False)
    
    # Validate --from-file usage
    if from_file and not use_kafka:
        logger.error("--from-file requires --use-kafka flag")
        return 1
    
    if from_file and args.collector:
        logger.error("--from-file cannot be used with --collector (collector requires API)")
        return 1

    if args.use_api in ("traffic", "both"):
        worked = True
        if from_file:
            # Load from JSON file and write to Kafka
            from dataset_builder import load_traffic_from_json_to_kafka
            load_traffic_from_json_to_kafka()
        elif args.collector:
            run_traffic_collector(
                interval_minutes=args.interval,
                duration_hours=args.duration,
                use_kafka=use_kafka,
            )
        else:
            # default or --current -> snapshot that overwrites file
            build_traffic_snapshot(use_kafka=use_kafka)

    if args.use_api in ("air", "both"):
        worked = True
        if from_file:
            # Load from JSON file and write to Kafka
            from dataset_builder import load_air_from_json_to_kafka
            load_air_from_json_to_kafka()
        else:
            build_air_dataset(hours=args.hours, use_kafka=use_kafka)

    if not worked:
        logger.warning("No dataset action performed. Check --use-api option.")

    logger.info("Dataset command completed.")
    return 0


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Lambda Architecture Prototype for HCMC Smart City IoT Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build both datasets (traffic snapshot + last 24h air quality)
  python main.py dataset --use-api both

  # Run traffic collector for 24h (30-minute intervals)
  python main.py dataset --use-api traffic --collector --interval 30 --duration 24

  # Execute full pipeline using offline datasets
  python main.py full

  # Run full pipeline with live APIs
  python main.py full --use-api both

Optional Flags Overview:
  --use-api {traffic,air,both,none}
      Control whether a command pulls fresh data from the APIs or uses dataset CSVs.
      (dataset: default=both, batch/speed/full: default=none -> use data/raw/*.csv)

  dataset command only:
      --current        Fetch a single traffic snapshot (overwrites traffic_data.csv).
      --collector      Run the traffic collector loop (requires --use-api traffic/both).
      --interval N     Minutes between collector requests (default 30).
      --duration H     Collector runtime in hours (default 24).
      --hours H        Air dataset history to fetch from OpenAQ (default 24h).

  speed command:
      --continuous     Keep polling the air API at --interval minutes.
      --max-iterations Limit number of polling loops (default: infinite).

  serving command:
      --no-plots       Skip generating visualizations.
        """
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Dataset command
    dataset_parser = subparsers.add_parser("dataset", help="Build traffic and/or air quality datasets")
    dataset_parser.add_argument(
        "--use-api",
        type=str,
        choices=["traffic", "air", "both"],
        default="both",
        help="Which dataset to build: 'both' (default), 'traffic', or 'air'",
    )
    dataset_parser.add_argument(
        "--current",
        action="store_true",
        help="Traffic dataset: fetch current snapshot and overwrite dataset file",
    )
    dataset_parser.add_argument(
        "--collector",
        action="store_true",
        help="Traffic dataset: run collector loop to append data over time",
    )
    dataset_parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Collector interval in minutes (default: 30)",
    )
    dataset_parser.add_argument(
        "--duration",
        type=int,
        default=24,
        help="Collector duration in hours (default: 24)",
    )
    dataset_parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Air dataset: number of hours of history to fetch (default: 24)",
    )
    dataset_parser.add_argument(
        "--use-kafka",
        action="store_true",
        help="Write datasets to Kafka topics instead of JSON files"
    )
    dataset_parser.add_argument(
        "--from-file",
        action="store_true",
        help="Load data from existing JSON files instead of API (only works with --use-kafka)"
    )
    
    # Batch layer command
    batch_parser = subparsers.add_parser("batch", help="Run batch layer processing")
    batch_parser.add_argument(
        "--traffic-file",
        type=str,
        help=f"Path to traffic data JSON file (default: {TRAFFIC_RAW_FILE_PATH})"
    )
    batch_parser.add_argument(
        "--output-file",
        type=str,
        help="Path to output batch view file (default: data/batch_views/traffic_batch_view.csv)"
    )
    batch_parser.add_argument(
        "--use-api",
        action="store_true",
        help="Fetch directly from API instead of reading from raw JSON file"
    )
    batch_parser.add_argument(
        "--use-kafka",
        action="store_true",
        help="Read data from Kafka topics instead of JSON files or API"
    )
    
    # Speed layer command
    speed_parser = subparsers.add_parser("speed", help="Run speed layer processing")
    speed_parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run in continuous polling mode"
    )
    speed_parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Polling interval in minutes (default: 5)"
    )
    speed_parser.add_argument(
        "--max-iterations",
        type=int,
        help="Maximum number of iterations for continuous mode (default: infinite)"
    )
    speed_parser.add_argument(
        "--use-api",
        action="store_true",
        help="Fetch directly from API instead of reading from raw JSON file"
    )
    speed_parser.add_argument(
        "--air-file",
        type=str,
        help=f"Path to air dataset JSON file (default: {AIR_RAW_FILE_PATH})"
    )
    speed_parser.add_argument(
        "--use-kafka",
        action="store_true",
        help="Read data from Kafka topics instead of JSON files or API"
    )
    
    # Serving layer command
    serving_parser = subparsers.add_parser("serving", help="Run serving layer processing")
    serving_parser.add_argument(
        "--batch-file",
        type=str,
        help="Path to batch view file (default: data/batch_views/traffic_batch_view.csv)"
    )
    serving_parser.add_argument(
        "--speed-file",
        type=str,
        help="Path to speed view file (default: data/speed_views/air_speed_layer_append.csv)"
    )
    serving_parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Skip creating visualization plots"
    )
    
    # Full pipeline command
    full_parser = subparsers.add_parser("full", help="Run full Lambda Architecture pipeline")
    full_parser.add_argument(
        "--traffic-file",
        type=str,
        help=f"Path to traffic data JSON file (default: {TRAFFIC_RAW_FILE_PATH})"
    )
    full_parser.add_argument(
        "--air-file",
        type=str,
        help=f"Path to air dataset JSON file (default: {AIR_RAW_FILE_PATH})"
    )
    full_parser.add_argument(
        "--use-api",
        type=str,
        choices=["both", "traffic", "air", "none"],
        default="none",
        help="API usage mode for batch/speed layers: 'both' (both APIs), 'traffic' (traffic API only), 'air' (air quality API only), 'none' (local files only, default). Dataset step always uses API unless --no-api is set."
    )
    full_parser.add_argument(
        "--no-api",
        action="store_true",
        help="Skip dataset creation step, use existing raw JSON files"
    )
    full_parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Skip creating visualization plots"
    )
    full_parser.add_argument(
        "--use-kafka",
        action="store_true",
        help="Use Kafka topics for reading/writing data instead of JSON files or API"
    )
    
    args = parser.parse_args()
    
    # Handle subcommands
    if args.command == "dataset":
        return run_dataset_builder(args)
    elif args.command == "full":
        return run_full_pipeline(args)
    elif args.command == "batch":
        return run_batch_layer(args)
    elif args.command == "speed":
        return run_speed_layer(args)
    elif args.command == "serving":
        return run_serving_layer_cmd(args)
    else:
        # No command specified, show help
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

