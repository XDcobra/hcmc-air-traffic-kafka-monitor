"""Kafka client for producing and consuming messages in the Lambda Architecture."""

import json
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Try to import kafka-python, but handle gracefully if not installed
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError, KafkaTimeoutError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning(
        "kafka-python not installed. Kafka functionality will be unavailable. "
        "Install with: pip install kafka-python"
    )


def check_kafka_connection(bootstrap_servers: str) -> bool:
    """
    Check if Kafka is reachable at the given bootstrap servers.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        
    Returns:
        bool: True if Kafka is reachable, False otherwise
    """
    if not KAFKA_AVAILABLE:
        return False
    
    producer = None
    try:
        # Try to create a producer - if this succeeds, Kafka is reachable
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000,
        )
        # Wait a bit for the connection to establish
        import time
        time.sleep(0.5)
        # If we got here without exception, Kafka is reachable
        return True
    except (KafkaError, KafkaTimeoutError) as e:
        logger.debug(f"Kafka connection check failed: {e}")
        return False
    except Exception as e:
        logger.debug(f"Kafka connection check failed with unexpected error: {e}")
        return False
    finally:
        if producer:
            try:
                producer.close()
            except Exception:
                pass


def write_traffic_to_kafka(
    messages: List[Dict],
    bootstrap_servers: str,
    topic: str,
    raise_on_error: bool = False,
) -> bool:
    """
    Write traffic data messages to Kafka topic.
    
    Args:
        messages: List of traffic data dictionaries to write
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        topic: Kafka topic name
        raise_on_error: If True, raise exception on error; if False, log and return False
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not KAFKA_AVAILABLE:
        error_msg = "kafka-python not installed. Cannot write to Kafka."
        logger.error(error_msg)
        if raise_on_error:
            raise ImportError(error_msg)
        return False
    
    if not messages:
        logger.warning("No messages to write to Kafka")
        return True
    
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )
        
        success_count = 0
        for message in messages:
            try:
                # Use timestamp as key for partitioning (if available)
                key = message.get("_fetched_at", "unknown")
                future = producer.send(topic, value=message, key=key)
                # Wait for send confirmation (with timeout)
                future.get(timeout=10)
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to send message to Kafka: {e}")
                if raise_on_error:
                    raise
        
        producer.flush()
        logger.info(f"Successfully wrote {success_count}/{len(messages)} messages to Kafka topic '{topic}'")
        return success_count == len(messages)
        
    except (KafkaError, KafkaTimeoutError) as e:
        error_msg = f"Kafka error while writing to topic '{topic}': {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return False
    except Exception as e:
        error_msg = f"Unexpected error writing to Kafka: {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return False
    finally:
        if producer:
            producer.close()


def write_air_quality_to_kafka(
    messages: List[Dict],
    bootstrap_servers: str,
    topic: str,
    raise_on_error: bool = False,
) -> bool:
    """
    Write air quality data messages to Kafka topic.
    
    Args:
        messages: List of air quality measurement dictionaries to write
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        topic: Kafka topic name
        raise_on_error: If True, raise exception on error; if False, log and return False
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not KAFKA_AVAILABLE:
        error_msg = "kafka-python not installed. Cannot write to Kafka."
        logger.error(error_msg)
        if raise_on_error:
            raise ImportError(error_msg)
        return False
    
    if not messages:
        logger.warning("No messages to write to Kafka")
        return True
    
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )
        
        success_count = 0
        for message in messages:
            try:
                # Extract key for partitioning
                # Try location (can be string or dict), then timestamp
                key = "unknown"
                if isinstance(message, dict):
                    # Location might be a string or a dict
                    location = message.get("location")
                    if isinstance(location, str):
                        key = location
                    elif isinstance(location, dict):
                        key = location.get("id", "unknown")
                    
                    # Fallback to timestamp if location not available
                    if key == "unknown":
                        period = message.get("period", {})
                        if isinstance(period, dict):
                            datetime_from = period.get("datetimeFrom", {})
                            if isinstance(datetime_from, dict):
                                key = datetime_from.get("local", "unknown")
                    
                    # Final fallback
                    if key == "unknown":
                        key = str(message.get("value", "unknown"))
                else:
                    # If message is not a dict, use string representation
                    key = str(message)
                
                future = producer.send(topic, value=message, key=str(key))
                future.get(timeout=10)
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to send message to Kafka: {e}")
                if raise_on_error:
                    raise
        
        producer.flush()
        logger.info(f"Successfully wrote {success_count}/{len(messages)} messages to Kafka topic '{topic}'")
        return success_count == len(messages)
        
    except (KafkaError, KafkaTimeoutError) as e:
        error_msg = f"Kafka error while writing to topic '{topic}': {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return False
    except Exception as e:
        error_msg = f"Unexpected error writing to Kafka: {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return False
    finally:
        if producer:
            producer.close()


def read_traffic_from_kafka(
    bootstrap_servers: str,
    topic: str,
    consumer_group: str,
    timeout_ms: int = 10000,
    max_messages: Optional[int] = None,
    raise_on_error: bool = False,
) -> List[Dict]:
    """
    Read traffic data messages from Kafka topic.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        topic: Kafka topic name
        consumer_group: Consumer group ID
        timeout_ms: Timeout in milliseconds for polling
        max_messages: Maximum number of messages to read (None = all available)
        raise_on_error: If True, raise exception on error; if False, log and return empty list
        
    Returns:
        List[Dict]: List of traffic data dictionaries
    """
    if not KAFKA_AVAILABLE:
        error_msg = "kafka-python not installed. Cannot read from Kafka."
        logger.error(error_msg)
        if raise_on_error:
            raise ImportError(error_msg)
        return []
    
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            consumer_timeout_ms=timeout_ms,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=consumer_group,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=True,
        )
        
        messages = []
        message_count = 0
        
        for message in consumer:
            try:
                messages.append(message.value)
                message_count += 1
                if max_messages and message_count >= max_messages:
                    break
            except Exception as e:
                logger.warning(f"Error deserializing message: {e}")
                continue
        
        logger.info(f"Read {len(messages)} messages from Kafka topic '{topic}' (consumer group: {consumer_group})")
        return messages
        
    except (KafkaError, KafkaTimeoutError) as e:
        error_msg = f"Kafka error while reading from topic '{topic}': {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return []
    except Exception as e:
        error_msg = f"Unexpected error reading from Kafka: {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return []
    finally:
        if consumer:
            consumer.close()


def read_air_quality_from_kafka(
    bootstrap_servers: str,
    topic: str,
    consumer_group: str,
    timeout_ms: int = 10000,
    max_messages: Optional[int] = None,
    raise_on_error: bool = False,
) -> List[Dict]:
    """
    Read air quality data messages from Kafka topic.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        topic: Kafka topic name
        consumer_group: Consumer group ID
        timeout_ms: Timeout in milliseconds for polling
        max_messages: Maximum number of messages to read (None = all available)
        raise_on_error: If True, raise exception on error; if False, log and return empty list
        
    Returns:
        List[Dict]: List of air quality measurement dictionaries
    """
    if not KAFKA_AVAILABLE:
        error_msg = "kafka-python not installed. Cannot read from Kafka."
        logger.error(error_msg)
        if raise_on_error:
            raise ImportError(error_msg)
        return []
    
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            consumer_timeout_ms=timeout_ms,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=consumer_group,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
        )
        
        messages = []
        message_count = 0
        
        for message in consumer:
            try:
                messages.append(message.value)
                message_count += 1
                if max_messages and message_count >= max_messages:
                    break
            except Exception as e:
                logger.warning(f"Error deserializing message: {e}")
                continue
        
        logger.info(f"Read {len(messages)} messages from Kafka topic '{topic}' (consumer group: {consumer_group})")
        return messages
        
    except (KafkaError, KafkaTimeoutError) as e:
        error_msg = f"Kafka error while reading from topic '{topic}': {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return []
    except Exception as e:
        error_msg = f"Unexpected error reading from Kafka: {e}"
        logger.error(error_msg)
        if raise_on_error:
            raise
        return []
    finally:
        if consumer:
            consumer.close()

