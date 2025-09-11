"""
Kafka utility functions for testing.
"""

import json
from kafka import KafkaProducer, KafkaConsumer


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create a Kafka producer for testing.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        
    Returns:
        Configured KafkaProducer instance
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )


def create_kafka_consumer(bootstrap_servers: str, topic: str, timeout_ms: int = 30_000) -> KafkaConsumer:
    """
    Create a Kafka consumer for testing.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Topic to consume from
        timeout_ms: Consumer timeout in milliseconds
        
    Returns:
        Configured KafkaConsumer instance
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
