"""
Kafka and Flink utility functions for testing.
"""

import json
import threading
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


def start_flink_job(bootstrap_servers: str, source_topic: str, sink_topic: str) -> threading.Thread:
    """
    Start a Flink job in a background thread.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        source_topic: Source topic name
        sink_topic: Sink topic name
        
    Returns:
        Thread running the Flink job
    """
    from src.flink_job import FlinkKafkaJob
    
    # Create the Flink job
    job = FlinkKafkaJob(kafka_bootstrap_servers=bootstrap_servers)
    
    def run_flink_job():
        try:
            job.run_job(source_topic, sink_topic)
        except Exception as e:
            print(f"Flink job error: {e}")
    
    # Start Flink job in background thread
    flink_thread = threading.Thread(target=run_flink_job, daemon=True)
    flink_thread.start()
    
    return flink_thread
