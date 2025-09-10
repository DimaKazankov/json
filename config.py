"""
Configuration settings for the Flink Kafka application.
"""

import os
from typing import Dict, Any


class Config:
    """Application configuration class."""
    
    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'flink-consumer-group')
    
    # Topic configuration
    SOURCE_TOPIC = os.getenv('SOURCE_TOPIC', 'topic-a')
    SINK_TOPIC = os.getenv('SINK_TOPIC', 'topic-b')
    
    # Flink configuration
    FLINK_PARALLELISM = int(os.getenv('FLINK_PARALLELISM', '1'))
    FLINK_CHECKPOINT_INTERVAL = int(os.getenv('FLINK_CHECKPOINT_INTERVAL', '60000'))
    
    # Processing configuration
    ENABLE_ENRICHMENT = os.getenv('ENABLE_ENRICHMENT', 'true').lower() == 'true'
    ENRICHMENT_VERSION = os.getenv('ENRICHMENT_VERSION', '1.0')
    
    @classmethod
    def get_kafka_properties(cls) -> Dict[str, str]:
        """Get Kafka connection properties."""
        return {
            'bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': cls.KAFKA_CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true'
        }
    
    @classmethod
    def get_flink_properties(cls) -> Dict[str, str]:
        """Get Flink job properties."""
        return {
            'parallelism.default': str(cls.FLINK_PARALLELISM),
            'execution.checkpointing.interval': str(cls.FLINK_CHECKPOINT_INTERVAL),
            'execution.checkpointing.mode': 'EXACTLY_ONCE',
            'execution.checkpointing.timeout': '600000',
            'execution.checkpointing.min-pause': '500'
        }
