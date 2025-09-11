"""
Flink Job for consuming messages from topic A and publishing enriched messages to topic B.
"""

import json
import time
from datetime import datetime
from typing import Dict, Any

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction


class MessageEnricher:
    """Utility class for enriching messages with additional metadata."""
    
    @staticmethod
    def enrich_message(original_message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich the original message with additional fields.
        
        Args:
            original_message: The original message from topic A
            
        Returns:
            Enriched message with additional metadata
        """
        enriched = original_message.copy()
        
        # Add processing timestamp
        enriched['processed_at'] = datetime.now().isoformat()
        enriched['processing_timestamp'] = int(time.time() * 1000)
        
        # Add message ID if not present
        if 'message_id' not in enriched:
            enriched['message_id'] = f"msg_{int(time.time() * 1000)}"
        
        # Add source information
        enriched['source_topic'] = 'topic-a'
        enriched['destination_topic'] = 'topic-b'
        
        # Add enrichment metadata
        enriched['enrichment_version'] = '1.0'
        enriched['enrichment_type'] = 'basic_metadata'
        
        # Add message size information
        message_str = json.dumps(original_message)
        enriched['original_message_size'] = len(message_str)
        enriched['enriched_message_size'] = len(json.dumps(enriched))
        
        # Add processing node information (simulated)
        enriched['processing_node'] = 'flink-taskmanager-1'
        
        return enriched


class MessageEnrichmentFunction(MapFunction):
    """Custom MapFunction to enrich Kafka messages."""
    
    def __init__(self, source_topic: str, sink_topic: str):
        self.source_topic = source_topic
        self.sink_topic = sink_topic
    
    def map(self, value: str) -> str:
        """
        Map function to enrich incoming messages.
        
        Args:
            value: JSON string from Kafka
            
        Returns:
            Enriched JSON string
        """
        try:
            # Parse the incoming message
            original_message = json.loads(value)
            
            # Enrich the message
            enriched_message = MessageEnricher.enrich_message(original_message)
            
            # Update topic information
            enriched_message['source_topic'] = self.source_topic
            enriched_message['destination_topic'] = self.sink_topic
            
            # Return enriched message as JSON string
            return json.dumps(enriched_message)
            
        except (json.JSONDecodeError, TypeError) as e:
            # Handle malformed messages
            error_message = {
                'error': 'Failed to parse message',
                'original_message': value,
                'error_details': str(e),
                'processed_at': datetime.now().isoformat(),
                'processing_timestamp': int(time.time() * 1000),
                'source_topic': self.source_topic,
                'destination_topic': self.sink_topic,
                'enrichment_version': '1.0',
                'enrichment_type': 'error_handling'
            }
            return json.dumps(error_message)


class FlinkKafkaJob:
    """Main Flink job class for Kafka message processing using DataStream API."""
    
    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Flink job.
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers configuration
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.env = StreamExecutionEnvironment.get_execution_environment()
        
        # Set parallelism
        self.env.set_parallelism(1)
        
        # Set up checkpointing for exactly-once processing
        self.env.enable_checkpointing(60000)  # Checkpoint every 60 seconds
        
    def create_kafka_consumer(self, topic: str, group_id: str = "flink-consumer-group"):
        """
        Create a Kafka consumer for reading messages.
        
        Args:
            topic: Kafka topic name
            group_id: Consumer group ID
            
        Returns:
            FlinkKafkaConsumer instance
        """
        properties = {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true'
        }
        
        return FlinkKafkaConsumer(
            topics=topic,
            deserialization_schema=SimpleStringSchema(),
            properties=properties
        )
    
    def create_kafka_producer(self, topic: str):
        """
        Create a Kafka producer for writing messages.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            FlinkKafkaProducer instance
        """
        properties = {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': '3',
            'batch.size': '16384',
            'linger.ms': '5',
            'buffer.memory': '33554432'
        }
        
        return FlinkKafkaProducer(
            topic=topic,
            serialization_schema=SimpleStringSchema(),
            producer_config=properties
        )
    
    def run_job(self, source_topic: str = "topic-a", sink_topic: str = "topic-b"):
        """
        Run the Flink job to process messages from source topic to sink topic.
        
        Args:
            source_topic: Source Kafka topic name
            sink_topic: Sink Kafka topic name
        """
        print(f"Starting Flink job: {source_topic} -> {sink_topic}")
        print(f"Kafka Bootstrap Servers: {self.kafka_bootstrap_servers}")
        
        # Create Kafka consumer and producer
        kafka_consumer = self.create_kafka_consumer(source_topic)
        kafka_producer = self.create_kafka_producer(sink_topic)
        
        # Create the data stream
        source_stream = self.env.add_source(kafka_consumer)
        
        # Apply enrichment transformation
        enriched_stream = source_stream.map(
            MessageEnrichmentFunction(source_topic, sink_topic)
        )
        
        # Add sink
        enriched_stream.add_sink(kafka_producer)
        
        # Execute the job
        print("Executing Flink job...")
        self.env.execute("Kafka Message Enrichment Job")
    
    def run_job_with_custom_processing(self, source_topic: str = "topic-a", sink_topic: str = "topic-b"):
        """
        Run the Flink job with additional custom processing logic.
        
        Args:
            source_topic: Source Kafka topic name
            sink_topic: Sink Kafka topic name
        """
        print(f"Starting Flink job with custom processing: {source_topic} -> {sink_topic}")
        print(f"Kafka Bootstrap Servers: {self.kafka_bootstrap_servers}")
        
        # Create Kafka consumer and producer
        kafka_consumer = self.create_kafka_consumer(source_topic)
        kafka_producer = self.create_kafka_producer(sink_topic)
        
        # Create the data stream
        source_stream = self.env.add_source(kafka_consumer)
        
        # Apply enrichment transformation
        enriched_stream = source_stream.map(
            MessageEnrichmentFunction(source_topic, sink_topic)
        )
        
        # Add additional processing (e.g., filtering, windowing, etc.)
        # For example, filter out error messages
        filtered_stream = enriched_stream.filter(
            lambda message: '"error"' not in message
        )
        
        # Add sink
        filtered_stream.add_sink(kafka_producer)
        
        # Execute the job
        print("Executing Flink job with custom processing...")
        self.env.execute("Kafka Message Enrichment Job with Custom Processing")


def main():
    """Main entry point for the Flink job."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Flink Kafka Message Processing Job')
    parser.add_argument('--kafka-bootstrap', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--source-topic', default='topic-a',
                       help='Source topic name (default: topic-a)')
    parser.add_argument('--sink-topic', default='topic-b',
                       help='Sink topic name (default: topic-b)')
    parser.add_argument('--custom-processing', action='store_true',
                       help='Use custom processing with filtering')
    
    args = parser.parse_args()
    
    # Create and run the job
    job = FlinkKafkaJob(kafka_bootstrap_servers=args.kafka_bootstrap)
    
    if args.custom_processing:
        job.run_job_with_custom_processing(args.source_topic, args.sink_topic)
    else:
        job.run_job(args.source_topic, args.sink_topic)


if __name__ == "__main__":
    main()
