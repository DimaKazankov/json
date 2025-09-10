"""
Flink Job for consuming messages from topic A and publishing enriched messages to topic B.
"""

import json
import time
from datetime import datetime
from typing import Dict, Any

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem, ConnectorDescriptor
from pyflink.table.types import DataTypes
from pyflink.table.window import Tumble


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


class FlinkKafkaJob:
    """Main Flink job class for Kafka message processing."""
    
    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the Flink job.
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers configuration
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.table_env = StreamTableEnvironment.create(self.env)
        
        # Set parallelism
        self.env.set_parallelism(1)
        
    def create_kafka_source_table(self, topic: str) -> str:
        """
        Create a Kafka source table for reading messages.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Table name for the source
        """
        table_name = f"kafka_source_{topic.replace('-', '_')}"
        
        create_table_ddl = f"""
        CREATE TABLE {table_name} (
            message_id STRING,
            original_data STRING,
            processing_timestamp BIGINT,
            processed_at STRING,
            source_topic STRING,
            destination_topic STRING,
            enrichment_version STRING,
            enrichment_type STRING,
            original_message_size INT,
            enriched_message_size INT,
            processing_node STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp BIGINT,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
            'properties.group.id' = 'flink-consumer-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """
        
        self.table_env.execute_sql(create_table_ddl)
        return table_name
        
    def create_kafka_sink_table(self, topic: str) -> str:
        """
        Create a Kafka sink table for writing messages.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Table name for the sink
        """
        table_name = f"kafka_sink_{topic.replace('-', '_')}"
        
        create_table_ddl = f"""
        CREATE TABLE {table_name} (
            message_id STRING,
            original_data STRING,
            processing_timestamp BIGINT,
            processed_at STRING,
            source_topic STRING,
            destination_topic STRING,
            enrichment_version STRING,
            enrichment_type STRING,
            original_message_size INT,
            enriched_message_size INT,
            processing_node STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
            'format' = 'json'
        )
        """
        
        self.table_env.execute_sql(create_table_ddl)
        return table_name
        
    def run_job(self, source_topic: str = "topic-a", sink_topic: str = "topic-b"):
        """
        Run the Flink job to process messages from source topic to sink topic.
        
        Args:
            source_topic: Source Kafka topic name
            sink_topic: Sink Kafka topic name
        """
        print(f"Starting Flink job: {source_topic} -> {sink_topic}")
        
        # Create source and sink tables
        source_table = self.create_kafka_source_table(source_topic)
        sink_table = self.create_kafka_sink_table(sink_topic)
        
        # Define the processing query
        # This query enriches messages and forwards them to the sink topic
        processing_query = f"""
        INSERT INTO {sink_table}
        SELECT 
            message_id,
            original_data,
            processing_timestamp,
            processed_at,
            source_topic,
            destination_topic,
            enrichment_version,
            enrichment_type,
            original_message_size,
            enriched_message_size,
            processing_node
        FROM {source_table}
        """
        
        # Execute the job
        print("Executing Flink job...")
        self.table_env.execute_sql(processing_query)
        
    def run_job_with_enrichment(self, source_topic: str = "topic-a", sink_topic: str = "topic-b"):
        """
        Run the Flink job with custom enrichment logic using DataStream API.
        
        Args:
            source_topic: Source Kafka topic name
            sink_topic: Sink Kafka topic name
        """
        print(f"Starting Flink job with enrichment: {source_topic} -> {sink_topic}")
        
        # Create source and sink tables
        source_table = self.create_kafka_source_table(source_topic)
        sink_table = self.create_kafka_sink_table(sink_topic)
        
        # Convert table to DataStream for custom processing
        source_stream = self.table_env.to_data_stream(
            self.table_env.from_path(source_table)
        )
        
        # Apply enrichment logic
        enriched_stream = source_stream.map(
            lambda row: self._enrich_row(row)
        )
        
        # Convert back to table and insert into sink
        enriched_table = self.table_env.from_data_stream(enriched_stream)
        self.table_env.create_temporary_view("enriched_data", enriched_table)
        
        # Insert enriched data into sink
        insert_query = f"""
        INSERT INTO {sink_table}
        SELECT 
            message_id,
            original_data,
            processing_timestamp,
            processed_at,
            source_topic,
            destination_topic,
            enrichment_version,
            enrichment_type,
            original_message_size,
            enriched_message_size,
            processing_node
        FROM enriched_data
        """
        
        print("Executing Flink job with enrichment...")
        self.table_env.execute_sql(insert_query)
        
    def _enrich_row(self, row) -> tuple:
        """
        Enrich a single row with additional metadata.
        
        Args:
            row: Input row tuple
            
        Returns:
            Enriched row tuple
        """
        # Extract original data
        original_data = row[1] if len(row) > 1 else "{}"
        
        try:
            # Parse original message
            original_message = json.loads(original_data) if isinstance(original_data, str) else original_data
        except (json.JSONDecodeError, TypeError):
            original_message = {"raw_data": str(original_data)}
        
        # Enrich the message
        enriched_message = MessageEnricher.enrich_message(original_message)
        
        # Return enriched row
        return (
            enriched_message.get('message_id', f"msg_{int(time.time() * 1000)}"),
            json.dumps(enriched_message),
            enriched_message.get('processing_timestamp', int(time.time() * 1000)),
            enriched_message.get('processed_at', datetime.now().isoformat()),
            enriched_message.get('source_topic', 'topic-a'),
            enriched_message.get('destination_topic', 'topic-b'),
            enriched_message.get('enrichment_version', '1.0'),
            enriched_message.get('enrichment_type', 'basic_metadata'),
            enriched_message.get('original_message_size', 0),
            enriched_message.get('enriched_message_size', 0),
            enriched_message.get('processing_node', 'flink-taskmanager-1')
        )


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
    parser.add_argument('--with-enrichment', action='store_true',
                       help='Use custom enrichment logic')
    
    args = parser.parse_args()
    
    # Create and run the job
    job = FlinkKafkaJob(kafka_bootstrap_servers=args.kafka_bootstrap)
    
    if args.with_enrichment:
        job.run_job_with_enrichment(args.source_topic, args.sink_topic)
    else:
        job.run_job(args.source_topic, args.sink_topic)


if __name__ == "__main__":
    main()
