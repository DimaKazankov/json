"""
Flink Job for consuming messages from topic A and publishing enriched messages to topic B.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy

from src.message_enrichment_function import MessageEnrichmentFunction


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
        
        # Add Kafka connector JAR files
        self._add_kafka_jars()
        
        # Set parallelism
        self.env.set_parallelism(1)
        
        # Set up checkpointing for exactly-once processing
        self.env.enable_checkpointing(60000)  # Checkpoint every 60 seconds
    
    def _add_kafka_jars(self):
        """Add Kafka connector JAR files to the Flink environment."""
        import os
        
        # Use local JAR files
        jar_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "jars")
        kafka_jars = [
            os.path.join(jar_dir, "flink-sql-connector-kafka-3.0.1-1.18.jar"),
            os.path.join(jar_dir, "flink-connector-kafka-3.0.1-1.18.jar")
        ]
        
        for jar_path in kafka_jars:
            if os.path.exists(jar_path):
                self.env.add_jars(f"file://{jar_path}")
            else:
                print(f"Warning: JAR file not found: {jar_path}")
        
    def create_kafka_source(self, topic: str, group_id: str = "flink-consumer-group"):
        """
        Create a Kafka source for reading messages.
        
        Args:
            topic: Kafka topic name
            group_id: Consumer group ID
            
        Returns:
            KafkaSource instance
        """
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.kafka_bootstrap_servers) \
            .set_topics(topic) \
            .set_group_id(group_id) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
    
    def create_kafka_sink(self, topic: str):
        """
        Create a Kafka sink for writing messages.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            KafkaSink instance
        """
        return KafkaSink.builder() \
            .set_bootstrap_servers(self.kafka_bootstrap_servers) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            ) \
            .build()

    def run_job(self, source_topic: str = "topic-a", sink_topic: str = "topic-b"):
        """
        Run the Flink job to process messages from source topic to sink topic.
        
        Args:
            source_topic: Source Kafka topic name
            sink_topic: Sink Kafka topic name
        """
        print(f"Starting Flink job: {source_topic} -> {sink_topic}")
        print(f"Kafka Bootstrap Servers: {self.kafka_bootstrap_servers}")
        
        try:
            # Create Kafka source and sink
            print("Creating Kafka source...")
            kafka_source = self.create_kafka_source(source_topic)
            print("Creating Kafka producer sink...")
            kafka_sink = self.create_kafka_sink(sink_topic)
            
            # Create the data stream
            print("Creating data stream...")
            source_stream = self.env.from_source(
                kafka_source, 
                WatermarkStrategy.no_watermarks(), 
                "Kafka Source",
                type_info=Types.STRING()
            )
            
            # Apply enrichment transformation
            print("Applying enrichment transformation...")
            enriched_stream = source_stream.map(
                MessageEnrichmentFunction(source_topic, sink_topic),
                output_type=Types.STRING()
            )

            enriched_stream.sink_to(kafka_sink)
            
            # Execute the job
            print("Executing Flink job...")
            self.env.execute("Kafka Message Enrichment Job")
            
        except Exception as e:
            print(f"Error in Flink job execution: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def run_job_with_custom_processing(self, source_topic: str = "topic-a", sink_topic: str = "topic-b"):
        """
        Run the Flink job with additional custom processing logic.
        
        Args:
            source_topic: Source Kafka topic name
            sink_topic: Sink Kafka topic name
        """
        print(f"Starting Flink job with custom processing: {source_topic} -> {sink_topic}")
        print(f"Kafka Bootstrap Servers: {self.kafka_bootstrap_servers}")
        
        # Create Kafka source and sink
        kafka_source = self.create_kafka_source(source_topic)
        kafka_sink = self.create_kafka_sink(sink_topic)
        
        # Create the data stream
        source_stream = self.env.from_source(
            kafka_source, 
            WatermarkStrategy.no_watermarks(), 
            "Kafka Source"
        )
        
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
        filtered_stream.sink_to(kafka_sink)
        
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
