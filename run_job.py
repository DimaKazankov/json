#!/usr/bin/env python3
"""
Simple runner script for the Flink Kafka job.
"""

import sys
import os
import argparse
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from flink_job import FlinkKafkaJob
from config import Config


def main():
    """Main entry point for running the Flink job."""
    parser = argparse.ArgumentParser(
        description='Run Flink Kafka Message Processing Job',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings
  python run_job.py
  
  # Run with custom Kafka bootstrap servers
  python run_job.py --kafka-bootstrap kafka:9092
  
  # Run with custom topics
  python run_job.py --source-topic input-topic --sink-topic output-topic
  
  # Run with enrichment enabled
  python run_job.py --with-enrichment
  
  # Run with environment variables
  KAFKA_BOOTSTRAP_SERVERS=kafka:9092 python run_job.py
        """
    )
    
    parser.add_argument('--kafka-bootstrap', 
                       default=Config.KAFKA_BOOTSTRAP_SERVERS,
                       help=f'Kafka bootstrap servers (default: {Config.KAFKA_BOOTSTRAP_SERVERS})')
    parser.add_argument('--source-topic', 
                       default=Config.SOURCE_TOPIC,
                       help=f'Source topic name (default: {Config.SOURCE_TOPIC})')
    parser.add_argument('--sink-topic', 
                       default=Config.SINK_TOPIC,
                       help=f'Sink topic name (default: {Config.SINK_TOPIC})')
    parser.add_argument('--with-enrichment', 
                       action='store_true',
                       default=Config.ENABLE_ENRICHMENT,
                       help='Use custom enrichment logic')
    parser.add_argument('--parallelism', 
                       type=int,
                       default=Config.FLINK_PARALLELISM,
                       help=f'Flink parallelism (default: {Config.FLINK_PARALLELISM})')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Flink Kafka Message Processing Job")
    print("=" * 60)
    print(f"Kafka Bootstrap Servers: {args.kafka_bootstrap}")
    print(f"Source Topic: {args.source_topic}")
    print(f"Sink Topic: {args.sink_topic}")
    print(f"Enrichment Enabled: {args.with_enrichment}")
    print(f"Parallelism: {args.parallelism}")
    print("=" * 60)
    
    try:
        # Create and run the job
        job = FlinkKafkaJob(kafka_bootstrap_servers=args.kafka_bootstrap)
        job.env.set_parallelism(args.parallelism)
        
        if args.with_enrichment:
            print("Starting job with custom enrichment...")
            job.run_job_with_enrichment(args.source_topic, args.sink_topic)
        else:
            print("Starting job with basic processing...")
            job.run_job(args.source_topic, args.sink_topic)
            
    except KeyboardInterrupt:
        print("\nJob interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error running job: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
