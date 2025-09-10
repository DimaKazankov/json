#!/usr/bin/env python3
"""
Example usage of the Flink Kafka message processing application.
This script demonstrates how to send test messages and verify the enrichment process.
"""

import time
import json
from kafka import KafkaProducer, KafkaConsumer
from src.flink_job import MessageEnricher


def send_test_messages(bootstrap_servers: str, topic: str, num_messages: int = 5):
    """Send test messages to a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )
    
    print(f"Sending {num_messages} test messages to topic '{topic}'...")
    
    for i in range(num_messages):
        message = {
            "message_id": f"test_msg_{i}",
            "user_id": f"user_{i}",
            "action": ["login", "purchase", "logout", "view", "search"][i % 5],
            "timestamp": time.time(),
            "data": {
                "session_id": f"session_{i}",
                "ip_address": f"192.168.1.{i + 1}",
                "user_agent": f"browser_{i % 3}"
            }
        }
        
        producer.send(topic, message)
        print(f"  Sent message {i + 1}: {message['action']} by {message['user_id']}")
    
    producer.flush()
    print("All messages sent successfully!")


def consume_messages(bootstrap_servers: str, topic: str, timeout_ms: int = 10000):
    """Consume messages from a Kafka topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    
    print(f"Consuming messages from topic '{topic}'...")
    messages = []
    
    for msg in consumer:
        messages.append(msg.value)
        print(f"  Received message: {msg.value.get('action', 'unknown')} by {msg.value.get('user_id', 'unknown')}")
        print(f"    Enriched with: {msg.value.get('enrichment_type', 'none')} v{msg.value.get('enrichment_version', 'unknown')}")
        print(f"    Size: {msg.value.get('original_message_size', 0)} -> {msg.value.get('enriched_message_size', 0)} bytes")
        print()
    
    print(f"Consumed {len(messages)} messages total")
    return messages


def demonstrate_enrichment():
    """Demonstrate the message enrichment process."""
    print("=" * 60)
    print("Message Enrichment Demonstration")
    print("=" * 60)
    
    # Create a sample message
    original_message = {
        "user_id": "demo_user",
        "action": "demo_action",
        "timestamp": time.time(),
        "data": {"key": "value"}
    }
    
    print("Original Message:")
    print(json.dumps(original_message, indent=2))
    print()
    
    # Enrich the message
    enriched_message = MessageEnricher.enrich_message(original_message)
    
    print("Enriched Message:")
    print(json.dumps(enriched_message, indent=2))
    print()
    
    # Show the differences
    print("Enrichment Summary:")
    print(f"  Original size: {enriched_message['original_message_size']} bytes")
    print(f"  Enriched size: {enriched_message['enriched_message_size']} bytes")
    print(f"  Size increase: {enriched_message['enriched_message_size'] - enriched_message['original_message_size']} bytes")
    print(f"  Enrichment type: {enriched_message['enrichment_type']}")
    print(f"  Processing node: {enriched_message['processing_node']}")


def main():
    """Main demonstration function."""
    print("Flink Kafka Message Processing - Example Usage")
    print("=" * 60)
    
    # Configuration
    kafka_bootstrap = "localhost:9092"
    source_topic = "topic-a"
    sink_topic = "topic-b"
    
    print(f"Kafka Bootstrap: {kafka_bootstrap}")
    print(f"Source Topic: {source_topic}")
    print(f"Sink Topic: {sink_topic}")
    print()
    
    # Demonstrate enrichment
    demonstrate_enrichment()
    
    print("\n" + "=" * 60)
    print("Note: To test the full Flink job integration:")
    print("1. Start Kafka and Flink using test containers")
    print("2. Run: python run_job.py")
    print("3. Send messages to topic-a")
    print("4. Check enriched messages in topic-b")
    print("=" * 60)


if __name__ == "__main__":
    main()
