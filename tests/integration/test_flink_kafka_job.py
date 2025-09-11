"""
Integration test for the Flink Kafka job using test containers.
"""

import time

from tests.fixtures import flink_and_kafka
from tests.kafka_utils import create_kafka_producer, create_kafka_consumer, start_flink_job


def test_flink_kafka_job_integration(flink_and_kafka):
    """Test the complete Flink Kafka job integration."""
    bootstrap = flink_and_kafka["kafka_bootstrap"]
    flink_rest = flink_and_kafka["flink_rest"]
    
    source_topic = "topic-a"
    sink_topic = "topic-b"
    
    # Create Kafka producer
    producer = create_kafka_producer(bootstrap)
    
    # Test messages to send
    test_messages = [
        {"user_id": "123", "action": "login", "timestamp": time.time()},
        {"user_id": "456", "action": "purchase", "amount": 99.99, "timestamp": time.time()},
        {"user_id": "789", "action": "logout", "session_duration": 3600, "timestamp": time.time()},
    ]
    
    # Create topics first
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id='test-admin'
    )
    
    try:
        # Create source topic
        source_topic_obj = NewTopic(name=source_topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics([source_topic_obj])
        print(f"Created topic: {source_topic}")
    except TopicAlreadyExistsError:
        print(f"Topic {source_topic} already exists")
    
    try:
        # Create sink topic
        sink_topic_obj = NewTopic(name=sink_topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics([sink_topic_obj])
        print(f"Created topic: {sink_topic}")
    except TopicAlreadyExistsError:
        print(f"Topic {sink_topic} already exists")
    
    # Start the actual Flink job first
    flink_thread = start_flink_job(bootstrap, source_topic, sink_topic)
    
    # Wait for Flink job to start processing
    print("Waiting for Flink job to start...")
    time.sleep(5)
    
    # Send test messages to source topic AFTER Flink job is running
    print(f"Sending {len(test_messages)} messages to {source_topic}")
    for msg in test_messages:
        producer.send(source_topic, msg)
    producer.flush()
    
    # Wait a moment for messages to be processed
    print("Waiting for messages to be processed...")
    time.sleep(15)
    
    # Create consumer after Flink job has been running
    print("Creating consumer for sink topic...")
    consumer = create_kafka_consumer(bootstrap, sink_topic, group_id="test-consumer-group")
    
    # Consume messages from sink topic
    print("Starting to consume messages from sink topic...")
    consumed_messages = []
    message_count = 0
    for msg in consumer:
        message_count += 1
        print(f"Received message {message_count}: {msg.value}")
        consumed_messages.append(msg.value)
        if len(consumed_messages) >= len(test_messages):
            break
    
    print(f"Total messages consumed: {len(consumed_messages)}")
    
    # Verify we received the expected number of messages
    assert len(consumed_messages) == len(test_messages), \
        f"Expected {len(test_messages)} messages, got {len(consumed_messages)}"
    
    # Verify message enrichment
    for i, consumed_msg in enumerate(consumed_messages):
        original_msg = test_messages[i]
        
        # Check that original data is preserved
        for key, value in original_msg.items():
            assert consumed_msg[key] == value, \
                f"Original field {key} not preserved: expected {value}, got {consumed_msg[key]}"
        
        # Check that enrichment fields are present
        enrichment_fields = [
            'processed_at', 'processing_timestamp', 'message_id',
            'source_topic', 'destination_topic', 'enrichment_version',
            'enrichment_type', 'original_message_size', 'enriched_message_size',
            'processing_node'
        ]
        
        for field in enrichment_fields:
            assert field in consumed_msg, f"Enrichment field {field} missing from message"
            assert consumed_msg[field] is not None, f"Enrichment field {field} is None"
        
        # Verify specific enrichment values
        assert consumed_msg['source_topic'] == source_topic
        assert consumed_msg['destination_topic'] == sink_topic
        assert consumed_msg['enrichment_version'] == '1.0'
        assert consumed_msg['enrichment_type'] == 'basic_metadata'
        assert consumed_msg['original_message_size'] > 0
        assert consumed_msg['enriched_message_size'] > consumed_msg['original_message_size']
    
    print(f"Successfully processed {len(consumed_messages)} messages with enrichment")
