import json
import time
from datetime import datetime

from pyflink.datastream import MapFunction

from src.message_enricher import MessageEnricher


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
            print(f"Processing message: {value}")

            # Parse the incoming message
            original_message = json.loads(value)

            # Enrich the message
            enriched_message = MessageEnricher.enrich_message(original_message)

            # Update topic information
            enriched_message['source_topic'] = self.source_topic
            enriched_message['destination_topic'] = self.sink_topic

            # Return enriched message as JSON string
            result = json.dumps(enriched_message)
            print(f"Enriched message: {result}")
            return result

        except (json.JSONDecodeError, TypeError) as e:
            # Handle malformed messages
            print(f"Error processing message: {e}")
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
