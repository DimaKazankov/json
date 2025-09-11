import json
import time
from datetime import datetime
from typing import Dict, Any


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
