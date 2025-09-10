# Flink Kafka Message Processing Application

This application demonstrates a Flink job that consumes messages from Kafka topic A and publishes enriched messages to Kafka topic B using test containers for integration testing.

## Features

- **Kafka Integration**: Consumes messages from source topic and publishes to sink topic
- **Message Enrichment**: Adds metadata like processing timestamps, message IDs, and size information
- **Test Container Support**: Uses test containers for reliable integration testing
- **Configurable**: Supports environment variables and command-line arguments
- **Two Processing Modes**: Basic table-based processing and custom DataStream enrichment

## Project Structure

```
flink-test-container/
├── src/
│   ├── __init__.py
│   └── flink_job.py          # Main Flink job implementation
├── tests/
│   └── integration/
│       ├── test_flink_kafka.py           # Original test containers test
│       └── test_flink_kafka_job.py       # Flink job integration test
├── config.py                 # Configuration management
├── run_job.py               # Job runner script
├── requirements.txt         # Python dependencies
├── pytest.ini              # Pytest configuration
└── README.md               # This file
```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running the Flink Job

#### Basic Usage
```bash
python run_job.py
```

#### With Custom Configuration
```bash
python run_job.py --kafka-bootstrap kafka:9092 --source-topic input-topic --sink-topic output-topic
```

#### With Enrichment Enabled
```bash
python run_job.py --with-enrichment
```

#### Using Environment Variables
```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export SOURCE_TOPIC=input-topic
export SINK_TOPIC=output-topic
export ENABLE_ENRICHMENT=true
python run_job.py
```

### Configuration Options

| Environment Variable | Command Line Option | Default | Description |
|---------------------|-------------------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `--kafka-bootstrap` | `localhost:9092` | Kafka bootstrap servers |
| `SOURCE_TOPIC` | `--source-topic` | `topic-a` | Source Kafka topic |
| `SINK_TOPIC` | `--sink-topic` | `topic-b` | Sink Kafka topic |
| `FLINK_PARALLELISM` | `--parallelism` | `1` | Flink parallelism |
| `ENABLE_ENRICHMENT` | `--with-enrichment` | `true` | Enable custom enrichment |

## Message Enrichment

The application enriches incoming messages with the following metadata:

- `processed_at`: ISO timestamp of processing
- `processing_timestamp`: Unix timestamp in milliseconds
- `message_id`: Unique message identifier
- `source_topic`: Source topic name
- `destination_topic`: Destination topic name
- `enrichment_version`: Version of enrichment logic
- `enrichment_type`: Type of enrichment applied
- `original_message_size`: Size of original message
- `enriched_message_size`: Size of enriched message
- `processing_node`: Processing node identifier

### Example Message Transformation

**Input Message (topic-a):**
```json
{
  "user_id": "123",
  "action": "login",
  "timestamp": 1640995200
}
```

**Output Message (topic-b):**
```json
{
  "user_id": "123",
  "action": "login",
  "timestamp": 1640995200,
  "processed_at": "2023-12-31T12:00:00.000Z",
  "processing_timestamp": 1640995200000,
  "message_id": "msg_1640995200000",
  "source_topic": "topic-a",
  "destination_topic": "topic-b",
  "enrichment_version": "1.0",
  "enrichment_type": "basic_metadata",
  "original_message_size": 45,
  "enriched_message_size": 234,
  "processing_node": "flink-taskmanager-1"
}
```

## Testing

### Run Integration Tests
```bash
pytest tests/integration/ -v
```

### Run Specific Test
```bash
pytest tests/integration/test_flink_kafka_job.py::test_flink_kafka_job_integration -v
```

### Test with Test Containers
The integration tests use test containers to spin up:
- Kafka broker (Confluent 7.4.0)
- Flink JobManager
- Flink TaskManager

This ensures tests run in a realistic environment without requiring external dependencies.

## Architecture

### Processing Modes

1. **Table-based Processing**: Uses Flink Table API for simple message forwarding
2. **DataStream Processing**: Uses Flink DataStream API with custom enrichment logic

### Components

- **FlinkKafkaJob**: Main job class that orchestrates the processing
- **MessageEnricher**: Utility class for enriching messages with metadata
- **Config**: Configuration management with environment variable support

## Development

### Adding New Enrichment Logic

1. Extend the `MessageEnricher` class:
```python
class CustomMessageEnricher(MessageEnricher):
    @staticmethod
    def enrich_message(original_message):
        enriched = super().enrich_message(original_message)
        # Add custom enrichment logic
        enriched['custom_field'] = 'custom_value'
        return enriched
```

2. Update the job to use the custom enricher:
```python
job = FlinkKafkaJob()
job.enricher = CustomMessageEnricher()
```

### Adding New Tests

1. Create test functions in `tests/integration/test_flink_kafka_job.py`
2. Use the `flink_and_kafka` fixture for test container setup
3. Test both message processing and enrichment logic

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**: Ensure Kafka is running and accessible
2. **Flink Job Submission**: Check Flink cluster status via REST API
3. **Message Serialization**: Verify JSON format of input messages
4. **Test Container Timeouts**: Increase timeout values for slower environments

### Debug Mode

Enable debug logging by setting environment variable:
```bash
export PYTHONPATH=src
export FLINK_LOG_LEVEL=DEBUG
python run_job.py
```

## Dependencies

- **apache-flink**: Flink Python API
- **pyflink**: Flink Table API
- **kafka-python-ng**: Kafka client
- **testcontainers**: Test container support
- **pytest**: Testing framework
- **requests**: HTTP client for Flink REST API

## License

This project is part of a demonstration setup for Flink and Kafka integration testing.
