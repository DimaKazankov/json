import time
import json
import requests
import pytest

from kafka import KafkaProducer, KafkaConsumer

from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.kafka import KafkaContainer


def wait_for_flink_ready(rest_base_url: str, timeout_s: int = 180) -> None:
    """Wait until Flink REST /config returns 200 OK."""
    deadline = time.time() + timeout_s
    last_err = None
    url = f"{rest_base_url}/config"
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return
        except Exception as e:
            last_err = e
        time.sleep(2)
    raise TimeoutError(f"Flink REST API did not become ready at {url}. Last error: {last_err}")


@pytest.fixture(scope="session")
def flink_and_kafka():
    """
    Bring up:
      - single custom Docker network
      - Kafka broker (Confluent 7.4.0) created directly on that network
      - Flink JobManager (REST 8081, hostname=jobmanager) on same network
      - Flink TaskManager, connected to JM via jobmanager.rpc.address
    """
    # Create a single, dedicated network (no name arg to keep compatibility)
    net = Network()
    net.create()

    # --- Kafka (IMPORTANT: create on the custom network; avoid default bridge) ---
    kafka = KafkaContainer(image="confluentinc/cp-kafka:7.4.0")
    # Place container directly on our custom network so it gets exactly one IP
    kafka.with_kwargs(network=net.name)
    kafka.with_network_aliases("kafka")
    kafka.start(timeout=300)  # generous timeout for slower machines
    kafka_bootstrap = kafka.get_bootstrap_server()

    # --- Flink JobManager ---
    jm = (
        DockerContainer("flink:1.20.0")
        .with_command("jobmanager")
        .with_exposed_ports(8081)  # Flink REST
        .with_kwargs(network=net.name, hostname="jobmanager")
        .with_network_aliases("jobmanager")
    )
    jm.start()
    jm_rest_port = jm.get_exposed_port(8081)
    flink_rest = f"http://localhost:{jm_rest_port}"

    # --- Flink TaskManager ---
    tm = (
        DockerContainer("flink:1.20.0")
        .with_command("taskmanager")
        .with_kwargs(network=net.name)
        .with_env("FLINK_PROPERTIES", "jobmanager.rpc.address: jobmanager")
    )
    tm.start()

    # Wait for Flink REST to be ready
    wait_for_flink_ready(flink_rest)

    try:
        yield {"flink_rest": flink_rest, "kafka_bootstrap": kafka_bootstrap}
    finally:
        # Teardown in reverse order
        try:
            tm.stop()
        finally:
            try:
                jm.stop()
            finally:
                try:
                    kafka.stop()
                finally:
                    net.remove()


def test_kafka_roundtrip(flink_and_kafka):
    """Produce one message to Kafka and consume it back."""
    bootstrap = flink_and_kafka["kafka_bootstrap"]
    topic = "test-topic"

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10_000,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    payload = {"hello": "world", "ts": time.time()}
    producer.send(topic, payload)
    producer.flush()

    got = None
    for msg in consumer:
        got = msg.value
        break

    assert got == payload, f"Expected {payload}, got {got}"


def test_flink_rest_overview(flink_and_kafka):
    """Ping Flink REST /overview to verify the cluster is up."""
    rest = flink_and_kafka["flink_rest"]
    r = requests.get(f"{rest}/overview", timeout=10)
    assert r.ok, f"/overview returned {r.status_code}"
    data = r.json()
    assert "taskmanagers" in data, f"Unexpected payload: {data}"
