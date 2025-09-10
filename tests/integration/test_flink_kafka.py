import time
import json
import requests
import pytest

from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.kafka import KafkaContainer

from kafka import KafkaProducer, KafkaConsumer


def wait_for_flink_ready(rest_base_url: str, timeout_s: int = 120) -> None:
    """Ждём готовности REST API Flink (/config -> 200)."""
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
    raise TimeoutError(f"Flink REST API не поднялся по {url}. Последняя ошибка: {last_err}")


@pytest.fixture(scope="session")
def flink_and_kafka():
    """
    Поднимаем:
      - общую Docker-сеть
      - Kafka broker
      - Flink JobManager (REST 8081, hostname=jobmanager)
      - Flink TaskManager (подключается по jobmanager.rpc.address: jobmanager)
    """
    # Network без имени — совместимо со старыми версиями testcontainers.
    net = Network()
    net.create()

    # --- Kafka ---
    kafka = KafkaContainer(image="confluentinc/cp-kafka:7.6.1")
    # ВАЖНО: передаём ОБЪЕКТ сети, а не строку!
    kafka.with_network(net)

    # Увеличиваем таймаут старта (по умолчанию ~30с часто недостаточно).
    kafka.start(timeout=180)
    kafka_bootstrap = kafka.get_bootstrap_server()

    # --- Flink JobManager ---
    jm = (
        DockerContainer("flink:1.20.0")
        .with_command("jobmanager")
        .with_exposed_ports(8081)  # REST API
        .with_network(net)         # объект сети
        .with_network_aliases("jobmanager")
        .with_kwargs(hostname="jobmanager")
    )
    jm.start()
    jm_rest_port = jm.get_exposed_port(8081)
    flink_rest = f"http://localhost:{jm_rest_port}"

    # --- Flink TaskManager ---
    tm = (
        DockerContainer("flink:1.20.0")
        .with_command("taskmanager")
        .with_network(net)  # объект сети
        .with_env("FLINK_PROPERTIES", "jobmanager.rpc.address: jobmanager")
    )
    tm.start()

    # Ждём готовность REST
    wait_for_flink_ready(flink_rest)

    try:
        yield {"flink_rest": flink_rest, "kafka_bootstrap": kafka_bootstrap}
    finally:
        # Тушим в обратном порядке
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
    """Пишем сообщение в Kafka и читаем обратно."""
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
    """Пингуем Flink REST /overview — проверяем, что кластер жив."""
    rest = flink_and_kafka["flink_rest"]
    r = requests.get(f"{rest}/overview", timeout=10)
    assert r.ok, f"Flink REST /overview returned {r.status_code}"
    data = r.json()
    assert "taskmanagers" in data, f"Unexpected payload: {data}"
