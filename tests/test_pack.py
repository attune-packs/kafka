from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

PACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACK_ROOT))

from lib import kafka_client


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeKafka:
    class TopicPartition:
        def __init__(self, topic, partition, offset=None):
            self.topic = topic
            self.partition = partition
            self.offset = offset


class FakeMessage:
    def __init__(self, topic="events", partition=0, offset=4, value=b"hello", key=b"key", headers=None, error=None):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._key = key
        self._headers = headers if headers is not None else [("kind", b"test")]
        self._error = error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return self._key

    def headers(self):
        return self._headers

    def timestamp(self):
        return 1, 1700000000000

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self):
        self.events = []
        self.commit_error = None

    def commit(self, **kwargs):
        self.events.append(("commit", kwargs))
        if self.commit_error is not None:
            raise self.commit_error
        return []

    def seek(self, partition):
        self.events.append(("seek", partition))

    def pause(self, partitions):
        self.events.append(("pause", partitions))


def metadata_fixture():
    partition = SimpleNamespace(id=0, leader=1, replicas=[1, 2], isrs=[1, 2], error=None)
    topics = {
        "events": SimpleNamespace(error=None, partitions={0: partition}),
        "__consumer_offsets": SimpleNamespace(error=None, partitions={0: partition}),
    }
    brokers = {1: SimpleNamespace(id=1, host="broker.example", port=9092)}
    return SimpleNamespace(cluster_id="cluster-1", controller_id=1, topics=topics, brokers=brokers)


class PackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sensor = load_module("kafka_sensor_test", PACK_ROOT / "sensors" / "kafka_consumer.py")

    def test_pack_and_flat_resource_contracts(self):
        pack = (PACK_ROOT / "pack.yaml").read_text(encoding="utf-8")
        self.assertIn('source_version: "2.0.0"', pack)
        self.assertIn('source_revision: "42ec262777308d655262e5b64029c655825eb98a"', pack)
        self.assertIn('license: "Apache-2.0"', pack)
        action_paths = list((PACK_ROOT / "actions").glob("*.yaml"))
        self.assertEqual({path.stem for path in action_paths}, {"produce", "inspect_metadata"})
        for path in action_paths:
            text = path.read_text(encoding="utf-8")
            self.assertIn(f"ref: kafka.{path.stem}", text)
            self.assertIn("entry_point: kafka_action.py", text)
            self.assertIn("parameter_delivery: stdin", text)
            self.assertIn("output_format: json", text)
            self.assertIn("default_execution_permission_set_refs: [standard]", text)
        sensor = (PACK_ROOT / "sensors" / "kafka_consumer.yaml").read_text(encoding="utf-8")
        trigger = (PACK_ROOT / "triggers" / "message.yaml").read_text(encoding="utf-8")
        self.assertIn("trigger_types: [kafka.message]", sensor)
        self.assertIn('pattern: "^/run/secrets/.+"', trigger)
        self.assertIn("group_id:", trigger)
        self.assertIn("value_base64:", trigger)

    def test_key_lookup_uses_current_sdk_signature(self):
        calls = {}
        get_key = ModuleType("attune.api_client.api.secrets.get_key")
        get_key.sync_detailed = lambda ref, *, client: calls.update(ref=ref, client=client) or SimpleNamespace(
            status_code=200,
            parsed=SimpleNamespace(data=SimpleNamespace(value={"kafka": {"bootstrap_servers": "broker:9092"}})),
        )
        secrets = ModuleType("attune.api_client.api.secrets")
        secrets.get_key = get_key
        attune = ModuleType("attune")
        attune.context = SimpleNamespace(client="execution-client")
        modules = {
            "attune": attune,
            "attune.api_client": ModuleType("attune.api_client"),
            "attune.api_client.api": ModuleType("attune.api_client.api"),
            "attune.api_client.api.secrets": secrets,
        }
        with patch.dict(sys.modules, modules):
            kafka_client.fetch_key("pack.kafka.credentials")
        self.assertEqual(calls, {"ref": "pack.kafka.credentials", "client": "execution-client"})

    def test_client_config_supports_tls_scram_and_oidc(self):
        scram = kafka_client.client_config(
            {
                "kafka": {
                    "bootstrap_servers": ["one:9093", "two:9093"],
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "SCRAM-SHA-512",
                    "sasl_username": "user",
                    "sasl_password": "synthetic-secret",
                    "ca_file": "/run/secrets/kafka/ca.pem",
                }
            }
        )
        self.assertEqual(scram["security.protocol"], "sasl_ssl")
        self.assertEqual(scram["sasl.mechanism"], "SCRAM-SHA-512")
        self.assertEqual(scram["bootstrap.servers"], "one:9093,two:9093")
        self.assertEqual(scram["ssl.endpoint.identification.algorithm"], "https")
        oauth = kafka_client.client_config(
            {
                "bootstrap_servers": "broker:9093",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "OAUTHBEARER",
                "oauth_client_id": "attune",
                "oauth_client_secret": "synthetic-secret",
                "oauth_token_endpoint_url": "https://identity.example/token",
            }
        )
        self.assertEqual(oauth["sasl.oauthbearer.method"], "oidc")
        with self.assertRaisesRegex(kafka_client.KafkaPackError, "HTTPS URL"):
            kafka_client.client_config(
                {
                    "bootstrap_servers": "broker:9092",
                    "security_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": "OAUTHBEARER",
                    "oauth_client_id": "id",
                    "oauth_client_secret": "secret",
                    "oauth_token_endpoint_url": "http://identity.example/token",
                }
            )

    def test_produce_preserves_key_headers_partition_timestamp_and_idempotence(self):
        module = ModuleType("confluent_kafka")
        calls = {}

        class Delivered:
            def topic(self): return "events"
            def partition(self): return 2
            def offset(self): return 8
            def timestamp(self): return 1, 1700000000000

        class Producer:
            def __init__(self, config):
                calls["config"] = config

            def produce(self, **kwargs):
                calls["produce"] = kwargs
                self.callback = kwargs["on_delivery"]

            def flush(self, timeout):
                calls["flush"] = timeout
                self.callback(None, Delivered())
                return 0

        module.Producer = Producer
        credentials = {"bootstrap_servers": "broker:9092", "security_protocol": "PLAINTEXT"}
        params = {
            "topic": "events",
            "message": "aGVsbG8=",
            "message_encoding": "base64",
            "key": "key",
            "headers": [
                {"key": "trace", "value": "one"},
                {"key": "trace", "value": "dHdv", "value_encoding": "base64"},
            ],
            "partition": 2,
            "timestamp_ms": 1700000000000,
        }
        with patch.dict(sys.modules, {"confluent_kafka": module}), patch.object(kafka_client, "fetch_key", return_value=credentials):
            result = kafka_client.produce(params)
        self.assertTrue(calls["config"]["enable.idempotence"])
        self.assertEqual(calls["config"]["acks"], "all")
        self.assertFalse(calls["config"]["allow.auto.create.topics"])
        self.assertEqual(calls["produce"]["value"], b"hello")
        self.assertEqual(calls["produce"]["key"], b"key")
        self.assertEqual(calls["produce"]["headers"], [("trace", b"one"), ("trace", b"two")])
        self.assertEqual(calls["produce"]["partition"], 2)
        self.assertEqual(calls["produce"]["timestamp"], 1700000000000)
        self.assertEqual(result["status"], "delivered")
        self.assertFalse(result["delivery_ambiguous"])

    def test_bounded_flush_without_report_is_ambiguous(self):
        module = ModuleType("confluent_kafka")

        class Producer:
            def __init__(self, config): pass
            def produce(self, **kwargs): pass
            def flush(self, timeout): return 1

        module.Producer = Producer
        credentials = {"bootstrap_servers": "broker:9092", "security_protocol": "PLAINTEXT"}
        with patch.dict(sys.modules, {"confluent_kafka": module}), patch.object(kafka_client, "fetch_key", return_value=credentials):
            with self.assertRaisesRegex(kafka_client.KafkaAmbiguousDeliveryError, "retry may duplicate"):
                kafka_client.produce({"topic": "events", "message": "hello"})

    def test_metadata_fetches_all_topics_then_filters_client_side(self):
        admin_module = ModuleType("confluent_kafka.admin")
        calls = {}

        class AdminClient:
            def __init__(self, config):
                calls["config"] = config

            def list_topics(self, **kwargs):
                calls["kwargs"] = kwargs
                return metadata_fixture()

        admin_module.AdminClient = AdminClient
        root_module = ModuleType("confluent_kafka")
        credentials = {"bootstrap_servers": "broker:9092", "security_protocol": "PLAINTEXT"}
        with patch.dict(sys.modules, {"confluent_kafka": root_module, "confluent_kafka.admin": admin_module}), patch.object(
            kafka_client, "fetch_key", return_value=credentials
        ):
            result = kafka_client.inspect_metadata({"topic": "events"})
        self.assertNotIn("topic", calls["kwargs"])
        self.assertFalse(calls["config"]["allow.auto.create.topics"])
        self.assertEqual(result["topic_count"], 1)
        self.assertEqual(result["topics"][0]["partitions"][0]["replicas"], [1, 2])

    def processor(self, config, emit, consumer=None, logger=None):
        consumer = consumer or FakeConsumer()
        processor = self.sensor.DeliveryProcessor(config, emit, lambda seconds: None, logger or Mock(), FakeKafka)
        processor.bind(consumer)
        return processor, consumer

    def test_consumer_commits_only_after_successful_emission(self):
        order = []
        consumer = FakeConsumer()

        def emit(payload):
            order.append(("emit", payload))
            return 42

        original_commit = consumer.commit
        consumer.commit = lambda **kwargs: order.append(("commit", kwargs)) or original_commit(**kwargs)
        processor, _ = self.processor({}, emit, consumer)
        processor.handle(FakeMessage())
        self.assertEqual([item[0] for item in order], ["emit", "commit"])
        payload = order[0][1]
        self.assertEqual(payload["value_base64"], "aGVsbG8=")
        self.assertEqual(payload["value_text"], "hello")
        self.assertEqual(payload["delivery_attempt"], 1)

    def test_emission_failure_seeks_same_offset_then_pauses_poison_without_commit(self):
        attempts = []
        processor, consumer = self.processor(
            {"max_retries": 1, "retry_delay_seconds": 0},
            lambda payload: attempts.append(payload["delivery_attempt"]) or None,
        )
        message = FakeMessage()
        processor.handle(message)
        processor.handle(message)
        self.assertEqual(attempts, [1, 2])
        self.assertEqual([event[0] for event in consumer.events], ["seek", "pause"])
        self.assertFalse(any(event[0] == "commit" for event in consumer.events))

    def test_oversized_record_pauses_partition_without_emission_or_commit(self):
        processor, consumer = self.processor(
            {"max_message_bytes": 3}, lambda payload: self.fail("oversized records must not emit")
        )
        processor.handle(FakeMessage(value=b"four", key=None, headers=[]))
        self.assertEqual([event[0] for event in consumer.events], ["pause"])

    def test_commit_failure_redelivery_is_not_reemitted_in_same_process(self):
        emitted = []
        consumer = FakeConsumer()
        consumer.commit_error = RuntimeError("synthetic commit failure")
        processor, _ = self.processor({}, lambda payload: emitted.append(payload) or 1, consumer)
        message = FakeMessage()
        with self.assertRaisesRegex(RuntimeError, "commit failed"):
            processor.handle(message)
        consumer.commit_error = None
        processor.handle(message)
        self.assertEqual(len(emitted), 1)
        self.assertEqual([event[0] for event in consumer.events], ["commit", "commit"])

    def test_tombstone_and_binary_values_are_not_deserialized(self):
        payloads = []
        processor, _ = self.processor({}, lambda payload: payloads.append(payload) or 1)
        processor.handle(FakeMessage(value=None, key=b"\xff", headers=[("null", None)]))
        payload = payloads[0]
        self.assertTrue(payload["value_is_null"])
        self.assertNotIn("value_base64", payload)
        self.assertEqual(payload["key_base64"], "/w==")
        self.assertNotIn("key_text", payload)
        self.assertTrue(payload["headers"][0]["value_is_null"])

    def test_sensor_credentials_are_confined_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "secrets"
            root.mkdir()
            allowed = root / "kafka.json"
            allowed.write_text(json.dumps({"kafka": {"bootstrap_servers": "broker:9092"}}), encoding="utf-8")
            outside = Path(directory) / "outside.json"
            outside.write_text("{}", encoding="utf-8")
            with patch.object(self.sensor, "SENSOR_CREDENTIALS_ROOT", root):
                self.assertEqual(self.sensor.read_credentials_file(str(allowed))["kafka"]["bootstrap_servers"], "broker:9092")
                with self.assertRaises(ValueError):
                    self.sensor.read_credentials_file(str(outside))
                allowed.write_text("x" * (self.sensor.MAX_CREDENTIAL_FILE_BYTES + 1), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "64 KiB"):
                    self.sensor.read_credentials_file(str(allowed))
        with self.assertRaises(ValueError):
            self.sensor.read_credentials_file("relative.json")

    def test_worker_configuration_disables_all_automatic_offset_progress(self):
        rule = SimpleNamespace(
            rule_id=1,
            trigger_params={"topics": ["events"], "group_id": "attune-events", "retry_delay_seconds": 0},
        )
        worker = self.sensor.ConsumerWorker(
            rule,
            {"bootstrap_servers": "broker:9092", "security_protocol": "PLAINTEXT"},
            Mock(),
            lambda payload: 1,
            FakeKafka,
        )
        config = worker._consumer_config()
        self.assertFalse(config["enable.auto.commit"])
        self.assertFalse(config["enable.auto.offset.store"])
        self.assertEqual(config["partition.assignment.strategy"], "cooperative-sticky")
        self.assertEqual(config["isolation.level"], "read_committed")
        self.assertFalse(config["allow.auto.create.topics"])

    def test_worker_stop_uses_poll_cancellation_without_cross_thread_consumer_calls(self):
        rule = SimpleNamespace(rule_id=1, trigger_params={"topics": ["events"], "group_id": "group"})
        worker = self.sensor.ConsumerWorker(
            rule,
            {"bootstrap_servers": "broker:9092", "security_protocol": "PLAINTEXT"},
            Mock(),
            lambda payload: 1,
            FakeKafka,
        )
        worker._thread = SimpleNamespace(join=Mock(), is_alive=lambda: False)
        self.assertTrue(worker.stop())
        self.assertTrue(worker._stop_event.is_set())
        worker._thread.join.assert_called_once_with(timeout=10)

    def test_entrypoint_rejects_malformed_json_without_echoing_secret(self):
        module = load_module("kafka_action_test", PACK_ROOT / "actions" / "kafka_action.py")
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(sys, "stdin", SimpleNamespace(read=lambda: '{"password":"synthetic-secret"')), redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(module.main(), 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertNotIn("synthetic-secret", stderr.getvalue())

    def test_no_unsafe_deserialization_or_embedded_credentials(self):
        forbidden = ["pickle" + ".loads", "yaml" + ".load(", "guest" + ":guest", "enable.auto.commit\": True"]
        for path in PACK_ROOT.rglob("*"):
            if path.is_file() and path.suffix in {".py", ".yaml", ".md", ".txt"}:
                text = path.read_text(encoding="utf-8")
                self.assertFalse(any(value in text for value in forbidden), str(path))


if __name__ == "__main__":
    unittest.main()
