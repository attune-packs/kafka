#!/usr/bin/env python3
"""Rule-targeted Kafka consumer with post-emission offset commits."""

from __future__ import annotations

import base64
import json
import math
import os
import stat
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

_PACK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PACK_ROOT not in sys.path:
    sys.path.insert(0, _PACK_ROOT)

from lib.kafka_client import client_config

SENSOR_CREDENTIALS_ROOT = Path("/run/secrets")
MAX_CREDENTIAL_FILE_BYTES = 65536


def read_credentials_file(path_value: Any) -> dict[str, Any]:
    if not isinstance(path_value, str) or not os.path.isabs(path_value):
        raise ValueError("credential_file must be an absolute path")
    root = SENSOR_CREDENTIALS_ROOT.resolve()
    candidate = Path(path_value).resolve()
    if candidate == root or root not in candidate.parents:
        raise ValueError("credential_file must be below /run/secrets")
    try:
        metadata = candidate.stat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_CREDENTIAL_FILE_BYTES:
            raise ValueError("credential_file must be a regular JSON file no larger than 64 KiB")
        raw = candidate.read_text(encoding="utf-8")
    except ValueError:
        raise
    except (OSError, UnicodeError) as exc:
        raise ValueError("credential_file must contain a readable JSON object") from exc
    if len(raw.encode("utf-8")) > MAX_CREDENTIAL_FILE_BYTES:
        raise ValueError("credential_file must be a regular JSON file no larger than 64 KiB")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("credential_file must contain a readable JSON object") from exc
    if not isinstance(value, dict) or not isinstance(value.get("kafka", value), dict):
        raise ValueError("credential_file must contain a Kafka JSON object")
    return value


def _integer(config: Mapping[str, Any], name: str, default: int, minimum: int, maximum: int) -> int:
    value = config.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _number(config: Mapping[str, Any], name: str, default: float, minimum: float, maximum: float) -> float:
    value = config.get(name, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result < minimum or result > maximum:
        raise ValueError(f"{name} must be between {minimum:g} and {maximum:g}")
    return result


def _bytes_fields(prefix: str, value: bytes | None) -> dict[str, Any]:
    result: dict[str, Any] = {f"{prefix}_is_null": value is None}
    if value is None:
        return result
    result[f"{prefix}_base64"] = base64.b64encode(value).decode("ascii")
    try:
        result[f"{prefix}_text"] = value.decode("utf-8")
    except UnicodeDecodeError:
        pass
    return result


def _headers(value: Any) -> tuple[list[dict[str, Any]], int]:
    result: list[dict[str, Any]] = []
    total = 0
    for name, raw in value or []:
        name = str(name)
        item: dict[str, Any] = {"key": name, "value_is_null": raw is None}
        total += len(name.encode("utf-8"))
        if raw is not None:
            if not isinstance(raw, bytes):
                raw = bytes(raw)
            total += len(raw)
            item["value_base64"] = base64.b64encode(raw).decode("ascii")
            try:
                item["value_text"] = raw.decode("utf-8")
            except UnicodeDecodeError:
                pass
        result.append(item)
    return result, total


class DeliveryProcessor:
    """Emit and then commit exactly one record; never batch offsets."""

    def __init__(
        self,
        config: Mapping[str, Any],
        emit: Callable[[dict[str, Any]], Any],
        sleeper: Callable[[float], None],
        logger: Any,
        kafka_module: Any,
    ) -> None:
        self.emit = emit
        self.sleeper = sleeper
        self.logger = logger
        self.kafka = kafka_module
        self.max_retries = _integer(config, "max_retries", 3, 0, 100)
        self.max_message_bytes = _integer(config, "max_message_bytes", 1048576, 1, 16777216)
        self.retry_delay = _number(config, "retry_delay_seconds", 1, 0, 30)
        self.consumer: Any = None
        self._attempts: dict[tuple[str, int, int], int] = {}
        self._emitted: OrderedDict[tuple[str, int, int], None] = OrderedDict()
        self._paused: set[tuple[str, int]] = set()

    def bind(self, consumer: Any) -> None:
        self.consumer = consumer
        self._paused.clear()

    @staticmethod
    def _coordinate(message: Any) -> tuple[str, int, int]:
        return message.topic(), message.partition(), message.offset()

    def partitions_revoked(self, partitions: list[Any]) -> None:
        revoked = {(partition.topic, partition.partition) for partition in partitions}
        self._paused.difference_update(revoked)

    def _pause_poison(self, message: Any, reason: str) -> None:
        coordinate = self._coordinate(message)
        partition_key = coordinate[:2]
        if partition_key not in self._paused:
            self.consumer.pause([self.kafka.TopicPartition(*coordinate)])
            self._paused.add(partition_key)
            self.logger.error(
                "paused poison record at %s partition %s offset %s (%s)",
                coordinate[0], coordinate[1], coordinate[2], reason,
            )

    def _payload(self, message: Any, attempt: int) -> tuple[dict[str, Any], int]:
        key = message.key()
        value = message.value()
        headers, header_size = _headers(message.headers())
        key_size = len(key) if key is not None else 0
        value_size = len(value) if value is not None else 0
        size = key_size + value_size + header_size
        timestamp_type, timestamp_ms = message.timestamp()
        payload: dict[str, Any] = {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "timestamp_type": timestamp_type,
            "timestamp_ms": timestamp_ms,
            "delivery_attempt": attempt,
            "size_bytes": size,
            "headers": headers,
        }
        payload.update(_bytes_fields("key", key))
        payload.update(_bytes_fields("value", value))
        return payload, size

    def _commit(self, message: Any) -> None:
        try:
            result = self.consumer.commit(message=message, asynchronous=False)
        except Exception as exc:
            raise RuntimeError(f"synchronous offset commit failed ({type(exc).__name__})") from exc
        for partition in result or []:
            error = getattr(partition, "error", None)
            if error is not None:
                try:
                    code = error.code()
                except Exception:  # noqa: BLE001
                    code = "unknown"
                raise RuntimeError(f"synchronous offset commit returned error code {code}")

    def handle(self, message: Any) -> None:
        error = message.error()
        if error is not None:
            try:
                code = error.code()
            except Exception:  # noqa: BLE001
                code = "unknown"
            raise RuntimeError(f"Kafka consume returned error code {code}")
        coordinate = self._coordinate(message)
        if coordinate in self._emitted:
            self._commit(message)
            self._emitted.pop(coordinate, None)
            self._attempts.pop(coordinate, None)
            return
        attempt = self._attempts.get(coordinate, 0) + 1
        payload, size = self._payload(message, attempt)
        if size > self.max_message_bytes:
            self._attempts[coordinate] = self.max_retries + 1
            self._pause_poison(message, "record exceeds max_message_bytes")
            return
        if attempt > self.max_retries + 1:
            self._pause_poison(message, "emission retries exhausted")
            return
        try:
            event_id = self.emit(payload)
            if event_id is None:
                raise RuntimeError("Attune event emission returned no event ID")
        except Exception:  # noqa: BLE001
            self._attempts[coordinate] = attempt
            if attempt > self.max_retries:
                self._pause_poison(message, "emission retries exhausted")
                return
            if self.retry_delay:
                self.sleeper(self.retry_delay)
            # Seeking the exact record prevents a later offset in this partition from passing it.
            self.consumer.seek(self.kafka.TopicPartition(*coordinate))
            return
        try:
            self._commit(message)
        except Exception:
            # A reconnect can commit this exact offset without emitting a duplicate in this process.
            self._emitted[coordinate] = None
            self._emitted.move_to_end(coordinate)
            while len(self._emitted) > 10000:
                self._emitted.popitem(last=False)
            raise
        self._attempts.pop(coordinate, None)


class ConsumerWorker:
    def __init__(
        self,
        rule: Any,
        credentials: Mapping[str, Any],
        logger: Any,
        emit: Callable[[dict[str, Any]], Any],
        kafka_module: Any | None = None,
    ) -> None:
        self.rule = rule
        self.rule_id = int(getattr(rule, "rule_id", 0) or 0)
        self.config = dict(rule.trigger_params or {})
        self.credentials = dict(credentials)
        self.logger = logger
        topics = self.config.get("topics")
        if (
            not isinstance(topics, list)
            or not topics
            or len(topics) > 100
            or any(not isinstance(topic, str) or not topic for topic in topics)
            or len(set(topics)) != len(topics)
        ):
            raise ValueError("topics must contain 1 to 100 unique non-empty strings")
        self.topics = list(topics)
        group_id = self.config.get("group_id")
        if not isinstance(group_id, str) or not group_id:
            raise ValueError("group_id must be a non-empty string")
        self.group_id = group_id
        client_id = self.config.get("client_id", "attune-kafka-consumer")
        if not isinstance(client_id, str) or not client_id:
            raise ValueError("client_id must be a non-empty string")
        self.client_id = client_id
        self.poll_timeout = _number(self.config, "poll_timeout_seconds", 1, 0.1, 5)
        if kafka_module is None:
            import confluent_kafka as kafka_module
        self.kafka = kafka_module
        self._stop_event = threading.Event()
        self.processor = DeliveryProcessor(self.config, emit, self._stop_event.wait, logger, self.kafka)
        self._thread = threading.Thread(target=self._run, name=f"kafka-rule-{self.rule_id}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> bool:
        self._stop_event.set()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            self.logger.warning("rule %s Kafka consumer did not stop within 10 seconds", self.rule_id)
            return False
        return True

    def _consumer_config(self) -> dict[str, Any]:
        auto_reset = self.config.get("auto_offset_reset", "error")
        isolation = self.config.get("isolation_level", "read_committed")
        if auto_reset not in {"earliest", "latest", "error"}:
            raise ValueError("auto_offset_reset must be earliest, latest, or error")
        if isolation not in {"read_committed", "read_uncommitted"}:
            raise ValueError("isolation_level must be read_committed or read_uncommitted")
        max_message = self.processor.max_message_bytes
        result = client_config(self.credentials, self.client_id)
        result.update(
            {
                "group.id": self.group_id,
                "group.protocol": "classic",
                "partition.assignment.strategy": "cooperative-sticky",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": auto_reset,
                "allow.auto.create.topics": False,
                "isolation.level": isolation,
                "check.crcs": True,
                "max.poll.interval.ms": _integer(self.config, "max_poll_interval_ms", 300000, 30000, 900000),
                "session.timeout.ms": _integer(self.config, "session_timeout_ms", 45000, 6000, 300000),
                "fetch.message.max.bytes": max_message,
                "fetch.max.bytes": min(max(max_message * 2, 1048576), 33554432),
                "queued.max.messages.kbytes": 32768,
            }
        )
        return result

    def _consume_once(self) -> None:
        consumer = self.kafka.Consumer(self._consumer_config())
        self.processor.bind(consumer)

        def on_revoke(_consumer: Any, partitions: list[Any]) -> None:
            # Successful records were already committed individually; never commit here.
            self.processor.partitions_revoked(partitions)

        def on_lost(_consumer: Any, partitions: list[Any]) -> None:
            # Ownership is already lost, so committing here would be unsafe and usually fails.
            self.processor.partitions_revoked(partitions)

        try:
            consumer.subscribe(self.topics, on_revoke=on_revoke, on_lost=on_lost)
            while not self._stop_event.is_set():
                message = consumer.poll(self.poll_timeout)
                if message is not None:
                    self.processor.handle(message)
        finally:
            # Auto commit is disabled; close only leaves the group and releases assignments.
            try:
                consumer.close()
            except Exception:  # noqa: BLE001,S110
                pass

    def _run(self) -> None:
        failures = 0
        while not self._stop_event.is_set():
            try:
                self._consume_once()
                failures = 0
                if not self._stop_event.is_set():
                    raise RuntimeError("Kafka consumer stopped unexpectedly")
            except Exception as exc:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                failures += 1
                delay = min(60.0, float(2 ** min(failures - 1, 6)))
                self.logger.warning("rule %s Kafka consumer failed: %s", self.rule_id, type(exc).__name__)
                self._stop_event.wait(delay)


def _production_sensor() -> type:
    import attune

    class KafkaConsumerSensor(attune.Sensor):
        def __init__(self) -> None:
            super().__init__()
            self._workers: dict[int, ConsumerWorker] = {}
            self._lock = threading.Lock()

        @staticmethod
        def _rule_id(rule: Any) -> int:
            return int(getattr(rule, "rule_id", 0) or 0)

        def _stop(self, rule_id: int) -> bool:
            with self._lock:
                worker = self._workers.get(rule_id)
            if worker is None:
                return True
            stopped = worker.stop()
            if stopped:
                with self._lock:
                    if self._workers.get(rule_id) is worker:
                        self._workers.pop(rule_id, None)
            return stopped

        def _start(self, rule: Any) -> None:
            rule_id = self._rule_id(rule)
            if not self._stop(rule_id):
                raise RuntimeError("existing Kafka consumer is still stopping")
            config = dict(rule.trigger_params or {})
            credentials = read_credentials_file(config.get("credential_file"))

            def emit(payload: dict[str, Any]) -> Any:
                return self.emit(payload, rule=rule, target_rule=True)

            worker = ConsumerWorker(rule, credentials, self.logger, emit)
            with self._lock:
                self._workers[rule_id] = worker
            worker.start()

        def on_rule_created(self, rule: Any) -> None:
            self._start(rule)

        def on_rule_enabled(self, rule: Any) -> None:
            self._start(rule)

        def on_rule_updated(self, rule: Any, old_params: dict[str, Any]) -> None:
            self._start(rule)

        def on_rule_disabled(self, rule: Any) -> None:
            self._stop(self._rule_id(rule))

        def on_rule_deleted(self, rule: Any) -> None:
            self._stop(self._rule_id(rule))

        def run(self) -> None:
            while not self.is_shutting_down:
                time.sleep(1)

        def cleanup(self) -> None:
            with self._lock:
                rule_ids = list(self._workers)
            for rule_id in rule_ids:
                self._stop(rule_id)

    return KafkaConsumerSensor


def main() -> None:
    import attune

    attune.run_sensor(_production_sensor())


if __name__ == "__main__":
    main()
