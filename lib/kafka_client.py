"""Bounded confluent-kafka clients for the Kafka Attune pack.

Adapted from StackStorm Exchange's Apache-2.0 kafka pack version 2.0.0.
"""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

MAX_RECORD_BYTES = 16 * 1024 * 1024
MAX_HEADERS = 100
MAX_HEADER_BYTES = 1024 * 1024


class KafkaPackError(RuntimeError):
    """Operator-facing error that excludes credentials and remote text."""


class KafkaAmbiguousDeliveryError(KafkaPackError):
    """A publish may have reached Kafka but no definitive report was received."""


def fetch_key(ref: str) -> dict[str, Any]:
    if not isinstance(ref, str) or not ref:
        raise KafkaPackError("credential_key must be a non-empty string")
    try:
        import attune
        from attune.api_client.api.secrets import get_key
    except ImportError as exc:
        raise KafkaPackError("attune-sdk is required to resolve credential_key") from exc
    try:
        response = get_key.sync_detailed(ref, client=attune.context.client)
    except Exception as exc:
        raise KafkaPackError(f"unable to read credential Key {ref!r}") from exc
    status = int(response.status_code)
    if status == 404:
        raise KafkaPackError(f"credential Key {ref!r} was not found")
    if status >= 400 or not response.parsed:
        raise KafkaPackError(f"credential Key lookup failed with status {status}")
    value = response.parsed.data.value
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise KafkaPackError("credential Key must contain a JSON object") from exc
    if not isinstance(value, dict):
        raise KafkaPackError("credential Key must contain an object")
    return value


def kafka_config(credentials: Mapping[str, Any]) -> dict[str, Any]:
    value = credentials.get("kafka", credentials)
    if not isinstance(value, dict):
        raise KafkaPackError("credential kafka value must be an object")
    return dict(value)


def _integer(config: Mapping[str, Any], name: str, default: int, minimum: int, maximum: int) -> int:
    value = config.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        raise KafkaPackError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _number(config: Mapping[str, Any], name: str, default: float, minimum: float, maximum: float) -> float:
    value = config.get(name, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise KafkaPackError(f"{name} must be a number")
    result = float(value)
    if not math.isfinite(result) or result < minimum or result > maximum:
        raise KafkaPackError(f"{name} must be between {minimum:g} and {maximum:g}")
    return result


def _string(config: Mapping[str, Any], name: str, required: bool = False) -> str | None:
    value = config.get(name)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise KafkaPackError(f"{name} must be a non-empty string")
    return value


def _bootstrap_servers(config: Mapping[str, Any]) -> str:
    value = config.get("bootstrap_servers")
    if isinstance(value, str):
        servers = [part.strip() for part in value.split(",")]
    elif isinstance(value, list):
        servers = value
    else:
        raise KafkaPackError("bootstrap_servers must be a string or array")
    if not servers or any(not isinstance(item, str) or not item.strip() for item in servers):
        raise KafkaPackError("bootstrap_servers must contain non-empty broker addresses")
    return ",".join(item.strip() for item in servers)


def client_config(credentials: Mapping[str, Any], client_id: str | None = None) -> dict[str, Any]:
    """Map the documented credential schema to a restricted librdkafka config."""
    config = kafka_config(credentials)
    protocol = config.get("security_protocol", "SSL")
    protocols = {
        "PLAINTEXT": "plaintext",
        "SSL": "ssl",
        "SASL_PLAINTEXT": "sasl_plaintext",
        "SASL_SSL": "sasl_ssl",
    }
    if protocol not in protocols:
        raise KafkaPackError("security_protocol must be PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL")
    selected_client_id = client_id if client_id is not None else config.get("client_id", "attune-kafka")
    if not isinstance(selected_client_id, str) or not selected_client_id:
        raise KafkaPackError("client_id must be a non-empty string")
    result: dict[str, Any] = {
        "bootstrap.servers": _bootstrap_servers(config),
        "client.id": selected_client_id,
        "security.protocol": protocols[protocol],
        "socket.timeout.ms": _integer(config, "socket_timeout_ms", 30000, 1000, 300000),
        "socket.connection.setup.timeout.ms": _integer(
            config, "connection_setup_timeout_ms", 30000, 1000, 300000
        ),
    }

    tls = protocol in {"SSL", "SASL_SSL"}
    verify_tls = config.get("verify_tls", True)
    if not isinstance(verify_tls, bool):
        raise KafkaPackError("verify_tls must be a boolean")
    if tls:
        result["enable.ssl.certificate.verification"] = verify_tls
        result["ssl.endpoint.identification.algorithm"] = "https" if verify_tls else "none"
        path_fields = {
            "ca_file": "ssl.ca.location",
            "client_cert_file": "ssl.certificate.location",
            "client_key_file": "ssl.key.location",
            "client_key_password": "ssl.key.password",
        }
        for source, target in path_fields.items():
            value = config.get(source)
            if value is not None:
                if not isinstance(value, str) or not value:
                    raise KafkaPackError(f"{source} must be a non-empty string")
                result[target] = value
        cert, key = config.get("client_cert_file"), config.get("client_key_file")
        if (cert is None) != (key is None):
            raise KafkaPackError("client_cert_file and client_key_file must be provided together")

    sasl = protocol in {"SASL_PLAINTEXT", "SASL_SSL"}
    if sasl:
        mechanism = config.get("sasl_mechanism")
        allowed = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"}
        if mechanism not in allowed:
            raise KafkaPackError("sasl_mechanism must be PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, or OAUTHBEARER")
        result["sasl.mechanism"] = mechanism
        if mechanism in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}:
            result["sasl.username"] = _string(config, "sasl_username", required=True)
            result["sasl.password"] = _string(config, "sasl_password", required=True)
        else:
            endpoint = _string(config, "oauth_token_endpoint_url", required=True)
            parsed = urlsplit(endpoint)
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
                raise KafkaPackError("oauth_token_endpoint_url must be an HTTPS URL without credentials or a fragment")
            result.update(
                {
                    "sasl.oauthbearer.method": "oidc",
                    "sasl.oauthbearer.client.id": _string(config, "oauth_client_id", required=True),
                    "sasl.oauthbearer.client.secret": _string(config, "oauth_client_secret", required=True),
                    "sasl.oauthbearer.token.endpoint.url": endpoint,
                }
            )
            for source, target in (
                ("oauth_scope", "sasl.oauthbearer.scope"),
                ("oauth_extensions", "sasl.oauthbearer.extensions"),
                ("https_ca_file", "https.ca.location"),
            ):
                value = _string(config, source)
                if value is not None:
                    result[target] = value
    return result


def _decode(value: Any, encoding: Any, name: str) -> bytes:
    if not isinstance(value, str) or encoding not in {"utf-8", "base64"}:
        raise KafkaPackError(f"{name} must be a string with utf-8 or base64 encoding")
    try:
        return value.encode("utf-8") if encoding == "utf-8" else base64.b64decode(value, validate=True)
    except (ValueError, UnicodeError) as exc:
        raise KafkaPackError(f"{name} is not valid for its encoding") from exc


def _record(params: Mapping[str, Any]) -> tuple[bytes, bytes | None, list[tuple[str, bytes]]]:
    value = _decode(params.get("message"), params.get("message_encoding", "utf-8"), "message")
    key_value = params.get("key")
    key = None if key_value is None else _decode(key_value, params.get("key_encoding", "utf-8"), "key")
    raw_headers = params.get("headers", [])
    if not isinstance(raw_headers, list) or len(raw_headers) > MAX_HEADERS:
        raise KafkaPackError(f"headers must be an array with at most {MAX_HEADERS} entries")
    headers: list[tuple[str, bytes]] = []
    header_bytes = 0
    for item in raw_headers:
        if not isinstance(item, dict):
            raise KafkaPackError("each header must be an object")
        name = item.get("key")
        if not isinstance(name, str) or not name or "\x00" in name:
            raise KafkaPackError("header keys must be non-empty strings without NUL")
        encoded = _decode(item.get("value"), item.get("value_encoding", "utf-8"), "header value")
        header_bytes += len(name.encode("utf-8")) + len(encoded)
        headers.append((name, encoded))
    if header_bytes > MAX_HEADER_BYTES:
        raise KafkaPackError("headers exceed the 1 MiB action limit")
    total = len(value) + (len(key) if key is not None else 0) + header_bytes
    if total > MAX_RECORD_BYTES:
        raise KafkaPackError("record exceeds the 16 MiB action limit")
    return value, key, headers


def _error_details(error: Any) -> tuple[int | None, str, bool]:
    try:
        code = int(error.code())
    except Exception:  # noqa: BLE001
        code = None
    try:
        name = str(error.name())
    except Exception:  # noqa: BLE001
        name = type(error).__name__
    try:
        retriable = bool(error.retriable())
    except Exception:  # noqa: BLE001
        retriable = False
    ambiguous_names = {"_MSG_TIMED_OUT", "_TIMED_OUT", "_TRANSPORT", "_ALL_BROKERS_DOWN"}
    return code, name, retriable or name in ambiguous_names


def produce(params: Mapping[str, Any]) -> dict[str, Any]:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise KafkaPackError("confluent-kafka is not installed") from exc
    credentials = fetch_key(str(params.get("credential_key", "pack.kafka.credentials")))
    topic = params.get("topic")
    if not isinstance(topic, str) or not topic:
        raise KafkaPackError("topic must be a non-empty string")
    delivery_timeout = _integer(params, "delivery_timeout_ms", 30000, 1000, 300000)
    flush_timeout = _integer(params, "flush_timeout_ms", 35000, 1000, 305000)
    if flush_timeout < delivery_timeout:
        raise KafkaPackError("flush_timeout_ms must be greater than or equal to delivery_timeout_ms")
    value, key, headers = _record(params)
    config = client_config(credentials)
    config.update(
        {
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": delivery_timeout,
            "allow.auto.create.topics": False,
            "message.max.bytes": MAX_RECORD_BYTES + MAX_HEADER_BYTES + 65536,
            "queue.buffering.max.kbytes": 32768,
        }
    )
    compression = kafka_config(credentials).get("compression_type", "none")
    if compression not in {"none", "gzip", "snappy", "lz4", "zstd"}:
        raise KafkaPackError("compression_type must be none, gzip, snappy, lz4, or zstd")
    config["compression.type"] = compression
    reports: list[tuple[Any, Any]] = []
    producer = Producer(config)
    kwargs: dict[str, Any] = {"topic": topic, "value": value, "key": key, "headers": headers, "on_delivery": lambda error, message: reports.append((error, message))}
    partition = params.get("partition")
    timestamp = params.get("timestamp_ms")
    if partition is not None:
        kwargs["partition"] = _integer(params, "partition", 0, 0, 2147483647)
    if timestamp is not None:
        kwargs["timestamp"] = _integer(params, "timestamp_ms", 0, 0, 9223372036854775807)
    try:
        producer.produce(**kwargs)
    except BufferError as exc:
        raise KafkaPackError("producer queue rejected the record before delivery") from exc
    except Exception as exc:
        raise KafkaPackError(f"Kafka rejected the record before delivery ({type(exc).__name__})") from exc
    try:
        remaining = producer.flush(flush_timeout / 1000.0)
    except Exception as exc:
        raise KafkaAmbiguousDeliveryError(
            f"delivery outcome is unknown after producer flush failure ({type(exc).__name__}); retry may duplicate"
        ) from exc
    if remaining or len(reports) != 1:
        raise KafkaAmbiguousDeliveryError("delivery outcome is unknown after the bounded flush; retry may duplicate")
    error, message = reports[0]
    if error is not None:
        code, name, ambiguous = _error_details(error)
        detail = f"{name} (code {code})" if code is not None else name
        if ambiguous:
            raise KafkaAmbiguousDeliveryError(f"delivery outcome is unknown after {detail}; retry may duplicate")
        raise KafkaPackError(f"Kafka did not deliver the record: {detail}")
    timestamp_type, timestamp_ms = message.timestamp()
    return {
        "status": "delivered",
        "delivery_ambiguous": False,
        "idempotence_enabled": True,
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "timestamp_type": timestamp_type,
        "timestamp_ms": timestamp_ms,
    }


def _metadata_error(error: Any) -> dict[str, Any] | None:
    if error is None:
        return None
    code, name, _ambiguous = _error_details(error)
    return {"code": code, "name": name}


def inspect_metadata(params: Mapping[str, Any]) -> dict[str, Any]:
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError as exc:
        raise KafkaPackError("confluent-kafka is not installed") from exc
    credentials = fetch_key(str(params.get("credential_key", "pack.kafka.credentials")))
    topic_filter = params.get("topic")
    if topic_filter is not None and (not isinstance(topic_filter, str) or not topic_filter):
        raise KafkaPackError("topic must be a non-empty string")
    include_internal = params.get("include_internal", False)
    if not isinstance(include_internal, bool):
        raise KafkaPackError("include_internal must be a boolean")
    max_topics = _integer(params, "max_topics", 100, 1, 1000)
    max_partitions = _integer(params, "max_partitions", 1000, 1, 10000)
    timeout = _number(params, "timeout_seconds", 10, 1, 60)
    config = client_config(credentials)
    config["allow.auto.create.topics"] = False
    try:
        # Never pass a topic here: named metadata requests can auto-create it on permissive brokers.
        metadata = AdminClient(config).list_topics(timeout=timeout)
    except Exception as exc:
        raise KafkaPackError(f"Kafka metadata request failed ({type(exc).__name__})") from exc
    names = sorted(metadata.topics)
    if topic_filter is not None:
        names = [name for name in names if name == topic_filter]
    elif not include_internal:
        names = [name for name in names if not name.startswith("__")]
    topics: list[dict[str, Any]] = []
    partition_count = 0
    truncated = len(names) > max_topics
    for name in names[:max_topics]:
        topic = metadata.topics[name]
        partitions: list[dict[str, Any]] = []
        for partition_id in sorted(topic.partitions):
            if partition_count >= max_partitions:
                truncated = True
                break
            partition = topic.partitions[partition_id]
            partitions.append(
                {
                    "id": partition.id,
                    "leader": partition.leader,
                    "replicas": list(partition.replicas),
                    "isrs": list(partition.isrs),
                    "error": _metadata_error(partition.error),
                }
            )
            partition_count += 1
        topics.append(
            {
                "name": name,
                "error": _metadata_error(topic.error),
                "partition_count": len(topic.partitions),
                "partitions": partitions,
                "partitions_truncated": len(partitions) < len(topic.partitions),
            }
        )
        if partition_count >= max_partitions:
            break
    brokers = [
        {"id": broker.id, "host": broker.host, "port": broker.port}
        for _broker_id, broker in sorted(metadata.brokers.items())
    ]
    return {
        "cluster_id": metadata.cluster_id,
        "controller_id": metadata.controller_id,
        "brokers": brokers,
        "topics": topics,
        "topic_filter": topic_filter,
        "topic_found": topic_filter is None or bool(topics),
        "topic_count": len(topics),
        "partition_count": partition_count,
        "truncated": truncated,
    }


OPERATIONS = {"produce": produce, "inspect_metadata": inspect_metadata}


def execute_action(operation: str, params: Mapping[str, Any]) -> dict[str, Any]:
    function = OPERATIONS.get(operation)
    if function is None:
        raise KafkaPackError("unknown Kafka action")
    return function(params)
