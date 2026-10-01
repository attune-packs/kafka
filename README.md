# Apache Kafka Attune Pack

Production-oriented Kafka actions and managed message events for Attune. This
is an Apache-2.0 adaptation of
[`StackStorm-Exchange/stackstorm-kafka`](https://github.com/StackStorm-Exchange/stackstorm-kafka)
version 2.0.0 (`42ec262777308d655262e5b64029c655825eb98a`).

## Scope And Requirements

- Python 3.10 or newer and `confluent-kafka` 2.15.x are required.
- Tests are deterministic and make no broker, identity-provider, or network calls.
- Actions decrypt a pack-owned Attune Key. Sensors use a protected JSON file
  below `/run/secrets` because managed sensors cannot decrypt Attune Keys.
- The implementation was reviewed against current Apache Kafka 4.3,
  librdkafka, and confluent-kafka 2.15.0 documentation on 2026-08-14. Test each
  target broker, security backend, and failover topology before production use.
- The source pack's narrow GCP/base64 sensor is intentionally not retained.
  Records are emitted generically as base64 with optional UTF-8 views.

## Credentials And Transport

Create a pack-owned encrypted Attune Key named `pack.kafka.credentials`. The sensor
file uses the same object shape. A SASL/SCRAM example is:

```json
{
  "kafka": {
    "bootstrap_servers": ["kafka-1.example.net:9093", "kafka-2.example.net:9093"],
    "client_id": "attune-kafka",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "SCRAM-SHA-512",
    "sasl_username": "attune",
    "sasl_password": "REDACTED",
    "ca_file": "/run/secrets/kafka/ca.pem",
    "client_cert_file": "/run/secrets/kafka/client.pem",
    "client_key_file": "/run/secrets/kafka/client-key.pem",
    "client_key_password": "REDACTED",
    "verify_tls": true,
    "socket_timeout_ms": 30000,
    "connection_setup_timeout_ms": 30000,
    "compression_type": "zstd"
  }
}
```

`security_protocol` supports `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and
`SASL_SSL`. SASL mechanisms are `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, and
`OAUTHBEARER`. PLAIN and SCRAM use `sasl_username` and `sasl_password`.
Credentials sent with `SASL_PLAINTEXT` are not encrypted and should not be used
across untrusted networks.

OAUTHBEARER uses librdkafka's maintained OIDC client-credentials flow, not an
unsecured JWT handler or a static token callback:

```json
{
  "kafka": {
    "bootstrap_servers": "kafka.example.net:9093",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "OAUTHBEARER",
    "oauth_client_id": "attune-kafka",
    "oauth_client_secret": "REDACTED",
    "oauth_token_endpoint_url": "https://identity.example.net/oauth/token",
    "oauth_scope": "kafka.read kafka.write",
    "oauth_extensions": "logicalCluster=lkc-123",
    "https_ca_file": "/run/secrets/kafka/idp-ca.pem",
    "ca_file": "/run/secrets/kafka/broker-ca.pem"
  }
}
```

The token endpoint must be HTTPS. Broker certificate and hostname verification
are enabled by default; `verify_tls: false` is available only for controlled
diagnostics and is unsafe in production. Client certificate and key paths must
be supplied together. Numeric connection settings are bounded. Client debug
configuration and arbitrary librdkafka pass-through options are deliberately
not accepted, preventing accidental secret logging or disabling delivery
guarantees.

## Actions

```bash
attune action execute kafka.produce \
  --params-json '{"topic":"events","message":"{\"status\":\"ok\"}","key":"deployment-42","headers":[{"key":"content-type","value":"application/json"}],"delivery_timeout_ms":30000,"flush_timeout_ms":35000}' --watch

attune action execute kafka.inspect_metadata \
  --params-json '{"topic":"events","max_partitions":1000,"timeout_seconds":10}' --watch
```

`produce` accepts UTF-8 or strict base64 message and key data, ordered duplicate
headers with independently encoded values, an explicit partition, and a Unix
millisecond timestamp. Records are limited to 16 MiB, headers to 100 entries and
1 MiB, and the local producer queue to 32 MiB. Broker and topic limits can be
lower.

Every producer enables idempotence, `acks=all`, effectively unlimited client
retries selected by librdkafka, FIFO ordering, and at most five in-flight
requests as enforced by librdkafka's idempotent configuration. Idempotence
prevents duplicates caused by retries in one producer session; it does not
deduplicate a new action execution. The action waits for exactly one delivery
report and uses a bounded flush. `flush_timeout_ms` must be at least
`delivery_timeout_ms`.

A successful result contains `status: delivered`, topic, partition, offset,
timestamp, and `delivery_ambiguous: false`. If the flush expires, the producer
fails while flushing, or a timeout/transport delivery report cannot prove the
result, the action fails with `delivery outcome is unknown; retry may duplicate`.
Do not blindly retry that result. Use a stable application key or event ID and
an idempotent consumer. A definitive negative delivery report also fails the
action but is identified as not delivered.

`inspect_metadata` is read-only. It always requests all cluster metadata and
applies an optional exact topic filter client-side because requesting metadata
for a named unknown topic can create it on permissive brokers. Client-side
auto-creation is disabled. Topic and partition output is deterministic and
bounded; `truncated` and `partitions_truncated` report incomplete output.
Internal topics are excluded by default. Cluster metadata ACLs still determine
what the principal can see.

## Sensor Setup

Mount a separate JSON credentials file under `/run/secrets` on every sensor
worker. It must be a regular UTF-8 JSON file no larger than 64 KiB and readable
only by the sensor service account. Secret and certificate rotation takes
effect after rule update, disable/enable, or sensor restart. Do not put secrets
in trigger parameters because those are ordinary rule metadata.

Create a rule for `kafka.message` with parameters such as:

```json
{
  "credential_file": "/run/secrets/kafka/consumer.json",
  "topics": ["events"],
  "group_id": "attune.production.events.v1",
  "client_id": "attune-kafka-consumer",
  "auto_offset_reset": "error",
  "isolation_level": "read_committed",
  "max_retries": 3,
  "retry_delay_seconds": 1,
  "max_message_bytes": 1048576,
  "poll_timeout_seconds": 1,
  "max_poll_interval_ms": 300000,
  "session_timeout_ms": 45000
}
```

Use a stable, purpose-specific group ID. Rules sharing a group ID and topics are
competing consumers, not fan-out subscribers. Give independent automations
different group IDs. `auto_offset_reset: error` is the safe default: an absent
or expired committed offset stops progress instead of silently skipping to the
end or replaying from the beginning. Select `earliest` or `latest` only as an
explicit bootstrap policy. `read_committed` hides aborted transactional records.

The client uses the classic group protocol with cooperative-sticky assignment
for compatibility and reduced partition movement. `max_poll_interval_ms` must
exceed the worst-case Attune emission latency plus retry delay. Broker bounds
can reject `session_timeout_ms`. Consumer auto topic creation is disabled.

## Consumption And Failure Semantics

The sensor is at-least-once, not exactly-once:

1. Auto commit and auto offset store are disabled. One polled record is emitted
   only to its matching rule. Its exact partition offset is synchronously
   committed only after Attune returns a non-null event ID. Commits never occur
   in a `finally`, cancellation, revoke, lost-partition, or shutdown path.
2. If emission fails, the consumer seeks the exact topic/partition/offset after
   a bounded delay. Later offsets from that partition cannot be committed past
   it. Other assigned partitions can continue between retries.
3. `max_retries` counts retries after the first attempt. When retries are
   exhausted, the partition is paused and the poison offset remains
   uncommitted. Oversized records are paused immediately without exposing their
   content. This deliberately blocks only that partition rather than losing a
   record. Fix the downstream failure or size policy, then update/restart the
   rule. A rebalance can move the uncommitted poison record and cause it to be
   attempted by another group member; Kafka has no portable per-record dead
   letter or retry counter in the classic consumer protocol.
4. If emission succeeds but the synchronous commit fails, the event may be
   duplicated. The worker remembers up to 10,000 such exact offsets and, after
   its own reconnect, retries the commit without re-emitting. A process crash,
   another group member, retention reset, or lost in-memory state can still
   duplicate. Downstream actions must be idempotent using topic, partition, and
   offset or an application event ID.
5. A commit is for one message only, so no later buffered offset is committed
   prematurely. During rebalance, successful messages have already been
   committed; pending, failed, and poison records are not. A commit racing lost
   ownership fails and the record is redelivered, favoring duplicates over
   loss.
6. Cancellation sets a stop event observed by the worker's bounded poll. The
   consumer is closed on its owning thread, leaves the group, and releases
   assignments. The main sensor thread never calls the non-thread-safe consumer
   concurrently. If event emission itself hangs longer than the ten-second
   sensor stop wait, replacement is refused rather than starting two workers.
7. Kafka preserves order only within a partition. Retries retain that
   partition's order, but multiple partitions, consumers, group rebalances,
   failover, and downstream Attune concurrency do not provide global order.
   Explicit producer partitioning by key and one active consumer for that
   partition provide the strongest local ordering.

The event includes topic, partition, offset, Kafka timestamp, delivery attempt,
ordered headers, total byte size, and key/value null markers. Non-null binary
values always have strict base64 fields; valid UTF-8 also has a text field.
Tombstones remain distinguishable from empty byte strings. No JSON, pickle,
Avro, Protobuf, schema-registry, or GCP-specific deserialization occurs. Message
content and headers may contain secrets and become Attune event data, so apply
appropriate event, rule, log, and downstream retention controls.

## Source Fidelity

| Source resource | Attune target | Fidelity and differences |
|---|---|---|
| `produce` | `kafka.produce` | Single-record publishing retained; moved from unmaintained pinned `kafka-python` behavior to confluent-kafka with keys, binary-safe values, ordered headers, partition/timestamp, TLS/SASL/OIDC, idempotence, bounded delivery/flush, limits, and explicit ambiguity. |
| No source action | `kafka.inspect_metadata` | Added bounded read-only cluster/topic/partition inspection with client-side filtering to prevent named-topic auto-creation. |
| Generic auto-commit sensor and embedded trigger | Managed `kafka.kafka_consumer` sensor and `kafka.message` trigger | Rebuilt per active rule with stable groups, manual post-emission synchronous commits, exact-offset retries, poison partition pause, size limits, cooperative rebalances, cancellation, and binary-safe payloads. |
| Narrow GCP decoding sensor/trigger | None | Removed; cloud-provider payload interpretation belongs downstream. |
| Pack config | Encrypted action Key and protected sensor file | Secrets no longer live in ordinary pack configuration or trigger metadata. |
| Rules, workflows, aliases, schedules, policies | None | No portable source resources required translation. |

## Current References

- [confluent-kafka Python API 2.15.0](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [librdkafka configuration reference](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
- [Kafka producer configuration](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)
- [Kafka consumer configuration](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
- [Apache Kafka design and delivery semantics](https://kafka.apache.org/43/design/design/)
- [Apache Kafka security documentation](https://kafka.apache.org/43/security/)

The new KIP-932 share consumer in confluent-kafka 2.15.0 is preview software and
requires newer broker capabilities, so this production pack intentionally uses
the stable classic consumer. KIP-848's consumer group protocol is also not
enabled by default here; migrate only after broker-specific rebalance testing.

## Validation

```bash
python -m unittest discover -s tests -v
/home/david/.cargo/bin/attune --output json pack check /home/david/Codebase/attune-packs/kafka
/home/david/.cargo/bin/attune pack test /home/david/Codebase/attune-packs/kafka --detailed
```

These checks do not prove broker reachability, ACLs, topic existence, broker
message limits, advertised listener routing, CA chains and hostname matching,
mTLS identity mapping, SCRAM credentials, OIDC scopes and refresh, compression
support, metadata visibility, delivery ambiguity under failover, commit behavior
during real rebalances, poison recovery, retention offset expiration, ordering,
or recovery across broker and sensor process crashes. Exercise those cases on a
non-production cluster before release.

## License

The upstream and this adaptation are licensed under Apache License 2.0. See
[LICENSE](LICENSE), [NOTICE](NOTICE), and [SOURCE.md](SOURCE.md).
