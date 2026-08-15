# Source Metadata

- Reference project: `StackStorm-Exchange/stackstorm-kafka`
- Source URL: https://github.com/StackStorm-Exchange/stackstorm-kafka
- Source release: `v2.0.0`
- Source pack version: `2.0.0`
- Source revision: `42ec262777308d655262e5b64029c655825eb98a`
- Source revision date: 2024-01-04
- Source license: Apache License 2.0
- Translation date: 2026-08-14

The release tag, pack version, commit, commit date, and license were verified
from the upstream Git repository. The upstream `LICENSE` is the unmodified
Apache License 2.0 reproduced here. Upstream pins `kafka-python==2.0.2` and
contains one producer action, a generic consumer sensor/trigger, a narrow GCP
decoding sensor/trigger, and one example rule. It contains no workflows,
aliases, schedules, or policies.

Current behavior was checked against Apache Kafka 4.3 documentation and the
confluent-kafka/librdkafka 2.15.0 API and configuration references for protocol
security, OIDC, idempotent production, delivery reports, metadata, manual
offset management, retries, polling, rebalances, and consumer cancellation.
See the README for references and material behavior differences.
