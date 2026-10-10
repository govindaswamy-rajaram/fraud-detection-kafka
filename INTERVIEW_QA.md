# Kafka interview Q&A, tied to this project

Short, accurate answers you can give while pointing at the code.

## Fundamentals

**What is Kafka and when would you use it instead of RabbitMQ?**
A distributed, durable, append-only log. Events are retained after being read, many consumer groups
read independently, and history can be replayed. Use Kafka for high throughput, event streams,
stream processing and many independent consumers. RabbitMQ fits task queues with flexible routing,
priorities and per-message acknowledgment.

**What are topics, partitions and offsets?**
A topic is a named stream. It is split into partitions, each an ordered log. An offset is a record's
position within a partition. Ordering is guaranteed only within a partition.

**How do you guarantee ordering?**
Use a key. Records with the same key go to the same partition. Here the key is `cardId`, so one
card's transactions are always processed in order while different cards run in parallel.

**How do you choose the number of partitions?**
It caps consumer parallelism per group. Estimate from target throughput divided by per-partition
throughput, and leave headroom. Partitions can be increased but not decreased, and increasing them
changes key-to-partition mapping (breaks ordering for existing keys). Here: 3.

**What is the difference between a consumer group and a consumer?**
A group shares the work: each partition is read by one member of the group. Different groups each
get all records. Here `audit`, `alert-notifier` and `fraud-detector` each see every event.

## Reliability

**How do you avoid losing messages on the producer side?**
`acks=all` plus `min.insync.replicas=2` with replication factor 3, so an acknowledged write exists on
at least two brokers. Handle send failures in the callback. See `KafkaProducerConfig`.

**What does `enable.idempotence=true` do?**
The broker de-duplicates producer retries using a producer id and per-partition sequence numbers, and
keeps order within a partition. It protects against duplicates from retries, not against the
application sending the same business event twice.

**What are ISR and leader election?**
Each partition has a leader and followers. The in-sync replicas (ISR) are followers that are caught up.
If the leader fails, a new leader is chosen from the ISR. Electing an out-of-sync replica
(`unclean.leader.election.enable`) can lose data, so it is off by default.

**What delivery guarantees does Kafka offer?**
At-most-once (commit before processing), at-least-once (commit after processing, may duplicate),
and exactly-once within Kafka (idempotent producer, transactions, `read_committed`, Streams EOS).
This project uses at-least-once on consumers with idempotent processing.

**How do you get "exactly-once" when writing to a database?**
Kafka cannot make an external write atomic with an offset commit. Use at-least-once plus an idempotent
write (unique key), or store the offset in the same database transaction. `AuditRepository.saveIfNew`
does the first.

**How do Kafka transactions work?**
A transactional producer (`transactional.id`) writes to several partitions and commits or aborts
atomically. Consumers with `isolation.level=read_committed` only see committed records. The
`transactional.id` fences zombie producers. See `sendAtomically` and the aborted-batch integration test.

**Why do consumers commit manually?**
Auto-commit can commit offsets for records not yet processed (loss on crash). Committing after
processing gives at-least-once. See `AuditConsumer`.

## Failure handling

**What is a poison message and how do you handle it?**
A record that always fails (corrupt bytes, invalid data), which would block a partition forever.
Wrap deserializers in `ErrorHandlingDeserializer`, retry transient errors with backoff, then publish
to a dead letter topic with the error in headers, and commit the offset so processing continues.
See `KafkaConsumerConfig`.

**Which errors should be retried?**
Transient ones (timeouts, database briefly down). Permanent ones (validation, bad data) should go
straight to the DLT. `InvalidTransactionException` is registered as not retryable.

**What do you do with the dead letter topic?**
Alert on arrivals, investigate, fix the bug or data, then replay to the original topic. Give it
retention and the same access control as the source topic, since it contains the same data.

**What happens when a consumer dies?**
It stops heartbeating (`session.timeout.ms`) or exceeds `max.poll.interval.ms`; the group
coordinator rebalances and reassigns its partitions. Uncommitted records are re-delivered, which is
why handlers must be idempotent.

**How do you reduce the impact of rebalances?**
Cooperative sticky assignor (only moved partitions pause), static membership (`group.instance.id`)
for quick restarts, short processing per poll, and the newer consumer group protocol (KIP-848) on
supporting versions.

## Kafka Streams

**Why Kafka Streams here?**
The velocity rule needs state (a count per card per time window). Streams provides windowing, a
fault-tolerant local state store backed by a changelog topic, and scales by running more instances
(tasks = partitions).

**Explain the windowed count in this project.**
`groupByKey` (key = card) then `windowedBy` a 1-minute tumbling window with a 10-second grace period
for late events, then `count`. An alert is emitted when the count reaches 4. Record caching is
disabled so every update is emitted and the "== 4" check cannot be skipped.

**KStream vs KTable?**
A KStream is an unbounded sequence of events; a KTable is the latest value per key (a changelog
interpreted as a table). Counts here are materialized as a windowed store.

**How does Streams achieve exactly-once?**
`processing.guarantee=exactly_once_v2` wraps consume, state update, produce and offset commit in one
Kafka transaction. It holds within Kafka; external side effects still need idempotence.

**Tumbling, hopping, sliding windows?**
Tumbling: fixed, non-overlapping. Hopping: fixed size, overlapping. Sliding: based on time
difference between records; most accurate for "N events within T" rules but costlier. The demo uses
tumbling for simplicity and documents the boundary weakness.

## Data and schemas

**Why Avro and a Schema Registry instead of JSON?**
Compact binary format, a contract between teams, and enforced compatibility. Messages carry only a
schema id; the registry stores schemas per subject (`transactions-value`).

**What are compatibility modes?**
BACKWARD (default): new schema can read old data, so consumers upgrade first. FORWARD: old schema can
read new data. FULL: both. Adding a field with a default is compatible; adding a required field
without a default is not.

**Retention vs log compaction?**
Retention deletes whole segments by age or size (`retention.ms`, `retention.bytes`). Compaction keeps
the latest record per key (good for current-state topics, tombstones delete keys). Transactions here
use retention (7 days); the DLT keeps 14 days.

## Operations

**What is consumer lag and why monitor it?**
Lag = latest offset minus the group's committed offset. Growing lag means consumers cannot keep up.
Check it in Kafka UI here; production exports metrics (Prometheus, Burrow, cloud tools).

**KRaft vs ZooKeeper?**
KRaft moves cluster metadata into Kafka itself (Raft-based controllers) and removes ZooKeeper. New
Kafka versions are KRaft-only. The Compose file runs one combined broker/controller node.

**How do you scale a consumer group?**
Add instances up to the partition count. Beyond that, extra consumers are idle. Watch for hot
partitions caused by skewed keys.

**How does Kafka handle back-pressure?**
Consumers pull at their own pace; the log buffers the difference up to the retention limit.

**Basic security?**
TLS for encryption in transit, SASL (SCRAM, OAuth, Kerberos) for authentication, ACLs for
authorization, and encryption at rest at the disk or cloud layer.

**What is Kafka Connect, CDC and the outbox pattern?**
Connect moves data between Kafka and other systems with configuration instead of code. CDC (for
example Debezium) streams database changes into Kafka. The outbox pattern writes the event to an
outbox table in the same database transaction as the business change and relays it to Kafka, avoiding
the dual-write problem.

## Testing

**How do you test Kafka applications?**
Unit-test Streams topologies with `TopologyTestDriver` (fast, no broker). Test end to end against a
real broker with Testcontainers. See `FraudDetectionTopologyTest` and `FraudPipelineIT`.

## "What would you change for production?"

Three or more brokers with RF=3 and `min.insync.replicas=2`; separate services; a real database with
the unique constraint; Avro schema compatibility checks in CI; TLS/SASL/ACLs; metrics and alerting
on lag, under-replicated partitions and DLT growth; a replay tool for the DLT; sliding windows or a
more precise rule engine; capacity planning for partitions.
