# Payment Fraud Detection with Apache Kafka

A small but complete event-streaming project: card transactions flow through Kafka, a Kafka Streams
topology flags suspicious ones, and separate consumers audit them, raise alerts and handle failures.

**Stack:** Java 17, Spring Boot 3.3, Spring Kafka, Kafka Streams, Avro + Confluent Schema Registry,
Docker Compose (Kafka in KRaft mode), JUnit 5, TopologyTestDriver, Testcontainers.

## Architecture

```
                        ┌──────────────────────────────────────────────┐
  REST API / simulator  │                    Kafka                      │
  (TransactionProducer) │                                               │
          │             │   transactions (3 partitions, key = cardId)   │
          └────────────►│        │              │               │       │
   idempotent producer  │        │              │               │       │
   + transactional batch│        ▼              ▼               ▼       │
                        │  Kafka Streams    AuditConsumer   (DLT path)  │
                        │  "fraud-detector" group "audit"        │      │
                        │  exactly-once v2     idempotent write  ▼      │
                        │        │              │         transactions.DLT
                        │        ▼              ▼               │       │
                        │   fraud-alerts     H2 audit_log       ▼       │
                        │        │                      DeadLetterConsumer
                        │        ▼                                      │
                        │  AlertConsumer                                │
                        │  group "alert-notifier"                       │
                        └──────────────────────────────────────────────┘
```

Fraud rules (`FraudDetectionTopology`):

| Rule | Type | Logic |
|---|---|---|
| `HIGH_AMOUNT` | stateless | single transaction above 10,000 EUR |
| `VELOCITY` | stateful (windowed) | more than 3 transactions on one card within a 1-minute window |

## Quick start

Requirements: JDK 17, Maven 3.9+, Docker.

```bash
# 1. Start Kafka, Schema Registry and the web UI
docker compose up -d

# 2. Run the application (builds the Avro classes first)
mvn spring-boot:run
```

| URL | What |
|---|---|
| http://localhost:8080 | the application's REST API |
| http://localhost:8090 | Kafka UI: topics, messages, consumer groups, lag, schemas |
| http://localhost:8081/subjects | Schema Registry |

## Demo script

Run each command and watch the application log and Kafka UI.

```bash
# Normal traffic on different cards: no alerts
curl -X POST "localhost:8080/api/simulate/normal?count=10"

# 8 transactions on one card in seconds: VELOCITY alert (Kafka Streams window)
curl -X POST "localhost:8080/api/simulate/velocity"

# One 25,000 EUR transaction: HIGH_AMOUNT alert
curl -X POST "localhost:8080/api/simulate/high-amount"
curl "localhost:8080/api/alerts"

# Idempotent consumer: same transaction sent twice, stored once
curl -X POST "localhost:8080/api/simulate/duplicate"
curl "localhost:8080/api/audit/count"

# Dead letter topic: business error (negative amount) and corrupt payload (not Avro)
curl -X POST "localhost:8080/api/simulate/invalid"
curl -X POST "localhost:8080/api/simulate/poison"
curl "localhost:8080/api/dlt"

# Kafka transactions: all-or-nothing batch of 4 records
curl -X POST "localhost:8080/api/simulate/atomic-batch?fail=false"   # all 4 become visible
curl -X POST "localhost:8080/api/simulate/atomic-batch?fail=true"    # aborted: none become visible
curl "localhost:8080/api/audit/count"
```

Send your own transaction:

```bash
curl -X POST localhost:8080/api/transactions -H "Content-Type: application/json" \
  -d '{"cardId":"card-42","merchant":"IKEA","country":"SE","amountCents":12900}'
```

## Where each Kafka topic is demonstrated

| Topic | Where to look |
|---|---|
| Topics, partitions, keys | `KafkaTopicsConfig` (3 partitions), `TransactionProducer.send` (key = `cardId`) |
| Producer reliability | `KafkaProducerConfig`: `acks=all`, idempotence, compression, `linger.ms`; send callback |
| Consumer groups | three independent groups: `audit`, `alert-notifier`, `fraud-detector` (Streams) |
| Offsets, manual commit, at-least-once | `KafkaConsumerConfig` (`AckMode.MANUAL`, auto-commit off), `AuditConsumer` (`ack.acknowledge()` after processing) |
| Idempotent consumer | `AuditRepository.saveIfNew`: primary key on `transaction_id` |
| Retry + dead letter topic | `KafkaConsumerConfig`: `DefaultErrorHandler`, exponential backoff, `DeadLetterPublishingRecoverer`; `DeadLetterConsumer` |
| Poison messages | `ErrorHandlingDeserializer`; `/simulate/poison` |
| Rebalancing | `CooperativeStickyAssignor`, `LoggingRebalanceListener` (see experiment below) |
| Kafka Streams (stateless + windowed state) | `FraudDetectionTopology` |
| Exactly-once | Streams `exactly_once_v2` (`FraudStreamsConfig`); transactional producer (`transactionalKafkaTemplate`); consumers with `read_committed` |
| Schema Registry + Avro | `src/main/avro/*.avsc`, serializers/deserializers in the config classes |
| Testing | `FraudDetectionTopologyTest` (TopologyTestDriver), `FraudPipelineIT` (Testcontainers) |
| Local infrastructure | `docker-compose.yml` (KRaft broker, Schema Registry, Kafka UI) |

## Tests

```bash
mvn test      # fast unit tests of the fraud rules, no Docker needed
mvn verify    # also runs the end-to-end tests against a real broker (needs Docker)
```

## Experiments worth doing before an interview

**1. Rebalancing.** Start a second instance while the first runs:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8082
```

Watch the `REBALANCE: partitions revoked / assigned` log lines. The 3 partitions are shared between
the instances. Stop one instance and see its partitions move to the other. Only the moved
partitions pause because of the cooperative assignor.

**2. At-least-once and idempotence.** Put a breakpoint in `AuditConsumer` before `ack.acknowledge()`,
kill the app, restart: the record is delivered again, and the duplicate is ignored by the database.

**3. Schema evolution.** In `Transaction.avsc` add an optional field with a default:

```json
{"name": "channel", "type": "string", "default": "ONLINE"}
```

Rebuild and run: version 2 registers in the Schema Registry (compatible). Now add a field *without*
a default: registration is rejected, because the registry enforces BACKWARD compatibility.
The point: old consumers keep working while producers evolve.

**4. Replay.** In Kafka UI, reset the offsets of group `audit` to the beginning (stop the app first)
and restart: every event is reprocessed, and the idempotent write keeps the table correct.

**5. Consumer lag.** Stop the app, send a few hundred events with the simulator in the UI or a loop,
and look at the lag of each group in Kafka UI.

## Design decisions and honest limitations

- **Single broker, replication factor 1** so it runs on a laptop. Production would use 3 brokers,
  `replication.factor=3`, `min.insync.replicas=2`, `acks=all` (already set on the producer).
- **One application plays every role** (producer, Streams app, consumers) for simplicity. In production
  these would be separate deployable services.
- **H2 in memory** stands in for the audit database; a restart empties it.
- **Tumbling window** for the velocity rule is simple but has boundary effects (4 transactions split
  3 + 1 across two windows are missed). Sliding windows or a custom state store would be more precise.
- **No security**: production needs TLS, SASL authentication and ACLs.
- **No monitoring stack**: consumer lag is visible in Kafka UI; production would export metrics to Prometheus.

See [INTERVIEW_QA.md](INTERVIEW_QA.md) for questions and answers on every concept used here.
