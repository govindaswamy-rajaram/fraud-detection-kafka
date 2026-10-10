package com.example.frauddetection.streams;

import com.example.frauddetection.Topics;
import com.example.frauddetection.avro.FraudAlert;
import com.example.frauddetection.avro.Transaction;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * Stateless + stateful stream processing:
 *
 *   transactions ──┬─ filter amount > 10,000 EUR ─────────────────────┐
 *                  │                                                    ├─► fraud-alerts
 *                  └─ count per card in 1-minute window, alert on 4th ─┘
 *
 * The windowed count is kept in a local state store (RocksDB) that is backed up to a Kafka
 * changelog topic, so state survives restarts and moves with partitions during rebalances.
 */
@Component
public class FraudDetectionTopology {

    static final long HIGH_AMOUNT_CENTS = 1_000_000L;  // 10,000.00 EUR
    static final long VELOCITY_THRESHOLD = 3;          // alert when the 4th transaction arrives in a minute

    private final Serde<Transaction> transactionSerde = new SpecificAvroSerde<>();
    private final Serde<FraudAlert> alertSerde = new SpecificAvroSerde<>();

    public FraudDetectionTopology(@Value("${app.kafka.schema-registry-url}") String schemaRegistryUrl) {
        Map<String, String> serdeConfig = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        transactionSerde.configure(serdeConfig, false);
        alertSerde.configure(serdeConfig, false);
    }

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {

        KStream<String, Transaction> transactions = builder
                .stream(Topics.TRANSACTIONS, Consumed.with(Serdes.String(), transactionSerde))
                .filter((cardId, tx) -> tx != null);

        // Rule 1: stateless, looks at one event at a time.
        KStream<String, FraudAlert> highAmountAlerts = transactions
                .filter((cardId, tx) -> tx.getAmountCents() > HIGH_AMOUNT_CENTS)
                .mapValues(tx -> alert(
                        tx.getCardId(),
                        "HIGH_AMOUNT",
                        String.format("Transaction of %.2f %s exceeds the limit", tx.getAmountCents() / 100.0, tx.getCurrency()),
                        tx.getTransactionId()));

        // Rule 2: stateful, counts events per card (the record key) in 1-minute tumbling windows.
        // Grace period: events up to 10s late are still counted.
        KStream<String, FraudAlert> velocityAlerts = transactions
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("tx-count-per-card-1m")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                // Fire exactly once per window: when the count first exceeds the threshold.
                .filter((window, count) -> count != null && count == VELOCITY_THRESHOLD + 1)
                .map((window, count) -> KeyValue.pair(
                        window.key(),
                        alert(window.key(), "VELOCITY",
                                count + " transactions within 1 minute", null)));

        highAmountAlerts
                .merge(velocityAlerts)
                .to(Topics.FRAUD_ALERTS, Produced.with(Serdes.String(), alertSerde));
    }

    private static FraudAlert alert(String cardId, String rule, String details, String triggeringTransactionId) {
        return FraudAlert.newBuilder()
                .setAlertId(UUID.randomUUID().toString())
                .setCardId(cardId)
                .setRule(rule)
                .setDetails(details)
                .setTriggeringTransactionId(triggeringTransactionId)
                .setDetectedAt(System.currentTimeMillis())
                .build();
    }
}
