package com.example.frauddetection;

import com.example.frauddetection.avro.FraudAlert;
import com.example.frauddetection.avro.Transaction;
import com.example.frauddetection.producer.TransactionFactory;
import com.example.frauddetection.streams.FraudDetectionTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fast unit tests of the Kafka Streams rules. No broker, no Docker: TopologyTestDriver runs the
 * topology in memory and "mock://" gives an in-memory Schema Registry.
 */
class FraudDetectionTopologyTest {

    private static final String REGISTRY = "mock://fraud-topology-test";
    private static final Instant BASE = Instant.parse("2026-01-01T10:00:10Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaction> input;
    private TestOutputTopic<String, FraudAlert> output;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new FraudDetectionTopology(REGISTRY).buildPipeline(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        driver = new TopologyTestDriver(builder.build(), props);

        SpecificAvroSerde<Transaction> txSerde = new SpecificAvroSerde<>();
        txSerde.configure(Map.of("schema.registry.url", REGISTRY), false);
        SpecificAvroSerde<FraudAlert> alertSerde = new SpecificAvroSerde<>();
        alertSerde.configure(Map.of("schema.registry.url", REGISTRY), false);

        input = driver.createInputTopic(Topics.TRANSACTIONS, new StringSerializer(), txSerde.serializer());
        output = driver.createOutputTopic(Topics.FRAUD_ALERTS, new StringDeserializer(), alertSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void normalTransactionsProduceNoAlerts() {
        input.pipeInput("card-1", tx("card-1", 2_500), BASE);
        input.pipeInput("card-2", tx("card-2", 9_900), BASE.plusSeconds(1));

        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    void highAmountTransactionRaisesAlert() {
        input.pipeInput("card-1", tx("card-1", 2_500_000), BASE);

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getRule()).isEqualTo("HIGH_AMOUNT");
        assertThat(alerts.get(0).getCardId()).isEqualTo("card-1");
    }

    @Test
    void fourthTransactionWithinOneMinuteRaisesVelocityAlert() {
        for (int i = 0; i < 3; i++) {
            input.pipeInput("card-1", tx("card-1", 1_000), BASE.plusSeconds(i));
        }
        assertThat(output.isEmpty()).as("3 transactions are still fine").isTrue();

        input.pipeInput("card-1", tx("card-1", 1_000), BASE.plusSeconds(3));

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getRule()).isEqualTo("VELOCITY");
        assertThat(alerts.get(0).getCardId()).isEqualTo("card-1");
    }

    @Test
    void furtherTransactionsInTheSameWindowDoNotRepeatTheAlert() {
        for (int i = 0; i < 7; i++) {
            input.pipeInput("card-1", tx("card-1", 1_000), BASE.plusSeconds(i));
        }

        assertThat(output.readValuesToList()).hasSize(1);
    }

    @Test
    void differentCardsAreCountedSeparately() {
        for (int i = 0; i < 3; i++) {
            input.pipeInput("card-A", tx("card-A", 1_000), BASE.plusSeconds(i));
            input.pipeInput("card-B", tx("card-B", 1_000), BASE.plusSeconds(i));
        }

        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    void transactionsInDifferentWindowsDoNotAccumulate() {
        for (int i = 0; i < 4; i++) {
            input.pipeInput("card-1", tx("card-1", 1_000), BASE.plus(Duration.ofMinutes(30L * i)));
        }

        assertThat(output.isEmpty()).isTrue();
    }

    private static Transaction tx(String cardId, long amountCents) {
        return TransactionFactory.random(cardId, amountCents);
    }
}
