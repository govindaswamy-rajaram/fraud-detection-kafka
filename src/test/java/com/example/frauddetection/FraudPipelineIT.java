package com.example.frauddetection;

import com.example.frauddetection.avro.Transaction;
import com.example.frauddetection.consumer.AuditRepository;
import com.example.frauddetection.consumer.DemoState;
import com.example.frauddetection.producer.TransactionFactory;
import com.example.frauddetection.producer.TransactionProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test against a REAL Kafka broker started by Testcontainers (needs Docker).
 * Schema Registry is replaced by Confluent's in-memory "mock://" implementation.
 *
 * Run with: mvn verify
 */
@SpringBootTest
@Testcontainers
class FraudPipelineIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("app.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.schema-registry-url", () -> "mock://integration-test");
    }

    @Autowired TransactionProducer producer;
    @Autowired AuditRepository audit;
    @Autowired DemoState state;

    @Test
    void burstOnOneCardIsAuditedAndRaisesVelocityAlert() throws Exception {
        String card = "card-it-" + shortId();
        long before = audit.count();

        for (int i = 0; i < 8; i++) {
            producer.send(TransactionFactory.random(card, 1_000)).get(10, TimeUnit.SECONDS);
        }

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(audit.count()).isEqualTo(before + 8);
            assertThat(state.alerts()).anyMatch(a -> a.contains(card) && a.contains("VELOCITY"));
        });
    }

    @Test
    void highAmountTransactionRaisesAlert() throws Exception {
        String card = "card-it-" + shortId();

        producer.send(TransactionFactory.random(card, 2_500_000)).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(state.alerts()).anyMatch(a -> a.contains(card) && a.contains("HIGH_AMOUNT")));
    }

    @Test
    void duplicateTransactionIsStoredOnlyOnce() throws Exception {
        String card = "card-it-" + shortId();
        long before = audit.count();
        Transaction duplicated = TransactionFactory.random(card, 4_200);

        producer.send(duplicated).get(10, TimeUnit.SECONDS);
        producer.send(duplicated).get(10, TimeUnit.SECONDS);
        // Same key => same partition => processed in order, so once this sentinel is stored
        // both duplicates have already been handled.
        producer.send(TransactionFactory.random(card, 100)).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(audit.count()).isEqualTo(before + 2));
    }

    @Test
    void invalidTransactionGoesToDeadLetterTopic() throws Exception {
        String card = "card-it-" + shortId();

        producer.send(TransactionFactory.random(card, -500)).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(state.deadLetters()).anyMatch(d -> d.contains("key=" + card)));
    }

    @Test
    void poisonMessageGoesToDeadLetterTopic() {
        String key = producer.sendPoison();

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(state.deadLetters()).anyMatch(d -> d.contains("key=" + key)));
    }

    @Test
    void committedTransactionalBatchIsVisible() {
        long before = audit.count();

        producer.sendAtomically(batch(4), false);

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(audit.count()).isEqualTo(before + 4));
    }

    @Test
    void abortedTransactionalBatchIsInvisibleToConsumers() throws Exception {
        long before = audit.count();

        assertThatThrownBy(() -> producer.sendAtomically(batch(4), true))
                .isInstanceOf(IllegalStateException.class);

        // A normal record afterwards proves the pipeline is alive...
        producer.send(TransactionFactory.random("card-it-" + shortId(), 100)).get(10, TimeUnit.SECONDS);

        // ...and the count must settle at +1 and stay there: the aborted records never show up.
        await().atMost(Duration.ofSeconds(60))
                .during(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(audit.count()).isEqualTo(before + 1));
    }

    private static List<Transaction> batch(int size) {
        String run = shortId();
        List<Transaction> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(TransactionFactory.random("card-it-batch-" + run + "-" + i, 3_000));
        }
        return list;
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
