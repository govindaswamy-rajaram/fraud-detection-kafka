package com.example.frauddetection.producer;

import com.example.frauddetection.Topics;
import com.example.frauddetection.avro.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class TransactionProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final KafkaTemplate<String, Transaction> transactionalKafkaTemplate;
    private final KafkaTemplate<String, byte[]> bytesKafkaTemplate;

    public TransactionProducer(
            @Qualifier("kafkaTemplate") KafkaTemplate<String, Transaction> kafkaTemplate,
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Transaction> transactionalKafkaTemplate,
            @Qualifier("bytesKafkaTemplate") KafkaTemplate<String, byte[]> bytesKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
        this.bytesKafkaTemplate = bytesKafkaTemplate;
    }

    /**
     * Asynchronous send. The KEY is the cardId, so every event of one card lands in the same
     * partition and is consumed in order. The callback reports success or failure.
     */
    public CompletableFuture<SendResult<String, Transaction>> send(Transaction tx) {
        return kafkaTemplate.send(Topics.TRANSACTIONS, tx.getCardId(), tx)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("SEND FAILED tx={}", tx.getTransactionId(), ex);
                    } else {
                        log.info("SENT tx={} card={} -> partition={} offset={}",
                                tx.getTransactionId(), tx.getCardId(),
                                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                });
    }

    /**
     * Sends a batch in ONE Kafka transaction: consumers using read_committed see all of it or none.
     * If failMidway is true the transaction is aborted after some records were already written.
     */
    public void sendAtomically(List<Transaction> batch, boolean failMidway) {
        transactionalKafkaTemplate.executeInTransaction(ops -> {
            for (int i = 0; i < batch.size(); i++) {
                Transaction tx = batch.get(i);
                ops.send(Topics.TRANSACTIONS, tx.getCardId(), tx);
                if (failMidway && i == batch.size() / 2) {
                    throw new IllegalStateException("Simulated failure: Kafka transaction is aborted");
                }
            }
            return null;
        });
        log.info("ATOMIC BATCH committed ({} records)", batch.size());
    }

    /** Writes bytes that are not valid Avro, to demonstrate poison message handling. Returns the key used. */
    public String sendPoison() {
        String key = "card-poison-" + UUID.randomUUID().toString().substring(0, 8);
        bytesKafkaTemplate.send(Topics.TRANSACTIONS, key, "this-is-not-avro".getBytes(StandardCharsets.UTF_8));
        log.info("POISON message sent with key={}", key);
        return key;
    }
}
