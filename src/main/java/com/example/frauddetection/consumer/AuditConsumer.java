package com.example.frauddetection.consumer;

import com.example.frauddetection.Topics;
import com.example.frauddetection.avro.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumer group "audit": stores every transaction. Independent of the fraud detector: it has its
 * own offsets and reads every record at its own pace.
 */
@Component
public class AuditConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditConsumer.class);

    private final AuditRepository repository;

    public AuditConsumer(AuditRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(id = "audit-listener", topics = Topics.TRANSACTIONS, groupId = "audit")
    public void onTransaction(ConsumerRecord<String, Transaction> record, Acknowledgment ack) {
        Transaction tx = record.value();

        // 1. Validate. A permanent failure goes to the DLT without retries (see KafkaConsumerConfig).
        if (tx.getAmountCents() <= 0) {
            throw new InvalidTransactionException(
                    "Amount must be positive but was " + tx.getAmountCents() + " (tx " + tx.getTransactionId() + ")");
        }

        // 2. Process idempotently.
        boolean isNew = repository.saveIfNew(tx);
        if (isNew) {
            log.info("AUDIT stored    tx={} card={} amount={} (partition={} offset={})",
                    tx.getTransactionId(), tx.getCardId(), tx.getAmountCents(), record.partition(), record.offset());
        } else {
            log.info("AUDIT duplicate ignored tx={}", tx.getTransactionId());
        }

        // 3. Commit the offset only AFTER the work is done => at-least-once.
        //    If we crash before this line, the record is re-delivered, and step 2 makes that harmless.
        ack.acknowledge();
    }
}
