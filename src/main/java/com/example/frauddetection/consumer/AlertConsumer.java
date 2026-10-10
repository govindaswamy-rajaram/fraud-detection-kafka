package com.example.frauddetection.consumer;

import com.example.frauddetection.Topics;
import com.example.frauddetection.avro.FraudAlert;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** Consumer group "alert-notifier": would page analysts / block the card in a real system. */
@Component
public class AlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertConsumer.class);

    private final DemoState state;

    public AlertConsumer(DemoState state) {
        this.state = state;
    }

    @KafkaListener(id = "alert-listener", topics = Topics.FRAUD_ALERTS, groupId = "alert-notifier")
    public void onAlert(ConsumerRecord<String, FraudAlert> record, Acknowledgment ack) {
        FraudAlert alert = record.value();
        String entry = "rule=%s card=%s details=%s".formatted(alert.getRule(), alert.getCardId(), alert.getDetails());
        log.warn("FRAUD ALERT {}", entry);
        state.addAlert(entry);
        ack.acknowledge();
    }
}
