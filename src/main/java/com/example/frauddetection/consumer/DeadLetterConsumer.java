package com.example.frauddetection.consumer;

import com.example.frauddetection.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Inspects the dead letter topic. In production this would raise an alert and feed a tool
 * that lets operators fix and replay records.
 */
@Component
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    private final DemoState state;

    public DeadLetterConsumer(DemoState state) {
        this.state = state;
    }

    @KafkaListener(id = "dlt-listener", topics = Topics.TRANSACTIONS_DLT, groupId = "dlt-inspector",
            containerFactory = "dltListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, byte[]> record) {
        String entry = "key=%s | from=%s | %s | %s".formatted(
                record.key(),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                header(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                abbreviate(header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)));
        log.warn("DEAD LETTER {}", entry);
        state.addDeadLetter(entry);
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null || h.value() == null ? "n/a" : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static String abbreviate(String s) {
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }
}
