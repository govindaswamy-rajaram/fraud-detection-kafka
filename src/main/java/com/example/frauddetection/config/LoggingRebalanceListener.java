package com.example.frauddetection.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;

/**
 * Makes rebalances visible in the logs. A real application would also flush in-flight work
 * or commit offsets for revoked partitions here.
 */
public class LoggingRebalanceListener implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingRebalanceListener.class);

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("REBALANCE: partitions revoked  {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("REBALANCE: partitions assigned {}", partitions);
    }
}
