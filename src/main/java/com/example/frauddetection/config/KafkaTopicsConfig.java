package com.example.frauddetection.config;

import com.example.frauddetection.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;

/**
 * Topics are declared as code; Spring's KafkaAdmin creates them at startup if missing.
 *
 * Local values: replicas=1, min.insync.replicas=1 (one broker).
 * Production values: replicas=3, min.insync.replicas=2, together with producer acks=all.
 */
@Configuration
public class KafkaTopicsConfig {

    /** Max consumer parallelism per group = number of partitions. */
    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 1;

    @Bean
    NewTopic transactionsTopic() {
        return TopicBuilder.name(Topics.TRANSACTIONS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(7).toMillis()))
                .build();
    }

    @Bean
    NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(Topics.FRAUD_ALERTS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    /** Same partition count as the source topic: the recoverer publishes to the same partition number. */
    @Bean
    NewTopic transactionsDeadLetterTopic() {
        return TopicBuilder.name(Topics.TRANSACTIONS_DLT)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(14).toMillis()))
                .build();
    }

    @Bean
    NewTopic fraudAlertsDeadLetterTopic() {
        return TopicBuilder.name(Topics.FRAUD_ALERTS_DLT)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }
}
