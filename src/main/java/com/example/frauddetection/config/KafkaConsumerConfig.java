package com.example.frauddetection.config;

import com.example.frauddetection.consumer.InvalidTransactionException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    private Map<String, Object> commonConsumerProps() {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // We commit offsets ourselves, AFTER processing => at-least-once delivery.
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Never see records from aborted Kafka transactions.
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Rebalances only move the partitions that must move, instead of stopping everyone.
        p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return p;
    }

    // ---------------------------------------------------------------- main consumers (Avro)

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> p = commonConsumerProps();

        // ErrorHandlingDeserializer wraps the real deserializer: a corrupt record no longer throws
        // inside poll() (which would loop forever); it is routed to the error handler / DLT instead.
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        p.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        p.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);

        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); // generated classes, not GenericRecord
        return new DefaultKafkaConsumerFactory<>(p);
    }

    /**
     * Failed records are published to "<topic>.DLT" with the exception details in headers.
     * Two templates: one for normal Avro values, one for raw bytes (records that could not be
     * deserialized are forwarded unchanged so nothing is lost).
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            @Qualifier("kafkaTemplate") KafkaTemplate<String, ?> avroTemplate,
            @Qualifier("bytesKafkaTemplate") KafkaTemplate<String, byte[]> bytesTemplate) {

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);   // checked first
        templates.put(Object.class, avroTemplate);

        return new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
    }

    /**
     * Transient failures (e.g. database down) are retried with exponential backoff: 0.5s, 1s, 2s.
     * Permanent failures (invalid data) skip retries and go straight to the DLT.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(500L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(4_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(InvalidTransactionException.class);
        // With manual acks, also commit the offset of a record that was sent to the DLT.
        handler.setCommitRecovered(true);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            @Qualifier("consumerFactory") ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3); // 3 consumer threads per listener = one per partition
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setConsumerRebalanceListener(new LoggingRebalanceListener());
        return factory;
    }

    // ---------------------------------------------------------------- DLT inspector (raw bytes)

    @Bean
    public ConsumerFactory<String, byte[]> dltConsumerFactory() {
        Map<String, Object> p = commonConsumerProps();
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(p);
    }

    /** Reads bytes only, so it can never fail on a bad payload (no "DLT of the DLT" loop). */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> dltListenerContainerFactory(
            @Qualifier("dltConsumerFactory") ConsumerFactory<String, byte[]> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(500L, 2L)));
        return factory;
    }
}
