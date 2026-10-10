package com.example.frauddetection.config;

import com.example.frauddetection.avro.Transaction;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer settings are explicit on purpose, so every reliability decision is visible.
 *
 * Three templates:
 *  - kafkaTemplate              : normal idempotent producer (Avro Transaction)
 *  - transactionalKafkaTemplate : transactional producer, all-or-nothing batches
 *  - bytesKafkaTemplate         : raw bytes, used to simulate poison messages
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    private Map<String, Object> reliableProducerProps() {
        Map<String, Object> p = new HashMap<>();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Wait for all in-sync replicas. With RF=3 and min.insync.replicas=2 no acknowledged write is lost
        // when one broker dies.
        p.put(ProducerConfig.ACKS_CONFIG, "all");

        // Retries cannot create duplicates or reorder records within a partition.
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Throughput: small wait to build bigger, compressed batches.
        p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return p;
    }

    private Map<String, Object> avroProducerProps() {
        Map<String, Object> p = reliableProducerProps();
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return p;
    }

    @Bean
    public ProducerFactory<String, Transaction> transactionProducerFactory() {
        return new DefaultKafkaProducerFactory<>(avroProducerProps());
    }

    @Bean
    public KafkaTemplate<String, Transaction> kafkaTemplate() {
        return new KafkaTemplate<>(transactionProducerFactory());
    }

    @Bean
    public ProducerFactory<String, Transaction> transactionalProducerFactory() {
        DefaultKafkaProducerFactory<String, Transaction> factory =
                new DefaultKafkaProducerFactory<>(avroProducerProps());
        // Setting a prefix turns on Kafka transactions (transactional.id = prefix + n).
        factory.setTransactionIdPrefix("fraud-demo-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Transaction> transactionalKafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, byte[]> bytesKafkaTemplate() {
        Map<String, Object> p = reliableProducerProps();
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(p));
    }
}
