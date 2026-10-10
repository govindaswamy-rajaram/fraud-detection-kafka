package com.example.frauddetection.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class FraudStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration(
            @Value("${app.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${app.kafka.schema-registry-url}") String schemaRegistryUrl) {

        Map<String, Object> p = new HashMap<>();
        // application.id doubles as consumer group id and prefix of internal topics / state stores.
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detector");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Exactly-once: read -> update state -> write alerts -> commit offsets happen atomically
        // (uses Kafka transactions internally; downstream readers must use read_committed).
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // A corrupt record is logged and skipped instead of crashing the whole topology.
        p.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        // Emit every window update (no record caching) so the count == threshold check sees each value.
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        // Internal changelog/repartition topics: 1 replica locally, use 3 in production.
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        return new KafkaStreamsConfiguration(p);
    }
}
