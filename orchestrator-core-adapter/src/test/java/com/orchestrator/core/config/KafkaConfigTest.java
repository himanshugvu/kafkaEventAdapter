package com.orchestrator.core.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConfigTest {

    private OrchestratorProperties properties;
    private KafkaConfig kafkaConfig;

    @BeforeEach
    void setUp() {
        properties = new OrchestratorProperties(
            new OrchestratorProperties.ConsumerConfig(
                "input-topic", "test-group", "localhost:9092", 3, 50,
                Duration.ofSeconds(3), false, Duration.ofSeconds(3),
                Duration.ofSeconds(30), 1, Duration.ofMillis(500),
                1048576, 65536, 131072
            ),
            new OrchestratorProperties.ProducerConfig(
                "output-topic", "localhost:9092", "all", 3,
                Duration.ofSeconds(30), false, null, 16384,
                Duration.ofMillis(10), "snappy", 33554432, 5,
                Duration.ofMinutes(2)
            ),
            new OrchestratorProperties.DatabaseConfig(
                OrchestratorProperties.DatabaseStrategy.ATOMIC_OUTBOX,
                Duration.ofMinutes(30), 3, Duration.ofDays(7), 100
            ),
            null,
            null
        );
        
        kafkaConfig = new KafkaConfig(properties);
    }

    @Test
    void testProducerFactory() {
        ProducerFactory<String, String> factory = kafkaConfig.producerFactory();
        
        assertNotNull(factory);
        assertEquals("localhost:9092", factory.getConfigurationProperties().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", factory.getConfigurationProperties().get(ProducerConfig.ACKS_CONFIG));
        assertEquals(3, factory.getConfigurationProperties().get(ProducerConfig.RETRIES_CONFIG));
    }

    @Test
    void testConsumerFactory() {
        ConsumerFactory<String, String> factory = kafkaConfig.consumerFactory();
        
        assertNotNull(factory);
        assertEquals("localhost:9092", factory.getConfigurationProperties().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("test-group", factory.getConfigurationProperties().get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(50, factory.getConfigurationProperties().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(false, factory.getConfigurationProperties().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    void testKafkaTemplate() {
        KafkaTemplate<String, String> template = kafkaConfig.kafkaTemplate();
        
        assertNotNull(template);
        assertNotNull(template.getProducerFactory());
    }

    @Test
    void testKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory();
        
        assertNotNull(factory);
        // Note: getConcurrency() is not available in Spring Kafka API, test other properties
        assertTrue(factory.isBatchListener());
        assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE, 
                    factory.getContainerProperties().getAckMode());
    }

    @Test
    void testProducerFactoryWithNullBootstrapServers() {
        OrchestratorProperties propsWithNullProducerServers = new OrchestratorProperties(
            properties.consumer(),
            new OrchestratorProperties.ProducerConfig(
                "output-topic", null, "all", 3,
                Duration.ofSeconds(30), false, null, 16384,
                Duration.ofMillis(10), "snappy", 33554432, 5,
                Duration.ofMinutes(2)
            ),
            properties.database(),
            null,
            null
        );
        
        KafkaConfig configWithNullProducerServers = new KafkaConfig(propsWithNullProducerServers);
        ProducerFactory<String, String> factory = configWithNullProducerServers.producerFactory();
        
        assertEquals("localhost:9092", factory.getConfigurationProperties().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }
}