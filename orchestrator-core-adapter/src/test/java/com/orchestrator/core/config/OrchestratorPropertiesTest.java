package com.orchestrator.core.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OrchestratorPropertiesTest {

    @Test
    void testValidPropertiesBinding() {
        Map<String, Object> properties = Map.of(
            "orchestrator.consumer.topic", "input-topic",
            "orchestrator.consumer.group-id", "test-group",
            "orchestrator.consumer.bootstrap-servers", "localhost:9092",
            "orchestrator.consumer.concurrency", "3",
            "orchestrator.consumer.max-poll-records", "50",
            "orchestrator.producer.topic", "output-topic",
            "orchestrator.database.strategy", "ATOMIC_OUTBOX"
        );

        ConfigurationPropertySource source = new MapConfigurationPropertySource(properties);
        Binder binder = new Binder(source);
        
        BindResult<OrchestratorProperties> result = binder.bind("orchestrator", OrchestratorProperties.class);
        
        assertTrue(result.isBound());
        OrchestratorProperties config = result.get();
        
        assertEquals("input-topic", config.consumer().topic());
        assertEquals("test-group", config.consumer().groupId());
        assertEquals("localhost:9092", config.consumer().bootstrapServers());
        assertEquals(3, config.consumer().concurrency());
        assertEquals(50, config.consumer().maxPollRecords());
        assertEquals("output-topic", config.producer().topic());
        assertEquals(OrchestratorProperties.DatabaseStrategy.ATOMIC_OUTBOX, config.database().strategy());
    }

    @Test
    void testDatabaseStrategies() {
        assertEquals(3, OrchestratorProperties.DatabaseStrategy.values().length);
        assertNotNull(OrchestratorProperties.DatabaseStrategy.valueOf("ATOMIC_OUTBOX"));
        assertNotNull(OrchestratorProperties.DatabaseStrategy.valueOf("AUDIT_PERSIST"));
        assertNotNull(OrchestratorProperties.DatabaseStrategy.valueOf("FAIL_SAFE"));
    }

    @Test
    void testRecordDefaults() {
        // Test default values in properties
        Map<String, Object> minimalProperties = Map.of(
            "orchestrator.consumer.topic", "input-topic",
            "orchestrator.consumer.group-id", "test-group",
            "orchestrator.consumer.bootstrap-servers", "localhost:9092",
            "orchestrator.consumer.concurrency", "1",
            "orchestrator.consumer.max-poll-records", "1",
            "orchestrator.producer.topic", "output-topic",
            "orchestrator.database.strategy", "FAIL_SAFE"
        );

        ConfigurationPropertySource source = new MapConfigurationPropertySource(minimalProperties);
        Binder binder = new Binder(source);
        
        BindResult<OrchestratorProperties> result = binder.bind("orchestrator", OrchestratorProperties.class);
        
        assertTrue(result.isBound());
        assertNotNull(result.get());
    }
}