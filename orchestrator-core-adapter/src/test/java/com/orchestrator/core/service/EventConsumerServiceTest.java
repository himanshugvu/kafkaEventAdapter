package com.orchestrator.core.service;

import com.orchestrator.core.config.OrchestratorProperties;
import com.orchestrator.core.metrics.LatencyTracker;
import com.orchestrator.core.store.Event;
import com.orchestrator.core.store.EventStore;
import com.orchestrator.core.transformer.MessageTransformer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventConsumerServiceTest {

    @Mock
    private EventStore eventStore;
    
    @Mock
    private EventPublisherService publisherService;
    
    @Mock
    private MessageTransformer messageTransformer;
    
    @Mock
    private LatencyTracker latencyTracker;
    
    @Mock
    private TransactionalEventService transactionalEventService;
    
    @Mock
    private Acknowledgment acknowledgment;
    
    @Mock
    private SendResult<String, String> sendResult;
    
    private OrchestratorProperties properties;
    private EventConsumerService consumerService;

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
        
        consumerService = new EventConsumerService(
            eventStore, publisherService, messageTransformer, 
            properties, latencyTracker, transactionalEventService
        );
    }

    @Test
    void testConsumeEventsAtomicOutbox() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "input-topic", 0, 100L, "key", "message"
        );
        record.headers().add("send_timestamp_ns", "123456789".getBytes());
        
        List<ConsumerRecord<String, String>> records = List.of(record);
        
        when(messageTransformer.transform("message")).thenReturn("transformed");
        when(publisherService.publishMessage("transformed"))
            .thenReturn(CompletableFuture.completedFuture(sendResult));
        
        consumerService.consumeEvents(records, acknowledgment);
        
        verify(transactionalEventService).bulkInsertAndCommit(anyList(), eq(acknowledgment));
    }

    @Test
    void testConsumeEventsAuditPersist() {
        OrchestratorProperties auditProperties = new OrchestratorProperties(
            properties.consumer(),
            properties.producer(),
            new OrchestratorProperties.DatabaseConfig(
                OrchestratorProperties.DatabaseStrategy.AUDIT_PERSIST,
                Duration.ofMinutes(30), 3, Duration.ofDays(7), 100
            ),
            null,
            null
        );
        
        consumerService = new EventConsumerService(
            eventStore, publisherService, messageTransformer, 
            auditProperties, latencyTracker, transactionalEventService
        );
        
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "input-topic", 0, 100L, "key", "message"
        );
        
        when(messageTransformer.transform("message")).thenReturn("transformed");
        when(publisherService.publishMessage("transformed"))
            .thenReturn(CompletableFuture.completedFuture(sendResult));
        
        consumerService.consumeEvents(List.of(record), acknowledgment);
        
        verify(messageTransformer).transform("message");
    }

    @Test
    void testConsumeEventsFailSafe() {
        OrchestratorProperties failSafeProperties = new OrchestratorProperties(
            properties.consumer(),
            properties.producer(),
            new OrchestratorProperties.DatabaseConfig(
                OrchestratorProperties.DatabaseStrategy.FAIL_SAFE,
                Duration.ofMinutes(30), 3, Duration.ofDays(7), 100
            ),
            null,
            null
        );
        
        consumerService = new EventConsumerService(
            eventStore, publisherService, messageTransformer, 
            failSafeProperties, latencyTracker, transactionalEventService
        );
        
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "input-topic", 0, 100L, "key", "message"
        );
        
        when(messageTransformer.transform("message")).thenReturn("transformed");
        when(publisherService.publishMessage("transformed"))
            .thenReturn(CompletableFuture.completedFuture(sendResult));
        
        consumerService.consumeEvents(List.of(record), acknowledgment);
        
        verify(acknowledgment).acknowledge();
        verify(messageTransformer).transform("message");
    }
}