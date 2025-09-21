package com.orchestrator.core.service;

import com.orchestrator.core.config.OrchestratorProperties;
import com.orchestrator.core.metrics.LatencyTracker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventPublisherServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Mock
    private LatencyTracker latencyTracker;
    
    @Mock
    private SendResult<String, String> sendResult;
    
    private OrchestratorProperties properties;
    private EventPublisherService publisherService;

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
        
        publisherService = new EventPublisherService(kafkaTemplate, properties, latencyTracker);
    }

    @Test
    void testPublishMessage() {
        String message = "test message";
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
        
        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);
        
        CompletableFuture<SendResult<String, String>> result = publisherService.publishMessage(message);
        
        assertNotNull(result);
        verify(kafkaTemplate).send(any(Message.class));
    }

    @Test
    void testPublishMessageWithKey() {
        String key = "test-key";
        String message = "test message";
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
        
        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);
        
        CompletableFuture<SendResult<String, String>> result = publisherService.publishMessage(key, message);
        
        assertNotNull(result);
        verify(kafkaTemplate).send(any(Message.class));
    }
}