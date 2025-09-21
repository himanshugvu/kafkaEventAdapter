package com.orchestrator.core.service;

import com.orchestrator.core.config.OrchestratorProperties;
import com.orchestrator.core.metrics.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class EventPublisherService {
    
    private static final Logger logger = LoggerFactory.getLogger(EventPublisherService.class);
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OrchestratorProperties properties;
    private final LatencyTracker latencyTracker;
    
    public EventPublisherService(
            KafkaTemplate<String, String> kafkaTemplate,
            OrchestratorProperties properties,
            LatencyTracker latencyTracker) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.latencyTracker = latencyTracker;
    }
    
    @Retryable(
        retryFor = {Exception.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000L)
    )
    public CompletableFuture<SendResult<String, String>> publishMessage(String message) {
        String targetTopic = properties.producer().topic();
        long sendTimeNs = System.nanoTime();
        
        logger.debug("Publishing message to topic: {}", targetTopic);
        
        var messageWithHeaders = MessageBuilder
            .withPayload(message)
            .setHeader(KafkaHeaders.TOPIC, targetTopic)
            .setHeader("message_send_time_ns", String.valueOf(sendTimeNs))
            .setHeader("orchestrator_send_time", String.valueOf(System.currentTimeMillis()))
            .build();
        
        return kafkaTemplate.send(messageWithHeaders);
    }
    
    @Retryable(
        retryFor = {Exception.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000L)
    )
    public CompletableFuture<SendResult<String, String>> publishMessage(String key, String message) {
        String targetTopic = properties.producer().topic();
        long sendTimeNs = System.nanoTime();
        
        logger.debug("Publishing message with key {} to topic: {}", key, targetTopic);
        
        var messageWithHeaders = MessageBuilder
            .withPayload(message)
            .setHeader(KafkaHeaders.TOPIC, targetTopic)
            .setHeader(KafkaHeaders.KEY, key)
            .setHeader("message_send_time_ns", String.valueOf(sendTimeNs))
            .setHeader("orchestrator_send_time", String.valueOf(System.currentTimeMillis()))
            .build();
        
        return kafkaTemplate.send(messageWithHeaders);
    }
    
}