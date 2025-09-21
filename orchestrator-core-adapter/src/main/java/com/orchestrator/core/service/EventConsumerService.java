package com.orchestrator.core.service;

import com.orchestrator.core.config.OrchestratorProperties;
import com.orchestrator.core.logging.EcsLogger;
import com.orchestrator.core.metrics.LatencyTracker;
import com.orchestrator.core.store.Event;
import com.orchestrator.core.store.EventStatus;
import com.orchestrator.core.store.EventStore;
import com.orchestrator.core.transformer.MessageTransformer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import jakarta.annotation.PreDestroy;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class EventConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(EventConsumerService.class);
    
    private final EventStore eventStore;
    private final EventPublisherService publisherService;
    private final MessageTransformer messageTransformer;
    private final OrchestratorProperties properties;
    private final LatencyTracker latencyTracker;
    private final TransactionalEventService transactionalEventService;
    private final ExecutorService virtualThreadExecutor;
    private final EcsLogger ecsLogger;
    
    public EventConsumerService(
            EventStore eventStore,
            EventPublisherService publisherService,
            MessageTransformer messageTransformer,
            OrchestratorProperties properties,
            LatencyTracker latencyTracker,
            TransactionalEventService transactionalEventService,
            EcsLogger ecsLogger) {
        this.eventStore = eventStore;
        this.publisherService = publisherService;
        this.messageTransformer = messageTransformer;
        this.properties = properties;
        this.latencyTracker = latencyTracker;
        this.transactionalEventService = transactionalEventService;
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.ecsLogger = ecsLogger;
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down EventConsumerService...");
        if (virtualThreadExecutor != null && !virtualThreadExecutor.isShutdown()) {
            virtualThreadExecutor.shutdown();
            logger.info("Virtual thread executor shutdown completed");
        }
    }
    
    @KafkaListener(
        topics = "${orchestrator.consumer.topic}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvents(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment) {
        
        Instant batchReceivedAt = Instant.now();
        String traceId = ecsLogger.startTrace();

        // Log batch received with ECS structure
        ConsumerRecord<String, String> firstRecord = records.get(0);
        ecsLogger.logEventProcessing(
            traceId,
            firstRecord.topic(),
            firstRecord.partition(),
            firstRecord.offset(),
            properties.database().strategy().name(),
            String.format("Batch of %d records", records.size()),
            "batch_received"
        );
        
        try {
            switch (properties.database().strategy()) {
                case NONE -> processNoneBatch(records, acknowledgment);
                case OUTBOX, ATOMIC_OUTBOX -> processAtomicOutboxBatch(records, acknowledgment);
                case RELIABLE, AUDIT_PERSIST -> processReliableBatch(records, acknowledgment);
                case LIGHTWEIGHT -> processLightweightBatch(records, acknowledgment);
                case FAIL_SAFE -> processFailSafeBatch(records, acknowledgment);
            }
            
        } catch (Exception e) {
            logger.error("CONSUMER BATCH ERROR: Failed to process {} records: {}", records.size(), e.getMessage(), e);
            throw e;
        }
    }
    
    private Long extractSendTimestamp(ConsumerRecord<String, String> record) {
        try {
            if (record.headers() != null) {
                // Check for orchestrator send time first (from producer)
                var orchestratorSendTime = record.headers().lastHeader("orchestrator_send_time");
                if (orchestratorSendTime != null) {
                    return Long.parseLong(new String(orchestratorSendTime.value()));
                }
                // Fallback to original send timestamp
                var timestampHeader = record.headers().lastHeader("send_timestamp_ns");
                if (timestampHeader != null) {
                    return Long.parseLong(new String(timestampHeader.value()));
                }
            }
        } catch (Exception e) {
            logger.debug("Could not extract send timestamp from headers: {}", e.getMessage());
        }
        return null;
    }
    
    private String extractMessageId(ConsumerRecord<String, String> record) {
        try {
            if (record.headers() != null) {
                var messageIdHeader = record.headers().lastHeader("message_id");
                if (messageIdHeader != null) {
                    return new String(messageIdHeader.value());
                }
            }
        } catch (Exception e) {
            logger.debug("Could not extract message ID from headers: {}", e.getMessage());
        }
        return record.key(); // Fallback to record key
    }
    
    private String extractSource(ConsumerRecord<String, String> record) {
        try {
            if (record.headers() != null) {
                var sourceHeader = record.headers().lastHeader("source");
                if (sourceHeader != null) {
                    return new String(sourceHeader.value());
                }
            }
        } catch (Exception e) {
            logger.debug("Could not extract source from headers: {}", e.getMessage());
        }
        return "unknown";
    }
    
    private Event createEventWithTiming(ConsumerRecord<String, String> record, Long sendTimestampNs, Instant receivedAt) {
        String eventId = UUID.randomUUID().toString();
        Event event = new Event(eventId, record.value(), record.topic(), record.partition(), record.offset());
        event.setSendTimestampNs(sendTimestampNs);
        event.setReceivedAt(receivedAt);
        event.setMessageSendTime(sendTimestampNs != null ? sendTimestampNs / 1_000_000 : null);
        return event;
    }
    
    private void processAtomicOutboxBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        Instant receivedAt = Instant.now();
        List<Event> events = records.stream()
            .map(record -> createEventWithTiming(record, extractSendTimestamp(record), receivedAt))
            .toList();
            
        try {
            transactionalEventService.bulkInsertAndCommit(events, acknowledgment);
            
            for (Event event : events) {
                CompletableFuture.runAsync(() -> {
                    try {
                        String transformedMessage = messageTransformer.transform(event.getSourcePayload());
                        
                        publisherService.publishMessage(transformedMessage)
                            .thenAccept(result -> {
                                event.setTransformedPayload(transformedMessage);
                                event.setDestinationTopic(result.getRecordMetadata().topic());
                                event.setDestinationPartition(result.getRecordMetadata().partition());
                                event.setDestinationOffset(result.getRecordMetadata().offset());
                                event.setMessageFinalSentTime(System.currentTimeMillis());
                                eventStore.updateStatus(event.getId(), EventStatus.SUCCESS);
                            })
                            .exceptionally(throwable -> {
                                eventStore.updateStatus(event.getId(), EventStatus.FAILED, throwable.getMessage());
                                return null;
                            });
                            
                    } catch (Exception e) {
                        eventStore.updateStatus(event.getId(), EventStatus.FAILED, e.getMessage());
                    }
                });
            }
            
        } catch (Exception e) {
            logger.error("ATOMIC_OUTBOX: Failed to insert batch of {} events: {}", events.size(), e.getMessage());
            throw e;
        }
    }
    
    
    private void processNoneBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        long startTime = System.currentTimeMillis();

        for (ConsumerRecord<String, String> record : records) {
            String eventId = UUID.randomUUID().toString();
            try {
                // Log message processing start
                ecsLogger.logEventProcessing(
                    eventId,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    "NONE",
                    record.value(),
                    "transform_start"
                );

                String transformedMessage = messageTransformer.transform(record.value());
                publisherService.publishMessage(transformedMessage);

                // Log successful processing
                ecsLogger.logEventProcessing(
                    eventId,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    "NONE",
                    transformedMessage,
                    "publish_success"
                );

            } catch (Exception e) {
                // Log processing failure with structured logging
                ecsLogger.logEventProcessing(
                    eventId,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    "NONE",
                    e.getMessage(),
                    "processing_failed"
                );
            }
        }
        acknowledgment.acknowledge();

        long duration = System.currentTimeMillis() - startTime;
        ecsLogger.logPerformanceMetrics(
            UUID.randomUUID().toString(),
            "none_batch_processing",
            duration,
            true,
            null
        );
    }

    private void processReliableBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        logger.info("RELIABLE STRATEGY: Processing {} records with full database logging", records.size());

        for (ConsumerRecord<String, String> record : records) {
            virtualThreadExecutor.submit(() -> {
                Event event = createEventWithTiming(record, extractSendTimestamp(record), Instant.now());

                try {
                    if (shouldStorePayload(true)) {
                        storeEventWithPayload(event);
                    }

                    String transformedMessage = messageTransformer.transform(record.value());

                    publisherService.publishMessage(transformedMessage)
                        .thenAccept(result -> {
                            event.setTransformedPayload(transformedMessage);
                            event.setDestinationTopic(result.getRecordMetadata().topic());
                            event.setDestinationPartition(result.getRecordMetadata().partition());
                            event.setDestinationOffset(result.getRecordMetadata().offset());
                            event.setMessageFinalSentTime(System.currentTimeMillis());

                            if (shouldStorePayload(true)) {
                                eventStore.updateStatus(event.getId(), EventStatus.SUCCESS);
                            }
                        })
                        .exceptionally(throwable -> {
                            event.setErrorMessage(throwable.getMessage());
                            eventStore.updateStatus(event.getId(), EventStatus.FAILED, throwable.getMessage());
                            return null;
                        });

                } catch (Exception e) {
                    event.setErrorMessage(e.getMessage());
                    eventStore.updateStatus(event.getId(), EventStatus.FAILED, e.getMessage());
                }
            });
        }
        acknowledgment.acknowledge();
    }

    private void processLightweightBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        logger.info("LIGHTWEIGHT STRATEGY: Processing {} records with failure-only database logging", records.size());

        for (ConsumerRecord<String, String> record : records) {
            virtualThreadExecutor.submit(() -> {
                try {
                    String transformedMessage = messageTransformer.transform(record.value());

                    publisherService.publishMessage(transformedMessage)
                        .exceptionally(throwable -> {
                            Event failedEvent = createEventWithTiming(record, extractSendTimestamp(record), Instant.now());
                            failedEvent.setStatus(EventStatus.FAILED);
                            failedEvent.setErrorMessage(throwable.getMessage());

                            if (shouldStorePayload(false)) {
                                storeEventWithPayload(failedEvent);
                            } else {
                                eventStore.bulkInsert(List.of(failedEvent));
                            }

                            logger.error("LIGHTWEIGHT: Logged failed event to database: {}", failedEvent.getId());
                            return null;
                        });

                } catch (Exception e) {
                    Event failedEvent = createEventWithTiming(record, extractSendTimestamp(record), Instant.now());
                    failedEvent.setStatus(EventStatus.FAILED);
                    failedEvent.setErrorMessage(e.getMessage());

                    if (shouldStorePayload(false)) {
                        storeEventWithPayload(failedEvent);
                    } else {
                        eventStore.bulkInsert(List.of(failedEvent));
                    }

                    logger.error("LIGHTWEIGHT: Logged processing failure to database: {}", failedEvent.getId());
                }
            });
        }
        acknowledgment.acknowledge();
    }

    private void processAuditPersistBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        for (ConsumerRecord<String, String> record : records) {
            try {
                String transformedMessage = messageTransformer.transform(record.value());
                
                publisherService.publishMessage(transformedMessage)
                    .thenAccept(result -> {
                        acknowledgment.acknowledge();
                        
                        CompletableFuture.runAsync(() -> {
                            Event successEvent = createEventForAuditPersist(record, transformedMessage, EventStatus.SUCCESS, null);
                            successEvent.setDestinationTopic(result.getRecordMetadata().topic());
                            successEvent.setDestinationPartition(result.getRecordMetadata().partition());
                            successEvent.setDestinationOffset(result.getRecordMetadata().offset());
                            successEvent.setMessageFinalSentTime(System.currentTimeMillis());
                            eventStore.bulkInsert(List.of(successEvent));
                        });
                    })
                    .exceptionally(throwable -> {
                        Event failedEvent = createEventForAuditPersist(record, null, EventStatus.FAILED, throwable.getMessage());
                        eventStore.bulkInsert(List.of(failedEvent));
                        acknowledgment.acknowledge();
                        return null;
                    });
                    
            } catch (Exception e) {
                Event failedEvent = createEventForAuditPersist(record, null, EventStatus.FAILED, e.getMessage());
                eventStore.bulkInsert(List.of(failedEvent));
                acknowledgment.acknowledge();
            }
        }
    }
    
    private Event createEventForAuditPersist(ConsumerRecord<String, String> record, String transformedPayload, EventStatus status, String errorMessage) {
        Event event = createEventWithTiming(record, extractSendTimestamp(record), Instant.now());
        event.setTransformedPayload(transformedPayload);
        event.setStatus(status);
        event.setErrorMessage(errorMessage);
        return event;
    }
    
    private void processFailSafeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        acknowledgment.acknowledge();
        
        for (ConsumerRecord<String, String> record : records) {
            try {
                String transformedMessage = messageTransformer.transform(record.value());
                
                publisherService.publishMessage(transformedMessage)
                    .exceptionally(throwable -> {
                        Event failedEvent = new Event(UUID.randomUUID().toString(), record.value(), 
                                                    record.topic(), record.partition(), record.offset());
                        failedEvent.setStatus(EventStatus.FAILED);
                        failedEvent.setErrorMessage(throwable.getMessage());
                        eventStore.bulkInsert(List.of(failedEvent));
                        logger.error("FAIL_SAFE: Logged failed event to dead letter: {}", failedEvent.getId());
                        return null;
                    });
                    
            } catch (Exception e) {
                Event failedEvent = new Event(UUID.randomUUID().toString(), record.value(), 
                                            record.topic(), record.partition(), record.offset());
                failedEvent.setStatus(EventStatus.FAILED);
                failedEvent.setErrorMessage(e.getMessage());
                eventStore.bulkInsert(List.of(failedEvent));
                logger.error("FAIL_SAFE: Logged processing failure to dead letter: {}", failedEvent.getId());
            }
        }
    }
    
    private void updateEventStatusWithTiming(String eventId, EventStatus status, Event event) {
        updateEventStatusWithTiming(eventId, status, event, null);
    }
    
    private void updateEventStatusWithTiming(String eventId, EventStatus status, Event event, String errorMessage) {
        CompletableFuture.runAsync(() -> {
            if (errorMessage != null) {
                eventStore.updateStatus(eventId, status, errorMessage);
            } else {
                eventStore.updateStatus(eventId, status);
            }
        });
    }
    
    private boolean shouldStorePayload(boolean isSuccess) {
        var payloadStorage = properties.database().payloadStorage();
        var storeOnFailureOnly = properties.database().storePayloadOnFailureOnly();

        if (payloadStorage == OrchestratorProperties.PayloadStorage.NONE) {
            return false;
        }

        return !storeOnFailureOnly || !isSuccess;
    }

    private void storeEventWithPayload(Event event) {
        var payloadStorage = properties.database().payloadStorage();

        switch (payloadStorage) {
            case NONE -> {
                event.setSourcePayload(null);
                event.setTransformedPayload(null);
            }
            case BYTES, TEXT -> {
                // Keep payload as-is - no filtering or tokenization
            }
        }

        eventStore.bulkInsert(List.of(event));
    }

    private CompletableFuture<Void> transformAndPublishAsyncWithTiming(Event event, Instant processingStart) {
        return CompletableFuture
            .supplyAsync(() -> messageTransformer.transform(event.getSourcePayload()))
            .thenCompose(transformedMessage -> {
                Instant publishStart = Instant.now();
                latencyTracker.recordProcessingLatency(processingStart, publishStart);
                
                return publisherService.publishMessage(transformedMessage)
                    .thenAccept(result -> {
                        Instant publishEnd = Instant.now();
                        event.setProcessedAt(publishStart);
                        event.setPublishedAt(publishEnd);
                        event.calculateTimingMetrics();
                        
                        updateEventStatusWithTiming(event.getId(), EventStatus.SUCCESS, event);
                        latencyTracker.recordPublishingLatency(publishStart, publishEnd);
                        
                        if (event.getTotalLatencyMs() != null) {
                            latencyTracker.recordEndToEndLatency(event.getTotalLatencyMs(), event.getSendTimestampNs());
                        }
                    });
            })
            .exceptionally(throwable -> {
                logger.error("Failed to transform and publish event: {}", event.getId(), throwable);
                updateEventStatusWithTiming(event.getId(), EventStatus.FAILED, event, throwable.getMessage());
                return null;
            });
    }
}