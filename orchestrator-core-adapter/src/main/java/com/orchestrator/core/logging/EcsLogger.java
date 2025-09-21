package com.orchestrator.core.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class EcsLogger {

    private static final Logger logger = LoggerFactory.getLogger(EcsLogger.class);
    private final MessageTokenizer messageTokenizer;

    public EcsLogger(MessageTokenizer messageTokenizer) {
        this.messageTokenizer = messageTokenizer;
    }

    /**
     * Log event processing with ECS structured fields
     */
    public void logEventProcessing(String eventId, String sourceTopic, int partition, long offset,
                                 String strategy, String message, String phase) {
        MessageTokenizer.TokenizedMessage tokenized = messageTokenizer.tokenize(message);

        try {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("event.id", eventId);
            fields.put("event.dataset", "kafka.event.processing");
            fields.put("event.action", phase);
            fields.put("event.category", "stream");
            fields.put("event.type", "info");
            fields.put("kafka.topic", sourceTopic);
            fields.put("kafka.partition", String.valueOf(partition));
            fields.put("kafka.offset", String.valueOf(offset));
            fields.put("orchestrator.strategy", strategy);
            fields.put("message.hash", tokenized.getHash());
            fields.put("message.length", String.valueOf(tokenized.getOriginalLength()));
            fields.put("trace.id", getOrCreateTraceId());
            setEcsFields(fields);

            logger.info("Event processing {} for topic {} - Message: {}",
                phase, sourceTopic, tokenized.getSummary());

        } finally {
            clearMDC();
        }
    }

    /**
     * Log performance metrics with ECS fields
     */
    public void logPerformanceMetrics(String eventId, String operation, long durationMs,
                                    boolean success, String errorMessage) {
        try {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("event.id", eventId);
            fields.put("event.dataset", "kafka.performance");
            fields.put("event.action", operation);
            fields.put("event.category", "process");
            fields.put("event.type", success ? "info" : "error");
            fields.put("event.duration", String.valueOf(durationMs * 1_000_000));
            fields.put("event.outcome", success ? "success" : "failure");
            fields.put("performance.duration_ms", String.valueOf(durationMs));
            fields.put("performance.operation", operation);
            fields.put("trace.id", getOrCreateTraceId());
            setEcsFields(fields);

            if (errorMessage != null) {
                MDC.put("error.message", errorMessage);
            }

            if (success) {
                logger.info("Performance metric: {} completed in {}ms", operation, durationMs);
            } else {
                logger.error("Performance metric: {} failed after {}ms - {}", operation, durationMs, errorMessage);
            }

        } finally {
            clearMDC();
        }
    }

    /**
     * Log security events with enhanced ECS fields
     */
    public void logSecurityEvent(String eventType, String description, String userId,
                               String sourceIp, boolean success) {
        Logger securityLogger = LoggerFactory.getLogger("com.orchestrator.core.security");

        try {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("event.id", UUID.randomUUID().toString());
            fields.put("event.dataset", "kafka.security");
            fields.put("event.action", eventType);
            fields.put("event.category", "authentication");
            fields.put("event.type", "info");
            fields.put("event.outcome", success ? "success" : "failure");
            fields.put("user.id", userId != null ? userId : "anonymous");
            fields.put("source.ip", sourceIp != null ? sourceIp : "unknown");
            fields.put("security.event_type", eventType);
            fields.put("trace.id", getOrCreateTraceId());
            setEcsFields(fields);

            if (success) {
                securityLogger.info("Security event: {} - {}", eventType, description);
            } else {
                securityLogger.warn("Security event: {} - {} (FAILED)", eventType, description);
            }

        } finally {
            clearMDC();
        }
    }

    /**
     * Log database operations with tokenized queries
     */
    public void logDatabaseOperation(String eventId, String operation, String tableName,
                                   int recordCount, long durationMs, boolean success, String query) {
        MessageTokenizer.TokenizedMessage tokenizedQuery = messageTokenizer.tokenize(query);

        try {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("event.id", eventId);
            fields.put("event.dataset", "kafka.database");
            fields.put("event.action", operation);
            fields.put("event.category", "database");
            fields.put("event.type", success ? "access" : "error");
            fields.put("event.duration", String.valueOf(durationMs * 1_000_000));
            fields.put("event.outcome", success ? "success" : "failure");
            fields.put("database.operation", operation);
            fields.put("database.table", tableName);
            fields.put("database.record_count", String.valueOf(recordCount));
            fields.put("database.query_hash", tokenizedQuery.getHash());
            fields.put("trace.id", getOrCreateTraceId());
            setEcsFields(fields);

            logger.info("Database operation: {} on {} - {} records in {}ms",
                operation, tableName, recordCount, durationMs);

        } finally {
            clearMDC();
        }
    }

    /**
     * Log Kafka producer/consumer operations
     */
    public void logKafkaOperation(String eventId, String operation, String topic, int partition,
                                long offset, long durationMs, boolean success, String errorMessage) {
        try {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("event.id", eventId);
            fields.put("event.dataset", "kafka.transport");
            fields.put("event.action", operation);
            fields.put("event.category", "network");
            fields.put("event.type", success ? "connection" : "error");
            fields.put("event.duration", String.valueOf(durationMs * 1_000_000));
            fields.put("event.outcome", success ? "success" : "failure");
            fields.put("kafka.operation", operation);
            fields.put("kafka.topic", topic);
            fields.put("kafka.partition", String.valueOf(partition));
            fields.put("kafka.offset", String.valueOf(offset));
            fields.put("trace.id", getOrCreateTraceId());
            setEcsFields(fields);

            if (errorMessage != null) {
                MDC.put("error.message", errorMessage);
            }

            if (success) {
                logger.info("Kafka {}: topic={} partition={} offset={} duration={}ms",
                    operation, topic, partition, offset, durationMs);
            } else {
                logger.error("Kafka {} failed: topic={} partition={} offset={} duration={}ms error={}",
                    operation, topic, partition, offset, durationMs, errorMessage);
            }

        } finally {
            clearMDC();
        }
    }

    private void setEcsFields(Map<String, String> fields) {
        // Set timestamp in ECS format
        MDC.put("@timestamp", Instant.now().toString());

        // Set service information
        MDC.put("service.name", "kafka-orchestrator");
        MDC.put("service.version", "1.0.0");
        MDC.put("service.environment", System.getenv().getOrDefault("ENVIRONMENT", "local"));

        // Set custom fields
        fields.forEach(MDC::put);
    }

    private String getOrCreateTraceId() {
        String traceId = MDC.get("trace.id");
        if (traceId == null) {
            traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            MDC.put("trace.id", traceId);
        }
        return traceId;
    }

    private void clearMDC() {
        MDC.clear();
    }

    /**
     * Start a new trace context
     */
    public String startTrace() {
        String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        MDC.put("trace.id", traceId);
        return traceId;
    }

    /**
     * Set correlation ID for request tracking
     */
    public void setCorrelationId(String correlationId) {
        MDC.put("correlation.id", correlationId);
    }
}