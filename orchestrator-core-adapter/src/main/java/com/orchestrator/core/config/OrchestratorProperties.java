package com.orchestrator.core.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@ConfigurationProperties(prefix = "orchestrator")
@Validated
public record OrchestratorProperties(
    @NotNull @Valid ConsumerConfig consumer,
    @NotNull @Valid ProducerConfig producer,
    @NotNull @Valid DatabaseConfig database,
    @Valid ResilienceConfig resilience,
    @Valid MonitoringConfig monitoring
) {
    
    public record ConsumerConfig(
        @NotBlank(message = "Consumer topic is mandatory") 
        String topic,
        
        @NotBlank(message = "Consumer group ID is mandatory") 
        String groupId,
        
        @NotBlank(message = "Consumer bootstrap servers is mandatory") 
        String bootstrapServers,
        
        @Positive int concurrency,
        @Positive int maxPollRecords,
        Duration pollTimeout,
        boolean enableAutoCommit,
        Duration heartbeatInterval,
        Duration sessionTimeout,
        int fetchMinBytes,
        Duration fetchMaxWait,
        int maxPartitionFetchBytes,
        int receiveBufferBytes,
        int sendBufferBytes
    ) {}
    
    public record ProducerConfig(
        @NotBlank(message = "Producer topic is mandatory")
        String topic,
        
        String bootstrapServers,
        
        String acks,
        @Positive int retries,
        Duration requestTimeout,
        boolean enableIdempotence,
        String transactionIdPrefix,
        int batchSize,
        Duration lingerMs,
        String compressionType,
        long bufferMemory,
        int maxInFlightRequestsPerConnection,
        Duration deliveryTimeout
    ) {}
    
    public record DatabaseConfig(
        DatabaseStrategy strategy,
        PayloadStorage payloadStorage,
        boolean storePayloadOnFailureOnly,
        Duration staleEventThreshold,
        int maxRetries,
        Duration retentionPeriod,
        int bulkSize,
        int executorThreads
    ) {}
    
    public record ResilienceConfig(
        Duration initialBackoff,
        Duration maxBackoff,
        double backoffMultiplier,
        boolean enableCircuitBreaker,
        int circuitBreakerFailureThreshold,
        Duration circuitBreakerRecoveryTimeout
    ) {}
    
    public record MonitoringConfig(
        boolean enableMetrics,
        boolean enableHealthChecks,
        String metricsPrefix
    ) {}
    
    public enum DatabaseStrategy {
        NONE,
        OUTBOX,
        RELIABLE,
        LIGHTWEIGHT,
        ATOMIC_OUTBOX,
        AUDIT_PERSIST,
        FAIL_SAFE
    }

    public enum PayloadStorage {
        NONE,
        BYTES,
        TEXT
    }
}