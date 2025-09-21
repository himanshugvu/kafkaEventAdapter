package com.orchestrator.core.store;

import java.time.Instant;
import java.util.Objects;

public class Event {
    
    // Core event identity and content
    private String id;
    private String sourcePayload;
    private String transformedPayload;
    
    // Source message metadata
    private String sourceTopic;
    private Integer sourcePartition;
    private Long sourceOffset;
    
    // Destination message metadata
    private String destinationTopic;
    private Integer destinationPartition;
    private Long destinationOffset;
    
    // Timing metadata for latency tracking
    private Long messageSendTime;           // When message was originally sent (milliseconds)
    private Long messageFinalSentTime;      // When message was successfully published (milliseconds)
    private Long sendTimestampNs;           // Original send timestamp in nanoseconds for precision
    
    // Event lifecycle timestamps
    private Instant createdAt;              // When event record was created
    private Instant receivedAt;             // When message was received by orchestrator
    private Instant processedAt;            // When message processing completed
    private Instant publishedAt;            // When message was published to output topic
    
    // Processing state and error handling
    private EventStatus status;
    private String errorMessage;
    private int retryCount;
    private Instant updatedAt;
    
    // Calculated performance metrics
    private Long totalLatencyMs;            // Total end-to-end latency
    private Boolean exceededOneSecond;      // Performance threshold flag
    
    public Event() {}
    
    public Event(String id, String sourcePayload, String sourceTopic, Integer sourcePartition, Long sourceOffset) {
        this.id = id;
        this.sourcePayload = sourcePayload;
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.status = EventStatus.RECEIVED;
        this.createdAt = Instant.now();
        this.receivedAt = Instant.now();
        this.updatedAt = Instant.now();
        this.retryCount = 0;
    }
    
    // Core identity and content
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getSourcePayload() { return sourcePayload; }
    public void setSourcePayload(String sourcePayload) { this.sourcePayload = sourcePayload; }
    
    @Deprecated(since = "1.0", forRemoval = true)
    public String getPayload() { return sourcePayload; }
    @Deprecated(since = "1.0", forRemoval = true)
    public void setPayload(String payload) { this.sourcePayload = payload; }
    
    public String getTransformedPayload() { return transformedPayload; }
    public void setTransformedPayload(String transformedPayload) { this.transformedPayload = transformedPayload; }
    
    // Source metadata
    public String getSourceTopic() { return sourceTopic; }
    public void setSourceTopic(String sourceTopic) { this.sourceTopic = sourceTopic; }
    
    public Integer getSourcePartition() { return sourcePartition; }
    public void setSourcePartition(Integer sourcePartition) { this.sourcePartition = sourcePartition; }
    
    public Long getSourceOffset() { return sourceOffset; }
    public void setSourceOffset(Long sourceOffset) { this.sourceOffset = sourceOffset; }
    
    @Deprecated(since = "1.0", forRemoval = true)
    public Long getOffset() { return sourceOffset; }
    @Deprecated(since = "1.0", forRemoval = true)
    public void setOffset(Long offset) { this.sourceOffset = offset; }
    
    // Destination metadata
    public String getDestinationTopic() { return destinationTopic; }
    public void setDestinationTopic(String destinationTopic) { this.destinationTopic = destinationTopic; }
    
    public Integer getDestinationPartition() { return destinationPartition; }
    public void setDestinationPartition(Integer destinationPartition) { this.destinationPartition = destinationPartition; }
    
    public Long getDestinationOffset() { return destinationOffset; }
    public void setDestinationOffset(Long destinationOffset) { this.destinationOffset = destinationOffset; }
    
    // Timing metadata
    public Long getMessageSendTime() { return messageSendTime; }
    public void setMessageSendTime(Long messageSendTime) { this.messageSendTime = messageSendTime; }
    
    public Long getMessageFinalSentTime() { return messageFinalSentTime; }
    public void setMessageFinalSentTime(Long messageFinalSentTime) { this.messageFinalSentTime = messageFinalSentTime; }
    
    public Long getSendTimestampNs() { return sendTimestampNs; }
    public void setSendTimestampNs(Long sendTimestampNs) { this.sendTimestampNs = sendTimestampNs; }
    
    // Event lifecycle timestamps
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    
    public Instant getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Instant receivedAt) { this.receivedAt = receivedAt; }
    
    public Instant getProcessedAt() { return processedAt; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }
    
    public Instant getPublishedAt() { return publishedAt; }
    public void setPublishedAt(Instant publishedAt) { this.publishedAt = publishedAt; }
    
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    
    // Processing state and error handling
    public EventStatus getStatus() { return status; }
    public void setStatus(EventStatus status) { 
        this.status = status; 
        this.updatedAt = Instant.now();
    }
    
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    
    public void incrementRetryCount() {
        this.retryCount++;
        this.updatedAt = Instant.now();
    }
    
    // Performance metrics
    public Long getTotalLatencyMs() { return totalLatencyMs; }
    public void setTotalLatencyMs(Long totalLatencyMs) { this.totalLatencyMs = totalLatencyMs; }
    
    public Boolean getExceededOneSecond() { return exceededOneSecond; }
    public void setExceededOneSecond(Boolean exceededOneSecond) { this.exceededOneSecond = exceededOneSecond; }
    
    public void calculateTimingMetrics() {
        if (sendTimestampNs != null && publishedAt != null) {
            long sendTimeMs = sendTimestampNs / 1_000_000; // Convert nanoseconds to milliseconds
            long publishedTimeMs = publishedAt.toEpochMilli();
            this.totalLatencyMs = publishedTimeMs - sendTimeMs;
            this.exceededOneSecond = this.totalLatencyMs > 1000;
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(id, event.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", status=" + status +
                ", createdAt=" + createdAt +
                ", retryCount=" + retryCount +
                ", totalLatencyMs=" + totalLatencyMs +
                '}';
    }
}