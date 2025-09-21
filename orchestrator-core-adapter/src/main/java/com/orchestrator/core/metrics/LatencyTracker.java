package com.orchestrator.core.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class LatencyTracker {
    
    private static final Logger logger = LoggerFactory.getLogger(LatencyTracker.class);
    
    private final Counter messagesExceedingOneSecond;
    private final Counter totalMessagesProcessed;
    private final Timer endToEndLatencyTimer;
    private final Timer consumerLatencyTimer;
    private final Timer processingLatencyTimer;
    private final Timer publishingLatencyTimer;
    
    private final AtomicLong slowMessageCount = new AtomicLong(0);
    private final AtomicLong totalMessageCount = new AtomicLong(0);
    
    public LatencyTracker(MeterRegistry meterRegistry) {
        this.messagesExceedingOneSecond = Counter.builder("orchestrator.messages.slow")
                .description("Count of messages that took more than 1 second end-to-end")
                .register(meterRegistry);
                
        this.totalMessagesProcessed = Counter.builder("orchestrator.messages.total")
                .description("Total count of messages processed")
                .register(meterRegistry);
                
        this.endToEndLatencyTimer = Timer.builder("orchestrator.latency.end_to_end")
                .description("End-to-end latency from send to completion")
                .register(meterRegistry);
                
        this.consumerLatencyTimer = Timer.builder("orchestrator.latency.consumer")
                .description("Time from message send to consumer receipt")
                .register(meterRegistry);
                
        this.processingLatencyTimer = Timer.builder("orchestrator.latency.processing")
                .description("Time spent processing the message")
                .register(meterRegistry);
                
        this.publishingLatencyTimer = Timer.builder("orchestrator.latency.publishing")
                .description("Time spent publishing the transformed message")
                .register(meterRegistry);
    }
    
    public void recordEndToEndLatency(long latencyMs, long sendTimestampNs) {
        totalMessageCount.incrementAndGet();
        totalMessagesProcessed.increment();
        
        Duration latencyDuration = Duration.ofMillis(latencyMs);
        endToEndLatencyTimer.record(latencyDuration);
        
        if (latencyMs > 1000) {
            slowMessageCount.incrementAndGet();
            messagesExceedingOneSecond.increment();
            
            logger.warn("SLOW MESSAGE DETECTED: End-to-end latency {}ms (sent at ns: {})", 
                       latencyMs, sendTimestampNs);
        } else {
            logger.debug("Message processed in {}ms (sent at ns: {})", 
                        latencyMs, sendTimestampNs);
        }
    }
    
    public void recordConsumerLatency(long sendTimestampNs, Instant receivedAt) {
        if (sendTimestampNs > 0 && receivedAt != null) {
            long sendTimeMs = sendTimestampNs / 1_000_000;
            long receivedTimeMs = receivedAt.toEpochMilli();
            long consumerLatencyMs = receivedTimeMs - sendTimeMs;
            
            consumerLatencyTimer.record(Duration.ofMillis(consumerLatencyMs));
            
            if (consumerLatencyMs > 500) { // Log if consumer latency > 500ms
                logger.warn("HIGH CONSUMER LATENCY: {}ms from send to consumer", consumerLatencyMs);
            } else {
                logger.debug("Consumer latency: {}ms", consumerLatencyMs);
            }
        }
    }
    
    public void recordProcessingLatency(Instant startTime, Instant endTime) {
        if (startTime != null && endTime != null) {
            long processingMs = Duration.between(startTime, endTime).toMillis();
            processingLatencyTimer.record(Duration.ofMillis(processingMs));
            
            if (processingMs > 100) { // Log if processing > 100ms
                logger.warn("SLOW PROCESSING: {}ms", processingMs);
            } else {
                logger.debug("Processing time: {}ms", processingMs);
            }
        }
    }
    
    public void recordPublishingLatency(Instant startTime, Instant endTime) {
        if (startTime != null && endTime != null) {
            long publishingMs = Duration.between(startTime, endTime).toMillis();
            publishingLatencyTimer.record(Duration.ofMillis(publishingMs));
            
            if (publishingMs > 200) { // Log if publishing > 200ms
                logger.warn("SLOW PUBLISHING: {}ms", publishingMs);
            } else {
                logger.debug("Publishing time: {}ms", publishingMs);
            }
        }
    }
    
    public long getSlowMessageCount() {
        return slowMessageCount.get();
    }
    
    public long getTotalMessageCount() {
        return totalMessageCount.get();
    }
    
    public double getSlowMessagePercentage() {
        long total = getTotalMessageCount();
        if (total == 0) return 0.0;
        return (double) getSlowMessageCount() / total * 100.0;
    }
    
    public void logPeriodicStats() {
        long total = getTotalMessageCount();
        long slow = getSlowMessageCount();
        double percentage = getSlowMessagePercentage();
        
        logger.info("LATENCY STATS: Total={}, Slow={}(>1s), Percentage={:.2f}%", 
                   total, slow, percentage);
    }
}