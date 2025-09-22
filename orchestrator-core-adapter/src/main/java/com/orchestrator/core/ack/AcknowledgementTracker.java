package com.orchestrator.core.ack;

import com.orchestrator.core.config.OrchestratorProperties;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AcknowledgementTracker {

    private static final Logger logger = LoggerFactory.getLogger(AcknowledgementTracker.class);

    private final OrchestratorProperties.CommitConfig commitConfig;
    private final ScheduledExecutorService scheduler;

    // Track pending acknowledgements: offset -> PendingAck
    private final ConcurrentNavigableMap<Long, PendingAck> pendingAcks = new ConcurrentSkipListMap<>();

    // Track highest contiguous acknowledged offset per partition
    private final ConcurrentHashMap<TopicPartition, AtomicLong> acknowledgedOffsets = new ConcurrentHashMap<>();

    // Batch commit tracking
    private final AtomicInteger messagesSinceLastCommit = new AtomicInteger(0);
    private volatile Instant lastCommitTime = Instant.now();

    // Current acknowledgment callback
    private volatile Acknowledgment currentAcknowledgment;

    public AcknowledgementTracker(OrchestratorProperties.CommitConfig commitConfig) {
        this.commitConfig = commitConfig;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ack-tracker-scheduler");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic commit check
        scheduler.scheduleWithFixedDelay(
            this::checkTimeBasedCommit,
            commitConfig.timeThresholdMs(),
            commitConfig.timeThresholdMs(),
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Register a new pending acknowledgement for tracking
     */
    public void registerPendingAck(TopicPartition partition, long offset,
                                   CompletableFuture<SendResult<String, String>> sendFuture) {

        PendingAck pendingAck = new PendingAck(partition, offset, sendFuture, Instant.now());
        pendingAcks.put(offset, pendingAck);

        // Set up completion handler
        sendFuture.whenComplete((result, throwable) -> {
            if (throwable == null) {
                handleSuccessfulSend(partition, offset);
            } else {
                handleFailedSend(partition, offset, throwable);
            }
        });

        logger.debug("Registered pending ack for offset: {} on partition: {}", offset, partition);
    }

    /**
     * Set the current acknowledgment callback for this batch
     */
    public void setCurrentAcknowledgment(Acknowledgment acknowledgment) {
        this.currentAcknowledgment = acknowledgment;
    }

    /**
     * Handle successful send - mark offset as acknowledged and check for commit
     */
    private void handleSuccessfulSend(TopicPartition partition, long offset) {
        pendingAcks.remove(offset);

        // Update highest contiguous acknowledged offset
        AtomicLong partitionOffset = acknowledgedOffsets.computeIfAbsent(partition, k -> new AtomicLong(-1));
        updateContiguousOffset(partition, offset, partitionOffset);

        // Check if we should commit
        int messageCount = messagesSinceLastCommit.incrementAndGet();
        if (messageCount >= commitConfig.messageThreshold()) {
            commitBatch("message threshold reached");
        }

        logger.debug("Successfully acknowledged offset: {} on partition: {}", offset, partition);
    }

    /**
     * Handle failed send - log failure and potentially move to DLQ
     */
    private void handleFailedSend(TopicPartition partition, long offset, Throwable throwable) {
        PendingAck failedAck = pendingAcks.remove(offset);
        if (failedAck != null) {
            logger.error("Send failed for offset: {} on partition: {} - Error: {}", offset, partition, throwable.getMessage(), throwable);

            // TODO: Implement retry logic or dead letter handling based on failure mode
            // For now, we don't advance the commit offset, ensuring at-least-once delivery
        }
    }

    /**
     * Update the highest contiguous acknowledged offset for a partition
     */
    private void updateContiguousOffset(TopicPartition partition, long acknowledgedOffset, AtomicLong partitionOffset) {
        long currentOffset = partitionOffset.get();

        // If this is the next expected offset, update and check for more contiguous offsets
        if (acknowledgedOffset == currentOffset + 1) {
            partitionOffset.set(acknowledgedOffset);

            // Check if we can advance further with already acknowledged offsets
            long nextOffset = acknowledgedOffset + 1;
            while (pendingAcks.containsKey(nextOffset) &&
                   pendingAcks.get(nextOffset).sendFuture.isDone() &&
                   !pendingAcks.get(nextOffset).sendFuture.isCompletedExceptionally()) {
                partitionOffset.set(nextOffset);
                pendingAcks.remove(nextOffset);
                nextOffset++;
            }
        }
    }

    /**
     * Check if time-based commit threshold has been reached
     */
    private void checkTimeBasedCommit() {
        if (Instant.now().toEpochMilli() - lastCommitTime.toEpochMilli() >= commitConfig.timeThresholdMs()) {
            if (messagesSinceLastCommit.get() > 0) {
                commitBatch("time threshold reached");
            }
        }
    }

    /**
     * Perform batch commit of acknowledged offsets
     */
    private synchronized void commitBatch(String reason) {
        if (currentAcknowledgment == null) {
            return;
        }

        try {
            // Create offset map for manual commit if needed
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

            for (Map.Entry<TopicPartition, AtomicLong> entry : acknowledgedOffsets.entrySet()) {
                long offset = entry.getValue().get();
                if (offset >= 0) {
                    // Kafka expects the offset of the next message to consume
                    offsetsToCommit.put(entry.getKey(), new OffsetAndMetadata(offset + 1));
                }
            }

            if (!offsetsToCommit.isEmpty()) {
                currentAcknowledgment.acknowledge();
                messagesSinceLastCommit.set(0);
                lastCommitTime = Instant.now();

                logger.info("Committed batch - reason: {}, offsets: {}", reason, offsetsToCommit.size());
            }

        } catch (Exception e) {
            logger.error("Failed to commit batch: {}", e.getMessage(), e);
        }
    }

    /**
     * Force commit any pending acknowledged offsets
     */
    public void forceCommit() {
        commitBatch("forced commit");
    }

    /**
     * Get pending acknowledgement count for monitoring
     */
    public int getPendingAckCount() {
        return pendingAcks.size();
    }

    /**
     * Get metrics for monitoring
     */
    public AckMetrics getMetrics() {
        return new AckMetrics(
            pendingAcks.size(),
            messagesSinceLastCommit.get(),
            Instant.now().toEpochMilli() - lastCommitTime.toEpochMilli(),
            acknowledgedOffsets.size()
        );
    }

    /**
     * Shutdown the tracker
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Internal class to track pending acknowledgements
     */
    private static class PendingAck {
        final TopicPartition partition;
        final long offset;
        final CompletableFuture<SendResult<String, String>> sendFuture;
        final Instant timestamp;

        PendingAck(TopicPartition partition, long offset,
                   CompletableFuture<SendResult<String, String>> sendFuture, Instant timestamp) {
            this.partition = partition;
            this.offset = offset;
            this.sendFuture = sendFuture;
            this.timestamp = timestamp;
        }
    }

    /**
     * Metrics data class
     */
    public record AckMetrics(
        int pendingAcks,
        int messagesSinceLastCommit,
        long timeSinceLastCommitMs,
        int trackedPartitions
    ) {}
}