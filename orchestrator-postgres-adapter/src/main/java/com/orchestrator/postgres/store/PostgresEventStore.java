package com.orchestrator.postgres.store;

import com.orchestrator.core.store.Event;
import com.orchestrator.core.store.EventStatus;
import com.orchestrator.core.store.EventStore;
import static com.orchestrator.postgres.store.PostgresEventQueries.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Repository
public class PostgresEventStore implements EventStore {
    
    private static final Logger logger = LoggerFactory.getLogger(PostgresEventStore.class);
    
    private final JdbcTemplate jdbcTemplate;
    
    public PostgresEventStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    
    @Override
    public void bulkInsert(List<Event> events) {
        if (events.isEmpty()) {
            return;
        }
        
        try {
            
            int[] results = jdbcTemplate.batchUpdate(BULK_INSERT_EVENT, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    Event event = events.get(i);
                    
                    if (event.getReceivedAt() == null) {
                        event.setReceivedAt(Instant.now());
                    }
                    if (event.getStatus() == null) {
                        event.setStatus(EventStatus.RECEIVED);
                    }
                    
                    ps.setString(1, event.getId());
                    ps.setString(2, event.getPayload());
                    ps.setString(3, event.getTopicPartition());
                    ps.setLong(4, event.getOffsetValue());
                    ps.setString(5, event.getStatus().name());
                    ps.setTimestamp(6, Timestamp.from(event.getReceivedAt()));
                    
                    if (event.getSendTimestampNs() != null) {
                        ps.setLong(7, event.getSendTimestampNs());
                    } else {
                        ps.setNull(7, java.sql.Types.BIGINT);
                    }
                    
                    if (event.getReceivedAtOrchestrator() != null) {
                        ps.setTimestamp(8, Timestamp.from(event.getReceivedAtOrchestrator()));
                    } else {
                        ps.setNull(8, java.sql.Types.TIMESTAMP);
                    }
                    
                    setLongOrNull(ps, 9, event.getTotalLatencyMs());
                    setLongOrNull(ps, 10, event.getConsumerLatencyMs());
                    setLongOrNull(ps, 11, event.getProcessingLatencyMs());
                    setLongOrNull(ps, 12, event.getPublishingLatencyMs());
                    
                    if (event.getProcessedAt() != null) {
                        ps.setTimestamp(13, Timestamp.from(event.getProcessedAt()));
                    } else {
                        ps.setNull(13, java.sql.Types.TIMESTAMP);
                    }
                    
                    if (event.getPublishedAt() != null) {
                        ps.setTimestamp(14, Timestamp.from(event.getPublishedAt()));
                    } else {
                        ps.setNull(14, java.sql.Types.TIMESTAMP);
                    }
                }
                
                @Override
                public int getBatchSize() {
                    return events.size();
                }
            });
            
            int totalInserted = java.util.Arrays.stream(results).sum();
            logger.debug("Bulk inserted {} events into PostgreSQL", totalInserted);
            
        } catch (Exception e) {
            logger.error("Failed to bulk insert {} events into PostgreSQL", events.size(), e);
            throw new RuntimeException("Bulk insert failed", e);
        }
    }
    
    private void setLongOrNull(PreparedStatement ps, int parameterIndex, Long value) throws SQLException {
        if (value != null) {
            ps.setLong(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.BIGINT);
        }
    }
    
    @Override
    public void updateStatus(String eventId, EventStatus status) {
        updateStatus(eventId, status, null);
    }
    
    @Override
    public void updateStatus(String eventId, EventStatus status, String errorMessage) {
        try {
            int rowsAffected;
            
            if (errorMessage != null) {
                rowsAffected = jdbcTemplate.update(UPDATE_EVENT_ERROR, status.name(), errorMessage, eventId);
            } else {
                rowsAffected = jdbcTemplate.update(UPDATE_EVENT_STATUS, status.name(), eventId);
            }
            
            if (rowsAffected == 0) {
                logger.warn("No event found with id {} to update status to {}", eventId, status);
            } else {
                logger.debug("Updated event {} status to {}", eventId, status);
            }
            
        } catch (Exception e) {
            logger.error("Failed to update event {} status to {}", eventId, status, e);
            throw new RuntimeException("Status update failed", e);
        }
    }
    
    @Override
    public List<Event> findStaleEvents(Duration threshold) {
        try {
            Instant cutoff = Instant.now().minus(threshold);
            String sql = """
                SELECT id, source_payload, transformed_payload, source_topic, source_partition, source_offset,
                       destination_topic, destination_partition, destination_offset, message_send_time,
                       message_final_sent_time, status, received_at, send_timestamp_ns,
                       processed_at, published_at, total_latency_ms, created_at, updated_at, error_message
                FROM events 
                WHERE status = 'RECEIVED' AND received_at < ?
                """;
            
            List<Event> staleEvents = jdbcTemplate.query(sql, (rs, rowNum) -> mapResultSetToEvent(rs), 
                Timestamp.from(cutoff));
            
            logger.debug("Found {} stale events older than {}", staleEvents.size(), threshold);
            return staleEvents;
            
        } catch (Exception e) {
            logger.error("Failed to find stale events", e);
            return List.of();
        }
    }
    
    private Event mapResultSetToEvent(ResultSet rs) throws SQLException {
        Event event = new Event(
            rs.getString("id"),
            rs.getString("source_payload"),
            rs.getString("source_topic"),
            rs.getInt("source_partition"),
            rs.getLong("source_offset")
        );
        
        // Set additional fields from new schema
        event.setTransformedPayload(rs.getString("transformed_payload"));
        event.setDestinationTopic(rs.getString("destination_topic"));
        
        Integer destPartition = rs.getInt("destination_partition");
        if (!rs.wasNull()) {
            event.setDestinationPartition(destPartition);
        }
        
        Long destOffset = rs.getLong("destination_offset");
        if (!rs.wasNull()) {
            event.setDestinationOffset(destOffset);
        }
        
        Long msgSendTime = rs.getLong("message_send_time");
        if (!rs.wasNull()) {
            event.setMessageSendTime(msgSendTime);
        }
        
        Long msgFinalSentTime = rs.getLong("message_final_sent_time");
        if (!rs.wasNull()) {
            event.setMessageFinalSentTime(msgFinalSentTime);
        }
        
        event.setStatus(EventStatus.valueOf(rs.getString("status")));
        event.setReceivedAt(rs.getTimestamp("received_at").toInstant());
        
        long sendTimestampNs = rs.getLong("send_timestamp_ns");
        if (!rs.wasNull()) {
            event.setSendTimestampNs(sendTimestampNs);
        }
        
        Timestamp processedAt = rs.getTimestamp("processed_at");
        if (processedAt != null) {
            event.setProcessedAt(processedAt.toInstant());
        }
        
        Timestamp publishedAt = rs.getTimestamp("published_at");
        if (publishedAt != null) {
            event.setPublishedAt(publishedAt.toInstant());
        }
        
        Timestamp createdAt = rs.getTimestamp("created_at");
        if (createdAt != null) {
            event.setCreatedAt(createdAt.toInstant());
        }
        
        Timestamp updatedAt = rs.getTimestamp("updated_at");
        if (updatedAt != null) {
            event.setUpdatedAt(updatedAt.toInstant());
        }
        
        long totalLatency = rs.getLong("total_latency_ms");
        if (!rs.wasNull()) {
            event.setTotalLatencyMs(totalLatency);
        }
        
        String errorMessage = rs.getString("error_message");
        if (errorMessage != null) {
            event.setErrorMessage(errorMessage);
        }
        
        long publishingLatency = rs.getLong("publishing_latency_ms");
        if (!rs.wasNull()) {
            event.setPublishingLatencyMs(publishingLatency);
        }
        
        event.setErrorMessage(rs.getString("error_message"));
        
        return event;
    }
    
    @Override
    public long countPendingEvents() {
        try {
            return jdbcTemplate.queryForObject(
                COUNT_BY_STATUS, Long.class, "RECEIVED");
        } catch (Exception e) {
            logger.error("Failed to count pending events", e);
            return 0;
        }
    }
    
    @Override
    public long countFailedEvents() {
        try {
            return jdbcTemplate.queryForObject(
                COUNT_BY_STATUS, Long.class, "FAILED");
        } catch (Exception e) {
            logger.error("Failed to count failed events", e);
            return 0;
        }
    }
    
    @Override
    public long countProcessedEvents() {
        try {
            return jdbcTemplate.queryForObject(
                COUNT_BY_STATUS, Long.class, "SUCCESS");
        } catch (Exception e) {
            logger.error("Failed to count processed events", e);
            return 0;
        }
    }
    
    @Override
    public long countSlowEvents() {
        try {
            return jdbcTemplate.queryForObject(
                COUNT_SLOW_EVENTS, Long.class);
        } catch (Exception e) {
            logger.error("Failed to count slow events", e);
            return 0;
        }
    }
    
    @Override
    public int cleanupOldEvents(Duration retentionPeriod) {
        try {
            Instant cutoff = Instant.now().minus(retentionPeriod);
            String sql = String.format(DELETE_OLD_EVENTS, retentionPeriod.toDays());
            int deletedCount = jdbcTemplate.update(sql);
            logger.info("Cleaned up {} old events older than {}", deletedCount, retentionPeriod);
            return deletedCount;
        } catch (Exception e) {
            logger.error("Failed to cleanup old events", e);
            return 0;
        }
    }
}