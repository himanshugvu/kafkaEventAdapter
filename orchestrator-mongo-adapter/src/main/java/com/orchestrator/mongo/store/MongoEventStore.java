package com.orchestrator.mongo.store;

import com.orchestrator.core.store.Event;
import com.orchestrator.core.store.EventStatus;
import com.orchestrator.core.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Repository
public class MongoEventStore implements EventStore {
    
    private static final Logger logger = LoggerFactory.getLogger(MongoEventStore.class);
    private static final String COLLECTION_NAME = "events";
    private final MongoTemplate mongoTemplate;
    public MongoEventStore(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }
    
    @Override
    public void bulkInsert(List<Event> events) {
        if (events.isEmpty()) {
            return;
        }
        try {
            BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, COLLECTION_NAME);
            events.forEach(event -> {
                if (event.getReceivedAt() == null) {
                    event.setReceivedAt(Instant.now());
                }
                if (event.getStatus() == null) {
                    event.setStatus(EventStatus.RECEIVED);
                }
                bulkOps.insert(event);
            });
            var result = bulkOps.execute();
            logger.debug("Bulk inserted {} events into MongoDB", result.getInsertedCount());
        } catch (Exception e) {
            logger.error("Failed to bulk insert {} events into MongoDB", events.size(), e);
            throw new RuntimeException("Bulk insert failed", e);
        }
    }
    
    @Override
    public void updateStatus(String eventId, EventStatus status) {
        updateStatus(eventId, status, null);
    }
    
    @Override
    public void updateStatus(String eventId, EventStatus status, String errorMessage) {
        try {
            Query query = new Query(Criteria.where("id").is(eventId));
            Update update = new Update()
                .set("status", status)
                .set("updatedAt", Instant.now());
            if (errorMessage != null) {
                update.set("errorMessage", errorMessage);
            }
            if (status == EventStatus.SUCCESS) {
                update.set("processedAt", Instant.now());
            }
            var result = mongoTemplate.updateFirst(query, update, COLLECTION_NAME);
            if (result.getModifiedCount() == 0) {
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
            Query query = new Query(
                Criteria.where("status").is(EventStatus.RECEIVED)
                    .and("receivedAt").lt(cutoff)
            );
            List<Event> staleEvents = mongoTemplate.find(query, Event.class, COLLECTION_NAME);
            logger.debug("Found {} stale events older than {}", staleEvents.size(), threshold);
            return staleEvents;
        } catch (Exception e) {
            logger.error("Failed to find stale events", e);
            return List.of();
        }
    }
    
    @Override
    public long countPendingEvents() {
        try {
            Query query = new Query(Criteria.where("status").is(EventStatus.RECEIVED));
            return mongoTemplate.count(query, COLLECTION_NAME);
        } catch (Exception e) {
            logger.error("Failed to count pending events", e);
            return 0;
        }
    }
    
    @Override
    public long countFailedEvents() {
        try {
            Query query = new Query(Criteria.where("status").is(EventStatus.FAILED));
            return mongoTemplate.count(query, COLLECTION_NAME);
        } catch (Exception e) {
            logger.error("Failed to count failed events", e);
            return 0;
        }
    }
    
    @Override
    public long countProcessedEvents() {
        try {
            Query query = new Query(Criteria.where("status").is(EventStatus.SUCCESS));
            return mongoTemplate.count(query, COLLECTION_NAME);
        } catch (Exception e) {
            logger.error("Failed to count processed events", e);
            return 0;
        }
    }
    
    @Override
    public long countSlowEvents() {
        try {
            Query query = new Query(
                Criteria.where("totalLatencyMs").gt(1000)
            );
            return mongoTemplate.count(query, COLLECTION_NAME);
        } catch (Exception e) {
            logger.error("Failed to count slow events", e);
            return 0;
        }
    }
    
    @Override
    public int cleanupOldEvents(Duration retentionPeriod) {
        try {
            Instant cutoff = Instant.now().minus(retentionPeriod);
            Query query = new Query(
                Criteria.where("createdAt").lt(cutoff)
            );
            var result = mongoTemplate.remove(query, COLLECTION_NAME);
            int deletedCount = (int) result.getDeletedCount();
            logger.info("Cleaned up {} old events older than {}", deletedCount, retentionPeriod);
            return deletedCount;
        } catch (Exception e) {
            logger.error("Failed to cleanup old events", e);
            return 0;
        }
    }
}