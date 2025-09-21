package com.orchestrator.mongo.store;

import com.orchestrator.core.store.Event;
import com.orchestrator.core.store.EventStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.UpdateResult;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MongoEventStoreTest {

    @Mock
    private MongoTemplate mongoTemplate;
    
    @Mock
    private BulkOperations bulkOperations;
    
    @Mock
    private BulkWriteResult bulkWriteResult;
    
    @Mock
    private UpdateResult updateResult;
    
    private MongoEventStore mongoEventStore;

    @BeforeEach
    void setUp() {
        mongoEventStore = new MongoEventStore(mongoTemplate);
    }

    @Test
    void testBulkInsert() {
        Event event1 = new Event("id1", "payload1", "topic", 0, 100L);
        Event event2 = new Event("id2", "payload2", "topic", 0, 101L);
        List<Event> events = List.of(event1, event2);
        
        when(mongoTemplate.bulkOps(any(BulkMode.class), anyString())).thenReturn(bulkOperations);
        when(bulkOperations.insert(any(Event.class))).thenReturn(bulkOperations);
        when(bulkOperations.execute()).thenReturn(bulkWriteResult);
        when(bulkWriteResult.getInsertedCount()).thenReturn(2);
        
        mongoEventStore.bulkInsert(events);
        
        verify(mongoTemplate).bulkOps(eq(BulkMode.UNORDERED), eq("events"));
        verify(bulkOperations, times(2)).insert(any(Event.class));
        verify(bulkOperations).execute();
    }

    @Test
    void testUpdateStatus() {
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), anyString())).thenReturn(updateResult);
        when(updateResult.getModifiedCount()).thenReturn(1L);
        
        mongoEventStore.updateStatus("test-id", EventStatus.SUCCESS);
        
        verify(mongoTemplate).updateFirst(any(Query.class), any(Update.class), eq("events"));
    }

    @Test
    void testUpdateStatusWithError() {
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), anyString())).thenReturn(updateResult);
        when(updateResult.getModifiedCount()).thenReturn(1L);
        
        mongoEventStore.updateStatus("test-id", EventStatus.FAILED, "error message");
        
        verify(mongoTemplate).updateFirst(any(Query.class), any(Update.class), eq("events"));
    }

    @Test
    void testFindStaleEvents() {
        Duration threshold = Duration.ofMinutes(30);
        when(mongoTemplate.find(any(Query.class), eq(Event.class), anyString()))
            .thenReturn(List.of(new Event("id", "payload", "topic", 0, 100L)));
        
        List<Event> staleEvents = mongoEventStore.findStaleEvents(threshold);
        
        assertNotNull(staleEvents);
        verify(mongoTemplate).find(any(Query.class), eq(Event.class), eq("events"));
    }

    @Test
    void testCountPendingEvents() {
        when(mongoTemplate.count(any(Query.class), anyString())).thenReturn(10L);
        
        long count = mongoEventStore.countPendingEvents();
        
        assertEquals(10L, count);
        verify(mongoTemplate).count(any(Query.class), eq("events"));
    }

    @Test
    void testCountFailedEvents() {
        when(mongoTemplate.count(any(Query.class), anyString())).thenReturn(5L);
        
        long count = mongoEventStore.countFailedEvents();
        
        assertEquals(5L, count);
        verify(mongoTemplate).count(any(Query.class), eq("events"));
    }

    @Test
    void testCountProcessedEvents() {
        when(mongoTemplate.count(any(Query.class), anyString())).thenReturn(100L);
        
        long count = mongoEventStore.countProcessedEvents();
        
        assertEquals(100L, count);
        verify(mongoTemplate).count(any(Query.class), eq("events"));
    }

    @Test
    void testCountSlowEvents() {
        when(mongoTemplate.count(any(Query.class), anyString())).thenReturn(3L);
        
        long count = mongoEventStore.countSlowEvents();
        
        assertEquals(3L, count);
        verify(mongoTemplate).count(any(Query.class), eq("events"));
    }

    @Test
    void testCleanupOldEvents() {
        Duration retentionPeriod = Duration.ofDays(7);
        com.mongodb.client.result.DeleteResult deleteResult = mock(com.mongodb.client.result.DeleteResult.class);
        when(deleteResult.getDeletedCount()).thenReturn(15L);
        when(mongoTemplate.remove(any(Query.class), eq(Event.class)))
            .thenReturn(deleteResult);
        
        int deletedCount = mongoEventStore.cleanupOldEvents(retentionPeriod);
        
        assertEquals(15, deletedCount);
        verify(mongoTemplate).remove(any(Query.class), eq(Event.class));
    }
}