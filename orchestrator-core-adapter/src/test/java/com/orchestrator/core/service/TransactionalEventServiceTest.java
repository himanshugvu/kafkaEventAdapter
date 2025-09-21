package com.orchestrator.core.service;

import com.orchestrator.core.store.Event;
import com.orchestrator.core.store.EventStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class TransactionalEventServiceTest {

    @Mock
    private EventStore eventStore;
    
    @Mock
    private Acknowledgment acknowledgment;
    
    private TransactionalEventService transactionalEventService;

    @BeforeEach
    void setUp() {
        transactionalEventService = new TransactionalEventService(eventStore);
    }

    @Test
    void testBulkInsertAndCommit() {
        Event event1 = new Event("id1", "payload1", "topic", 0, 100L);
        Event event2 = new Event("id2", "payload2", "topic", 0, 101L);
        List<Event> events = List.of(event1, event2);
        
        transactionalEventService.bulkInsertAndCommit(events, acknowledgment);
        
        verify(eventStore).bulkInsert(events);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testBulkInsertAndCommitWithException() {
        Event event = new Event("id", "payload", "topic", 0, 100L);
        List<Event> events = List.of(event);
        
        doThrow(new RuntimeException("DB error")).when(eventStore).bulkInsert(events);
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> 
            transactionalEventService.bulkInsertAndCommit(events, acknowledgment)
        );
        assertEquals("DB error", exception.getMessage());
        
        verify(eventStore).bulkInsert(events);
        verify(acknowledgment, never()).acknowledge();
    }
}