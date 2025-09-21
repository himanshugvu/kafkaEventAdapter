package com.orchestrator.core.store;

import java.time.Duration;
import java.util.List;

public interface EventStore {
    
    void bulkInsert(List<Event> events);
    
    void updateStatus(String eventId, EventStatus status);
    
    void updateStatus(String eventId, EventStatus status, String errorMessage);
    
    List<Event> findStaleEvents(Duration threshold);
    
    long countPendingEvents();
    
    long countFailedEvents();
    
    long countProcessedEvents();
    
    int cleanupOldEvents(Duration retentionPeriod);
    
    long countSlowEvents();
}