package com.orchestrator.core.service;

import com.orchestrator.core.store.Event;
import com.orchestrator.core.store.EventStore;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class TransactionalEventService {
    
    private final EventStore eventStore;
    
    public TransactionalEventService(EventStore eventStore) {
        this.eventStore = eventStore;
    }
    
    @Transactional(rollbackFor = Exception.class)
    public void bulkInsertAndCommit(List<Event> events, Acknowledgment acknowledgment) {
        eventStore.bulkInsert(events);
        acknowledgment.acknowledge();
    }
}