package com.orchestrator.core.controller;

import com.orchestrator.core.metrics.LatencyTracker;
import com.orchestrator.core.store.EventStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetricsControllerTest {

    @Mock
    private LatencyTracker latencyTracker;
    
    @Mock
    private EventStore eventStore;
    
    private MetricsController metricsController;

    @BeforeEach
    void setUp() {
        metricsController = new MetricsController(latencyTracker, eventStore);
    }

    @Test
    void testGetLatencyMetrics() {
        when(latencyTracker.getTotalMessageCount()).thenReturn(100L);
        when(latencyTracker.getSlowMessageCount()).thenReturn(3L);
        when(latencyTracker.getSlowMessagePercentage()).thenReturn(3.0);
        
        Map<String, Object> response = metricsController.getLatencyMetrics();
        
        assertNotNull(response);
        assertEquals(100L, response.get("totalMessages"));
        assertEquals(3L, response.get("slowMessages"));
        assertEquals("3.00%", response.get("slowPercentage"));
        assertEquals(97L, response.get("fastMessages"));
        assertNotNull(response.get("timestamp"));
    }

    @Test
    void testGetDatabaseMetrics() {
        when(eventStore.countPendingEvents()).thenReturn(10L);
        when(eventStore.countProcessedEvents()).thenReturn(100L);
        when(eventStore.countFailedEvents()).thenReturn(5L);
        when(eventStore.countSlowEvents()).thenReturn(3L);
        
        Map<String, Object> response = metricsController.getDatabaseMetrics();
        
        assertNotNull(response);
        assertEquals(10L, response.get("pendingEvents"));
        assertEquals(100L, response.get("processedEvents"));
        assertEquals(5L, response.get("failedEvents"));
        assertEquals(3L, response.get("slowEvents"));
        assertNotNull(response.get("timestamp"));
    }

    @Test
    void testGetSummary() {
        when(latencyTracker.getTotalMessageCount()).thenReturn(100L);
        when(latencyTracker.getSlowMessageCount()).thenReturn(3L);
        when(latencyTracker.getSlowMessagePercentage()).thenReturn(3.0);
        when(eventStore.countPendingEvents()).thenReturn(5L);
        when(eventStore.countFailedEvents()).thenReturn(1L);
        
        Map<String, Object> response = metricsController.getSummary();
        
        assertNotNull(response);
        assertEquals("running", response.get("status"));
        assertEquals(100L, response.get("totalProcessed"));
        assertEquals(3L, response.get("slowMessages"));
        assertEquals("3.00%", response.get("slowPercentage"));
        assertEquals(5L, response.get("databasePending"));
        assertEquals(1L, response.get("databaseFailed"));
    }
}