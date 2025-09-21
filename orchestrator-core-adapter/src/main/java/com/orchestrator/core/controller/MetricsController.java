package com.orchestrator.core.controller;

import com.orchestrator.core.metrics.LatencyTracker;
import com.orchestrator.core.store.EventStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {
    
    private final LatencyTracker latencyTracker;
    private final EventStore eventStore;
    
    public MetricsController(LatencyTracker latencyTracker, EventStore eventStore) {
        this.latencyTracker = latencyTracker;
        this.eventStore = eventStore;
    }
    
    @GetMapping("/latency")
    public Map<String, Object> getLatencyMetrics() {
        long totalMessages = latencyTracker.getTotalMessageCount();
        long slowMessages = latencyTracker.getSlowMessageCount();
        double slowPercentage = latencyTracker.getSlowMessagePercentage();
        
        latencyTracker.logPeriodicStats();
        
        return Map.of(
            "totalMessages", totalMessages,
            "slowMessages", slowMessages,
            "slowPercentage", String.format("%.2f%%", slowPercentage),
            "fastMessages", totalMessages - slowMessages,
            "latencyThreshold", "1000ms",
            "timestamp", System.currentTimeMillis()
        );
    }
    
    @GetMapping("/database")
    public Map<String, Object> getDatabaseMetrics() {
        return Map.of(
            "pendingEvents", eventStore.countPendingEvents(),
            "failedEvents", eventStore.countFailedEvents(),
            "processedEvents", eventStore.countProcessedEvents(),
            "slowEvents", eventStore.countSlowEvents(),
            "timestamp", System.currentTimeMillis()
        );
    }
    
    @GetMapping("/summary")
    public Map<String, Object> getSummary() {
        long totalMessages = latencyTracker.getTotalMessageCount();
        long slowMessages = latencyTracker.getSlowMessageCount();
        
        return Map.of(
            "status", "running",
            "totalProcessed", totalMessages,
            "slowMessages", slowMessages,
            "slowPercentage", String.format("%.2f%%", latencyTracker.getSlowMessagePercentage()),
            "databasePending", eventStore.countPendingEvents(),
            "databaseFailed", eventStore.countFailedEvents(),
            "targetTPS", 1000,
            "latencyThreshold", "1 second"
        );
    }
}