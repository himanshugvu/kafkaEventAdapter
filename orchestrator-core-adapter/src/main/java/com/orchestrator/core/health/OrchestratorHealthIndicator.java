package com.orchestrator.core.health;

import com.orchestrator.core.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnClass(name = "org.springframework.boot.actuator.health.HealthIndicator")
public class OrchestratorHealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(OrchestratorHealthIndicator.class);
    private final EventStore eventStore;

    public OrchestratorHealthIndicator(EventStore eventStore) {
        this.eventStore = eventStore;
        logger.info("OrchestratorHealthIndicator initialized");
    }

    public boolean isHealthy() {
        try {
            long failedEvents = eventStore.countFailedEvents();
            return failedEvents < 100; // Healthy if less than 100 failed events
        } catch (Exception e) {
            logger.error("Health check failed", e);
            return false;
        }
    }
}