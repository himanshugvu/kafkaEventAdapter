package com.orchestrator.core.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Clock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class LatencyTrackerTest {

    @Mock
    private MeterRegistry meterRegistry;
    
    @Mock
    private Timer timer;
    
    @Mock
    private Timer.Sample sample;
    
    @Mock
    private MeterRegistry.Config config;
    
    @Mock 
    private Clock clock;
    
    private LatencyTracker latencyTracker;

    @BeforeEach
    void setUp() {
        when(meterRegistry.config()).thenReturn(config);
        when(config.clock()).thenReturn(clock);
        when(clock.monotonicTime()).thenReturn(System.nanoTime());
        when(meterRegistry.timer(anyString())).thenReturn(timer);
        when(meterRegistry.counter(anyString())).thenReturn(mock(io.micrometer.core.instrument.Counter.class));
        lenient().when(Timer.start(meterRegistry)).thenReturn(sample);
        latencyTracker = new LatencyTracker(meterRegistry);
    }

    @Test
    void testRecordProcessingLatency() {
        Instant start = Instant.now().minusMillis(100);
        Instant end = Instant.now();
        
        latencyTracker.recordProcessingLatency(start, end);
        
        verify(meterRegistry).timer("orchestrator.processing.latency");
    }

    @Test
    void testRecordPublishingLatency() {
        Instant start = Instant.now().minusMillis(50);
        Instant end = Instant.now();
        
        latencyTracker.recordPublishingLatency(start, end);
        
        verify(meterRegistry).timer("orchestrator.publishing.latency");
    }

    @Test
    void testRecordEndToEndLatency() {
        Long latencyMs = 1500L;
        Long sendTimestampNs = System.nanoTime();
        
        latencyTracker.recordEndToEndLatency(latencyMs, sendTimestampNs);
        
        verify(meterRegistry).timer("orchestrator.end.to.end.latency");
    }

    @Test
    void testGetSlowMessageCount() {
        long count = latencyTracker.getSlowMessageCount();
        assertTrue(count >= 0);
    }

    @Test
    void testGetTotalMessageCount() {
        long count = latencyTracker.getTotalMessageCount();
        assertTrue(count >= 0);
    }

    @Test
    void testGetSlowMessagePercentage() {
        double percentage = latencyTracker.getSlowMessagePercentage();
        assertTrue(percentage >= 0.0 && percentage <= 100.0);
    }

    @Test
    void testLogPeriodicStats() {
        assertDoesNotThrow(() -> latencyTracker.logPeriodicStats());
    }
}