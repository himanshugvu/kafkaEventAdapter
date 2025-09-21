package com.orchestrator.core.store;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class EventTest {

    @Test
    void testEventCreation() {
        String id = "test-id";
        String payload = "test-payload";
        String topic = "test-topic";
        Integer partition = 1;
        Long offset = 100L;

        Event event = new Event(id, payload, topic, partition, offset);

        assertEquals(id, event.getId());
        assertEquals(payload, event.getSourcePayload());
        assertEquals(topic, event.getSourceTopic());
        assertEquals(partition, event.getSourcePartition());
        assertEquals(offset, event.getSourceOffset());
        assertEquals(EventStatus.RECEIVED, event.getStatus());
        assertNotNull(event.getCreatedAt());
        assertNotNull(event.getReceivedAt());
        assertEquals(0, event.getRetryCount());
    }

    @Test
    void testDeprecatedMethods() {
        Event event = new Event("id", "payload", "topic", 1, 100L);
        
        assertEquals("payload", event.getPayload());
        event.setPayload("new-payload");
        assertEquals("new-payload", event.getSourcePayload());
        
        assertEquals(100L, event.getOffset());
        event.setOffset(200L);
        assertEquals(200L, event.getSourceOffset());
    }

    @Test
    void testStatusUpdate() throws InterruptedException {
        Event event = new Event("id", "payload", "topic", 1, 100L);
        Instant before = Instant.now();
        
        Thread.sleep(1); // Ensure time difference
        event.setStatus(EventStatus.SUCCESS);
        
        assertEquals(EventStatus.SUCCESS, event.getStatus());
        assertTrue(event.getUpdatedAt().isAfter(before) || event.getUpdatedAt().equals(before));
    }

    @Test
    void testRetryCountIncrement() throws InterruptedException {
        Event event = new Event("id", "payload", "topic", 1, 100L);
        Instant before = Instant.now();
        
        Thread.sleep(1); // Ensure time difference
        event.incrementRetryCount();
        
        assertEquals(1, event.getRetryCount());
        assertTrue(event.getUpdatedAt().isAfter(before) || event.getUpdatedAt().equals(before));
    }

    @Test
    void testTimingCalculation() {
        Event event = new Event("id", "payload", "topic", 1, 100L);
        
        long sendTimeNs = System.nanoTime() - 2_000_000_000L; // 2 seconds ago
        event.setSendTimestampNs(sendTimeNs);
        event.setPublishedAt(Instant.now());
        
        event.calculateTimingMetrics();
        
        assertNotNull(event.getTotalLatencyMs());
        assertTrue(event.getTotalLatencyMs() >= 1000); 
        assertTrue(event.getExceededOneSecond());
    }

    @Test
    void testEqualsAndHashCode() {
        Event event1 = new Event("same-id", "payload1", "topic1", 1, 100L);
        Event event2 = new Event("same-id", "payload2", "topic2", 2, 200L);
        Event event3 = new Event("different-id", "payload1", "topic1", 1, 100L);

        assertEquals(event1, event2); // Same ID
        assertNotEquals(event1, event3); // Different ID
        assertEquals(event1.hashCode(), event2.hashCode());
    }

    @Test
    void testAllSettersAndGetters() {
        Event event = new Event("id", "payload", "topic", 1, 100L);
        
        event.setTransformedPayload("transformed");
        assertEquals("transformed", event.getTransformedPayload());
        
        event.setDestinationTopic("dest-topic");
        assertEquals("dest-topic", event.getDestinationTopic());
        
        event.setDestinationPartition(2);
        assertEquals(2, event.getDestinationPartition());
        
        event.setDestinationOffset(500L);
        assertEquals(500L, event.getDestinationOffset());
        
        event.setMessageSendTime(123456L);
        assertEquals(123456L, event.getMessageSendTime());
        
        event.setMessageFinalSentTime(654321L);
        assertEquals(654321L, event.getMessageFinalSentTime());
        
        event.setErrorMessage("error");
        assertEquals("error", event.getErrorMessage());
        
        event.setTotalLatencyMs(1500L);
        assertEquals(1500L, event.getTotalLatencyMs());
        
        event.setExceededOneSecond(true);
        assertTrue(event.getExceededOneSecond());
    }
}