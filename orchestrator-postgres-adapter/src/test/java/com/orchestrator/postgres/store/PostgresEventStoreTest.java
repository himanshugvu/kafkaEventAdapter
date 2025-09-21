package com.orchestrator.postgres.store;

import com.orchestrator.core.store.Event;
import com.orchestrator.core.store.EventStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PostgresEventStoreTest {

    @Mock
    private JdbcTemplate jdbcTemplate;
    
    @Mock
    private ResultSet resultSet;
    
    private PostgresEventStore postgresEventStore;

    @BeforeEach
    void setUp() {
        postgresEventStore = new PostgresEventStore(jdbcTemplate);
    }

    @Test
    void testBulkInsert() {
        Event event1 = new Event("id1", "payload1", "topic", 0, 100L);
        Event event2 = new Event("id2", "payload2", "topic", 0, 101L);
        List<Event> events = List.of(event1, event2);
        
        when(jdbcTemplate.batchUpdate(anyString(), any())).thenReturn(new int[]{1, 1});
        
        postgresEventStore.bulkInsert(events);
        
        verify(jdbcTemplate).batchUpdate(anyString(), any());
    }

    @Test
    void testUpdateStatus() {
        when(jdbcTemplate.update(anyString(), any(), any())).thenReturn(1);
        
        postgresEventStore.updateStatus("test-id", EventStatus.SUCCESS);
        
        verify(jdbcTemplate).update(anyString(), eq(EventStatus.SUCCESS.name()), eq("test-id"));
    }

    @Test
    void testUpdateStatusWithError() {
        when(jdbcTemplate.update(anyString(), any(), any(), any())).thenReturn(1);
        
        postgresEventStore.updateStatus("test-id", EventStatus.FAILED, "error message");
        
        verify(jdbcTemplate).update(anyString(), eq(EventStatus.FAILED.name()), eq("error message"), eq("test-id"));
    }

    @Test
    void testFindStaleEvents() throws SQLException {
        Duration threshold = Duration.ofMinutes(30);
        
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(Timestamp.class)))
            .thenReturn(List.of(new Event("id", "payload", "topic", 0, 100L)));
        
        List<Event> staleEvents = postgresEventStore.findStaleEvents(threshold);
        
        assertNotNull(staleEvents);
        verify(jdbcTemplate).query(anyString(), any(RowMapper.class), any(Timestamp.class));
    }

    @Test
    void testCountPendingEvents() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(10L);
        
        long count = postgresEventStore.countPendingEvents();
        
        assertEquals(10L, count);
        verify(jdbcTemplate).queryForObject(anyString(), eq(Long.class), eq(EventStatus.RECEIVED.name()));
    }

    @Test
    void testCountFailedEvents() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(5L);
        
        long count = postgresEventStore.countFailedEvents();
        
        assertEquals(5L, count);
        verify(jdbcTemplate).queryForObject(anyString(), eq(Long.class), eq(EventStatus.FAILED.name()));
    }

    @Test
    void testCountProcessedEvents() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(100L);
        
        long count = postgresEventStore.countProcessedEvents();
        
        assertEquals(100L, count);
        verify(jdbcTemplate).queryForObject(anyString(), eq(Long.class), eq(EventStatus.SUCCESS.name()));
    }

    @Test
    void testCountSlowEvents() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class))).thenReturn(3L);
        
        long count = postgresEventStore.countSlowEvents();
        
        assertEquals(3L, count);
        verify(jdbcTemplate).queryForObject(anyString(), eq(Long.class));
    }

    @Test
    void testCleanupOldEvents() {
        Duration retentionPeriod = Duration.ofDays(7);
        when(jdbcTemplate.update(anyString())).thenReturn(15);
        
        int deletedCount = postgresEventStore.cleanupOldEvents(retentionPeriod);
        
        assertEquals(15, deletedCount);
        verify(jdbcTemplate).update(anyString());
    }
}