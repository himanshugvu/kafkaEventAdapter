package com.orchestrator.postgres.store;

public final class PostgresEventQueries {
    
    private PostgresEventQueries() {}
    
    public static final String BULK_INSERT_EVENT = """
        INSERT INTO events (
            id, source_payload, transformed_payload, source_topic, source_partition, source_offset,
            destination_topic, destination_partition, destination_offset, message_send_time, 
            message_final_sent_time, status, received_at, send_timestamp_ns, 
            processed_at, published_at, total_latency_ms, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;
    
    public static final String UPDATE_EVENT_STATUS = """
        UPDATE events 
        SET status = ?::VARCHAR, updated_at = CURRENT_TIMESTAMP 
        WHERE id = ?
        """;
    
    public static final String UPDATE_EVENT_PROCESSING_COMPLETE = """
        UPDATE events 
        SET status = ?::VARCHAR, processed_at = ?, published_at = ?, 
            total_latency_ms = ?, consumer_latency_ms = ?, 
            processing_latency_ms = ?, publishing_latency_ms = ?, 
            updated_at = CURRENT_TIMESTAMP 
        WHERE id = ?
        """;
    
    public static final String UPDATE_EVENT_ERROR = """
        UPDATE events 
        SET status = ?::VARCHAR, error_message = ?, updated_at = CURRENT_TIMESTAMP 
        WHERE id = ?
        """;
    
    public static final String FIND_STALE_EVENTS = """
        SELECT * FROM events 
        WHERE status = 'RECEIVED' 
        AND received_at < (CURRENT_TIMESTAMP - INTERVAL '%d minutes')
        """;
    
    public static final String COUNT_BY_STATUS = """
        SELECT COUNT(*) FROM events WHERE status = ?::VARCHAR
        """;
    
    public static final String COUNT_SLOW_EVENTS = """
        SELECT COUNT(*) FROM events 
        WHERE total_latency_ms > 1000 AND total_latency_ms IS NOT NULL
        """;
    
    public static final String DELETE_OLD_EVENTS = """
        DELETE FROM events 
        WHERE created_at < (CURRENT_TIMESTAMP - INTERVAL '%d days')
        """;
    
    public static final String SELECT_EVENT_BY_ID = """
        SELECT * FROM events WHERE id = ?
        """;
    
    private static final String EVENT_ROW_MAPPER_FIELDS = """
        id, source_payload, transformed_payload, source_topic, source_partition, source_offset,
        destination_topic, destination_partition, destination_offset, message_send_time,
        message_final_sent_time, status, received_at, send_timestamp_ns, 
        processed_at, published_at, total_latency_ms, created_at, updated_at, error_message
        """;
}