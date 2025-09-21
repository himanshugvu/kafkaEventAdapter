-- PostgreSQL Events Table Schema
CREATE TABLE IF NOT EXISTS events (
    id VARCHAR(255) PRIMARY KEY,
    source_payload TEXT NOT NULL,
    transformed_payload TEXT,
    source_topic VARCHAR(255),
    source_partition INTEGER,
    source_offset BIGINT,
    destination_topic VARCHAR(255),
    destination_partition INTEGER,
    destination_offset BIGINT,
    message_send_time BIGINT,
    message_final_sent_time BIGINT,
    status VARCHAR(50) NOT NULL DEFAULT 'RECEIVED',
    received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    send_timestamp_ns BIGINT,
    received_at_orchestrator TIMESTAMP WITH TIME ZONE,
    published_at TIMESTAMP WITH TIME ZONE,
    total_latency_ms BIGINT,
    consumer_latency_ms BIGINT,
    processing_latency_ms BIGINT,
    publishing_latency_ms BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- Legacy fields for backward compatibility
    topic_partition VARCHAR(255),
    offset_value BIGINT
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
CREATE INDEX IF NOT EXISTS idx_events_received_at ON events(received_at);
CREATE INDEX IF NOT EXISTS idx_events_total_latency ON events(total_latency_ms);