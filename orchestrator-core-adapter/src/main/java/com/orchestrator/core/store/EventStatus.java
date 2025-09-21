package com.orchestrator.core.store;

public enum EventStatus {
    RECEIVED,   // Event received from Kafka topic
    SUCCESS,    // Event successfully processed and published
    FAILED,     // Event processing failed
}