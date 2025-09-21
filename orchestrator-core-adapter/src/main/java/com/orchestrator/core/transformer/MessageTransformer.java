package com.orchestrator.core.transformer;

public interface MessageTransformer {
    
    String transform(String input);
    
    default boolean isValidMessage(String input) {
        return input != null && !input.trim().isEmpty();
    }
    
    default String getTransformerName() {
        return this.getClass().getSimpleName();
    }
}