package com.orchestrator.example.stream.transformer;

import com.orchestrator.core.transformer.MessageTransformer;
import org.springframework.stereotype.Component;

@Component
public class StreamMessageTransformer implements MessageTransformer {

    @Override
    public String transform(String input) {
        // High-performance transformation - minimal processing
        return "{\"streamProcessed\":true,\"data\":" + input + ",\"timestamp\":" + System.currentTimeMillis() + "}";
    }

    @Override
    public String getTransformerName() {
        return "StreamMessageTransformer";
    }
}