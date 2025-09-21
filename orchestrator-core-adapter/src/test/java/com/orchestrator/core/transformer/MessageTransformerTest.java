package com.orchestrator.core.transformer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MessageTransformerTest {

    private final MessageTransformer transformer = new TestTransformer();

    @Test
    void testValidMessage() {
        assertTrue(transformer.isValidMessage("valid message"));
        assertTrue(transformer.isValidMessage(" valid message "));
    }

    @Test
    void testInvalidMessage() {
        assertFalse(transformer.isValidMessage(null));
        assertFalse(transformer.isValidMessage(""));
        assertFalse(transformer.isValidMessage("   "));
    }

    @Test
    void testTransformerName() {
        assertEquals("TestTransformer", transformer.getTransformerName());
    }

    @Test
    void testDefaultMessageTransformer() {
        DefaultMessageTransformer defaultTransformer = new DefaultMessageTransformer();
        String input = "test message";
        assertEquals(input, defaultTransformer.transform(input));
        assertEquals("DefaultMessageTransformer", defaultTransformer.getTransformerName());
    }

    private static class TestTransformer implements MessageTransformer {
        @Override
        public String transform(String input) {
            return "transformed: " + input;
        }
    }
}