package com.orchestrator.example.payment.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PaymentMessageTransformerTest {

    private PaymentMessageTransformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new PaymentMessageTransformer();
    }

    @Test
    void testTransformValidJson() {
        String input = "{\"amount\":100.50,\"currency\":\"USD\",\"accountId\":\"12345\"}";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"payment_processed\":true"));
        assertTrue(result.contains("\"original_message\":" + input));
        assertTrue(result.contains("\"processor\":\"payment-orchestrator\""));
        assertTrue(result.contains("\"processed_at\":"));
    }

    @Test
    void testTransformInvalidJson() {
        String input = "invalid json";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"original_message\":\"invalid json\""));
        assertTrue(result.contains("\"payment_processed\":true"));
    }

    @Test
    void testTransformNullInput() {
        String result = transformer.transform(null);
        assertNotNull(result);
        assertTrue(result.contains("\"original_message\":null"));
    }

    @Test
    void testTransformEmptyInput() {
        String result = transformer.transform("");
        assertNotNull(result);
        assertTrue(result.contains("\"original_message\":\"\""));
    }

    @Test
    void testTransformMissingFields() {
        String input = "{\"amount\":100.50}";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"original_message\":{\"amount\":100.50}"));
        assertTrue(result.contains("\"payment_processed\":true"));
    }

    @Test
    void testTransformNegativeAmount() {
        String input = "{\"amount\":-100.50,\"currency\":\"USD\",\"accountId\":\"12345\"}";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"original_message\":{\"amount\":-100.50,\"currency\":\"USD\",\"accountId\":\"12345\"}"));
        assertTrue(result.contains("\"payment_processed\":true"));
    }

    @Test
    void testTransformerDoesNotThrow() {
        assertDoesNotThrow(() -> transformer.transform("any input"));
    }

    @Test
    void testTransformerAlwaysReturnsValidJson() {
        String result1 = transformer.transform("valid input");
        String result2 = transformer.transform(null);
        String result3 = transformer.transform("");
        
        assertNotNull(result1);
        assertNotNull(result2);
        assertNotNull(result3);
        assertTrue(result1.contains("\"payment_processed\":true"));
        assertTrue(result2.contains("\"payment_processed\":true"));
        assertTrue(result3.contains("\"payment_processed\":true"));
    }
}