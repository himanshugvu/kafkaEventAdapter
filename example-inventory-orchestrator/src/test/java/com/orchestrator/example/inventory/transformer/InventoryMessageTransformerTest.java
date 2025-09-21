package com.orchestrator.example.inventory.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InventoryMessageTransformerTest {

    private InventoryMessageTransformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new InventoryMessageTransformer();
    }

    @Test
    void testTransformValidJson() {
        String input = "{\"productId\":\"PROD123\",\"quantity\":50,\"operation\":\"ADD\",\"warehouseId\":\"WH001\"}";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"inventory_updated\":true"));
        assertTrue(result.contains("\"original_message\":" + input));
        assertTrue(result.contains("\"processor\":\"inventory-orchestrator\""));
        assertTrue(result.contains("\"updated_at\":"));
    }

    @Test
    void testTransformRemoveOperation() {
        String input = "{\"productId\":\"PROD123\",\"quantity\":25,\"operation\":\"REMOVE\",\"warehouseId\":\"WH001\"}";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"inventory_updated\":true"));
        assertTrue(result.contains("\"original_message\":" + input));
    }

    @Test
    void testTransformInvalidJson() {
        String input = "invalid json";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"original_message\":\"invalid json\""));
        assertTrue(result.contains("\"inventory_updated\":true"));
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
        String input = "{\"productId\":\"PROD123\"}";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"original_message\":{\"productId\":\"PROD123\"}"));
        assertTrue(result.contains("\"inventory_updated\":true"));
    }

    @Test
    void testTransformInvalidOperation() {
        String input = "{\"productId\":\"PROD123\",\"quantity\":50,\"operation\":\"INVALID\",\"warehouseId\":\"WH001\"}";
        String result = transformer.transform(input);
        
        assertNotNull(result);
        assertTrue(result.contains("\"inventory_updated\":true"));
        assertTrue(result.contains("\"original_message\":" + input));
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
        assertTrue(result1.contains("\"inventory_updated\":true"));
        assertTrue(result2.contains("\"inventory_updated\":true"));
        assertTrue(result3.contains("\"inventory_updated\":true"));
    }
}