package com.orchestrator.example.inventory.transformer;

import com.orchestrator.core.transformer.MessageTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class InventoryMessageTransformer implements MessageTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(InventoryMessageTransformer.class);
    
    @Override
    public String transform(String input) {
        logger.debug("Transforming inventory message: {}", input);
        
        // Inventory-specific transformation logic
        // Add inventory tracking metadata and stock management info
        String transformed = String.format("""
            {
                "inventory_updated": true,
                "original_message": %s,
                "updated_at": %d,
                "processor": "inventory-orchestrator",
                "version": "1.0.0",
                "metadata": {
                    "stock_level_checked": true,
                    "availability_updated": true,
                    "reorder_point_evaluated": true
                },
                "tracking": {
                    "transaction_id": "inv_%d",
                    "processed_by": "inventory-service"
                }
            }
            """, input, System.currentTimeMillis(), System.nanoTime());
        
        logger.debug("Inventory transformation completed");
        return transformed;
    }
}