package com.orchestrator.example.payment.transformer;

import com.orchestrator.core.transformer.MessageTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PaymentMessageTransformer implements MessageTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(PaymentMessageTransformer.class);
    
    @Override
    public String transform(String input) {
        logger.debug("Transforming payment message: {}", input);
        
        // Payment-specific transformation logic
        // Add payment processing metadata and compliance tracking
        String transformed = String.format("""
            {
                "payment_processed": true,
                "original_message": %s,
                "processed_at": %d,
                "processor": "payment-orchestrator",
                "version": "1.0.0",
                "compliance": {
                    "pci_compliant": true,
                    "audit_trail": "payment_transform_v1"
                }
            }
            """, input, System.currentTimeMillis());
        
        logger.debug("Payment transformation completed");
        return transformed;
    }
}