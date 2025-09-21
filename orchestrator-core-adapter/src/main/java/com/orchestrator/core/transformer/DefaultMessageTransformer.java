package com.orchestrator.core.transformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultMessageTransformer implements MessageTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageTransformer.class);
    
    @Override
    public String transform(String input) {
        logger.debug("Identity transformation for message: {}", input);
        return input;
    }
    
    @Override
    public String getTransformerName() {
        return "DefaultMessageTransformer";
    }
}