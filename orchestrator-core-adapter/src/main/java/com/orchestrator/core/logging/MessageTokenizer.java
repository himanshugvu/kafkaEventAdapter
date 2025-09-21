package com.orchestrator.core.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

@Component
public class MessageTokenizer {

    private static final Logger logger = LoggerFactory.getLogger(MessageTokenizer.class);
    private final MessageDigest digest;

    public MessageTokenizer() {
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Create a simple hash and summary for message tracking
     * @param originalMessage The original message content
     * @return TokenizedMessage containing summary and hash for correlation
     */
    public TokenizedMessage tokenize(String originalMessage) {
        if (originalMessage == null || originalMessage.trim().isEmpty()) {
            return new TokenizedMessage("", "", 0);
        }

        String hash = generateHash(originalMessage);
        String summary = originalMessage.length() > 500 ?
            originalMessage.substring(0, 500) + "..." : originalMessage;

        return new TokenizedMessage(summary, hash, originalMessage.length());
    }


    private String generateHash(String input) {
        byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hashBytes).substring(0, 12); // First 12 chars
    }

    /**
     * Data class for message information with hash for correlation
     */
    public static class TokenizedMessage {
        private final String summary;
        private final String hash;
        private final int originalLength;

        public TokenizedMessage(String summary, String hash, int originalLength) {
            this.summary = summary;
            this.hash = hash;
            this.originalLength = originalLength;
        }

        public String getSummary() { return summary; }
        public String getHash() { return hash; }
        public int getOriginalLength() { return originalLength; }

        @Override
        public String toString() {
            return String.format("TokenizedMessage{hash='%s', length=%d, summary='%s'}",
                hash, originalLength, summary);
        }
    }
}