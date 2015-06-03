package com.spotify.heroic.exceptions;

public class ConsumerSchemaWriteException extends Exception {
    private static final long serialVersionUID = 5044774250593068540L;

    public ConsumerSchemaWriteException(String message) {
        super(message);
    }

    public ConsumerSchemaWriteException(String message, Exception cause) {
        super(message, cause);
    }
}
