package com.spotify.heroic.consumer;

public class ConsumerSchemaException extends Exception {
    private static final long serialVersionUID = 853911819780782213L;

    public ConsumerSchemaException(String message) {
        super(message);
    }

    public ConsumerSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
