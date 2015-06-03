package com.spotify.heroic.exceptions;

public class ConsumerSchemaException extends Exception {
    private static final long serialVersionUID = 6466567629077867363L;

    public ConsumerSchemaException(String message) {
        super(message);
    }

    public ConsumerSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
