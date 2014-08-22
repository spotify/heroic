package com.spotify.heroic.consumer.exceptions;

public class SchemaValidationException extends SchemaException {
    private static final long serialVersionUID = 853911819780782213L;

    public SchemaValidationException(String message) {
        super(message);
    }

    public SchemaValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
