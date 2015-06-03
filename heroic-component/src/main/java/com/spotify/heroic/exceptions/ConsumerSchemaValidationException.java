package com.spotify.heroic.exceptions;

public class ConsumerSchemaValidationException extends ConsumerSchemaException {
    private static final long serialVersionUID = 853911819780782213L;

    public ConsumerSchemaValidationException(String message) {
        super(message);
    }

    public ConsumerSchemaValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
