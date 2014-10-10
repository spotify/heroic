package com.spotify.heroic.consumer.exceptions;

public class SchemaException extends Exception {
    private static final long serialVersionUID = 6466567629077867363L;

    public SchemaException(String message) {
        super(message);
    }

    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
