package com.spotify.heroic.consumer.exceptions;

/**
 * Exception thrown when schema consumption has caused a circumstance which forces the consumer to stop.
 *
 * @author udoprog
 */
public class FatalSchemaException extends SchemaException {
    private static final long serialVersionUID = 7268983184508351773L;

    public FatalSchemaException(String message) {
        super(message);
    }

    public FatalSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
