package com.spotify.heroic.yaml;

public class ValidationException extends RuntimeException {
    private static final long serialVersionUID = -8053371486081707276L;

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
