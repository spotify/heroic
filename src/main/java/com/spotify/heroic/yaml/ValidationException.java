package com.spotify.heroic.yaml;

public class ValidationException extends Exception {
    private static final long serialVersionUID = -8053371486081707276L;

    public ValidationException() {
        super();
    }

    public ValidationException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }
}
