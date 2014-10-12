package com.spotify.heroic.metric.exceptions;

public class BackendOperationException extends Exception {
    private static final long serialVersionUID = -843631350724606059L;

    public BackendOperationException(String message) {
        super(message);
    }
}
