package com.spotify.heroic.exceptions;

public class IngestionFatalException extends Exception {
    private static final long serialVersionUID = -664571093269219848L;

    public IngestionFatalException(String message, Throwable e) {
        super(message, e);
    }
}
