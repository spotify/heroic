package com.spotify.heroic.exceptions;

public class IngestionInvalidException extends Exception {
    private static final long serialVersionUID = -664571093269219848L;

    public IngestionInvalidException(String message, Throwable e) {
        super(message, e);
    }
}
