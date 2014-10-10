package com.spotify.heroic.ingestion.exceptions;

public class IngestionException extends Exception {
    private static final long serialVersionUID = -664571093269219848L;

    public IngestionException(String message, Throwable e) {
        super(message, e);
    }
}
