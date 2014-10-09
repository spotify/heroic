package com.spotify.heroic.ingestion;

public class FatalIngestionException extends Exception {
    private static final long serialVersionUID = -664571093269219848L;

    public FatalIngestionException(String message, Throwable e) {
        super(message, e);
    }
}
