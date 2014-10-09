package com.spotify.heroic.consumer.exceptions;

public class WriteException extends Exception {
    private static final long serialVersionUID = 5044774250593068540L;

    public WriteException(String message) {
        super(message);
    }

    public WriteException(String message, Exception cause) {
        super(message, cause);
    }
}
