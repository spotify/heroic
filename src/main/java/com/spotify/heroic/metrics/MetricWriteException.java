package com.spotify.heroic.metrics;

public class MetricWriteException extends Exception {
    private static final long serialVersionUID = 2361809062288857078L;

    public MetricWriteException(String string) {
        super(string);
    }
}
