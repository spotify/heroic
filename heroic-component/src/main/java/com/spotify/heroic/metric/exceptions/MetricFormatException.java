package com.spotify.heroic.metric.exceptions;

public class MetricFormatException extends Exception {
    private static final long serialVersionUID = 2361809062288857078L;

    public MetricFormatException(String string) {
        super(string);
    }
}
