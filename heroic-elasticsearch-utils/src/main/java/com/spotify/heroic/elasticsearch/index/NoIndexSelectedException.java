package com.spotify.heroic.elasticsearch.index;

public class NoIndexSelectedException extends Exception {
    private static final long serialVersionUID = -3244136192891487626L;

    public NoIndexSelectedException() {
        super("no index available within the given range");
    }
}