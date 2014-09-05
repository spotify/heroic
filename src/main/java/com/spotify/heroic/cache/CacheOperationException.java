package com.spotify.heroic.cache;

public class CacheOperationException extends Exception {
    private static final long serialVersionUID = 2201862841576041417L;

    public CacheOperationException(String message) {
        super(message);
    }
}
