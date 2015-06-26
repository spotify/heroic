package com.spotify.heroic.elasticsearch;

import java.util.concurrent.ExecutionException;

public class RateLimitExceededException extends Exception {

    public RateLimitExceededException(ExecutionException e) {
        super(e);
    }

    public RateLimitExceededException() {
        super();
    }

}
