package com.spotify.heroic.metrics;

public interface Clock {
    long getTick();

    static Clock systemClock() {
        return new Clock() {
            @Override
            public long getTick() {
                return System.nanoTime();
            }
        };
    }
}