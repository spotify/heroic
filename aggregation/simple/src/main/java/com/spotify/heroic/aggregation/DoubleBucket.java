package com.spotify.heroic.aggregation;


public interface DoubleBucket<T> extends Bucket<T> {
    double value();
}