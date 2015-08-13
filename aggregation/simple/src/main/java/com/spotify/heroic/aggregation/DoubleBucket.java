package com.spotify.heroic.aggregation;

import com.spotify.heroic.model.TimeData;

public interface DoubleBucket<T extends TimeData> extends Bucket<T> {
    double value();
}