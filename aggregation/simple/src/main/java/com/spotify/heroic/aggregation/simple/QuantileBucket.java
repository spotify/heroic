package com.spotify.heroic.aggregation.simple;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.aggregation.simple.util.SampleQuantile;
import com.spotify.heroic.model.DataPoint;

@RequiredArgsConstructor
public class QuantileBucket implements Bucket {
    private final long timestamp;
    private final SampleQuantile sample;

    public QuantileBucket(long timestamp, double quantile) {
        this.timestamp = timestamp;
        this.sample = new SampleQuantile(quantile, 0.1);
    }

    @Override
    public void update(DataPoint d) {
        sample.insert(d.getValue());
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    public double value() {
        final Double value = sample.snapshot();

        if (value == null)
            return Double.NaN;

        return value;
    }
}
