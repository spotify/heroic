package com.spotify.heroic.aggregation.simple;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.model.DataPoint;

@RequiredArgsConstructor
public class StdDevBucket implements Bucket {
    private final long timestamp;
    private double mean = 0.0;
    private double s = 0.0;
    private long count = 0;

    @Override
    public synchronized void update(DataPoint d) {
        double value = d.getValue();

        long count = this.count + 1;
        double delta = value - this.mean;
        double mean = this.mean + delta / count;
        double s = this.s + delta * (value - mean);

        this.mean = mean;
        this.s = s;
        this.count = count;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    public synchronized double value() {
        if (count <= 1)
            return Double.NaN;

        return Math.sqrt(s / (count - 1));
    }
}
