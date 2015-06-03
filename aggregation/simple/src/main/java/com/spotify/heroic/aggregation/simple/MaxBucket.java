package com.spotify.heroic.aggregation.simple;

import java.util.Map;

import lombok.Data;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.model.DataPoint;

/**
 * A bucket implementation that retains the largest (max) value seen.
 *
 * @author udoprog
 */
@Data
public class MaxBucket implements Bucket<DataPoint> {
    private final long timestamp;
    private final AtomicDouble value = new AtomicDouble(Double.NaN);

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(Map<String, String> tags, DataPoint d) {
        while (true) {
            double current = value.get();

            if (current != Double.NaN && current > d.getValue()) {
                break;
            }

            if (value.compareAndSet(current, d.getValue())) {
                break;
            }
        }
    }

    public double value() {
        return value.get();
    }
}
