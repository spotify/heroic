package com.spotify.heroic.aggregation.simple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.model.DataPoint;

/**
 * Bucket that keeps track of the amount of data points seen, and there summed value.
 *
 * Take care to not blindly trust {@link #value()} since it is initialized to 0 for simplicity. Always check
 * {@link #count()}, which if zero indicates that the {@link #value()} is undefined (e.g. NaN).
 *
 * @author udoprog
 */
@Data
public class SumBucket implements Bucket<DataPoint> {
    private final long timestamp;
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicDouble value = new AtomicDouble(0);

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(Map<String, String> tags, DataPoint d) {
        value.addAndGet(d.getValue());
        count.incrementAndGet();
    }

    public long count() {
        return count.get();
    }

    public double value() {
        return value.get();
    }
}
