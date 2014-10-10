package com.spotify.heroic.aggregation.simple;

import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.model.DataPoint;

@Data
public class SumBucket implements Bucket {
    private final long timestamp;
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicDouble value = new AtomicDouble(0);

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(DataPoint d) {
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
