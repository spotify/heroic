package com.spotify.heroic.aggregation.simple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;

import lombok.Data;

@Data
public class AverageBucket extends AbstractBucket implements DoubleBucket {
    private final long timestamp;
    private final AtomicDouble value = new AtomicDouble();
    private final AtomicLong count = new AtomicLong();

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void updatePoint(Map<String, String> tags, Point d) {
        value.addAndGet(d.getValue());
        count.incrementAndGet();
    }

    @Override
    public double value() {
        final long count = this.count.get();

        if (count == 0) {
            return Double.NaN;
        }

        return value.get() / count;
    }
}