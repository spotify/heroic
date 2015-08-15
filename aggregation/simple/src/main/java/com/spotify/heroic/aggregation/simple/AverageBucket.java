package com.spotify.heroic.aggregation.simple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Data;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;

@Data
public class AverageBucket implements DoubleBucket<Point> {
    private final long timestamp;
    private final AtomicDouble value = new AtomicDouble();
    private final AtomicLong count = new AtomicLong();

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(Map<String, String> tags, MetricType type, Point d) {
        value.addAndGet(d.getValue());
        count.incrementAndGet();
    }

    @Override
    public double value() {
        final long count = this.count.get();

        if (count == 0)
            return Double.NaN;

        return value.get() / count;
    }
}