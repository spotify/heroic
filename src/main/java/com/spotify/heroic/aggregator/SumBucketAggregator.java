package com.spotify.heroic.aggregator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.Resolution;

@Slf4j
public abstract class SumBucketAggregator implements Aggregator {
    public static abstract class JSON implements Aggregator.JSON {
        @Getter
        @Setter
        private Resolution sampling = Resolution.DEFAULT_RESOLUTION;

        @Override
        public abstract SumBucketAggregator build(Date start, Date end);
    }

    public static final class Bucket {
        @Getter
        private final long timestamp;
        private final AtomicInteger count = new AtomicInteger(0);
        private final AtomicDouble value = new AtomicDouble(0);

        public Bucket(long timestamp) {
            this.timestamp = timestamp;
        }

        public int getCount() {
            return count.get();
        }

        public double getValue() {
            return value.get();
        }
    }

    private final long count;
    private final long width;
    private final long offset;
    private final List<Bucket> buckets;

    public SumBucketAggregator(Date start, Date end, Resolution resolution) {
        final long width = resolution.getWidth();
        final long diff = end.getTime() - start.getTime();
        final long count = diff / width;

        this.count = count;
        this.width = width;
        this.offset = start.getTime() - (start.getTime() % width);
        this.buckets = initializeBuckets(offset, width, count);
    }

    @Override
    public long getIntervalHint() {
        return width;
    }

    @Override
    public boolean isStreamable() {
        return true;
    }

    @Override
    public List<DataPoint> aggregate(Iterable<DataPoint> datapoints) {
        stream(datapoints);
        return result();
    }

    @Override
    public void stream(Iterable<DataPoint> datapoints) {
        long oob = 0;

        for (final DataPoint datapoint : datapoints) {
            final long timestamp = datapoint.getTimestamp();
            final int index = (int) ((timestamp - offset) / width);

            if (index < 0 || index >= buckets.size()) {
                oob += 1;
                continue;
            }

            final Bucket bucket = buckets.get(index);

            bucket.value.addAndGet(datapoint.getValue());
            bucket.count.incrementAndGet();
        }

        if (oob > 0) {
            log.warn("datapoints out-of-bounds: " + oob);
        }
    }

    @Override
    public List<DataPoint> result() {
        final List<DataPoint> result = new ArrayList<DataPoint>((int) count);

        for (final Bucket bucket : buckets) {
            final DataPoint d = buildDataPoint(bucket);

            if (d == null)
                continue;

            result.add(d);
        }

        return result;
    }

    private List<Bucket> initializeBuckets(long offset, long width, long count) {
        final List<Bucket> buckets = new ArrayList<Bucket>((int) count);

        for (int i = 0; i < count; i++) {
            long timestamp = offset + width * i + width;
            buckets.add(new Bucket(timestamp));
        }
        return buckets;
    }

    abstract protected DataPoint buildDataPoint(Bucket bucket);
}