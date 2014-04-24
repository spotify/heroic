package com.spotify.heroic.aggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.Resolution;

public abstract class SumBucketAggregator implements Aggregator {
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

        public static int getApproximateMemoryUse() {
            // Long + AtomicInteger + AtomicDouble
            return 8 + 10 + 10;
        }
    }

    private class Session implements Aggregator.Session {
        private final AtomicLong sampleSize = new AtomicLong(0);
        private final AtomicLong outOfBounds = new AtomicLong(0);

        private final List<Bucket> buckets;

        public Session(List<Bucket> buckets) {
            this.buckets = buckets;
        }

        @Override
        public void stream(Iterable<DataPoint> datapoints) {
            long oob = 0;
            long size = 0;

            for (final DataPoint datapoint : datapoints) {
                final long timestamp = datapoint.getTimestamp();
                final int index = (int) ((timestamp - offset) / width);
                size += 1;

                if (index < 0 || index >= buckets.size()) {
                    oob += 1;
                    continue;
                }

                final Bucket bucket = buckets.get(index);

                bucket.value.addAndGet(datapoint.getValue());
                bucket.count.incrementAndGet();
            }

            this.outOfBounds.addAndGet(oob);
            this.sampleSize.addAndGet(size);
        }

        @Override
        public Result result() {
            final List<DataPoint> result = new ArrayList<DataPoint>((int) count);

            for (final Bucket bucket : buckets) {
                final DataPoint d = buildDataPoint(bucket);

                if (d == null)
                    continue;

                result.add(d);
            }

            return new Result(result, sampleSize.get(), outOfBounds.get());
        }
    }

    private final long count;
    private final long width;
    private final long offset;

    public SumBucketAggregator(DateRange range, Resolution resolution) {
        final long width = resolution.getWidth();
        final long diff = range.diff();
        final long start = range.start();
        final long count = diff / width;

        this.count = count;
        this.width = width;
        this.offset = start - (start % width);
    }

    @Override
    public Aggregator.Session session() {
        final List<Bucket> buckets = initializeBuckets();
        return new Session(buckets);
    }

    @Override
    public long getIntervalHint() {
        return width;
    }

    @Override
    public long getCalculationMemoryMagnitude() {
        final int bucketSize = Bucket.getApproximateMemoryUse();
        return count * bucketSize;
    }

    private List<Bucket> initializeBuckets() {
        final List<Bucket> buckets = new ArrayList<Bucket>((int) count);

        for (int i = 0; i < count; i++) {
            final long timestamp = offset + width * i + width;
            buckets.add(new Bucket(timestamp));
        }
        return buckets;
    }

    abstract protected DataPoint buildDataPoint(Bucket bucket);
}