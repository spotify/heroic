package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;

@ToString(of={"sampling"})
@EqualsAndHashCode(of={"sampling"})
public abstract class BucketAggregation implements Aggregation {
    @RequiredArgsConstructor
    protected static final class Bucket {
        @Getter
        private final long timestamp;
        private final AtomicInteger count = new AtomicInteger(0);
        private final AtomicDouble value = new AtomicDouble(0);

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

    @RequiredArgsConstructor
    private static final class Session implements Aggregation.Session {
        private long sampleSize = 0;
        private long outOfBounds = 0;
        private long uselessScan = 0;

        private final BucketAggregation aggregator;
        private final Bucket[] buckets;
        private final long offset;
        private final long size;
        private final long extent;

        @Override
        public void update(final Iterable<DataPoint> datapoints) {
            long outOfBounds = 0;
            long sampleSize = 0;
            long uselessScan = 0;

            for (final DataPoint d : datapoints) {
                ++sampleSize;

                final long offset = d.getTimestamp() - this.offset;
                final int first = Math.max(0, (int) (offset / size));
                final int last = Math.min(buckets.length,
                        (int) ((offset + extent) / size));

                if (first > last) {
                    ++outOfBounds;
                    continue;
                }

                for (int i = first; i <= last; i++) {
                    final long c = (i * size) - offset;

                    if (!(c >= 0 && c <= extent)) {
                        ++uselessScan;
                        continue;
                    }

                    final Bucket bucket = buckets[i];
                    bucket.value.addAndGet(d.getValue());
                    bucket.count.incrementAndGet();
                }
            }

            synchronized (this) {
                this.outOfBounds += outOfBounds;
                this.sampleSize += sampleSize;
                this.uselessScan += uselessScan;
            }
        }

        @Override
        public Result result() {
            final List<DataPoint> result = new ArrayList<DataPoint>(
                    buckets.length);

            final float max = calculateMax();

            for (final Bucket bucket : buckets) {
                final DataPoint d;

                if (bucket.getCount() == 0) {
                    d = new DataPoint(bucket.getTimestamp(), Double.NaN,
                            Float.NaN);
                } else {
                    float p = calculateP(max, bucket);
                    d = aggregator.build(bucket, p);
                }

                result.add(d);
            }

            final Statistics.Aggregator statistics = new Statistics.Aggregator(
                    sampleSize, outOfBounds, uselessScan);

            return new Result(result, statistics);
        }

        private float calculateP(final float max, final Bucket bucket) {
            return ((float) Math.round((bucket.getCount() / max) * 100)) / 100;
        }

        private float calculateMax() {
            int max = 0;

            for (final Bucket bucket : buckets) {
                max = Math.max(max, bucket.getCount());
            }

            return max;
        }
    }

    @Getter
    private final Sampling sampling;

    public BucketAggregation(Sampling sampling) {
        this.sampling = sampling;
    }

    @Override
    public long getCalculationMemoryMagnitude(DateRange range) {
        final long width = sampling.getSize();
        final long diff = range.diff();
        final long count = diff / width;
        final int bucketSize = Bucket.getApproximateMemoryUse();
        return count * bucketSize;
    }

    @Override
    public Aggregation.Session session(DateRange range) {
        final long size = sampling.getSize();

        final long diff = range.diff();
        final long start = range.start();
        final long count = diff / size;

        long offset = start - (start % size);

        final Bucket[] buckets = buildBuckets(count, offset, size);
        return new Session(this, buckets, offset, size, sampling.getExtent());
    }

    private Bucket[] buildBuckets(long count, long offset, long size) {
        final Bucket[] buckets = new Bucket[(int) count + 1];

        for (int i = 0; i <= count; i++) {
            buckets[i] = new Bucket(offset + size * i);
        }

        return buckets;
    }

    abstract protected DataPoint build(Bucket bucket, float p);
}