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
        private final long timestamp;
        private final AtomicInteger count = new AtomicInteger(0);
        private final AtomicDouble value = new AtomicDouble(0);

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

                for (int i = first; i < last; i++) {
                    final Bucket bucket = buckets[i];
                    final long c = bucket.timestamp - d.getTimestamp();

                    if (!(c >= 0 && c <= extent)) {
                        ++uselessScan;
                        continue;
                    }

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
                final long count = bucket.count.get();
                final DataPoint d;

                if (count == 0) {
                    d = new DataPoint(bucket.timestamp, Double.NaN, Float.NaN);
                } else {
                    d = aggregator.build(bucket.timestamp, count,
                            bucket.value.get(), calculateP(count, max));
                }

                result.add(d);
            }

            final Statistics.Aggregator statistics = new Statistics.Aggregator(
                    sampleSize, outOfBounds, uselessScan);

            return new Result(result, statistics);
        }

        private float calculateP(float count, float max) {
            return ((float) Math.round((count / max) * 100)) / 100;
        }

        private float calculateMax() {
            int max = 0;

            for (final Bucket bucket : buckets) {
                max = Math.max(max, bucket.count.get());
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
    public Aggregation.Session session(DateRange original) {
        final long size = sampling.getSize();
        final DateRange range = original.rounded(sampling.getSize());

        final Bucket[] buckets = buildBuckets(range, size);
        return new Session(this, buckets, range.start(), size,
                sampling.getExtent());
    }

    private Bucket[] buildBuckets(final DateRange range, long size) {
        final long start = range.start();
        final long count = range.diff() / size;

        final Bucket[] buckets = new Bucket[(int) count];

        for (int i = 0; i < count; i++) {
            buckets[i] = new Bucket(start + size * i + size);
        }

        return buckets;
    }

    abstract protected DataPoint build(long timestamp, long count,
            double value, float p);
}