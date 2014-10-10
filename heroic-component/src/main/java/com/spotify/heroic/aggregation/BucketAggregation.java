package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Statistics;

@Data
public abstract class BucketAggregation<T extends Bucket> implements Aggregation {
    @Data
    private static final class Session<T extends Bucket> implements Aggregation.Session {
        private long sampleSize = 0;
        private long outOfBounds = 0;
        private long uselessScan = 0;

        private final BucketAggregation<T> aggregator;
        private final List<T> buckets;
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

                final long first = d.getTimestamp();
                final long last = first + extent;

                for (long start = first; start < last; start += size) {
                    int i = (int) ((start - offset) / size);

                    if (i < 0 || i >= buckets.size())
                        continue;

                    final Bucket bucket = buckets.get(i);

                    final long c = bucket.timestamp() - first;

                    // check that the current bucket is _within_ the extent.
                    if (!(c >= 0 && c <= extent))
                        continue;

                    bucket.update(d);
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
            final List<DataPoint> result = new ArrayList<DataPoint>(buckets.size());

            for (final T bucket : buckets) {
                result.add(aggregator.build(bucket));
            }

            final Statistics.Aggregator statistics = new Statistics.Aggregator(sampleSize, outOfBounds, uselessScan);

            return new Result(result, statistics);
        }
    }

    private final Sampling sampling;

    public BucketAggregation(Sampling sampling) {
        this.sampling = sampling;
    }

    @Override
    public Aggregation.Session session(DateRange original) {
        final long size = sampling.getSize();
        final DateRange range = original.rounded(sampling.getSize());

        final List<T> buckets = buildBuckets(range, size);
        return new Session<T>(this, buckets, range.start(), size, sampling.getExtent());
    }

    private List<T> buildBuckets(final DateRange range, long size) {
        final long start = range.start();
        final long count = range.diff() / size;

        final List<T> buckets = new ArrayList<T>((int) count);

        for (int i = 0; i < count; i++) {
            buckets.add(buildBucket(start + size * i + size));
        }

        return buckets;
    }

    abstract protected T buildBucket(long timestamp);

    abstract protected DataPoint build(T bucket);
}