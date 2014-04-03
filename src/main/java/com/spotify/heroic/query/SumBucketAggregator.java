package com.spotify.heroic.query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.backend.kairosdb.DataPoint;

@Slf4j
public abstract class SumBucketAggregator implements Aggregator {
    @Getter
    @Setter
    private Resolution sampling = Resolution.DEFAULT_RESOLUTION;

    public static final class SumBucket {
        @Getter
        private final long timestamp;
        @Getter
        private int count = 0;
        @Getter
        private double value = 0;

        public SumBucket(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    @Override
    public List<DataPoint> aggregate(Date start, Date end,
            List<DataPoint> datapoints) {
        final long width = sampling.getWidth();
        final long diff = end.getTime() - start.getTime();
        final long count = diff / width;
        final long first = start.getTime() - (start.getTime() % width);

        final List<SumBucket> buckets = initializeBuckets(first, width, count);
        calculateBuckets(datapoints, first, width, buckets);
        return buildResult(count, buckets);
    }

    private List<SumBucket> initializeBuckets(long first, long width, long count) {
        final List<SumBucket> buckets = new ArrayList<SumBucket>((int) count);

        for (int i = 0; i < count; i++) {
            buckets.add(new SumBucket(first + width * i));
        }
        return buckets;
    }

    private List<DataPoint> buildResult(long count,
            final List<SumBucket> buckets) {
        final List<DataPoint> result = new ArrayList<DataPoint>((int) count);

        for (final SumBucket bucket : buckets) {
            final DataPoint d = buildDataPoint(bucket);

            if (d == null)
                continue;

            result.add(d);
        }

        return result;
    }

    private void calculateBuckets(List<DataPoint> datapoints, long first,
            long width, final List<SumBucket> buckets) {
        long oob = 0;

        for (final DataPoint datapoint : datapoints) {
            final long timestamp = datapoint.getTimestamp();
            final int index = (int) ((timestamp - first) / width);

            if (index < 0 || index >= buckets.size()) {
                oob += 1;
                continue;
            }

            final SumBucket bucket = buckets.get(index);

            bucket.value += datapoint.getValue();
            bucket.count += 1;
        }

        if (oob > 0) {
            log.warn("datapoints out-of-bounds: " + oob);
        }
    }

    abstract protected DataPoint buildDataPoint(SumBucket bucket);
}