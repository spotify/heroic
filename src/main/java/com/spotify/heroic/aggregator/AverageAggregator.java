package com.spotify.heroic.aggregator;

import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.Resolution;

public class AverageAggregator extends SumBucketAggregator {
    public static class Definition extends SumBucketAggregator.Definition {
        @Override
        public SumBucketAggregator build(long start, long end) {
            return new AverageAggregator(start, end, getSampling());
        }
    }

    public AverageAggregator(long start, long end, Resolution resolution) {
        super(start, end, resolution);
    }

    @Override
    protected DataPoint buildDataPoint(Bucket bucket) {
        if (bucket.getCount() == 0)
            return new DataPoint(bucket.getTimestamp(), 0);

        return new DataPoint(bucket.getTimestamp(), bucket.getValue()
                / bucket.getCount());
    }
}
