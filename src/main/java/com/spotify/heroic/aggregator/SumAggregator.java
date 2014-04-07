package com.spotify.heroic.aggregator;

import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.Resolution;

public class SumAggregator extends SumBucketAggregator {
    public static class Definition extends SumBucketAggregator.Definition {
        @Override
        public SumBucketAggregator build(long start, long end) {
            return new SumAggregator(start, end, getSampling());
        }
    }

    public SumAggregator(long start, long end, Resolution resolution) {
        super(start, end, resolution);
    }

    @Override
    protected DataPoint buildDataPoint(Bucket bucket) {
        if (bucket.getCount() == 0)
            return new DataPoint(bucket.getTimestamp(), 0);

        return new DataPoint(bucket.getTimestamp(), bucket.getValue());
    }
}
