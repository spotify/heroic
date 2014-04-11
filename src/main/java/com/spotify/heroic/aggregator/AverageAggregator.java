package com.spotify.heroic.aggregator;

import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.Resolution;

public class AverageAggregator extends SumBucketAggregator {
    public static class Definition extends SumBucketAggregator.Definition {
        @Override
        public SumBucketAggregator build(DateRange range) {
            return new AverageAggregator(range, getSampling());
        }
    }

    public AverageAggregator(DateRange range, Resolution resolution) {
        super(range, resolution);
    }

    @Override
    protected DataPoint buildDataPoint(Bucket bucket) {
        if (bucket.getCount() == 0)
            return new DataPoint(bucket.getTimestamp(), 0);

        return new DataPoint(bucket.getTimestamp(), bucket.getValue()
                / bucket.getCount());
    }
}
