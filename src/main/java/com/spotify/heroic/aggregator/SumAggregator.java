package com.spotify.heroic.aggregator;

import java.util.Date;

import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.Resolution;

public class SumAggregator extends SumBucketAggregator {
    public static class JSON extends SumBucketAggregator.JSON {
        @Override
        public SumBucketAggregator build(Date start, Date end) {
            return new SumAggregator(start, end, getSampling());
        }
    }

    public SumAggregator(Date start, Date end, Resolution resolution) {
        super(start, end, resolution);
    }

    @Override
    protected DataPoint buildDataPoint(Bucket bucket) {
        if (bucket.getCount() == 0)
            return null;

        return new DataPoint(bucket.getTimestamp(), bucket.getValue());
    }
}
