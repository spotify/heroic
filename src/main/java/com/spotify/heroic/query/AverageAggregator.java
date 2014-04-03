package com.spotify.heroic.query;

import com.spotify.heroic.backend.kairosdb.DataPoint;

public class AverageAggregator extends SumBucketAggregator {
    @Override
    protected DataPoint buildDataPoint(SumBucket bucket) {
        if (bucket.getCount() == 0)
            return null;

        return new DataPoint(bucket.getTimestamp(), bucket.getValue()
                / bucket.getCount());
    }
}
