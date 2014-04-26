package com.spotify.heroic.aggregator;

import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.model.Resolution;
import com.spotify.heroic.query.DateRange;

public class SumAggregator extends SumBucketAggregator {
    public SumAggregator(DateRange range, Resolution resolution) {
        super(range, resolution);
    }

    @Override
    protected DataPoint buildDataPoint(Bucket bucket) {
        if (bucket.getCount() == 0)
            return new DataPoint(bucket.getTimestamp(), 0);

        return new DataPoint(bucket.getTimestamp(), bucket.getValue());
    }
}
