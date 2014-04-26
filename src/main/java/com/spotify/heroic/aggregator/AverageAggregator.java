package com.spotify.heroic.aggregator;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Resolution;
import com.spotify.heroic.query.DateRange;

public class AverageAggregator extends SumBucketAggregator {
    public AverageAggregator(Aggregation aggregation, DateRange range,
            Resolution resolution) {
        super(aggregation, range, resolution);
    }

    @Override
    protected DataPoint buildDataPoint(Bucket bucket) {
        if (bucket.getCount() == 0)
            return new DataPoint(bucket.getTimestamp(), 0);

        return new DataPoint(bucket.getTimestamp(), bucket.getValue()
                / bucket.getCount());
    }
}
