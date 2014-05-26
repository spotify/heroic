package com.spotify.heroic.aggregator;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Resolution;

public class AverageAggregator extends SumBucketAggregator {
    public AverageAggregator(Aggregation aggregation,
            Resolution resolution) {
        super(aggregation, resolution);
    }

    @Override
    protected DataPoint buildDataPoint(Bucket bucket) {
        if (bucket.getCount() == 0)
            return new DataPoint(bucket.getTimestamp(), 0);

        return new DataPoint(bucket.getTimestamp(), bucket.getValue()
                / bucket.getCount());
    }
}
