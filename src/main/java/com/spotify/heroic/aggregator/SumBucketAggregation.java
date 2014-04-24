package com.spotify.heroic.aggregator;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.Resolution;

@ToString(of = { "sampling" })
public abstract class SumBucketAggregation implements Aggregation {
    @Getter
    @Setter
    private Resolution sampling = Resolution.DEFAULT_RESOLUTION;

    @Override
    public abstract SumBucketAggregator build(DateRange range);
}