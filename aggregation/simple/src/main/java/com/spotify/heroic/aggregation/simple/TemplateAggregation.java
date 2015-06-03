package com.spotify.heroic.aggregation.simple;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME" })
public class TemplateAggregation extends BucketAggregation<DataPoint, DataPoint, SumBucket> {
    public static final String NAME = "tpl";

    public TemplateAggregation(Sampling sampling) {
        super(sampling, DataPoint.class, DataPoint.class);
    }

    @JsonCreator
    public static TemplateAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new TemplateAggregation(sampling);
    }

    @Override
    protected SumBucket buildBucket(long timestamp) {
        return new SumBucket(timestamp);
    }

    @Override
    protected DataPoint build(SumBucket bucket) {
        return new DataPoint(bucket.timestamp(), 0.0);
    }
}