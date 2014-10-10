package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.aggregation.model.QueryAggregation;
import com.spotify.heroic.aggregation.model.QuerySampling;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@JsonSerialize
public class MinAggregation extends BucketAggregation<MinBucket> {
    @Data
    public static class Query implements QueryAggregation {
        private final QuerySampling sampling;

        @Override
        public Aggregation build() {
            return new MinAggregation(sampling.build());
        }

        @JsonCreator
        public static Query create(@JsonProperty(value = "sampling", required = true) QuerySampling sampling) {
            return new Query(sampling);
        }
    }

    public MinAggregation(Sampling sampling) {
        super(sampling);
    }

    @JsonCreator
    public static MinAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new MinAggregation(sampling);
    }

    @Override
    protected MinBucket buildBucket(long timestamp) {
        return new MinBucket(timestamp);
    }

    @Override
    protected DataPoint build(MinBucket bucket) {
        return new DataPoint(bucket.timestamp(), bucket.value());
    }
}