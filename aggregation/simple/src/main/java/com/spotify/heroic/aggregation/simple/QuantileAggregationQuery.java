package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.model.AggregationQuery;
import com.spotify.heroic.aggregation.model.AggregationSampling;

@Data
@JsonTypeName("quantile")
public class QuantileAggregationQuery implements AggregationQuery {
    public static final double DEFAULT_QUANTILE = 0.5;
    public static final double DEFAULT_ERROR = 0.01;

    private final AggregationSampling sampling;
    private final double q;
    private final double error;

    @Override
    public Aggregation build() {
        return new QuantileAggregation(sampling.build(), q, error);
    }

    @JsonCreator
    public static QuantileAggregationQuery create(
            @JsonProperty(value = "sampling", required = true) AggregationSampling sampling,
            @JsonProperty("q") Double q, @JsonProperty("error") Double error) {
        if (q == null)
            q = DEFAULT_QUANTILE;

        if (error == null)
            error = DEFAULT_ERROR;

        return new QuantileAggregationQuery(sampling, q, error);
    }
}