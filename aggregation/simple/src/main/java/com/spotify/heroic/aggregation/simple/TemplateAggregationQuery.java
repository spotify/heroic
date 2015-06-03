package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.AggregationQuery;

@Data
public class TemplateAggregationQuery implements AggregationQuery<TemplateAggregation> {
    private final AggregationSamplingQuery sampling;

    @JsonCreator
    public TemplateAggregationQuery(@JsonProperty("sampling") AggregationSamplingQuery sampling) {
        this.sampling = Optional.fromNullable(sampling).or(AggregationSamplingQuery.DEFAULT_SUPPLIER);
    }

    @Override
    public TemplateAggregation build() {
        return new TemplateAggregation(sampling.build());
    }
}