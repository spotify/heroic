package com.spotify.heroic.aggregation;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class ChainAggregationQuery implements AggregationQuery<ChainAggregation> {
    private final List<Aggregation> chain;

    @JsonCreator
    public ChainAggregationQuery(@JsonProperty("chain") List<AggregationQuery<?>> chain) {
        if (chain == null || chain.isEmpty())
            throw new IllegalArgumentException("chain must be specified and non-empty");

        this.chain = ChainAggregation.convertQueries(chain);
    }

    @Override
    public ChainAggregation build() {
        return new ChainAggregation(chain);
    }
}