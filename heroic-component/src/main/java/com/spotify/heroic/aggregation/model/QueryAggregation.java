package com.spotify.heroic.aggregation.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.aggregation.Aggregation;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface QueryAggregation {
    public Aggregation build();
}
