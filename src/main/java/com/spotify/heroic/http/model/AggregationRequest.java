package com.spotify.heroic.http.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.aggregation.Aggregation;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SumAggregationRequest.class, name = "sum"),
        @JsonSubTypes.Type(value = AverageAggregationRequest.class, name = "average") })
public interface AggregationRequest {
    public Aggregation makeAggregation();
}
