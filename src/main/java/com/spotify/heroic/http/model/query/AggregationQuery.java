package com.spotify.heroic.http.model.query;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AverageAggregation;
import com.spotify.heroic.aggregation.SumAggregation;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AggregationQuery.Sum.class, name = "sum"),
        @JsonSubTypes.Type(value = AggregationQuery.Average.class, name = "average") })
public interface AggregationQuery {
    @Data
    public class Average implements AggregationQuery {
        private final SamplingQuery sampling;

        @Override
        public Aggregation makeAggregation() {
            return new AverageAggregation(sampling.makeSampling());
        }

        @JsonCreator
        public static Average create(
                @JsonProperty(value = "sampling", required = true) SamplingQuery sampling) {
            return new Average(sampling);
        }
    }

    @Data
    public class Sum implements AggregationQuery {
        private final SamplingQuery sampling;

        @Override
        public Aggregation makeAggregation() {
            return new SumAggregation(sampling.makeSampling());
        }

        @JsonCreator
        public static Sum create(
                @JsonProperty(value = "sampling", required = true) SamplingQuery sampling) {
            return new Sum(sampling);
        }
    }

    public Aggregation makeAggregation();
}
