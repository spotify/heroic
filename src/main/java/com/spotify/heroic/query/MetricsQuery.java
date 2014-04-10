package com.spotify.heroic.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AverageAggregator;
import com.spotify.heroic.aggregator.SumAggregator;

@ToString(of = { "key", "tags", "range", "aggregators" })
public class MetricsQuery {
    private static final DateRange DEFAULT_DATE_RANGE = new RelativeDateRange(
            TimeUnit.DAYS, 7);

    @Getter
    private String key;

    @Getter
    private Map<String, String> tags;

    @Getter
    private List<String> groupBy;

    @Getter
    private final DateRange range = DEFAULT_DATE_RANGE;

    @Getter
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = SumAggregator.Definition.class, name = "sum"),
            @JsonSubTypes.Type(value = AverageAggregator.Definition.class, name = "average") })
    private List<Aggregator.Definition> aggregators;
}
