package com.spotify.heroic.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AverageAggregation;
import com.spotify.heroic.aggregator.SumAggregation;

@ToString(of = { "key", "tags", "range", "aggregators" })
public class MetricsQuery {
    private static final DateRange DEFAULT_DATE_RANGE = new RelativeDateRange(
            TimeUnit.DAYS, 7);

    @Getter
    private final String key = null;

    @Getter
    private final Map<String, String> tags = new HashMap<String, String>();

    @Getter
    private final List<String> groupBy = new ArrayList<String>();

    @Getter
    private final DateRange range = DEFAULT_DATE_RANGE;

    @Getter
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = SumAggregation.class, name = "sum"),
            @JsonSubTypes.Type(value = AverageAggregation.class, name = "average") })
    private List<Aggregation> aggregators;
}
