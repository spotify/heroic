package com.spotify.heroic.http.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AverageAggregation;
import com.spotify.heroic.aggregation.SumAggregation;

@ToString(of = { "key", "tags", "groupBy", "range", "noCache", "aggregators" })
@EqualsAndHashCode(of = { "key", "tags", "groupBy", "range", "noCache", "aggregators" })
public class MetricsQuery {
    private static final DateRangeQuery DEFAULT_DATE_RANGE = new RelativeDateRangeQuery(
            TimeUnit.DAYS, 7);
    private static final List<Aggregation> EMPTY_AGGREGATIONS = new ArrayList<Aggregation>();

    @Getter
    private final String key = null;

    @Getter
    private final Map<String, String> tags = new HashMap<String, String>();

    @Getter
    private final List<String> groupBy = new ArrayList<String>();

    @Getter
    private final DateRangeQuery range = DEFAULT_DATE_RANGE;

    @Getter
    private final boolean noCache = false;

    @Getter
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = SumAggregation.class, name = "sum"),
            @JsonSubTypes.Type(value = AverageAggregation.class, name = "average") })
    private final List<Aggregation> aggregators = EMPTY_AGGREGATIONS;
}
