package com.spotify.heroic.http.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationQuery;
import com.spotify.heroic.aggregation.ChainAggregation;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Event;
import com.spotify.heroic.model.TimeData;

@Data
public class QueryMetrics {
    private static final QueryDateRange DEFAULT_DATE_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);
    private static final List<AggregationQuery<?>> EMPTY_AGGREGATIONS = new ArrayList<>();
    private static final Map<String, String> DEFAULT_TAGS = new HashMap<String, String>();
    private static final boolean DEFAULT_NO_CACHE = false;
    private static final Class<? extends TimeData> DEFAULT_SOURCE = DataPoint.class;

    private final String query;
    private final String key;
    private final Map<String, String> tags;
    private final Filter filter;
    private final List<String> groupBy;
    private final QueryDateRange range;
    private final boolean noCache;
    private final List<AggregationQuery<?>> aggregators;
    private final Class<? extends TimeData> source;

    @JsonCreator
    public QueryMetrics(@JsonProperty("query") String query, @JsonProperty("key") String key,
            @JsonProperty("tags") Map<String, String> tags, @JsonProperty("filter") Filter filter,
            @JsonProperty("groupBy") List<String> groupBy, @JsonProperty("range") QueryDateRange range,
            @JsonProperty("noCache") Boolean noCache,
            @JsonProperty("aggregators") List<AggregationQuery<?>> aggregators,
            @JsonProperty("source") String sourceName) {
        this.query = query;
        this.key = key;
        this.tags = Optional.fromNullable(tags).or(DEFAULT_TAGS);
        this.filter = filter;
        this.groupBy = groupBy;
        this.range = Optional.fromNullable(range).or(DEFAULT_DATE_RANGE);
        this.noCache = Optional.fromNullable(noCache).or(DEFAULT_NO_CACHE);
        this.aggregators = Optional.fromNullable(aggregators).or(EMPTY_AGGREGATIONS);
        this.source = convertSource(sourceName);
    }

    private static Class<? extends TimeData> convertSource(String sourceName) {
        if (sourceName == null) {
            return DEFAULT_SOURCE;
        }

        if ("series".equals(sourceName)) {
            return DataPoint.class;
        }

        if ("events".equals(sourceName)) {
            return Event.class;
        }

        throw new IllegalArgumentException("invalid source: " + sourceName);
    }

    public Aggregation makeAggregation() {
        return new ChainAggregation(ChainAggregation.convertQueries(aggregators));
    }
}
