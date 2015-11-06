package com.spotify.heroic.http.query;

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;

import lombok.Data;

@Data
public class QueryBatch {
    final Map<String, QueryMetrics> queries;
    final Optional<DateRange> range;

    @JsonCreator
    public QueryBatch(@JsonProperty("queries") Map<String, QueryMetrics> queries, @JsonProperty("range") QueryDateRange range) {
        this.queries = Optional.ofNullable(queries).orElseGet(ImmutableMap::of);
        this.range = Optional.ofNullable(range).flatMap(QueryDateRange::buildDateRange);
    }
}