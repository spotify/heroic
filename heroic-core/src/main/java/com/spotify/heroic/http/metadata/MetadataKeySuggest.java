package com.spotify.heroic.http.metadata;

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.impl.TrueFilterImpl;
import com.spotify.heroic.http.query.QueryDateRange;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.suggest.model.MatchOptions;

@Data
public class MetadataKeySuggest {
    private static final Filter DEFAULT_FILTER = TrueFilterImpl.get();
    private static final QueryDateRange DEFAULT_DATE_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);
    private static final int DEFAULT_LIMIT = 10;
    private static final MatchOptions DEFAULT_MATCH = MatchOptions.builder().fuzzy(false).build();

    private final Filter filter;
    private final DateRange range;
    private final int limit;
    private final MatchOptions match;
    private final String key;

    @JsonCreator
    public MetadataKeySuggest(@JsonProperty("filter") Filter filter, @JsonProperty("key") String key,
            @JsonProperty("limit") Integer limit, @JsonProperty("range") QueryDateRange range,
            @JsonProperty("match") MatchOptions fuzzy) {
        this.filter = Optional.fromNullable(filter).or(DEFAULT_FILTER);
        this.range = Optional.fromNullable(range).or(DEFAULT_DATE_RANGE).buildDateRange();
        this.limit = Optional.fromNullable(limit).or(DEFAULT_LIMIT);
        this.match = Optional.fromNullable(fuzzy).or(DEFAULT_MATCH);
        this.key = key;
    }

    public static MetadataKeySuggest createDefault() {
        return new MetadataKeySuggest(null, null, null, null, null);
    }
}