package com.spotify.heroic.http.query;

/**
 * Created by lucile on 09/05/17.
 */

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.QueryBuilder;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.Chain;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.spotify.heroic.common.Optionals.firstPresent;

@Data
public class QueryHeatmap {
    private final Optional<String> query;
    private final Optional<Aggregation> aggregation;
    private final Optional<MetricType> source;
    private final Optional<QueryDateRange> range;
    private final Optional<Filter> filter;
    private final Optional<QueryOptions> options;

    /* legacy state */
    private final Optional<String> key;
    private final Optional<Map<String, String>> tags;
    private final Optional<List<String>> groupBy;
    private final Optional<FeatureSet> features;

    public QueryHeatmap(
        Optional<String> query, Optional<Aggregation> aggregation, Optional<MetricType> source,
        Optional<QueryDateRange> range, Optional<Filter> filter, Optional<QueryOptions> options
    ) {
        this.query = query;
        this.aggregation = aggregation;
        this.source = source;
        this.range = range;
        this.filter = filter;
        this.options = options;

        this.key = Optional.empty();
        this.tags = Optional.empty();
        this.groupBy = Optional.empty();
        this.features = Optional.empty();
    }

    @JsonCreator
    public QueryHeatmap(
        @JsonProperty("query") Optional<String> query,
        @JsonProperty("aggregation") Optional<Aggregation> aggregation,
        @JsonProperty("aggregators") Optional<List<Aggregation>> aggregators,
        @JsonProperty("source") Optional<String> source,
        @JsonProperty("range") Optional<QueryDateRange> range,
        @JsonProperty("filter") Optional<Filter> filter,
        @JsonProperty("key") Optional<String> key,
        @JsonProperty("tags") Optional<Map<String, String>> tags,
        @JsonProperty("groupBy") Optional<List<String>> groupBy,
        @JsonProperty("options") Optional<QueryOptions> options,
        @JsonProperty("features") Optional<FeatureSet> features,
        /* ignored */ @JsonProperty("noCache") Boolean noCache
    ) {
        this.query = query;
        this.aggregation =
            firstPresent(aggregation, aggregators.filter(c -> !c.isEmpty()).map(Chain::fromList));
        this.source = source.flatMap(MetricType::fromIdentifier);
        this.range = range;
        this.filter = filter;
        this.options = options;

        this.key = key;
        this.tags = tags;
        this.groupBy = groupBy;
        this.features = features;
    }

    public QueryBuilder toQueryBuilder(final Function<String, QueryBuilder> stringToQuery) {
        final Supplier<? extends QueryBuilder> supplier = () -> {
            return new QueryBuilder()
                .key(key)
                .tags(tags)
                .groupBy(groupBy)
                .filter(filter)
                .range(range)
                .aggregation(aggregation)
                .source(source)
                .options(options);
        };

        return query
            .map(stringToQuery)
            .orElseGet(supplier)
            .rangeIfAbsent(range)
            .optionsIfAbsent(options)
            .features(features);
    }
}
