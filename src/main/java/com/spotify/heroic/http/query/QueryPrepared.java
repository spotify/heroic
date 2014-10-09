package com.spotify.heroic.http.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Data;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.metric.MetricQueryException;
import com.spotify.heroic.model.DateRange;

@Data
public class QueryPrepared {
    private final String backendGroup;
    private final Filter filter;
    private final List<String> groupBy;
    private final DateRange range;
    private final AggregationGroup aggregation;

    public static QueryPrepared create(String backendGroup, QueryMetrics query) throws MetricQueryException {
        if (query == null)
            throw new MetricQueryException("Query must be defined");

        if (query.getRange() == null)
            throw new MetricQueryException("Range must be specified");

        final DateRange range = query.getRange().buildDateRange();

        if (!(range.start() < range.end()))
            throw new MetricQueryException("Range start must come before its end");

        final AggregationGroup aggregation;

        {
            final List<Aggregation> aggregators = query.makeAggregators();

            if (aggregators == null || aggregators.isEmpty()) {
                aggregation = null;
            } else {
                aggregation = new AggregationGroup(aggregators, aggregators.get(0).getSampling());
            }
        }

        final List<String> groupBy = query.getGroupBy();
        final Filter filter = buildFilter(query);

        if (filter == null)
            throw new MetricQueryException("Filter must not be empty when querying");

        final QueryPrepared stored = new QueryPrepared(backendGroup, filter, groupBy, range, aggregation);

        return stored;
    }

    /**
     * Convert a MetricsRequest into a filter.
     *
     * This is meant to stay backwards compatible, since every filtering in MetricsRequest can be expressed as filter
     * objects.
     *
     * @param query
     * @return
     */
    private static Filter buildFilter(QueryMetrics query) {
        final List<Filter> statements = new ArrayList<>();

        if (query.getTags() != null && !query.getTags().isEmpty()) {
            for (final Map.Entry<String, String> entry : query.getTags().entrySet()) {
                statements.add(new MatchTagFilter(entry.getKey(), entry.getValue()));
            }
        }

        if (query.getKey() != null)
            statements.add(new MatchKeyFilter(query.getKey()));

        if (query.getFilter() != null)
            statements.add(query.getFilter());

        if (statements.size() == 0)
            return null;

        if (statements.size() == 1)
            return statements.get(0);

        return new AndFilter(statements).optimize();
    }
}