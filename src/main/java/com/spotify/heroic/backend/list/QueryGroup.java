package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.model.FindRowGroups;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.model.DateRange;

@Slf4j
public class QueryGroup {
    private final List<MetricBackend> backends;
    private final AggregationCache cache;

    public QueryGroup(List<MetricBackend> backends, AggregationCache cache) {
        this.backends = backends;
        this.cache = cache;
    }

    public Callback<QueryMetricsResult> execute(FindRowGroups criteria,
            final AggregatorGroup aggregator, boolean noCache) {

        final List<Callback<FindRowGroups.Result>> queries = new ArrayList<Callback<FindRowGroups.Result>>();

        for (final MetricBackend backend : backends) {
            try {
                queries.add(backend.findRowGroups(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final DateRange range = criteria.getRange();

        return ConcurrentCallback.newReduce(queries, new FindRowGroupsReducer(range))
            .transform(new RowGroupsHandle(cache, aggregator));
    }
}
