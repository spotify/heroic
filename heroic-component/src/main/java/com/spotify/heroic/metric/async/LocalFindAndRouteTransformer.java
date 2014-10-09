package com.spotify.heroic.metric.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.model.FindTimeSeriesGroups;
import com.spotify.heroic.metric.model.PreparedQuery;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public final class LocalFindAndRouteTransformer implements
        Callback.Transformer<FindTimeSeriesGroups, List<PreparedQuery>> {
    private final MetricManager metrics;
    private final Filter filter;
    private final String backendGroup;
    private final int groupLimit;
    private final int groupLoadLimit;

    @Override
    public List<PreparedQuery> transform(final FindTimeSeriesGroups result) throws Exception {
        final List<PreparedQuery> queries = new ArrayList<>();

        final Map<Map<String, String>, Set<Series>> groups = result.getGroups();

        if (groups.size() > groupLimit)
            throw new IllegalArgumentException("The current query is too heavy! (More than " + groupLimit
                    + " timeseries would be sent to your browser).");

        for (final Entry<Map<String, String>, Set<Series>> entry : groups.entrySet()) {
            final Set<Series> series = entry.getValue();

            if (series.isEmpty())
                continue;

            if (series.size() > groupLoadLimit)
                throw new IllegalArgumentException("The current query is too heavy! (More than " + groupLoadLimit
                        + " original time series would be loaded from Cassandra).");

            final PreparedQuery query = new LocalQuery(metrics, backendGroup, filter, entry.getKey(), series);

            queries.add(query);
        }

        return queries;
    }
}