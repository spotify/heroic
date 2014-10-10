package com.spotify.heroic.metric;

import java.util.List;
import java.util.Set;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.error.BackendOperationException;
import com.spotify.heroic.metric.model.BufferedWriteMetric;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.model.DateRange;

public interface MetricManager {
    public void flushWrites(List<BufferedWriteMetric> bufferedWrites) throws Exception;

    public List<MetricBackend> findBackends(Set<String> groups);

    public List<MetricBackend> findBackends(String group);

    public MetricBackend findOneBackend(String group);

    public List<MetricBackend> getBackends();

    public Future<MetricGroups> directQueryMetrics(final String backendGroup, final Filter filter,
            final List<String> groupBy, final DateRange range, final AggregationGroup aggregation);

    public MetricBackends useDefaultGroup() throws BackendOperationException;

    public MetricBackends useGroup(final String group) throws BackendOperationException;

    public MetricBackends useGroups(final Set<String> groups) throws BackendOperationException;

    public void scheduleFlush();
}
