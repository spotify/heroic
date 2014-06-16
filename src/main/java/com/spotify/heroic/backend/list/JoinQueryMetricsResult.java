package com.spotify.heroic.backend.list;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.Statistics;
import com.spotify.heroic.backend.model.MetricGroup;
import com.spotify.heroic.backend.model.MetricGroups;

/**
 * Callback GroupHandle that joins multiple QueryMetricResult's.
 * 
 * @author udoprog
 */
@Slf4j
public final class JoinQueryMetricsResult implements
        Callback.Reducer<MetricGroups, MetricGroups> {
    @Override
    public MetricGroups done(Collection<MetricGroups> results,
            Collection<Exception> errors, Collection<CancelReason> cancelled)
            throws Exception {
        if (!errors.isEmpty()) {
            log.error("There were {} error(s) when quering for metrics", errors.size());

            int i = 0;

            for (final Throwable error : errors) {
                log.error("Query #{} failed", i++, error);
            }
        }

        final List<MetricGroup> groups = new LinkedList<MetricGroup>();

        Statistics statistics = new Statistics();

        for (final MetricGroups result : results) {
            statistics = statistics.merge(result.getStatistics());
            groups.addAll(result.getGroups());
        }

        return new MetricGroups(groups, statistics);
    }
}