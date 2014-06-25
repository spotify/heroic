package com.spotify.heroic.metrics.async;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.Statistics;

/**
 * Callback GroupHandle that joins multiple QueryMetricResult's.
 * 
 * @author udoprog
 */
@Slf4j
public final class MergeMetricGroups implements
        Callback.Reducer<MetricGroups, MetricGroups> {
    @Override
    public MetricGroups resolved(Collection<MetricGroups> results,
            Collection<Exception> errors, Collection<CancelReason> cancelled)
            throws Exception {
        if (!errors.isEmpty()) {
            log.error("There were {} error(s) when quering for metrics",
                    errors.size());

            int i = 0;

            for (final Throwable error : errors) {
                log.error("Query #{} failed", i++, error);
            }
        }

        final List<MetricGroup> groups = new LinkedList<MetricGroup>();

        Statistics statistics = Statistics.EMPTY;

        for (final MetricGroups result : results) {
            statistics = statistics.merge(result.getStatistics());
            groups.addAll(result.getGroups());
        }

        return new MetricGroups(groups, statistics);
    }
}