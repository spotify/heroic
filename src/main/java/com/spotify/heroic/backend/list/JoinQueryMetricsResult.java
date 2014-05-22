package com.spotify.heroic.backend.list;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.BackendManager.DataPointGroup;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.RowStatistics;

/**
 * Callback GroupHandle that joins multiple QueryMetricResult's.
 * 
 * @author udoprog
 */
@Slf4j
public final class JoinQueryMetricsResult implements
        Callback.Reducer<QueryMetricsResult, QueryMetricsResult> {
    @Override
    public QueryMetricsResult done(Collection<QueryMetricsResult> results,
            Collection<Throwable> errors, Collection<CancelReason> cancelled)
            throws Exception {
        if (!errors.isEmpty()) {
            log.error("There were {} error(s) when quering for metrics", errors.size());

            int i = 0;

            for (final Throwable error : errors) {
                log.error("Query #{} failed", i++, error);
            }
        }

        final List<DataPointGroup> groups = new LinkedList<DataPointGroup>();

        long sampleSize = 0;
        long outOfBounds = 0;
        int rowSuccessful = 0;
        int rowFailed = 0;
        int rowCancelled = 0;

        for (final QueryMetricsResult result : results) {
            sampleSize += result.getSampleSize();
            outOfBounds += result.getOutOfBounds();

            final RowStatistics rowStatistics = result.getRowStatistics();

            rowSuccessful += rowStatistics.getSuccessful();
            rowFailed += rowStatistics.getFailed();
            rowCancelled += rowStatistics.getCancelled();

            groups.addAll(result.getGroups());
        }

        final RowStatistics rowStatistics = new RowStatistics(
                rowSuccessful, rowFailed, rowCancelled);

        return new QueryMetricsResult(groups, sampleSize, outOfBounds,
                rowStatistics);
    }
}