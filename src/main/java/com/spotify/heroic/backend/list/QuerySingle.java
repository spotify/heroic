package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CallbackStream;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;

@Slf4j
public class QuerySingle {
    private final List<MetricBackend> backends;
    private final Timer timer;

    public QuerySingle(List<MetricBackend> backends, Timer timer) {
        this.backends = backends;
        this.timer = timer;
    }

    private final class HandleFindRowsResult implements
            CallbackGroup.Handle<MetricBackend.FindRowsResult> {
        private final Date start;
        private final Date end;
        private final Callback<QueryMetricsResult> callback;
        private final AggregatorGroup aggregators;

        private HandleFindRowsResult(Date start, Date end,
                Callback<QueryMetricsResult> callback,
                AggregatorGroup aggregators) {
            this.start = start;
            this.end = end;
            this.callback = callback;
            this.aggregators = aggregators;
        }

        @Override
        public void done(Collection<MetricBackend.FindRowsResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final List<Callback<MetricBackend.DataPointsResult>> queries = new LinkedList<Callback<MetricBackend.DataPointsResult>>();

            long totalCount = 0;

            /* check number of results */
            for (final MetricBackend.FindRowsResult result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();
                final Long columnCount = backend.getColumnCount(
                        result.getRows(), start, end, 100000l);

                if (columnCount == null) {
                    callback.cancel(new CancelReason(
                            "Trying to fetch too many columns"));
                    return;
                }

                totalCount += columnCount;
            }

            if (totalCount > 100000l) {
                callback.cancel(new CancelReason(
                        "Trying to fetch too many columns"));
                return;
            }

            for (final MetricBackend.FindRowsResult result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();
                queries.addAll(backend.query(result.getRows(), start, end));
            }

            final CallbackStream<MetricBackend.DataPointsResult> stream;

            final Aggregator.Session session = aggregators.session();

            if (session == null) {
                log.warn("Returning raw results, this will most probably kill your machine!");
                stream = new CallbackStream<MetricBackend.DataPointsResult>(
                        queries, new SimpleCallbackStream(null, callback));
            } else {
                stream = new CallbackStream<MetricBackend.DataPointsResult>(
                        queries, new AggregatedCallbackStream(null, session,
                                callback));
            }

            callback.register(stream);
        }
    }

    public Callback<QueryMetricsResult> execute(
            MetricBackend.FindRows criteria, AggregatorGroup aggregators) {
        final List<Callback<MetricBackend.FindRowsResult>> queries = new ArrayList<Callback<MetricBackend.FindRowsResult>>();

        for (final MetricBackend backend : backends) {
            try {
                queries.add(backend.findRows(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        final Date start = criteria.getStart();
        final Date end = criteria.getEnd();

        final CallbackGroup<MetricBackend.FindRowsResult> group = new CallbackGroup<MetricBackend.FindRowsResult>(
                queries, new HandleFindRowsResult(start, end, callback,
                        aggregators));

        final Timer.Context context = timer.time();

        return callback.register(group).register(new Callback.Ended() {
            @Override
            public void ended() throws Exception {
                context.stop();
            }
        });
    }
}
