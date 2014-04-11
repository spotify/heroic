package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
import com.spotify.heroic.backend.MetricBackend.DataPointsResult;
import com.spotify.heroic.backend.MetricBackend.FindRowsResult;

@Slf4j
public class QuerySingle {
    private final List<MetricBackend> backends;
    private final Timer timer;
    private final long maxQueriableDataPoints;

    public QuerySingle(List<MetricBackend> backends, Timer timer,
            long maxQueriableDataPoints) {
        this.backends = backends;
        this.timer = timer;
        this.maxQueriableDataPoints = maxQueriableDataPoints;
    }

    private static final class ColumnCountCallbackStreamHandle implements
            CallbackStream.Handle<Long> {
        private final Runnable runnable;
        private final long maxQueriableDataPoints;
        private final Callback<QueryMetricsResult> callback;

        private final AtomicLong totalColumnCount = new AtomicLong(0);

        private ColumnCountCallbackStreamHandle(Runnable runnable,
                long maxQueriableDataPoints,
                Callback<QueryMetricsResult> callback) {
            this.runnable = runnable;
            this.maxQueriableDataPoints = maxQueriableDataPoints;
            this.callback = callback;
        }

        @Override
        public void finish(Callback<Long> currentCallback, Long result)
                throws Exception {
            final long currentValue = totalColumnCount.addAndGet(result);

            if (currentValue > maxQueriableDataPoints) {
                callback.cancel(new CancelReason("Too many datapoints"));
            }
        }

        @Override
        public void error(Callback<Long> callback, Throwable error)
                throws Exception {
            log.error("An error occurred during counting columns.", error);
        }

        @Override
        public void cancel(Callback<Long> callback, CancelReason reason)
                throws Exception {
        }

        @Override
        public void done(int successful, int failed, int cancelled)
                throws Exception {
            runnable.run();
        }
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
        public void done(
                final Collection<MetricBackend.FindRowsResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final List<Callback<Long>> columnCountCallbacks = new LinkedList<Callback<Long>>();
            final Aggregator.Session session = aggregators.session();

            for (final FindRowsResult result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();
                if (session == null) {
                    // We check the number of data points only if no aggregation
                    // is requested
                    columnCountCallbacks.addAll(backend.getColumnCount(
                            result.getRows(), start, end));
                }
            }

            if (!columnCountCallbacks.isEmpty()) {
                final CallbackStream<Long> stream = new CallbackStream<Long>(
                        columnCountCallbacks,
                        new ColumnCountCallbackStreamHandle(new Runnable() {

                            @Override
                            public void run() {
                                processDataPoints(results, session);
                            }
                        }, maxQueriableDataPoints, callback));

                callback.register(stream);
            } else {
                processDataPoints(results, session);
            }
        }

        private void processDataPoints(Collection<FindRowsResult> results,
                final Aggregator.Session session) {
            final List<Callback<DataPointsResult>> queries = new LinkedList<Callback<DataPointsResult>>();
            for (final FindRowsResult result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();
                queries.addAll(backend.query(result.getRows(), start, end));
            }
            final CallbackStream<DataPointsResult> stream;

            if (session == null) {
                log.warn("Returning raw results, this will most probably kill your machine!");
                stream = new CallbackStream<MetricBackend.DataPointsResult>(
                        queries, new SimpleCallbackStream(null, callback));
            } else {
                stream = new CallbackStream<MetricBackend.DataPointsResult>(
                        queries, new AggregatedCallbackStream(null, session,
                                callback));
            }

            callback.register(new Callback.Cancelled() {
                @Override
                public void cancel(CancelReason reason) throws Exception {
                    stream.cancel(reason);
                }
            });
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
