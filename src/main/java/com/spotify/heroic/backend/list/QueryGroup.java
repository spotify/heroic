package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CallbackStream;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.DataPointGroup;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.backend.RowStatistics;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;

@Slf4j
public class QueryGroup {
    private final List<MetricBackend> backends;
    private final Timer timer;

    public QueryGroup(List<MetricBackend> backends, Timer timer) {
        this.backends = backends;
        this.timer = timer;
    }

    private final class FindRowGroupsHandle implements
            CallbackGroup.Handle<MetricBackend.FindRowGroupsResult> {
        private final Date start;
        private final Date end;
        private final Callback<QueryMetricsResult> callback;
        private final AggregatorGroup aggregators;

        private FindRowGroupsHandle(Date start, Date end,
                Callback<QueryMetricsResult> callback,
                AggregatorGroup aggregators) {
            this.start = start;
            this.end = end;
            this.callback = callback;
            this.aggregators = aggregators;
        }

        @Override
        public void done(Collection<MetricBackend.FindRowGroupsResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> mappedQueries = prepareGroups(results);

            final List<Callback<QueryMetricsResult>> queries = prepareQueries(mappedQueries);

            final CallbackGroup<QueryMetricsResult> group = new CallbackGroup<QueryMetricsResult>(
                    queries, new CallbackGroup.Handle<QueryMetricsResult>() {
                        @Override
                        public void done(
                                Collection<QueryMetricsResult> results,
                                Collection<Throwable> errors,
                                Collection<CancelReason> cancelled)
                                throws Exception {
                            final List<DataPointGroup> groups = new LinkedList<DataPointGroup>();

                            long sampleSize = 0;
                            long outOfBounds = 0;
                            int rowSuccessful = 0;
                            int rowFailed = 0;
                            int rowCancelled = 0;

                            for (final QueryMetricsResult result : results) {
                                sampleSize += result.getSampleSize();
                                outOfBounds += result.getOutOfBounds();

                                final RowStatistics rowStatistics = result
                                        .getRowStatistics();

                                rowSuccessful += rowStatistics.getSuccessful();
                                rowFailed += rowStatistics.getFailed();
                                rowCancelled += rowStatistics.getCancelled();

                                groups.addAll(result.getGroups());
                            }

                            final RowStatistics rowStatistics = new RowStatistics(
                                    rowSuccessful, rowFailed, rowCancelled);

                            callback.finish(new QueryMetricsResult(groups,
                                    sampleSize, outOfBounds, rowStatistics));
                        }
                    });

            callback.register(group);
        }

        private List<Callback<QueryMetricsResult>> prepareQueries(
                final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> mappedQueries)
                throws Exception {
            final List<Callback<QueryMetricsResult>> queries = new ArrayList<Callback<QueryMetricsResult>>();

            for (final Map.Entry<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> entry : mappedQueries
                    .entrySet()) {
                final Map<String, String> tags = entry.getKey();
                final List<Callback<MetricBackend.DataPointsResult>> callbacks = entry
                        .getValue();

                final Aggregator.Session session = aggregators.session();

                final CallbackStream<MetricBackend.DataPointsResult> callbackStream;

                final Callback<QueryMetricsResult> partial = new ConcurrentCallback<QueryMetricsResult>();

                if (session == null) {
                    callbackStream = new CallbackStream<MetricBackend.DataPointsResult>(
                            callbacks, new SimpleCallbackStream(tags,
                                    partial));
                } else {
                    callbackStream = new CallbackStream<MetricBackend.DataPointsResult>(
                            callbacks, new AggregatedCallbackStream(tags,
                                    session, partial));
                }

                partial.register(new Callback.Cancelled() {
                    @Override
                    public void cancel(CancelReason reason) throws Exception {
                        callbackStream.cancel(reason);
                    }
                });

                queries.add(partial);
            }

            return queries;
        }

        private final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> prepareGroups(
                Collection<MetricBackend.FindRowGroupsResult> results)
                throws QueryException {

            final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> mappedQueries = new HashMap<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>>();

            for (final MetricBackend.FindRowGroupsResult result : results) {
                final MetricBackend backend = result.getBackend();

                for (final Map.Entry<Map<String, String>, List<DataPointsRowKey>> entry : result
                        .getRowGroups().entrySet()) {
                    final Map<String, String> tags = entry.getKey();
                    final List<DataPointsRowKey> rows = entry.getValue();

                    List<Callback<MetricBackend.DataPointsResult>> callbacks = mappedQueries
                            .get(tags);

                    if (callbacks == null) {
                        callbacks = new ArrayList<Callback<MetricBackend.DataPointsResult>>();
                        mappedQueries.put(tags, callbacks);
                    }

                    callbacks.addAll(backend.query(rows, start, end));
                }
            }

            return mappedQueries;
        }
    }

    public Callback<QueryMetricsResult> execute(
            MetricBackend.FindRowGroups criteria,
            final AggregatorGroup aggregators) {

        final List<Callback<MetricBackend.FindRowGroupsResult>> queries = new ArrayList<Callback<MetricBackend.FindRowGroupsResult>>();

        for (final MetricBackend backend : backends) {
            try {
                queries.add(backend.findRowGroups(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        final Date start = criteria.getStart();
        final Date end = criteria.getEnd();
        final CallbackGroup<MetricBackend.FindRowGroupsResult> group = new CallbackGroup<MetricBackend.FindRowGroupsResult>(
                queries, new FindRowGroupsHandle(start, end, callback,
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
