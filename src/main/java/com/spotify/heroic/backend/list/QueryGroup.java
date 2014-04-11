package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.query.DateRange;

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
        private final DateRange range;
        private final Callback<QueryMetricsResult> callback;
        private final AggregatorGroup aggregators;

        private FindRowGroupsHandle(DateRange range,
                Callback<QueryMetricsResult> callback,
                AggregatorGroup aggregators) {
            this.range = range;
            this.callback = callback;
            this.aggregators = aggregators;
        }

        @Override
        public void done(Collection<MetricBackend.FindRowGroupsResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> mappedQueries = prepareGroups(results);

            final List<Callback<QueryMetricsResult>> queries = prepareQueries(mappedQueries);
            final JoinQueryMetricsResult join = new JoinQueryMetricsResult();

            callback.reduce(queries, timer, join);
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

                final Callback<QueryMetricsResult> partial = new ConcurrentCallback<QueryMetricsResult>();
                final Callback.StreamReducer<MetricBackend.DataPointsResult, QueryMetricsResult> reducer;

                if (session == null) {
                    reducer = new SimpleCallbackStream(tags);
                } else {
                    reducer = new AggregatedCallbackStream(tags, session);
                }

                queries.add(partial.reduce(callbacks, timer, reducer));
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

                    callbacks.addAll(backend.query(rows, range));
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

        final DateRange range = criteria.getRange();

        final CallbackGroup<MetricBackend.FindRowGroupsResult> group = new CallbackGroup<MetricBackend.FindRowGroupsResult>(
                queries, new FindRowGroupsHandle(range, callback,
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
