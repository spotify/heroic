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
import com.spotify.heroic.backend.model.FetchDataPoints;
import com.spotify.heroic.backend.model.FindRowGroups;
import com.spotify.heroic.model.DateRange;

@Slf4j
public class QueryGroup {
    private final List<MetricBackend> backends;
    private final Timer timer;

    public QueryGroup(List<MetricBackend> backends, Timer timer) {
        this.backends = backends;
        this.timer = timer;
    }

    private final class FindRowGroupsHandle implements
            CallbackGroup.Handle<FindRowGroups.Result> {
        private final DateRange range;
        private final Callback<QueryMetricsResult> callback;
        private final AggregatorGroup aggregator;

        private FindRowGroupsHandle(DateRange range,
                Callback<QueryMetricsResult> callback,
                AggregatorGroup aggregator) {
            this.range = range;
            this.callback = callback;
            this.aggregator = aggregator;
        }

        @Override
        public void done(Collection<FindRowGroups.Result> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final Map<Map<String, String>, List<Callback<FetchDataPoints.Result>>> mappedQueries = prepareGroups(results);

            final List<Callback<QueryMetricsResult>> queries = prepareQueries(mappedQueries);
            final JoinQueryMetricsResult join = new JoinQueryMetricsResult();

            callback.reduce(queries, timer, join);
        }

        private List<Callback<QueryMetricsResult>> prepareQueries(
                final Map<Map<String, String>, List<Callback<FetchDataPoints.Result>>> mappedQueries)
                throws Exception {
            final List<Callback<QueryMetricsResult>> queries = new ArrayList<Callback<QueryMetricsResult>>();

            for (final Map.Entry<Map<String, String>, List<Callback<FetchDataPoints.Result>>> entry : mappedQueries
                    .entrySet()) {
                final Map<String, String> tags = entry.getKey();
                final List<Callback<FetchDataPoints.Result>> callbacks = entry
                        .getValue();

                final Aggregator.Session session = aggregator.session(range);

                final Callback<QueryMetricsResult> partial = new ConcurrentCallback<QueryMetricsResult>();
                final Callback.StreamReducer<FetchDataPoints.Result, QueryMetricsResult> reducer;

                if (session == null) {
                    reducer = new SimpleCallbackStream(tags);
                } else {
                    reducer = new AggregatedCallbackStream(tags, session);
                }

                queries.add(partial.reduce(callbacks, timer, reducer));
            }

            return queries;
        }

        private final Map<Map<String, String>, List<Callback<FetchDataPoints.Result>>> prepareGroups(
                Collection<FindRowGroups.Result> results) throws QueryException {

            final Map<Map<String, String>, List<Callback<FetchDataPoints.Result>>> mappedQueries = new HashMap<Map<String, String>, List<Callback<FetchDataPoints.Result>>>();

            for (final FindRowGroups.Result result : results) {
                final MetricBackend backend = result.getBackend();

                for (final Map.Entry<Map<String, String>, List<DataPointsRowKey>> entry : result
                        .getRowGroups().entrySet()) {
                    final Map<String, String> tags = entry.getKey();
                    final List<DataPointsRowKey> rows = entry.getValue();

                    List<Callback<FetchDataPoints.Result>> callbacks = mappedQueries
                            .get(tags);

                    if (callbacks == null) {
                        callbacks = new ArrayList<Callback<FetchDataPoints.Result>>();
                        mappedQueries.put(tags, callbacks);
                    }

                    callbacks.addAll(backend.query(new FetchDataPoints(rows,
                            range)));
                }
            }

            return mappedQueries;
        }
    }

    public Callback<QueryMetricsResult> execute(FindRowGroups criteria,
            final AggregatorGroup aggregator) {

        final List<Callback<FindRowGroups.Result>> queries = new ArrayList<Callback<FindRowGroups.Result>>();

        for (final MetricBackend backend : backends) {
            try {
                queries.add(backend.findRowGroups(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        final DateRange range = criteria.getRange();

        final CallbackGroup<FindRowGroups.Result> group = new CallbackGroup<FindRowGroups.Result>(
                queries, new FindRowGroupsHandle(range, callback, aggregator));

        final Timer.Context context = timer.time();

        return callback.register(group).register(new Callback.Finishable() {
            @Override
            public void finish() throws Exception {
                context.stop();
            }
        });
    }
}
