package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@Slf4j
public class QuerySingle {
    private final List<MetricBackend> backends;
    private final Timer timer;
    private final long maxQueriableDataPoints;
    private final AggregationCache cache;

    public QuerySingle(List<MetricBackend> backends, Timer timer,
            long maxQueriableDataPoints, AggregationCache cache) {
        this.backends = backends;
        this.timer = timer;
        this.maxQueriableDataPoints = maxQueriableDataPoints;
        this.cache = cache;
    }

    public Callback<QueryMetricsResult> execute(final FindRows criteria,
            final AggregatorGroup aggregator, boolean noCache) {
        if (cache != null && !noCache) {
            return executeSingleWithCache(criteria, aggregator);
        }

        return executeSingle(criteria, aggregator);
    }

    private Callback<QueryMetricsResult> executeSingleWithCache(
            final FindRows criteria, final AggregatorGroup aggregator) {

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        final TimeSerie timeSerie = new TimeSerie(criteria.getKey(),
                criteria.getFilter());
        final TimeSerieSlice slice = timeSerie.slice(criteria.getRange());

        cache.get(slice, aggregator).register(new CacheGetHandle("single.cache-query", timer, callback, criteria.getFilter(), cache) {
            @Override
            public Callback<QueryMetricsResult> cacheMiss(TimeSerieSlice slice)
                    throws Exception {
                return executeSingle(criteria.withRange(slice.getRange()), aggregator);
            }
        });

        return callback;
    }

    private Callback<QueryMetricsResult> executeSingle(FindRows criteria,
            AggregatorGroup aggregator) {
        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        final List<Callback<FindRows.Result>> queries = new ArrayList<Callback<FindRows.Result>>();

        for (final MetricBackend backend : backends) {
            try {
                queries.add(backend.findRows(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final TimeSerie timeSerie = new TimeSerie(criteria.getKey(), criteria.getFilter());
        final DateRange range = criteria.getRange();
        final TimeSerieSlice slice = new TimeSerieSlice(timeSerie, range);

        final CallbackGroup<FindRows.Result> group = new CallbackGroup<FindRows.Result>(
                queries, new FindRowsHandle(timer, slice, callback, aggregator, maxQueriableDataPoints));

        final Timer.Context context = timer.time();

        return callback.register(group).register(new Callback.Finishable() {
            @Override
            public void finish() throws Exception {
                context.stop();
            }
        });
    }
}
