package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.GroupedTimeSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@RequiredArgsConstructor
public final class TimeSeriesTransformer implements
        Callback.DeferredTransformer<List<GroupedTimeSeries>, MetricGroups> {
    private final AggregationCache cache;
    private final AggregationGroup aggregation;
    private final DateRange range;

    @Override
    public Callback<MetricGroups> transform(List<GroupedTimeSeries> result)
            throws Exception {
        if (cache == null || aggregation == null)
            return ConcurrentCallback.newReduce(execute(result),
                    new MergeMetricGroups());

        return ConcurrentCallback.newReduce(executeCached(result),
                new MergeMetricGroups());
    }

    private List<Callback<MetricGroups>> execute(
            final List<GroupedTimeSeries> result) throws Exception {
        final List<Callback<MetricGroups>> queries = new ArrayList<Callback<MetricGroups>>();

        for (final GroupedTimeSeries r : result) {
            for (final Map.Entry<TimeSerie, Set<TimeSerie>> entry : r.getGroups()
                    .entrySet()) {
                final TimeSerieSlice slice = entry.getKey().slice(range);
                queries.add(buildLookup(r.getBackend(), slice, entry.getValue()));
            }
        }

        return queries;
    }

    private List<Callback<MetricGroups>> executeCached(
            List<GroupedTimeSeries> result) {
        final List<Callback<MetricGroups>> callbacks = new ArrayList<Callback<MetricGroups>>();

        for (final GroupedTimeSeries r : result) {
            final MetricBackend backend = r.getBackend();

            for (Entry<TimeSerie, Set<TimeSerie>> entry : r.getGroups()
                    .entrySet()) {
                callbacks.add(buildCachedLookup(backend, entry.getKey(),
                        entry.getValue()));
            }
        }

        return callbacks;
    }

    private Callback<MetricGroups> buildCachedLookup(
            final MetricBackend backend, final TimeSerie timeSerie,
            final Set<TimeSerie> timeSeries) {
        final CacheGetTransformer transformer = new CacheGetTransformer(
                timeSerie, cache) {
            @Override
            public Callback<MetricGroups> cacheMiss(TimeSerieSlice slice)
                    throws Exception {
                return buildLookup(backend, slice, timeSeries);
            }
        };

        return cache.get(timeSerie.slice(range), aggregation).transform(
                transformer);
    }

    private Callback<MetricGroups> buildLookup(final MetricBackend backend,
            final TimeSerieSlice slice, final Set<TimeSerie> series) {
        final List<Callback<FetchDataPoints.Result>> callbacks = new ArrayList<Callback<FetchDataPoints.Result>>();

        final DateRange range = modifiedRange(slice);

        for (final TimeSerie serie : series) {
            callbacks.addAll(backend.query(serie, range));
        }

        return ConcurrentCallback.newReduce(callbacks, buildReducer(slice));
    }

    private DateRange modifiedRange(final TimeSerieSlice slice) {
      if (aggregation == null)
        return slice.getRange();

      return slice.getRange().shiftStart(
              -aggregation.getSampling().getExtent());
    }

    private Callback.StreamReducer<FetchDataPoints.Result, MetricGroups> buildReducer(
            TimeSerieSlice slice) {
        if (aggregation == null)
            return new SimpleCallbackStream(slice);

        final Aggregation.Session session = aggregation.session(slice
                .getRange());
        return new AggregatedCallbackStream(slice, session);
    }
}