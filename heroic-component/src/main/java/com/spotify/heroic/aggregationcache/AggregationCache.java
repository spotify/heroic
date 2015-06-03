package com.spotify.heroic.aggregationcache;

import java.util.List;
import java.util.Map;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregationcache.model.CachePutResult;
import com.spotify.heroic.aggregationcache.model.CacheQueryResult;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

import eu.toolchain.async.AsyncFuture;

public interface AggregationCache {
    public boolean isConfigured();

    public AsyncFuture<CacheQueryResult> get(Filter filter, Map<String, String> group, Aggregation aggregation,
            DateRange range) throws CacheOperationException;

    public AsyncFuture<CachePutResult> put(Filter filter, Map<String, String> group, Aggregation aggregation,
            List<DataPoint> datapoints) throws CacheOperationException;
}
