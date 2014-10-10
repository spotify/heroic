package com.spotify.heroic.aggregationcache;

import java.util.List;
import java.util.Map;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregationcache.model.CachePutResult;
import com.spotify.heroic.aggregationcache.model.CacheQueryResult;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

public interface AggregationCache {
    public boolean isConfigured();

    public Future<CacheQueryResult> get(Filter filter, Map<String, String> group, final AggregationGroup aggregation,
            DateRange range) throws CacheOperationException;

    public Future<CachePutResult> put(Filter filter, Map<String, String> group, AggregationGroup aggregation,
            List<DataPoint> datapoints) throws CacheOperationException;
}
