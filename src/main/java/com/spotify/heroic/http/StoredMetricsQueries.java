package com.spotify.heroic.http;

import java.util.concurrent.ConcurrentHashMap;

import com.spotify.heroic.http.model.MetricsQuery;

/**
 * Storage for prepared queries.
 *
 * @author udoprog
 */
public class StoredMetricsQueries {
    private final ConcurrentHashMap<String, MetricsQuery> storedQueries = new ConcurrentHashMap<String, MetricsQuery>();

    public void put(String id, MetricsQuery query) {
        storedQueries.put(id, query);
    }

    public MetricsQuery get(String id) {
        return storedQueries.get(id);
    }
}
