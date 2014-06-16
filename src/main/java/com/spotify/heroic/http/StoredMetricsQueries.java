package com.spotify.heroic.http;

import java.util.concurrent.ConcurrentHashMap;

import com.spotify.heroic.http.model.MetricsRequest;

/**
 * Storage for prepared queries.
 *
 * @author udoprog
 */
public class StoredMetricsQueries {
    private final ConcurrentHashMap<String, MetricsRequest> storedQueries = new ConcurrentHashMap<String, MetricsRequest>();

    public void put(String id, MetricsRequest query) {
        storedQueries.put(id, query);
    }

    public MetricsRequest get(String id) {
        return storedQueries.get(id);
    }
}
